from io import IOBase
import logging
import tempfile
from itertools import islice
import json
import sys

from videohash import VideoHash, FFmpegFailedToExtractFrames
import hydrus_api
import hydrus_api.utils
from numpy import base_repr, binary_repr, bitwise_xor
from tqdm import tqdm
from rich import print as rprint

from .config import HYDRUS_LOCAL_TAG_SERVICE_NAME, HYDRUS_PHASH_TAG

class HydrusVideoDeduplicator():
    hydlog = logging.getLogger("hydlog")
    search_distance: int = 4
    _DEBUG = False

    REQUIRED_PERMISSIONS = (
        hydrus_api.Permission.IMPORT_URLS,
        hydrus_api.Permission.IMPORT_FILES,
        hydrus_api.Permission.ADD_TAGS,
        hydrus_api.Permission.SEARCH_FILES,
        hydrus_api.Permission.MANAGE_PAGES,
        hydrus_api.Permission.MANAGE_DATABASE,
        hydrus_api.Permission.ADD_NOTES,
        hydrus_api.Permission.MANAGE_FILE_RELATIONSHIPS,
    )

    def __init__(self, client: hydrus_api.Client,
                 verify_connection: bool = True):
        self.client = client
        if verify_connection:
            self.verify_api_connection()
        self.hydlog.setLevel(logging.WARNING)
        
        # Commonly used things from the database
        # If any of these are large they should probably be lazily loaded
        self.all_services = self.client.get_services()
        self.local_tag_service = [service for service in self.all_services["local_tags"] if service["name"] == HYDRUS_LOCAL_TAG_SERVICE_NAME][0]

    # Verify client connection and permissions
    # Will throw a hydrus_api.APIError if something is wrong
    def verify_api_connection(self):
        self.hydlog.info(f"Client API version: v{self.client.VERSION} | Endpoint API version: v{self.client.get_api_version()['version']}")
        hydrus_api.utils.verify_permissions(self.client, hydrus_api.utils.Permission)

    # This is the master function of the class
    def deduplicate(self, add_missing: bool, overwrite: bool, custom_query: list | None = None, skip_hashing: bool | None = False):
        # Add perceptual hashes to videos
        search_tags = ["system:filetype=video", f"{HYDRUS_PHASH_TAG}:*"]
        if custom_query is not None:
            custom_query = [x for x in custom_query if x.strip()] # Remove whitespace and empty strings
            if len(custom_query) > 0:
                search_tags.extend(custom_query)
                rprint(f"[yellow] Custom Query: {custom_query}")

        if not skip_hashing:
            self._add_perceptual_hash_to_videos(add_missing=add_missing, overwrite=overwrite, custom_query=custom_query)
        else:
            rprint("[yellow] Skipping perceptual hashing")

        print("Checking for duplicates among all video files with perceptual hash tags...\n")
        # SHA256 hashes of videos with perceptual hash tag
        video_hashes = self.client.search_files(search_tags,
                                                return_file_ids=False,
                                                return_hashes=True)["hashes"]

        # Get video files and their perceptual hashes from Hydrus
        # Stored as [(sha256, phash), ...]
        # This should probably be chunked because this might be HUGE on large libraries.
        video_hash_phash = self._get_stored_video_perceptual_hashes(video_hashes)
        self._find_potential_duplicates(video_hash_phash, overwrite)
        self.hydlog.info("Deduplication done.")

    # Return similarity of two bitstrings given a threshold
    @staticmethod
    def is_similar(a: str, b: str, hamming_distance_threshold: int) -> bool:
        _bitlist_a = list(map(int, a.replace("0b", "")))
        _bitlist_b = list(map(int, b.replace("0b", "")))
        h_distance = len(bitwise_xor(_bitlist_a,_bitlist_b,).nonzero()[0])
        return h_distance <= hamming_distance_threshold

    # Get the perceptual hash of a video.
    @staticmethod
    def calc_perceptual_hash(video_file: IOBase):
        # TODO: Can I do higher frame_intervals for shorter videos? It should work...
        return VideoHash(video_file=video_file, frame_interval=2)

    # Calculate the video perceptual hash of a file from the response of a hydrus_api client.get_file() request
    def _calc_perceptual_hash_hydrus(self, video) -> VideoHash:
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            # TODO: Do I need to check for valid request here?
            # spooled file stays in RAM unless it's too big
            with tempfile.SpooledTemporaryFile(mode="w+b", dir=tmp_dir_name) as tmp_vid_file:
                tmp_vid_file.write(video.content)
                tmp_vid_file.seek(0)
                video_perceptual_hash = HydrusVideoDeduplicator.calc_perceptual_hash(video_file=tmp_vid_file)
        return video_perceptual_hash

    # Update perceptual hash for videos
    # By default only adds missing phashes
    def _add_perceptual_hash_to_videos(self, add_missing: bool, overwrite: bool, custom_query: list | None = None) -> None:
        search_tags = ["system:filetype=video"]
        if custom_query is not None:
            custom_query = [x for x in custom_query if x.strip()] # Remove whitespace and empty strings
            search_tags.extend(custom_query)
        
        # Add phash tag to files without one
        if add_missing and not overwrite:
            search_tags.append(f"-{HYDRUS_PHASH_TAG}:*")
            print("Adding perceptual hash tags to files without one...")
        # Overwrite existing phash tag but don't add any to files without one
        elif not add_missing and overwrite:
            search_tags.append(f"{HYDRUS_PHASH_TAG}:*")
            print("Updating existing perceptual hash tags...")
        # Add phash tags to all files and overwrite existing ones
        elif add_missing and overwrite:
            if custom_query is not None:
                print("Updating perceptual hash tags for ALL files from custom query...")
            else:
                print("Updating perceptual hash tags for ALL files...")
        else:
            print("Both add new and overwrite perceptual tags are false. Skipping add tags...")
            return None
        
        
        # GET video files SHA256 with no perceptual hash tag and store for later
        print(f"Retrieving video file hashes from {self.client.api_url}")
        percep_tagged_video_hashes = self.client.search_files(search_tags, return_hashes=True)["hashes"]
        
        print("Calculating perceptual hashes:")
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            for video_hash in tqdm(percep_tagged_video_hashes):
                # TODO: Check for valid request?
                video_response = self.client.get_file(hash_=video_hash)
                try:
                    # spooled file means it stays in RAM unless it's too big
                    with tempfile.SpooledTemporaryFile(mode="w+b", dir=tmp_dir_name) as tmp_vid_file:
                        tmp_vid_file.write(video_response.content)
                        tmp_vid_file.seek(0)
                        video_percep_hash = HydrusVideoDeduplicator.calc_perceptual_hash(video_file=tmp_vid_file)
                except FFmpegFailedToExtractFrames as exc:
                    rprint("[red] Failed to calculate a perceptual hash.")
                    self.hydlog.error(f"Bad file hash: {video_hash}")
                    self.hydlog.error(exc)
                    continue

                # Store the perceptual hash in base 36 because it's really long
                short_video_percep_hash = base_repr(int(video_percep_hash.hash, base=2), base=36)
                assert(f"0b{binary_repr(int(short_video_percep_hash, base=36), width=len(video_percep_hash.hash)-2)}" == video_percep_hash.hash)
                percep_hash_tag = f'{HYDRUS_PHASH_TAG}:{short_video_percep_hash}'

                self.hydlog.debug(f"Perceptual hash calculated: {percep_hash_tag}")

                #print("Uploading perceptual hash tag to Hydrus...")
                d = {}
                d[self.local_tag_service["service_key"]] = [percep_hash_tag]
                self.client.add_tags(hashes=[video_hash], service_keys_to_tags=d)
        rprint("[green]All perceptual hash tags have been added to video files.\n")

    @staticmethod
    # Given a lexicographically SORTED list of tags, find the tag given a namespace
    # TODO: Do binary search since the tags are sorted
    def find_tag_in_tags(target_tag_namespace: str, tags: list) -> str:
        namespace_len = len(target_tag_namespace)
        for tag in tags:
            if tag[0:namespace_len] == target_tag_namespace:
                return tag[namespace_len:]
        return ""

    # Returns perceptual hashes from Hydrus converted back to hex
    # Tuple is (sha256, phash) 
    def _get_stored_video_perceptual_hashes(self, video_hashes: list[str]) -> list[tuple[str, str]]:
        phashes = []
        for video_hash in video_hashes:
            # TODO: batch this api call in batches of 256 (that's what Hydrus Client does)
            video_metadata = self.client.get_file_metadata(hashes=[video_hash], only_return_basic_information=False)
            # Why does video_tag_services contain tuples as the values?
            # The tuple is just the service as the key with value dict...
            video_tag_services = video_metadata["metadata"][0]["tags"]
            
            # tag_services are hex strings
            all_known_tags = "all known tags".encode("utf-8").hex()
            video_tags = video_tag_services[all_known_tags]["storage_tags"]["0"]
            
            # add : to PHASH_TAG in case it is not the complete namespace value for searching e.g. phasv15:
            phash = HydrusVideoDeduplicator.find_tag_in_tags(target_tag_namespace=f"{HYDRUS_PHASH_TAG}:", tags=video_tags)
            # TODO: I think VideoHash works with hex which will probably be faster
            decoded_tag = f"0b{binary_repr(int(phash, base=36), width=64)}"
            phashes.append(decoded_tag)

        return list(zip(video_hashes, phashes))
    
    # Get the filename from the filename tag if it exists in Hydrus
    # This is just used for debugging.
    # TODO: Clean this up it's a mess
    def get_file_names_hydrus(self, file_hashes: list[str]) -> list[str]:
        err_msg = "Cannot get file name from Hydrus."
        result = []
        files_metadata = self.client.get_file_metadata(hashes=file_hashes, only_return_basic_information=False)
        all_known_tags = "all known tags".encode("utf-8").hex()
        for file_metadata in files_metadata["metadata"]:
            # Try to get file extension
            try:
                ext = file_metadata["ext"]
            except KeyError:
                ext = ""
                
            # Try to get the file name
            try:
                tag_services = file_metadata["tags"]
            except KeyError:
                self.hydlog.warning(f"{err_msg} Hash: {file_metadata['hash']}")
            else:
                tags = tag_services[all_known_tags]["storage_tags"]["0"]
                tag = HydrusVideoDeduplicator.find_tag_in_tags(target_tag_namespace="filename:", tags=tags)
                # Don't show extension if filename doesn't exist
                if tag != "":
                    tag = f"{tag}{ext}"
                else:
                    self.hydlog.warning(f"{err_msg} Hash: {file_metadata['hash']}")

            result.append(tag)

        return result
    
    def get_potential_duplicate_count_hydrus(self) -> int:
        return self.client.get_potentials_count(file_service_keys=[self.all_services["all_local_files"][0]["service_key"]])["potential_duplicates_count"]
    
    # Store video_hash in list of tuples for the key phash
    # tuple is (phash, sha256)
    # Sliding window duplicate comparisons
    # i = 0
    # Iterate over list starting at i
        # if is_similar() then mark as duplicate (TODO: Defer this call to the API for speed)
        # i++

    # TODO: Split this into two functions, one checks and one uploads
    # Overwrite means if it was a potential duplicate and it's not now then remove that pair
    def _find_potential_duplicates(self, video_hash_phash: list[tuple[str, str]], overwrite_status: bool): 

        # Number of potential duplicates before adding more. Just for user info.
        pre_dedupe_count = self.get_potential_duplicate_count_hydrus()

        similar_files_found_count = 0
        for i, video in enumerate(tqdm(video_hash_phash, desc="Finding duplicates")):
            video_hash, video_phash = video[0], video[1]
            for video2 in islice(video_hash_phash, i+1, None):
                video2_hash, video2_phash = video2[0], video2[1]
                
                similar = HydrusVideoDeduplicator.is_similar(video_phash, video2_phash, self.search_distance)
                
                if similar:
                    similar_files_found_count += 1
                    if self._DEBUG:
                        file_names = self.get_file_names_hydrus([video_hash, video2_hash])
                        self.hydlog.info(f"Duplicates filenames: {file_names}")
                        #self.hydlog.info(f"\"Duplicates hashes: {video_hash}\" and \"{video2_hash}\"")
                    
                    new_relationship = {
                        "hash_a": str(video_hash),
                        "hash_b": str(video2_hash),
                        "relationship": int(hydrus_api.DuplicateStatus.POTENTIAL_DUPLICATES),
                        "do_default_content_merge": True,
                    }
                
                    # This throws always because set_file_relationships
                    # in the Hydrus API doesn't have a response or something.
                    # TODO: Defer this API call to speed up processing
                    try:
                        self.client.set_file_relationships([new_relationship])
                    except json.decoder.JSONDecodeError:
                        pass

        # Statistics for user
        video_count = len(video_hash_phash)
        if similar_files_found_count > 0:
            rprint(f"[blue] {similar_files_found_count}/{video_count} total similar videos found")
            post_dedupe_count = self.get_potential_duplicate_count_hydrus()
            new_dedupes_count = post_dedupe_count-pre_dedupe_count
            if new_dedupes_count > 0:
                rprint(f"[green] {new_dedupes_count} new potential duplicates marked for processing!")
            else:
                rprint("[green] No new potential duplicates")
        else:
            rprint(f"[yellow] No potential duplicates found out of {video_count} videos")
