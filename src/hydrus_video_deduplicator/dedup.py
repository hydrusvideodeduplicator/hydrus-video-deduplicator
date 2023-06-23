from io import IOBase
import logging
import tempfile
from itertools import islice
import json
import shelve

import hydrus_api
import hydrus_api.utils
from numpy import base_repr, binary_repr, bitwise_xor
from tqdm import tqdm
from rich import print as rprint

from .config import DEDUP_DATABASE_NAME
from .dedup_util import find_tag_in_tags, get_file_names_hydrus
from .vpdq import VPDQSignal

from .vpdq_util import (
    VPDQ_QUERY_MATCH_THRESHOLD_PERCENT,
)

class HydrusVideoDeduplicator():
    hydlog = logging.getLogger("hydlog")
    threshold: float = 0.8
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

    # Verify client connection and permissions
    # Will throw a hydrus_api.APIError if something is wrong
    def verify_api_connection(self):
        self.hydlog.info(f"Client API version: v{self.client.VERSION} | Endpoint API version: v{self.client.get_api_version()['version']}")
        hydrus_api.utils.verify_permissions(self.client, hydrus_api.utils.Permission)
    
    # This is the master function of the class
    def deduplicate(self, overwrite: bool = False, custom_query: list | None = None, skip_hashing: bool | None = False):
        # Add perceptual hashes to videos
        search_tags = ["system:filetype=video"]
        if custom_query is not None:
            custom_query = [x for x in custom_query if x.strip()] # Remove whitespace and empty strings
            if len(custom_query) > 0:
                search_tags.extend(custom_query)
                rprint(f"[yellow] Custom Query: {custom_query}")

        if not skip_hashing:
            self._add_perceptual_hash_to_videos(overwrite=overwrite, custom_query=custom_query)
        else:
            rprint("[yellow] Skipping perceptual hashing")

        self._find_potential_duplicates()
        self.hydlog.info("Deduplication done.")

    # Update perceptual hash for videos
    # By default only adds missing phashes
    # TODO: Allow batching, where if a video is already in the DB get another until no videos left or count is 0
    def _add_perceptual_hash_to_videos(self, overwrite: bool, custom_query: list | None = None) -> None:
        search_tags = ["system:filetype=video"]
        if custom_query is not None:
            custom_query = [x for x in custom_query if x.strip()] # Remove whitespace and empty strings
            search_tags.extend(custom_query)
        
        # GET video files SHA256 with no perceptual hash tag and store for later
        print(f"Retrieving video file hashes from {self.client.api_url}")
        percep_tagged_video_hashes = self.client.search_files(search_tags, return_hashes=True)["hashes"]
        
        print("Calculating perceptual hashes:")
        
        with shelve.open(DEDUP_DATABASE_NAME) as hashdb:
            with tempfile.TemporaryDirectory() as tmp_dir_name:
                for video_hash in tqdm(percep_tagged_video_hashes):
                    # don't calc new value unless overwrite is true
                    if not overwrite and video_hash in hashdb:
                        continue

                    # TODO: Check for valid request?
                    video_response = self.client.get_file(hash_=video_hash)
                    try:
                        # spooled file means it stays in RAM unless it's too big
                        with tempfile.NamedTemporaryFile(mode="w+b", dir=tmp_dir_name) as tmp_vid_file:
                            tmp_vid_file.write(video_response.content)
                            tmp_vid_file.seek(0)
                            
                            hashdb[video_hash] = VPDQSignal.hash_from_file(tmp_vid_file.name)
                            self.hydlog.debug(f"Perceptual hash calculated and written to DB.")
                    except KeyboardInterrupt:
                        rprint("[red] Perceptual hash generation was interrupted!\n")
                        return None
                    # TODO: Don't catch everything.
                    except Exception as exc:
                        rprint("[red] Failed to calculate a perceptual hash.")
                        self.hydlog.error(f"Bad file hash: {video_hash}")
                        self.hydlog.error(exc)
                        continue

        rprint("[green]All perceptual hash tags have been added to video files.\n")
    
    def get_potential_duplicate_count_hydrus(self) -> int:
        return self.client.get_potentials_count(file_service_keys=[self.all_services["all_local_files"][0]["service_key"]])["potential_duplicates_count"]
    
    # Return similarity of two bitstrings given a threshold
    @staticmethod
    def is_similar(a: str, b: str, min_percent_similarity: float = 0.8) -> bool:
        return VPDQSignal.compare_hash(a, b, min_percent_similarity, min_percent_similarity)

    # Sliding window duplicate comparisons
    # Alternatively, I could scan duplicates while adding and never do it again. I should do that instead.
    # Or, since dictionaries are ordered, store the index per hash where it ended its last search. If it's not the end, keep going until the end.
    def _find_potential_duplicates(self): 

        # TODO: Add support for query where it will get a list of the hashes from
        # the query and iterate over them instead of the entire hashdb

        # Number of potential duplicates before adding more. Just for user info.
        pre_dedupe_count = self.get_potential_duplicate_count_hydrus()

        similar_files_found_count = 0
        with shelve.open(DEDUP_DATABASE_NAME) as hashdb:
            video_count = len(hashdb)
            for i, video in enumerate(tqdm(hashdb.items(), desc="Finding duplicates")):
                video_hash, video_phash = video[0], video[1]
                for video2 in islice(hashdb.items(), i+1, None):
                    video2_hash, video2_phash = video2[0], video2[1]
                    
                    
                    similar = HydrusVideoDeduplicator.is_similar(video_phash, video2_phash, self.threshold)
                    
                    if similar:
                        similar_files_found_count += 1
                        if self._DEBUG:
                            file_names = get_file_names_hydrus(self.client, [video_hash, video2_hash])
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
        # if user does duplicates processing while the script is running this count will be wrong.
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
