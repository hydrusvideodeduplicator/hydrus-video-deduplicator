import os
from io import IOBase
import logging
import tempfile
from itertools import islice
import json
from pathlib import Path
import subprocess
from joblib import Parallel, delayed

import hydrusvideodeduplicator.hydrus_api as hydrus_api
import hydrusvideodeduplicator.hydrus_api.utils
from tqdm import tqdm
from rich import print as rprint
from sqlitedict import SqliteDict

from .config import DEDUP_DATABASE_FILE, DEDUP_DATABASE_DIR, DEDUP_DATABASE_NAME
from .dedup_util import database_accessible, find_tag_in_tags, get_file_names_hydrus, ThreadSafeCounter
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
        
        # Commonly used things from the Hydrus database
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
            self._add_perceptual_hashes_to_db(overwrite=overwrite, custom_query=custom_query)
        else:
            rprint("[yellow] Skipping perceptual hashing")

        self._find_potential_duplicates()
        self.hydlog.info("Deduplication done.")

    @classmethod
    # dir is where you're writing the video file
    def _add_perceptual_hash_to_db(cls, video_hash: str, video: str | bytes, db) -> None:
        with tempfile.NamedTemporaryFile(mode="w+b") as tmp_vid_file:
            # Write video to file to be able to calculate hash
            tmp_vid_file.write(video)
            tmp_vid_file.flush()

            ffprobe_cmd = [
                        'ffprobe',
                        '-v',
                        'error',
                        '-select_streams',
                        'v:0',
                        '-show_entries',
                        'stream=codec_name',
                        '-of',
                        'default=noprint_wrappers=1:nokey=1',
                        tmp_vid_file.name
                        ]

            # Execute the ffprobe command and capture the output
            video_codec = subprocess.check_output(ffprobe_cmd).decode('utf-8').strip()

            # These are found by trial and error. If you find an unsupported codec, create an issue on GitHub please.
            # Unsupported codecs appear to be an OpenCV issue more than an FFmpeg issue but I can't solve it at the moment.
            # For now, just transcode the video to avc1
            unsupported_codecs = ["av1"]

            if video_codec in unsupported_codecs:
                rprint(f"[yellow] Video file has unsupported codec: {video_codec}")
                rprint("[yellow] Falling back to transcoding (this may take a bit)")

                try:

                    with tempfile.NamedTemporaryFile(mode="w+b", suffix=".mp4") as tmp_vid_file_transcoded:
                        ffmpeg_cmd = [
                            'ffmpeg',
                            '-y',
                            '-i',
                            tmp_vid_file.name,
                            '-c:v',
                            'libx264',
                            '-preset',
                            'veryfast',
                            '-crf',
                            '28',
                            tmp_vid_file_transcoded.name,
                        ]

                        # Execute the ffmpeg command
                        with open(os.devnull, "w") as devnull: subprocess.call(ffmpeg_cmd, stdout=devnull, stderr=devnull)

                        perceptual_hash = VPDQSignal.hash_from_file(tmp_vid_file_transcoded.name)

                        rprint("[yellow] Fallback to transcode successful.")
                except Exception as exc:
                    rprint("[red] Transcode and/or hashing the transcode failed.")
                    cls.hydlog.error(f"Error transcode video hash: {video_hash}")
                    cls.hydlog.error(exc)
                    return None
            else:
                perceptual_hash = VPDQSignal.hash_from_file(tmp_vid_file.name)

            
            row = db.get(video_hash, {})
            row["perceptual_hash"] = perceptual_hash
            db[video_hash] = row
            cls.hydlog.debug(f"Perceptual hash calculated and added to DB.")
    
    # Add perceptual hash for videos to the database
    def _add_perceptual_hashes_to_db(self, overwrite: bool, custom_query: list | None = None) -> None:

        # Create database folder if it doesn't already exist
        if os.path.exists(DEDUP_DATABASE_FILE):
            with SqliteDict(str(DEDUP_DATABASE_FILE), tablename = "videos") as hashdb:
                rprint(f"[green] Database found with {len(hashdb)} videos already hashed.")
                self.hydlog.info(f"Found existing DB of length {len(hashdb)}, size {os.path.getsize(DEDUP_DATABASE_FILE)}")
        else:
            rprint(f"[yellow] Database not found. Creating one at {DEDUP_DATABASE_FILE}")
            os.makedirs(DEDUP_DATABASE_DIR, exist_ok=True)
            self.hydlog.info(f"Created DB dir {DEDUP_DATABASE_DIR}")

        search_tags = ["system:filetype=video"]
        if custom_query is not None:
            custom_query = [x for x in custom_query if x.strip()] # Remove whitespace and empty strings
            search_tags.extend(custom_query)
        
        with SqliteDict(str(DEDUP_DATABASE_FILE), tablename = "videos") as hashdb:
            print(f"Retrieving video file hashes...")
            all_video_hashes = self.client.search_files(search_tags, file_sort_type=hydrus_api.FileSortType.FILE_SIZE, return_hashes=True, file_sort_asc=True, return_file_ids=False)["hashes"]
            print("Calculating perceptual hashes:")
            with tqdm(dynamic_ncols=True, total=len(all_video_hashes), unit="video", colour="BLUE") as pbar:
                for chunk_video_hashes in hydrus_api.utils.yield_chunks(all_video_hashes, chunk_size=16):
                    for video_hash in chunk_video_hashes:
                        pbar.update(1)
                        # Only calculate new hash if it's missing or if overwrite is true
                        if not overwrite and video_hash in hashdb and hashdb[video_hash].get("perceptual_hash", None) is not None:
                            continue
                        
                        try:
                            video_response = self.client.get_file(hash_=video_hash)
                            if video_response.content is None:
                                continue
                        except hydrus_api.HydrusAPIException:
                            rprint("[red] Error getting file from database.")
                            continue

                        try:
                            self._add_perceptual_hash_to_db(video_hash=video_hash,
                                                            video=video_response.content,
                                                            db=hashdb)
                        except KeyboardInterrupt:
                            rprint("[red] Perceptual hash generation was interrupted!\n")
                            hashdb.commit()
                            return None
                        except Exception as exc:
                            rprint("[red] Failed to calculate a perceptual hash.")
                            self.hydlog.error(exc)
                            self.hydlog.error(f"Errored file hash: {video_hash}")
                            
                    # Commit at the end of a chunk
                    hashdb.commit()

            # Commit at the end of all processing
            hashdb.commit()
            
        rprint("[green] Finished perceptual hash processing.\n")
    
    def get_potential_duplicate_count_hydrus(self) -> int:
        return self.client.get_potentials_count(file_service_keys=[self.all_services["all_local_files"][0]["service_key"]])["potential_duplicates_count"]
    
    # Return similarity of two bitstrings given a threshold
    @staticmethod
    def is_similar(a: str, b: str, min_percent_similarity: float = 0.8) -> bool:
        return VPDQSignal.compare_hash(a, b, min_percent_similarity, min_percent_similarity)
    

    def compare_videos(self, video1_hash, video2_hash, video1_phash, video2_phash):
        similar = HydrusVideoDeduplicator.is_similar(video1_phash, video2_phash, self.threshold)

        if similar:
            if self._DEBUG:
                #file_names = get_file_names_hydrus(self.client, [video_hash, video2_hash])
                #self.hydlog.info(f"Duplicates filenames: {file_names}")
                self.hydlog.info(f"\"Duplicates hashes: {video1_hash}\" and \"{video2_hash}\"")
            
            new_relationship = {
                "hash_a": str(video1_hash),
                "hash_b": str(video2_hash),
                "relationship": int(hydrus_api.DuplicateStatus.POTENTIAL_DUPLICATES),
                "do_default_content_merge": True,
            }
        
            self.client.set_file_relationships([new_relationship])
    
    # Delete cache row in database
    @staticmethod
    def clear_search_cache():
        try:
            with SqliteDict(str(DEDUP_DATABASE_FILE), tablename="videos", flag="c") as hashdb:
                for key in hashdb:
                    row = hashdb[key]
                    if "farthest_search_index" in row:
                        del row["farthest_search_index"]
                    hashdb[key] = row
                hashdb.commit()
        except OSError:
            rprint(f"[red] Database does not exist. Cannot clear search cache.")

    # Sliding window duplicate comparisons
    # Alternatively, I could scan duplicates while adding and never do it again. I should do that instead.
    # Or, since dictionaries are ordered, store the index per hash where it ended its last search. If it's not the end, keep going until the end.
    # TODO: Add support for query where it will get a list of the hashes from
    # the query and iterate over them instead of the entire hashdb
    def _find_potential_duplicates(self): 

        if not database_accessible(DEDUP_DATABASE_FILE, tablename="videos"):
            rprint(f"[red] Could not search for duplicates.")
            return None

        # Number of potential duplicates before adding more. Just for user info.
        pre_dedupe_count = self.get_potential_duplicate_count_hydrus()
        
        count_since_last_commit = 0
        commit_interval = 8
            
        video_counter = 0
        with SqliteDict(str(DEDUP_DATABASE_FILE), tablename="videos", flag="c") as hashdb:
            try:
                with tqdm(dynamic_ncols=True, total=len(hashdb), desc="Finding duplicates", unit="video", colour="BLUE") as pbar:
                    # -1 is all cores, -2 is all cores but one
                    with Parallel(n_jobs=-2) as parallel:
                        for i, video1_hash in enumerate(hashdb):
                            pbar.update(1)
                            video_counter+=1

                            # Store last furthest searched position in the database for each element
                            # This way you only have to start searching at that place instead of at i+1 if it exists
                            row = hashdb[video1_hash]
                            row.setdefault("farthest_search_index", i+1)

                            # This is not necessary but may increase speed by avoiding any of the code below
                            if row["farthest_search_index"] >= len(hashdb)-1:
                                continue

                            parallel(delayed(self.compare_videos)(video1_hash, video2_hash, hashdb[video1_hash]["perceptual_hash"], hashdb[video2_hash]["perceptual_hash"]) for video2_hash in islice(hashdb, row["farthest_search_index"], None))

                            # Update furthest search position to the current length of the table
                            row["farthest_search_index"] = len(hashdb)-1
                            hashdb[video1_hash] = row

                            count_since_last_commit+=1

                            if count_since_last_commit >= commit_interval:
                                hashdb.commit()
                                count_since_last_commit = 0

            except KeyboardInterrupt:
                pass
            finally:
                hashdb.commit()

        # Statistics for user
        post_dedupe_count = self.get_potential_duplicate_count_hydrus()
        new_dedupes_count = post_dedupe_count-pre_dedupe_count
        if new_dedupes_count > 0:
            rprint(f"[green] {new_dedupes_count} new potential duplicates marked for processing!")
        else:
            rprint("[green] No new potential duplicates found.")
