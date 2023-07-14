from __future__ import annotations

import logging
import os
from itertools import islice
from pathlib import Path
from typing import TYPE_CHECKING

from joblib import Parallel, delayed
from rich import print as rprint
from sqlitedict import SqliteDict
from tqdm import tqdm

if TYPE_CHECKING:
    from collections.abc import Generator, Iterable, Sequence
    from typing import Any

import hydrusvideodeduplicator.hydrus_api as hydrus_api
import hydrusvideodeduplicator.hydrus_api.utils as hydrus_api_utils

from .config import DEDUP_DATABASE_DIR, DEDUP_DATABASE_FILE
from .dedup_util import database_accessible
from .vpdqpy.vpdqpy import Vpdq


class HydrusVideoDeduplicator:
    hydlog = logging.getLogger("hydlog")
    threshold: float = 75.0
    _DEBUG = False

    def __init__(self, client: hydrus_api.Client, verify_connection: bool = True):
        self.client = client
        if verify_connection:
            self.verify_api_connection()
        self.hydlog.setLevel(logging.WARNING)

        # Commonly used things from the Hydrus database
        # If any of these are large they should probably be lazily loaded
        self.all_services = self.client.get_services()

    # Verify client connection and permissions
    # Will throw a hydrus_api.APIError if something is wrong
    def verify_api_connection(self) -> None:
        self.hydlog.info(
            (
                f"Client API version: v{self.client.VERSION}"
                "| Endpoint API version: v{self.client.get_api_version()['version']}"
            )
        )
        hydrus_api_utils.verify_permissions(self.client, hydrus_api.utils.Permission)

    # Verify that the supplied file_service_key is a valid key for a local file service
    def verify_file_service_keys(self, file_service_keys: Iterable[str]) -> None:
        VALID_SERVICE_TYPES = [hydrus_api.ServiceType.ALL_LOCAL_FILES, hydrus_api.ServiceType.FILE_DOMAIN]

        for file_service_key in file_service_keys:
            file_service = self.all_services['services'].get(file_service_key)
            if file_service is None:
                raise KeyError(f"Invalid file service key: '{file_service_key}'")

            service_type = file_service.get('type')
            if service_type not in VALID_SERVICE_TYPES:
                raise KeyError("File service key must be a local file service")

    # This is the master function of the class
    def deduplicate(
        self,
        overwrite: bool = False,
        custom_query: Sequence[str] | None = None,
        skip_hashing: bool = False,
        file_service_keys: Sequence[str] | None = None,
    ) -> None:
        # Add perceptual hashes to video files
        # system:filetype tags are really inconsistent
        search_tags = [
            'system:filetype=video, gif, apng',
            'system:has duration',
            'system:file service is not currently in trash',
        ]

        query = False
        if custom_query is not None:
            # Remove whitespace and empty strings
            custom_query = [x for x in custom_query if x.strip()]
            if len(custom_query) > 0:
                search_tags.extend(custom_query)
                rprint(f"[yellow] Custom Query: {custom_query}")
                query = True

        # Set the file service keys to be used for hashing
        # Default is "all local files"
        if file_service_keys is None or not file_service_keys:
            file_service_keys = [self.all_services["all_local_files"][0]["service_key"]]
        else:
            file_service_keys = [x for x in file_service_keys if x.strip()]
        self.verify_file_service_keys(file_service_keys)

        video_hashes = None
        if skip_hashing:
            rprint("[yellow] Skipping perceptual hashing")
            if query:
                video_hashes = set(self._retrieve_video_hashes(search_tags, file_service_keys))
        else:
            all_video_hashes = self._retrieve_video_hashes(search_tags, file_service_keys)
            self._add_perceptual_hashes_to_db(overwrite=overwrite, video_hashes=all_video_hashes)

        if query and not skip_hashing:
            video_hashes = set(self._retrieve_video_hashes(search_tags, file_service_keys))

        if query:
            self._find_potential_duplicates(limited_video_hashes=video_hashes, file_service_keys=file_service_keys)
        else:
            self._find_potential_duplicates(limited_video_hashes=None, file_service_keys=file_service_keys)

        self.hydlog.info("Deduplication done.")

    @staticmethod
    def _calculate_perceptual_hash(video: Path | str | bytes) -> str:
        perceptual_hash = Vpdq.vpdq_to_json(Vpdq.computeHash(video))
        assert perceptual_hash != "[]"
        return perceptual_hash

    def _retrieve_video_hashes(
        self, search_tags: Iterable[str], file_service_keys: Iterable[str] | None = None
    ) -> Iterable[str]:
        all_video_hashes = self.client.search_files(
            tags=search_tags,
            file_service_keys=file_service_keys,
            file_sort_type=hydrus_api.FileSortType.FILE_SIZE,
            return_hashes=True,
            file_sort_asc=True,
            return_file_ids=False,
        )["hashes"]
        return all_video_hashes

    def _add_perceptual_hashes_to_db(self, overwrite: bool, video_hashes: Sequence[str]) -> None:
        # Create database folder
        try:
            os.makedirs(DEDUP_DATABASE_DIR, exist_ok=False)
            # Exception before this log if directory already exists
            self.hydlog.info(f"Created DB dir {DEDUP_DATABASE_DIR}")
        except OSError:
            pass

        with SqliteDict(str(DEDUP_DATABASE_FILE), tablename="videos", flag="c") as hashdb:
            dblen = len(hashdb)
            dbsize = os.path.getsize(DEDUP_DATABASE_FILE)

            if dblen > 0:
                self.hydlog.info(f"Database found of length {dblen}, size {dbsize} bytes")
            else:
                self.hydlog.info(f"Database not found. Creating one at {DEDUP_DATABASE_FILE}")

            try:
                with tqdm(total=len(video_hashes), dynamic_ncols=True, unit="video", colour="BLUE") as pbar:
                    count_since_last_commit = 0
                    COMMIT_INTERVAL = 16

                    for video_hash in video_hashes:
                        pbar.update(1)
                        # Only calculate new hash if it's missing or if overwrite is true
                        if not overwrite and video_hash in hashdb and "perceptual_hash" in hashdb[video_hash]:
                            continue

                        # Get video file from Hydrus
                        try:
                            video_response = self.client.get_file(hash_=video_hash)
                        except hydrus_api.HydrusAPIException:
                            rprint("[red] Failed to get video from Hydrus.")
                            self.hydlog.error("Error getting video from Hydrus.")
                            continue

                        # Calculate perceptual_hash
                        try:
                            perceptual_hash = self._calculate_perceptual_hash(video_response.content)
                        except Exception as exc:
                            rprint("[red] Failed to calculate a perceptual hash.")
                            self.hydlog.exception(exc)
                            self.hydlog.error(f"Errored file hash: {video_hash}")
                        else:
                            # Write perceptual hash to DB
                            row = hashdb.get(video_hash, {})
                            row["perceptual_hash"] = perceptual_hash
                            hashdb[video_hash] = row

                            # Batch DB commits to avoid excessive writes
                            count_since_last_commit += 1
                            if count_since_last_commit >= COMMIT_INTERVAL:
                                hashdb.commit()
                                count_since_last_commit = 0
                                self.hydlog.debug("Committed perceptual hashes to database.")

                            self.hydlog.debug("Perceptual hash calculated.")

            except KeyboardInterrupt:
                interrupt_msg = "Perceptual hash processing was interrupted!"
                rprint(f"[yellow] {interrupt_msg}")
                self.hydlog.error(interrupt_msg)

            else:
                rprint("[green] Finished perceptual hash processing.")

            finally:
                hashdb.commit()
                rprint(f"[green] Added {len(hashdb) - dblen} new videos to database.")
                self.hydlog.info("Finished perceptual hash processing.")

    def get_potential_duplicate_count_hydrus(self, file_service_keys: Iterable[str]) -> int:
        return self.client.get_potentials_count(file_service_keys=file_service_keys)["potential_duplicates_count"]

    def compare_videos(self, video1_hash: str, video2_hash: str, video1_phash: str, video2_phash: str) -> None:
        vpdq_hash1 = Vpdq.json_to_vpdq(video1_phash)
        vpdq_hash2 = Vpdq.json_to_vpdq(video2_phash)
        similar, similarity = Vpdq.is_similar(vpdq_hash1, vpdq_hash2, self.threshold)

        if similar:
            if self._DEBUG:
                # Getting the file names will be VERY slow because of the API call
                # file_names = get_file_names_hydrus(self.client, [video1_hash, video2_hash])
                # self.hydlog.info(f"Duplicates filenames: {file_names}")
                self.hydlog.info(f"\"Similar {similarity}%: {video1_hash}\" and \"{video2_hash}\"")

            new_relationship = {
                "hash_a": str(video1_hash),
                "hash_b": str(video2_hash),
                "relationship": int(hydrus_api.DuplicateStatus.POTENTIAL_DUPLICATES),
                "do_default_content_merge": True,
            }

            self.client.set_file_relationships([new_relationship])

    # Delete cache search index value for each video in database
    @staticmethod
    def clear_search_cache() -> None:
        if not database_accessible(DEDUP_DATABASE_FILE, tablename="videos"):
            return
        with SqliteDict(str(DEDUP_DATABASE_FILE), tablename="videos", flag="c") as hashdb:
            for key in hashdb:
                row = hashdb[key]
                if "farthest_search_index" in row:
                    del row["farthest_search_index"]
                hashdb[key] = row
            hashdb.commit()
        rprint("[green] Cleared search cache.")

    def _find_potential_duplicates(
        self, limited_video_hashes: Sequence[str] | None = None, file_service_keys: Iterable[str] | None = None
    ) -> None:
        if not database_accessible(DEDUP_DATABASE_FILE, tablename="videos", verbose=True):
            rprint("[red] Could not search for duplicates.")
            return

        # Number of potential duplicates before adding more. Just for user info.
        pre_dedupe_count = self.get_potential_duplicate_count_hydrus(file_service_keys)

        # BUG: If this process is interrupted, the farthest_search_index will not save for ANY entries.
        #      I think it might be because every entry in the column needs an entry for SQlite but I'm not sure.
        video_counter = 0
        with SqliteDict(str(DEDUP_DATABASE_FILE), tablename="videos", flag="c") as hashdb:
            try:
                if limited_video_hashes is not None:
                    total = len(limited_video_hashes)
                else:
                    total = len(hashdb)

                with tqdm(
                    dynamic_ncols=True, total=total, desc="Finding duplicates", unit="video", colour="BLUE"
                ) as pbar:
                    # -1 is all cores, -2 is all cores but one
                    with Parallel(n_jobs=-2) as parallel:
                        if limited_video_hashes is not None:
                            # Avoid checking if in hashdb for each hash. Just do it now.
                            clean_all_retrieved_video_hashes = [
                                video_hash for video_hash in limited_video_hashes if video_hash in hashdb
                            ]

                            for i, video1_hash in enumerate(clean_all_retrieved_video_hashes):
                                video_counter += 1
                                pbar.update(1)
                                parallel(
                                    delayed(self.compare_videos)(
                                        video1_hash,
                                        clean_all_retrieved_video_hashes[j],
                                        hashdb[video1_hash]["perceptual_hash"],
                                        hashdb[clean_all_retrieved_video_hashes[j]]["perceptual_hash"],
                                    )
                                    for j in range(i + 1, len(clean_all_retrieved_video_hashes))
                                )

                        else:
                            count_since_last_commit = 0
                            commit_interval = 32

                            for i, video1_hash in enumerate(hashdb):
                                video_counter += 1
                                pbar.update(1)

                                row = hashdb[video1_hash]

                                # Store last furthest searched position in the database for each element
                                # This way you only have to start searching at that place instead of at i+1 if it exists
                                farthest_search_index = row.setdefault("farthest_search_index", i + 1)

                                assert farthest_search_index <= total
                                if farthest_search_index == total:
                                    # This file has already been searched for dupes against all other videos in the DB
                                    continue

                                parallel(
                                    delayed(self.compare_videos)(
                                        video1_hash,
                                        video2_hash,
                                        hashdb[video1_hash]["perceptual_hash"],
                                        hashdb[video2_hash]["perceptual_hash"],
                                    )
                                    for video2_hash in islice(hashdb, row["farthest_search_index"], None)
                                )

                                # Video has now been compared against all other videos for dupes,
                                # so update farthest_search_index to the current length of the table
                                row["farthest_search_index"] = total
                                hashdb[video1_hash] = row
                                count_since_last_commit += 1

                                if count_since_last_commit >= commit_interval:
                                    hashdb.commit()
                                    count_since_last_commit = 0

            except KeyboardInterrupt:
                rprint("[yellow] Duplicate search was interrupted!")
            finally:
                hashdb.commit()

        # Statistics for user
        post_dedupe_count = self.get_potential_duplicate_count_hydrus(file_service_keys)
        new_dedupes_count = post_dedupe_count - pre_dedupe_count
        if new_dedupes_count > 0:
            rprint(f"[green] {new_dedupes_count} new potential duplicates marked for processing!")
        else:
            rprint("[green] No new potential duplicates found.")

    @staticmethod
    def batched(iterable: Iterable, n: int) -> Generator[tuple, Any, None]:
        """
        Batch data into tuples of length n. The last batch may be shorter."
        batched('ABCDEFG', 3) --> ABC DEF G
        DO NOT use this for iterating over the database. Use batched_and_save_db instead.
        """
        assert n >= 1
        it = iter(iterable)
        while batch := tuple(islice(it, n)):
            yield batch

    @staticmethod
    def batched_and_save_db(
        db: SqliteDict,
        n: int,
        chunk_size: int | None = None,
    ) -> Generator[dict[str, dict[str, Any]], Any, None]:
        """
        Batch rows of into rows of length n and save changes after each batch or after chunk_size batches.
        """
        assert n >= 1
        assert chunk_size is None or chunk_size >= 1
        if chunk_size is None:
            chunk_size = n
        it = iter(db.items())
        while batch_items := dict(islice(it, n)):
            batch_dict = {
                key: {col: val for col, val in row.items() if col != 'key'} for key, row in batch_items.items()
            }

            yield batch_dict

            # Save changes after each batch
            db.commit()

    def is_files_deleted_hydrus(self, file_hashes: Iterable[str]) -> dict[str, bool]:
        """
        Check if files are trashed or deleted in Hydrus
        Returns a dictionary of hash : trashed_or_not
        """
        videos_metadata = self.client.get_file_metadata(hashes=file_hashes, only_return_basic_information=False)[
            "metadata"
        ]

        result = {}
        for video_metadata in videos_metadata:
            # This should never happen, but it shouldn't break the program if it does
            if "hash" not in video_metadata:
                logging.error("Hash not found for potentially trashed file.")
                continue
            video_hash = video_metadata["hash"]
            is_deleted: bool = video_metadata.get("is_deleted", False)
            result[video_hash] = is_deleted
        return result

    @staticmethod
    def update_search_cache(new_total: int | None = None) -> None:
        """
        Update the search cache to clamp the farthest_search_index to the current length of the database
        """
        assert new_total is None or new_total >= 0

        if not database_accessible(DEDUP_DATABASE_FILE, tablename="videos"):
            return

        CHUNK_SIZE = 256
        with SqliteDict(str(DEDUP_DATABASE_FILE), tablename="videos", flag="c", outer_stack=False) as hashdb:
            if new_total is None:
                new_total = len(hashdb)
            for batched_items in HydrusVideoDeduplicator.batched_and_save_db(hashdb, CHUNK_SIZE):
                for item in batched_items.items():
                    row = hashdb[item[0]]
                    if 'farthest_search_index' in row and row['farthest_search_index'] > new_total:
                        row['farthest_search_index'] = new_total
                        hashdb[item[0]] = row

    def clear_trashed_files_from_db(self) -> None:
        """
        Delete trashed and deleted files from Hydrus from the database
        TODO: This doesn't have to run everytime. Run it every couple startups or something.
        """
        if not database_accessible(DEDUP_DATABASE_FILE, tablename="videos"):
            return

        try:
            with SqliteDict(str(DEDUP_DATABASE_FILE), tablename="videos", flag="c", outer_stack=False) as hashdb:
                # This is EXPENSIVE. sqlitedict gets len by iterating over the entire database!
                if (total := len(hashdb)) < 1:
                    return

                delete_count = 0
                rprint(f"[blue] Database found with {total} videos already hashed.")
                try:
                    with tqdm(
                        dynamic_ncols=True,
                        total=total,
                        desc="Clearing trashed videos",
                        unit="video",
                        colour="BLUE",
                    ) as pbar:
                        CHUNK_SIZE = 32
                        for batched_items in self.batched_and_save_db(hashdb, CHUNK_SIZE):
                            is_trashed_result = self.is_files_deleted_hydrus(batched_items.keys())
                            for result in is_trashed_result.items():
                                video_hash = result[0]
                                is_trashed = result[1]
                                if is_trashed is True:
                                    del hashdb[video_hash]
                                    delete_count += 1
                            pbar.update(min(CHUNK_SIZE, total - pbar.n))
                except Exception as exc:
                    rprint("[red] Error while clearing trashed videos cache.")
                    print(exc)
                    self.hydlog.error(exc)
                finally:
                    if delete_count > 0:
                        print(f"Cleared {delete_count} trashed videos from the database.")
                    self.update_search_cache(total - delete_count)

        except OSError as exc:
            self.hydlog.info(exc)
