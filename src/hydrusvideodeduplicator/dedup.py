from __future__ import annotations

import logging
from dataclasses import dataclass
from itertools import islice
from typing import TYPE_CHECKING

from joblib import Parallel, delayed
from rich import print
from sqlitedict import SqliteDict
from tqdm import tqdm

if TYPE_CHECKING:
    from collections.abc import Sequence

    FileHash = str

import hydrusvideodeduplicator.hydrus_api as hydrus_api

from .client import HVDClient
from .db import DedupeDB, vptree
from .hashing import (
    compute_phash,
    decode_phash_from_str,
    encode_phash_to_str,
    get_phash_similarity,
)
from .page_logger import HydrusPageLogger


@dataclass
class PerceptuallyHashedFile:
    """Class for perceptually hashed files."""

    file_hash: FileHash
    perceptual_hash: str


@dataclass
class FailedPerceptuallyHashedFile:
    """Class for failed perceptually hashed files."""

    file_hash: FileHash


class FailedPerceptualHashException(Exception):
    """Exception for when files are failed to be perceptually hashed."""

    def __init__(self, file_hash: FileHash):
        super().__init__()
        self.file_hash = file_hash


class HydrusVideoDeduplicator:
    hydlog = logging.getLogger("hvd")
    hydlog.setLevel(logging.INFO)
    threshold: float = 75.0
    _DEBUG = False

    def __init__(
        self,
        db: DedupeDB.DedupeDb,
        client: HVDClient,
        job_count: int = -2,
        failed_page_name: str | None = None,
    ):
        self.db = db
        self.client = client
        self.job_count = job_count
        self.page_logger = None if failed_page_name is None else HydrusPageLogger(self.client, failed_page_name)

    def deduplicate(
        self,
        overwrite: bool = False,
        custom_query: Sequence[str] | None = None,
        skip_hashing: bool = False,
    ) -> None:
        """
        Run all deduplicate functions:
        1. Retrieve video hashes
        2. Calculate perceptual hashes
        3. Find potential duplicates
        """

        # Add perceptual hashes to video files
        # system:filetype tags are really inconsistent
        search_tags = [
            "system:filetype=video, gif, apng",
            "system:has duration",
            "system:file service is not currently in trash",
        ]

        if custom_query is not None:
            # Remove whitespace and empty strings
            custom_query = [x for x in custom_query if x.strip()]
            if len(custom_query) > 0:
                search_tags.extend(custom_query)
                print(f"[yellow] Custom Query: {custom_query}")

        if skip_hashing:
            print("[yellow] Skipping perceptual hashing")
        else:
            video_hashes = list(self.client.get_video_hashes(search_tags))
            if not overwrite:
                video_hashes = self.filter_unhashed(video_hashes)
            print(f"[blue] Found {len(video_hashes)} eligible files to perceptually hash.")
            self.add_perceptual_hashes_to_db(video_hashes)

        # Number of potential duplicates before adding more.
        # This is just to print info for the user.
        # Note: This will be inaccurate if the user searches for duplicates in the Hydrus client
        #       while this is running.
        pre_dedupe_count = self.client.get_potential_duplicate_count_hydrus()

        # old:
        # self.find_potential_duplicates_old()

        # new:
        self.find_potential_duplicates()

        # Statistics for user
        post_dedupe_count = self.client.get_potential_duplicate_count_hydrus()
        new_dedupes_count = post_dedupe_count - pre_dedupe_count
        if new_dedupes_count > 0:
            print(f"[green] {new_dedupes_count} new potential duplicate pairs marked for manual processing!")
        else:
            print("[green] No new potential duplicate pairs found.")

        self.hydlog.info("Deduplication done.")

    def fetch_and_hash_file_exception_safe(
        self, file_hash: str
    ) -> PerceptuallyHashedFile | FailedPerceptuallyHashedFile:
        """
        Joblib can't handle exceptions, so this is used to wrap fetch_and_hash_file and
        convert any exceptions to the failed file class.
        """
        try:
            return self.fetch_and_hash_file(file_hash)
        except FailedPerceptualHashException as exc:
            return FailedPerceptuallyHashedFile(exc.file_hash)

    def fetch_and_hash_file(self, video_hash: str) -> PerceptuallyHashedFile | FailedPerceptualHashException:
        """
        Retrieves the video from Hydrus and calculates its perceptual hash.

        Throws FailedPerceptualHashException with the failed video hash if there's any errors.
        """
        try:
            video_response = self.client.client.get_file(hash_=video_hash)
        except hydrus_api.HydrusAPIException:
            print("[red] Failed to get video from Hydrus.")
            self.hydlog.error("Error getting video from Hydrus.")
            raise FailedPerceptualHashException(video_hash)

        # Calculate perceptual_hash
        try:
            phash = compute_phash(video_response.content)
            phash_str: str = encode_phash_to_str(phash)
        except Exception as exc:
            print("[red] Failed to calculate a perceptual hash.")
            self.hydlog.exception(exc)
            self.hydlog.error(f"Errored file hash: {video_hash}")
            raise FailedPerceptualHashException(video_hash)
        else:
            # "just in case" error checking
            if phash_str is None or phash_str == "[]":
                raise FailedPerceptualHashException(video_hash)

            return PerceptuallyHashedFile(video_hash, phash_str)

    def filter_unhashed(self, file_hashes: list[FileHash]) -> list[FileHash]:
        """
        Get only the files that have not been perceptually hashed in the db from a list of files.
        """

        # new:
        all_phashed_files = self.db.execute(
            "SELECT file_hash FROM files WHERE hash_id IN (SELECT hash_id FROM shape_perceptual_hash_map)"
        ).fetchall()

        all_phashed_files = [row[0] for row in all_phashed_files]

        return [file_hash for file_hash in file_hashes if file_hash not in all_phashed_files]

        # old:
        # with SqliteDict(
        #    str(DedupeDB.get_db_file_path()), tablename="videos", flag="r", outer_stack=False
        # ) as videos_table:
        #    return [
        #        file_hash
        #        for file_hash in file_hashes
        #        if file_hash not in videos_table or "perceptual_hash" not in videos_table[file_hash]
        #    ]

    def add_perceptual_hashes_to_db(self, video_hashes: Sequence[str]) -> None:
        """
        Retrieves the video from Hydrus,
        calculates the perceptual hash,
        and then add it to the database.
        """
        success_hash_count = 0
        failed_hash_count = 0
        self.hydlog.info("Starting perceptual hash processing")
        try:
            with (
                tqdm(total=len(video_hashes), dynamic_ncols=True, unit="video", colour="BLUE") as pbar,
                Parallel(n_jobs=self.job_count, return_as="generator_unordered") as parallel,
            ):
                result_generator = parallel(
                    delayed(self.fetch_and_hash_file_exception_safe)(video_hash) for video_hash in video_hashes
                )
                for result in result_generator:
                    if isinstance(result, FailedPerceptuallyHashedFile):
                        if self.page_logger:
                            self.page_logger.add_failed_video(result.file_hash)
                        failed_hash_count += 1
                        pbar.update(1)
                        continue
                    self.db.add_file(result.file_hash)
                    self.db.add_perceptual_hash(result.perceptual_hash)
                    self.db.associate_file_with_perceptual_hash(result.file_hash, result.perceptual_hash)
                    # We don't want files to exist in the database without a perceptual hash because we don't
                    # have proper error checking right now for this in vptree.
                    # So we need to wait to commit until after all the above is done.
                    self.db.commit()

                    success_hash_count += 1
                    pbar.update(1)

        except KeyboardInterrupt:
            print("[yellow] Perceptual hash processing was interrupted!")

        else:
            print("[green] Finished perceptual hash processing.")

        finally:
            if failed_hash_count > 0:
                print(f"[yellow] Perceptual hash processing had {failed_hash_count} failed files.")
                if self.page_logger is None:
                    print(
                        "\nTip: You can see what files failed directly in Hydrus by "
                        "creating a page with the name 'failed' and "
                        "running the program with '--failed-page-name=failed'\n"
                    )
            print(f"[green] Added {success_hash_count} new videos to the perceptual hash database.")

    def compare_videos(self, video1_hash: str, video2_hash: str, video1_phash: str, video2_phash: str) -> None:
        """Compare videos and mark them as potential duplicates in Hydrus if they are similar."""
        hash_a = decode_phash_from_str(video1_phash)
        hash_b = decode_phash_from_str(video2_phash)
        similarity = get_phash_similarity(hash_a, hash_b)

        if similarity >= self.threshold:
            if self._DEBUG:
                # Getting the file names will be VERY slow because of the API call
                # file_names = get_file_names_hydrus(self.client.client, [video1_hash, video2_hash])
                # self.hydlog.info(f"Duplicates filenames: {file_names}")
                self.hydlog.info(f'"Similar {similarity}%: {video1_hash}" and "{video2_hash}"')

            self.mark_videos_as_duplicates(video1_hash, video2_hash)

    def mark_videos_as_duplicates(self, video1_hash: str, video2_hash: str):
        """Mark a pair of videos as duplicates in Hydrus."""
        new_relationship = {
            "hash_a": video1_hash,
            "hash_b": video2_hash,
            "relationship": int(hydrus_api.DuplicateStatus.POTENTIAL_DUPLICATES),
            "do_default_content_merge": True,
        }

        self.client.client.set_file_relationships([new_relationship])

    def find_potential_duplicates(
        self,
    ) -> None:
        """Find potential duplicates in the database and mark them in Hydrus."""
        # TODO: Should we turn the inside of this function into a generator? It might make testing super easy.
        tree = vptree.VpTreeManager(self.db)
        search_threshold = vptree.fix_vpdq_similarity((self.threshold))
        assert search_threshold > 0 and isinstance(search_threshold, int)

        if tree.MaintenanceDue(search_threshold):
            tree.maintain_tree()

        files = self.db.execute(
            "SELECT hash_id FROM shape_search_cache WHERE searched_distance is NULL or searched_distance < :threshold",
            {"threshold": search_threshold},
        ).fetchall()

        with tqdm(
            dynamic_ncols=True, total=len(files), desc="Finding potential duplicates", unit="video", colour="BLUE"
        ) as pbar:
            for hash_id in files:
                hash_id = hash_id[0]
                # print(f"Searching for duplicates for hash_id: '{hash_id}'")
                result = tree.SearchFile(hash_id, max_hamming_distance=2)
                # print(f"File Hash: '{file_hash}'")
                # print(result)
                file_hash_a = self.db.get_file_hash(hash_id)
                for similar_hash_id, distance in result:
                    file_hash_b = self.db.get_file_hash(similar_hash_id)
                    if hash_id != similar_hash_id:
                        self.mark_videos_as_duplicates(file_hash_a, file_hash_b)

                # TODO:
                # Do we need to add the below line here? See _PerceptualHashesSearchForPotentialDuplicates in Hydrus.
                # group_of_hash_ids = self._STL( self._Execute( 'SELECT hash_id FROM shape_search_cache WHERE searched_distance IS NULL or searched_distance < ?;', ( search_distance, ) ).fetchmany( 10 ) )   # noqa: E501
                # Update the search cache
                self.db.execute(
                    "UPDATE shape_search_cache SET searched_distance = ? WHERE hash_id = ?;",
                    (search_threshold, hash_id),
                )

                self.db.commit()
                pbar.update(1)

    def find_potential_duplicates_old(
        self,
    ) -> None:
        """Old brute-force search. Find potential duplicates in the database and mark them in Hydrus."""
        with SqliteDict(
            str(DedupeDB.get_db_file_path()), tablename="videos", flag="c", autocommit=True, outer_stack=False
        ) as videos_table:
            current_hash = None
            try:
                # Make a copy of the video hashes here so we can preserve their order because SqliteDict row order
                # changes during writes for the farthest search index. This is a bandaid solution.
                # This assumes SqliteDict row order is preserved when opened and closed, even if it's not preserved
                # while modifying elements.
                video_hashes = [video_hash for video_hash in videos_table]
                total = len(video_hashes)

                with tqdm(
                    dynamic_ncols=True, total=total, desc="Finding potential duplicates", unit="video", colour="BLUE"
                ) as pbar:
                    # -1 is all cores, -2 is all cores but one
                    with Parallel(n_jobs=self.job_count) as parallel:
                        for i, video1_hash in enumerate(video_hashes):
                            pbar.update(1)
                            current_hash = video1_hash

                            row = videos_table[video1_hash]

                            # We only care about combinations of pairs, not permutations,
                            # so start at the next unique comparison.
                            start_index = i + 1

                            # Start at the last furthest searched position in the database for each element.
                            # This way you only have to start searching at that place instead of at i+1, if it exists
                            if "farthest_search_index" in row:
                                start_index = row["farthest_search_index"]

                            assert start_index <= total
                            if start_index == total:
                                # This file has already been searched for dupes against all other videos in the DB
                                continue

                            parallel(
                                delayed(self.compare_videos)(
                                    video1_hash,
                                    video2_hash,
                                    row["perceptual_hash"],
                                    videos_table[video2_hash]["perceptual_hash"],
                                )
                                for video2_hash in islice(video_hashes, start_index, None)
                            )

                            # Video has now been compared against all other videos for dupes,
                            # so update farthest_search_index to the current length of the table
                            row["farthest_search_index"] = total
                            videos_table[video1_hash] = row

            except KeyboardInterrupt:
                print("[yellow] Duplicate search was interrupted!")
            else:
                # current_hash can be None if Hydrus DB has no files...
                if current_hash is not None:
                    # Set the last element farthest_search_index to the end of the
                    # table since it won't get hashed because of the islice optimization
                    row = videos_table[current_hash]
                    row["farthest_search_index"] = total
                    videos_table[current_hash] = row
