from __future__ import annotations

import logging
import time
import unittest
from typing import TYPE_CHECKING

import hydrusvideodeduplicator.hydrus_api as hydrus_api
from hydrusvideodeduplicator.client import HVDClient
from hydrusvideodeduplicator.dedup import HydrusVideoDeduplicator

if TYPE_CHECKING:
    pass

TEST_API_URL = "https://localhost:45869"
TEST_API_ACCESS_KEY = "3b3cf10cc13862818ea95ddecfe434bed0828fb319b1ff56413917b471b566ab"


@unittest.skip("Skipped Hydrus dedupe: implementation not finished")
class TestDedupe(unittest.TestCase):
    log = logging.getLogger(__name__)
    log.setLevel(logging.WARNING)
    logging.basicConfig()

    def setUp(self):
        """TODO: Check if Hydrus Docker container is accessible before this."""
        """TODO: Clear database before this for repeated runs."""

        # Try to connect to Hydrus
        connect_attempts = 0
        max_attempts = 3
        while connect_attempts < max_attempts:
            TestDedupe.log.info(f"Attempting connection to Hydrus... {connect_attempts}/{max_attempts}")
            try:
                # Create Hydrus connection
                self.hvdclient = HVDClient(
                    file_service_keys=None,
                    api_url=TEST_API_URL,
                    access_key=TEST_API_ACCESS_KEY,
                    verify_cert=False,
                )
            except Exception as exc:
                connect_attempts += 1
                time.sleep(0.5)
                TestDedupe.log.warning(exc)

            else:
                break
        if connect_attempts == max_attempts:
            TestDedupe.log.error(f"Failed to connect to Hydrus client after {connect_attempts} tries.")
            self.fail("Failed to connect to Hydrus.")
        try:
            self.assertNotEqual(self.hvdclient, None)
        except AttributeError:
            self.fail("Failed to connect to Hydrus.")

        self.hvd = HydrusVideoDeduplicator(
            self.hvdclient,
            # job_count=-2,  # TODO: Do tests for single and multi-threaded.
        )

    def test_temp(self):
        self.assertTrue(True)

    def test_set_similar_files_duplicates(self):
        """
        Check two files are not set as potential duplicates.

        Set two files as potential duplicates using the Hydrus API.

        Check those two files are now potential duplicates.
        """
        initial_dedupe_count = self.hvdclient.get_potential_duplicate_count_hydrus()
        self.assertEqual(
            initial_dedupe_count,
            0,
            "Initial potential duplicates must be 0."
            f" Potential duplicates: {initial_dedupe_count}. Reset the database before running tests.",
        )

        file_hash_pair = tuple(
            [
                "3011a3d7dc742d6c0f37194ba8273e6b09b90fe768d5f11386ff140bc6745d52",
                "e131ad42621442758a3acb899bfbdfeeab0b40c7e2f7c7e66683f58a09a99aee",
            ]
        )

        # Check two files are not set as potential duplicates
        before_relationships = self.hvdclient.client.get_file_relationships(hashes=file_hash_pair)
        if file_hash_pair[0] in before_relationships["file_relationships"]:
            self.assertFalse(
                file_hash_pair[1]
                in before_relationships["file_relationships"][file_hash_pair[0]][
                    str(hydrus_api.DuplicateStatus.POTENTIAL_DUPLICATES)
                ]
            )
        if file_hash_pair[1] in before_relationships["file_relationships"]:
            self.assertFalse(
                file_hash_pair[0]
                in before_relationships["file_relationships"][file_hash_pair[1]][
                    str(hydrus_api.DuplicateStatus.POTENTIAL_DUPLICATES)
                ]
            )

        # Set two files as potential duplicates using the Hydrus API
        self.hvdclient.set_file_pair_as_potential_duplicates(file_hash_pair)

        # Check those two files are now potential duplicates
        # Check filehash is in potential duplicates for other file
        file_relationships_0 = self.hvdclient.client.get_file_relationships(hashes=[file_hash_pair[0]])
        self.assertEqual(
            file_hash_pair[1],
            file_relationships_0["file_relationships"][file_hash_pair[0]][
                str(hydrus_api.DuplicateStatus.POTENTIAL_DUPLICATES)
            ][0],
        )

        # Check other filehash is in potential duplicates for file
        file_relationships_1 = self.hvdclient.client.get_file_relationships(hashes=[file_hash_pair[1]])
        self.assertEqual(
            file_hash_pair[0],
            file_relationships_1["file_relationships"][file_hash_pair[1]][
                str(hydrus_api.DuplicateStatus.POTENTIAL_DUPLICATES)
            ][0],
        )


if __name__ == "__main__":
    unittest.main(module="test_dedupe")
