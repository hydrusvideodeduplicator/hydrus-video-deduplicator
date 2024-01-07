from __future__ import annotations

import logging
import time
import unittest
from typing import TYPE_CHECKING

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

        initial_dedupe_count = self.hvdclient.get_potential_duplicate_count_hydrus()
        self.assertEqual(
            initial_dedupe_count,
            0,
            f"Initial potential duplicates must be 0."
            f"Potential duplicates: {initial_dedupe_count}. Reset the database before running tests.",
        )

    def test_temp(self):
        self.assertTrue(True)


if __name__ == "__main__":
    unittest.main(module="test_dedupe")
