import unittest
from pathlib import Path

from hydrusvideodeduplicator.vpdqpy.vpdqpy import Vpdq


class TestHashing(unittest.TestCase):
    def setUp(self):
        all_vids_dir = Path(__file__).parent / "videos"
        vids_dirs = ["big_buck_bunny"]
        self.all_vids = []
        for vids_dir in vids_dirs:
            self.all_vids.extend(Path(all_vids_dir / vids_dir).glob("*"))

    def test_hashing(self):
        print("Hashing:")
        for vid in self.all_vids:
            print(vid.name)

            perceptual_hash = Vpdq.computeHash(vid)
            self.assertTrue(len(perceptual_hash) > 0)

            perceptual_hash_json = Vpdq.vpdq_to_json(perceptual_hash)
            self.assertTrue(perceptual_hash_json != "[]")


if __name__ == "__main__":
    unittest.main()
