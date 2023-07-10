"""

These tests use clips from the Big Buck Bunny movie,
which is licensed under Creative Commons Attribution 3.0
(https://creativecommons.org/licenses/by/3.0/).
(c) copyright 2008, Blender Foundation / www.bigbuckbunny.org
Blender Foundation | www.blender.org

"""

import unittest
from pathlib import Path

from hydrusvideodeduplicator.vpdqpy.vpdqpy import Vpdq


class TestVpdq(unittest.TestCase):
    def setUp(self):
        all_vids_dir = Path(__file__).parent / "videos"
        vids_dirs = ["big_buck_bunny"]
        self.all_vids: list[Path] = []
        for vids_dir in vids_dirs:
            self.all_vids.extend(Path(all_vids_dir / vids_dir).glob("*"))

        # Strange vids are videos that should hash but not be compared.
        # They're used to test that the program doesn't crash
        # when it encounters a video with odd characteristics like extremely short length
        # or a video that has tiny dimensions, etc. The more of these added the better.
        # They shouldn't be compared because they might be similar to other videos, but not all of them in a group.
        strange_vids_dir = "strange"
        self.strange_vids: list[Path] = Path(all_vids_dir / strange_vids_dir).glob("*")

    # Return if two videos are supposed to be similar
    # This uses the prefix SXX where XX is an abitrary group number
    # If two videos have the same SXX they should be similar
    # If they don't they should NOT be similar
    def similar_group(self, vid1: Path, vid2: Path) -> bool:
        # If either video doesn't have a group, they're not similar
        if vid1.name.split("_")[0][0] != "S" or vid2.name.split("_")[0][0] != "S":
            return False

        vid1_group = vid1.name.split("_")[0]
        vid2_group = vid2.name.split("_")[0]
        return vid1_group == vid2_group

    # Hash videos
    def calc_hashes(self, vids: list[Path]) -> dict[Path, list[Vpdq]]:
        vids_hashes = {}
        for vid in vids:
            perceptual_hash = Vpdq.computeHash(vid)
            vids_hashes[vid] = perceptual_hash
            self.assertTrue(len(perceptual_hash) > 0)
            for feature in perceptual_hash:
                self.assertFalse(
                    str(feature.pdq_hash) == "0000000000000000000000000000000000000000000000000000000000000000"
                )

            perceptual_hash_json = Vpdq.vpdq_to_json(perceptual_hash)
            self.assertTrue(perceptual_hash_json != "[]")
        return vids_hashes

    # Hash all videos. They should all have hashes.
    def test_hashing(self):
        self.calc_hashes(self.all_vids)
        self.calc_hashes(self.strange_vids)

    # Compare similar videos. They should be similar if they're in the same similarity group.
    def test_compare_similarity_true(self):
        vids_hashes = self.calc_hashes(self.all_vids)
        for vid1 in vids_hashes.items():
            for vid2 in vids_hashes.items():
                if vid1[0] == vid2[0]:
                    continue

                similar, similarity = Vpdq.is_similar(vid1[1], vid2[1])
                self.assertTrue(0 <= similarity <= 100)

                with self.subTest(msg=f"Similar: {similar}", vid1=vid1[0].name, vid2=vid2[0].name):
                    if self.similar_group(vid1[0], vid2[0]):
                        self.assertTrue(similar)
                    else:
                        self.assertFalse(similar)


if __name__ == "__main__":
    unittest.main()
