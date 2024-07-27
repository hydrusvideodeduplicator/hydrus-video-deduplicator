"""

These tests use clips from the Big Buck Bunny movie and Sintel movie,
which are licensed under Creative Commons Attribution 3.0
(https://creativecommons.org/licenses/by/3.0/).
(c) copyright 2008, Blender Foundation / www.bigbuckbunny.org
(c) copyright Blender Foundation | durian.blender.org
Blender Foundation | www.blender.org

"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from hydrusvideodeduplicator.vpdqpy.vpdqpy import Vpdq, VpdqHash
from .check_testdb import check_testdb_exists

if TYPE_CHECKING:
    pass

check_testdb_exists()

all_phashes_dir = Path(__file__).parent / "testdb" / "video hashes"

video_hashes_paths: list[Path] = []
video_hashes_paths.extend(Path(all_phashes_dir).glob("*"))
video_phashes: list[VpdqHash] = list()
for video_hash_file in video_hashes_paths:
    with open(video_hash_file) as file:
        video_hash = Vpdq.json_to_vpdq(file.readline())
        video_phashes.append(video_hash)

pairs = []
for i, phash1 in enumerate(video_phashes):
    for j, phash2 in enumerate(video_phashes):
        if j < i:
            continue
        pairs.append((phash1, phash2))


def profile_vpdq_similarity():
    """Profile VPDQ similarity"""
    for pair in pairs:
        Vpdq.is_similar(pair[0], pair[1], threshold=75)


# To use this, run "python -m cProfile tests/profile_vpdq.py"
if __name__ == "__main__":
    import cProfile
    import pstats

    profiler = cProfile.Profile()
    profiler.enable()
    profile_vpdq_similarity()
    profiler.disable()
    stats = pstats.Stats(profiler).sort_stats("cumtime")
    stats.print_stats()
