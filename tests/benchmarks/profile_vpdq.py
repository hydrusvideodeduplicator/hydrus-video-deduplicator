"""

These tests use clips from the Big Buck Bunny movie and Sintel movie,
which are licensed under Creative Commons Attribution 3.0
(https://creativecommons.org/licenses/by/3.0/).
(c) copyright 2008, Blender Foundation / www.bigbuckbunny.org
(c) copyright Blender Foundation | durian.blender.org
Blender Foundation | www.blender.org

"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING

from hydrusvideodeduplicator.vpdqpy.vpdqpy import Vpdq, VpdqHash

if TYPE_CHECKING:
    pass


def check_testdb_exists():
    """
    Check if the testdb submodule is pulled.
    Throws RuntimeError if it's not updated.
    """
    testdb_dir = Path(__file__).parents[1] / "testdb"
    if len(os.listdir(testdb_dir)) == 0:
        raise RuntimeError("Video hashes dir is missing. Is the testdb submodule pulled?")


def profile_vpdq_similarity():
    """Profile VPDQ similarity"""

    all_phashes_dir = Path(__file__).parents[1] / "testdb" / "video hashes"
    video_hashes_paths: list[Path] = []
    video_hashes_paths.extend(Path(all_phashes_dir).glob("*"))
    video_phashes: list[VpdqHash] = list()
    for video_hash_file in video_hashes_paths:
        with open(video_hash_file) as file:
            video_hash = Vpdq.json_to_vpdq(file.readline())
            video_phashes.append(video_hash)
    assert len(video_phashes) > 0

    pairs = []
    for i, phash1 in enumerate(video_phashes):
        for j, phash2 in enumerate(video_phashes):
            if j < i:
                continue
            pairs.append((phash1, phash2))

    profiler = cProfile.Profile()
    with profiler:
        for pair in pairs:
            Vpdq.is_similar(pair[0], pair[1], threshold=75)
    stats = pstats.Stats(profiler).sort_stats("cumtime")
    stats.print_stats()


def profile_vpdq_hashing():
    """Benchmark VPDQ hashing"""
    """Currently around 7.5 seconds on my PC"""
    all_vids_dir = Path(__file__).parents[1] / "testdb" / "videos"

    vids_dirs = ["sintel"]
    similarity_vids: list[Path] = []
    for vids_dir in vids_dirs:
        similarity_vids.extend(Path(all_vids_dir / vids_dir).glob("*"))
    assert len(similarity_vids) > 0

    profiler = cProfile.Profile()
    with profiler:
        for vid in similarity_vids:
            perceptual_hash = Vpdq.computeHash(vid)
            assert len(perceptual_hash) > 0
    stats = pstats.Stats(profiler).sort_stats("cumtime")
    stats.print_stats()


# To use this, run "python profile_vpdq.py"
if __name__ == "__main__":
    import cProfile
    import pstats

    check_testdb_exists()

    profile_vpdq_similarity()
    profile_vpdq_hashing()
