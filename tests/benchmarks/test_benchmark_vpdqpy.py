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

import pytest

from hydrusvideodeduplicator.vpdqpy.vpdqpy import Vpdq, VpdqHash
from hvdaccelerators import vpdq

from ..check_testdb import check_testdb_exists

if TYPE_CHECKING:
    pass


@pytest.mark.benchmark(group="hashing", min_time=0.1, max_time=0.5, min_rounds=1, disable_gc=False, warmup=False)
def test_vpdq_hashing(benchmark):
    """Benchmark VPDQ hashing"""
    """Currently around 7.5 seconds on my PC"""
    all_vids_dir = Path(__file__).parents[1] / "testdb" / "videos"

    vids_dirs = ["sintel"]
    similarity_vids: list[Path] = []
    for vids_dir in vids_dirs:
        similarity_vids.extend(Path(all_vids_dir / vids_dir).glob("*"))
    vids_hashes = {}
    assert len(similarity_vids) > 0

    @benchmark
    def run():
        for vid in similarity_vids:
            perceptual_hash = Vpdq.computeHash(vid)
            vids_hashes[vid] = perceptual_hash
            assert len(perceptual_hash) > 0


@pytest.mark.benchmark(group="similarity", min_time=0.1, max_time=0.5, min_rounds=1, disable_gc=False, warmup=False)
def test_vpdq_similarity(benchmark):
    """Benchmark VPDQ similarity"""
    all_phashes_dir = Path(__file__).parents[1] / "testdb" / "video hashes"

    video_hashes_paths: list[Path] = []
    video_hashes_paths.extend(Path(all_phashes_dir).glob("*"))
    video_phashes: list[VpdqHash] = list()
    for video_hash_file in video_hashes_paths:
        with open(video_hash_file) as file:
            video_hash = vpdq.VpdqHash.from_string(file.readline())
            video_phashes.append(video_hash)

    pairs = []
    for i, phash1 in enumerate(video_phashes):
        for j, phash2 in enumerate(video_phashes):
            if j < i:
                continue
            pairs.append((phash1, phash2))
    assert len(pairs) > 0

    @benchmark
    def run():
        for pair in pairs:
            _ = Vpdq.is_similar(pair[0], pair[1], threshold=75)


if __name__ == "__main__":
    check_testdb_exists()
    test_vpdq_hashing()
    test_vpdq_similarity()
