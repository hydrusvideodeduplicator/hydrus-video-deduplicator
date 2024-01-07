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

from hydrusvideodeduplicator.vpdqpy.vpdqpy import Vpdq

if TYPE_CHECKING:
    pass


@pytest.mark.benchmark(group="hashing", min_time=0.1, max_time=0.5, min_rounds=1, disable_gc=False, warmup=False)
def test_vpdq_hashing(benchmark):
    """Benchmark VPDQ hashing"""
    """Currently around 7.5 seconds on my PC"""
    all_vids_dir = Path(__file__).parent / "videos"

    vids_dirs = ["sintel"]
    similarity_vids: list[Path] = []
    for vids_dir in vids_dirs:
        similarity_vids.extend(Path(all_vids_dir / vids_dir).glob("*"))
    vids_hashes = {}

    @benchmark
    def run():
        for vid in similarity_vids:
            perceptual_hash = Vpdq.computeHash(vid)
            vids_hashes[vid] = perceptual_hash
            assert len(perceptual_hash) > 0


if __name__ == "__main__":
    test_vpdq_hashing()
