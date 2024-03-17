from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Annotated

    from .typing_utils import ValueRange
    from .vpdqpy.vpdqpy import VpdqHash

from .vpdqpy.vpdqpy import Vpdq

"""TODO: Rework this with into a hashing interface that is used by hashers."""


def compute_phash(video: Path | str | bytes) -> VpdqHash:
    """
    Calculate the perceptual hash of a video.

    Returns the perceptual hash of the video.
    """
    phash = Vpdq.computeHash(video)
    return phash


def encode_phash_to_str(phash: VpdqHash) -> str:
    """
    Encode the perceptual hash of a video into a string.

    Returns the perceptual hash encoded as a string.
    """
    encoded_phash = Vpdq.vpdq_to_json(phash)
    return encoded_phash


def decode_phash_from_str(phash_str: str) -> VpdqHash:
    """
    Encode the perceptual hash of a video into a string.

    Returns the perceptual hash encoded as a string.
    """
    phash = Vpdq.json_to_vpdq(phash_str)
    return phash


def get_phash_similarity(
    hash_a: VpdqHash,
    hash_b: VpdqHash,
) -> Annotated[float, ValueRange(0.0, 100.0)]:
    """
    Check if video is similar by comparing their list of features
    Threshold is minimum similarity to be considered similar
    """
    similarity = Vpdq.match_hash(query_features=hash_a, target_features=hash_b)
    assert similarity >= 0.0 and similarity <= 100.0
    return similarity
