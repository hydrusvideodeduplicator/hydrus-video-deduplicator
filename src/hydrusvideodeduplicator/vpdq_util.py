# Copyright (c) Meta Platforms, Inc. and affiliates.

import json
from dataclasses import dataclass

import vpdq

from .pdq_utils import PDQ_HEX_STR_LEN

QUALITY = "quality"
HASH = "hash"
TIMESTAMP = "timestamp"
VPDQ_TIMESTAMP_PRECISION = 3
VPDQ_QUALITY_THRESHOLD = 50
VPDQ_DISTANCE_THRESHOLD = 31
VPDQ_QUERY_MATCH_THRESHOLD_PERCENT = 75.0
VPDQ_INDEX_MATCH_THRESHOLD_PERCENT = 0.0


@dataclass
class VPDQMatchResult:
    """Data class for VPDQ match result"""

    query_match_percent: float = 0.0
    compared_match_percent: float = 0.0


@dataclass
class VpdqCompactFeature:
    """A VPDQ Feature with a subset of fields needed for matching"""

    pdq_hex: str
    quality: int
    timestamp: float

    def assert_valid(self) -> "VpdqCompactFeature":
        """Checks the bounds of all the elements, throws ValueError if invalid"""
        if len(self.pdq_hex) != PDQ_HEX_STR_LEN:
            raise ValueError("malformed pdq hash")
        int(self.pdq_hex, 16)  # For ValueError
        if not (0 <= self.quality <= 100):
            raise ValueError("invalid VPDQ quality")
        if self.timestamp < 0:
            raise ValueError("invalid timestamp")
        return self

    @classmethod
    def from_vpdq_feature(cls, feature: vpdq.VpdqFeature) -> "VpdqCompactFeature":
        return cls(feature.hex, feature.quality, feature.timestamp)

    @classmethod
    def from_str(cls, serialized: str) -> "VpdqCompactFeature":
        """Convert from a string back to the class - the inverse of __str__"""
        parts = serialized.split(",")
        try:
            pdq_hex, qual_str, time_str = parts  # Wrong count = ValueError
            return cls(pdq_hex, int(qual_str), float(time_str)).assert_valid()
        except ValueError:
            raise ValueError(f"invalid {cls.__name__} serialization: {serialized}")

    def __str__(self) -> str:
        return f"{self.pdq_hex},{self.quality},{self.timestamp:.{VPDQ_TIMESTAMP_PRECISION}}"


def hash_file_compact(filepath: str, seconds_per_hash: float = 1.0) -> list[VpdqCompactFeature]:
    """Wrapper around computeHash to instead return compact features"""
    vpdq_hashes = vpdq.computeHash(str(filepath), seconds_per_hash=seconds_per_hash)
    return [VpdqCompactFeature.from_vpdq_feature(f) for f in vpdq_hashes]


def vpdq_to_json(vpdq_features: list[VpdqCompactFeature], *, indent: int | None = None) -> str:
    """Convert from VPDQ features to json object and return the json object as a str"""
    return json.dumps([str(f.assert_valid()) for f in vpdq_features], indent=indent)


def json_to_vpdq(json_str: str) -> list[VpdqCompactFeature]:
    """Load a str as a json object and convert from json object to VPDQ features"""
    return [VpdqCompactFeature.from_str(s) for s in json.loads(json_str or "[]")]


def dedupe(features: list[VpdqCompactFeature]) -> list[VpdqCompactFeature]:
    """Filter out the VPDQ feature with exact same hash in a list of VPDQ features

    Args:
        features

    Returns:
        List of VPDQ Features with unique features
    """
    unique_features = set()
    ret = []
    for h in features:
        if h.pdq_hex not in unique_features:
            ret.append(h)
            unique_features.add(h.pdq_hex)
    return ret


def quality_filter(features: list[VpdqCompactFeature], quality_tolerance: int) -> list[VpdqCompactFeature]:
    """Filter VPDQ feature that has a quality lower than quality_tolerance

    Args:
        features
        quality_tolerance : If frames is this quality level then it will be ignored

    Returns:
        List of VPDQFeatures with quality higher than quality_tolerance
    """
    return [f for f in features if f.quality >= quality_tolerance]


def prepare_vpdq_feature(signal_str: str, quality_tolerance: int) -> list[VpdqCompactFeature]:
    """Convert signal_str to deduped and quality-filtered vdqp features

    Args:
    quality_tolerance : The quality tolerance of VPDQ Feature.
    If VPDQ Feature is below this quality level then it will not be added
    """
    features = json_to_vpdq(signal_str)
    return dedupe(quality_filter(features, quality_tolerance))
