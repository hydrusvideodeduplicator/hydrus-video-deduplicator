from __future__ import annotations

import io
import json
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import av
from PIL import Image

from ..pdqhashing.hasher.pdq_hasher import PDQHasher

if TYPE_CHECKING:
    from typing import Annotated, Generator

    from .typing_utils import ValueRange

    from ..pdqhashing.types.containers import HashAndQuality

from ..pdqhashing.types.hash256 import Hash256


@dataclass(slots=True)
class VpdqFeature:
    pdq_hash: Hash256  # 64 char hex string
    quality: float  # 0 to 100
    frame_number: int

    def assert_valid(self) -> VpdqFeature:
        """Checks the bounds of all the elements, throws ValueError if invalid"""
        if len(str(self.pdq_hash)) < Hash256.HASH256_HEX_NUM_NYBBLES:
            raise ValueError("invalid PDQ hash")
        if not (0 <= self.quality <= 100):
            raise ValueError("invalid VPDQ quality")
        if self.frame_number < 0:
            raise ValueError("invalid frame number")
        return self

    @staticmethod
    def from_str(serialized: str) -> VpdqFeature:
        """Convert from a string back to the class - the inverse of __str__"""
        parts = serialized.split(",")
        try:
            pdq_hex, qual_str, time_str = parts  # Wrong count = ValueError
            return VpdqFeature(Hash256.fromHexString(pdq_hex), float(qual_str), float(time_str)).assert_valid()
        except ValueError:
            raise ValueError(f"invalid {Vpdq.__name__} serialization: {serialized}")

    def __str__(self) -> str:
        return f"{self.pdq_hash},{self.quality},{self.frame_number}"


class Vpdq:
    # Get the bytes of a video
    @staticmethod
    def get_video_bytes(video_file: Path | str | bytes) -> bytes:
        video: bytes = None
        if isinstance(video_file, (Path, str)):
            try:
                with open(str(video_file), "rb") as file:
                    video = file.read()
            except OSError as exc:
                raise ValueError("Failed to get video file bytes. Invalid object type.") from exc
        elif isinstance(video_file, bytes):
            video = video_file
        else:
            raise ValueError("Failed to get video file bytes. Invalid object type.")

        return video

    # Filter out the VPDQ feature with exact same hash
    @staticmethod
    def dedupe_features(features: list[VpdqFeature]) -> list[VpdqFeature]:
        unique_features = set()
        ret = []
        for feature in features:
            if str(feature.pdq_hash) not in unique_features:
                ret.append(feature)
                unique_features.add(str(feature.pdq_hash))
        return ret

    # Remove features that are below a certain quality threshold
    @staticmethod
    def filter_features(
        vpdq_features: list[VpdqFeature], threshold: Annotated[float, ValueRange(0.0, 100.0)]
    ) -> list[VpdqFeature]:
        return [feature for feature in vpdq_features if feature.quality >= threshold]

    # Get number of matching features for query and target
    @staticmethod
    def feature_match_count(
        query_features: list[VpdqFeature],
        target_features: list[VpdqFeature],
        distance_tolerance: float,
    ) -> int:
        return sum(
            any(
                query_feature.pdq_hash.hammingDistance(target_feature.pdq_hash) <= distance_tolerance
                for target_feature in target_features
            )
            for query_feature in query_features
        )

    @staticmethod
    def match_hash(
        query_features: list[VpdqFeature],
        target_features: list[VpdqFeature],
        quality_tolerance: float = 50.0,
        distance_tolerance: float = 31.0,
    ):
        query_filtered = Vpdq.filter_features(query_features, quality_tolerance)
        target_filtered = Vpdq.filter_features(target_features, quality_tolerance)

        # Avoid divide by zero
        if len(query_filtered) <= 0 or len(target_filtered) <= 0:
            return 0.0

        result = Vpdq.feature_match_count(query_filtered, target_filtered, distance_tolerance)
        return result * 100 / len(query_filtered)

    @staticmethod
    def frame_extract_pyav(video: bytes) -> Generator[Image.Image]:
        with av.open(io.BytesIO(video), metadata_encoding='utf-8', metadata_errors='ignore') as container:
            video = container.streams.video[0]
            video.thread_type = "AUTO"

            average_fps: int = round(video.average_rate)

            for index, frame in enumerate(container.decode(video)):
                if index % average_fps == 0:
                    yield frame

    # Perceptually hash video from a file path or the bytes
    @staticmethod
    def computeHash(
        video_file: Path | str | bytes,
    ) -> list[VpdqFeature]:
        video = Vpdq.get_video_bytes(video_file)
        if video is None:
            raise ValueError

        pdq = PDQHasher()
        features: list[VpdqFeature] = []

        for second, frame in enumerate(Vpdq.frame_extract_pyav(video)):
            pdq_hash_and_quality = pdq.fromBufferedImage(frame.to_image())
            pdq_frame = VpdqFeature(pdq_hash_and_quality.getHash(), pdq_hash_and_quality.getQuality(), second)
            features.append(pdq_frame)

        deduped_features = Vpdq.dedupe_features(features)
        return deduped_features

    # Check if video is similar by comparing their list of VpdqFeature's
    # Threshold is minimum similarity to be considered similar
    @staticmethod
    def is_similar(
        vpdq_features1: list[VpdqFeature],
        vpdq_features2: list[VpdqFeature],
        threshold: Annotated[float, ValueRange(0.0, 100.0)] = 75.0,
    ) -> tuple[bool, float]:
        similarity = Vpdq.match_hash(query_features=vpdq_features1, target_features=vpdq_features2)
        return similarity >= threshold, similarity

    @staticmethod
    def vpdq_to_json(vpdq_features: list[VpdqFeature], *, indent: int | None = None) -> str:
        """Convert from VPDQ features to json object and return the json object as a str"""
        return json.dumps([str(f.assert_valid()) for f in vpdq_features], indent=indent)

    @staticmethod
    def json_to_vpdq(json_str: str) -> list[VpdqFeature]:
        """Load a str as a json object and convert from json object to VPDQ features"""
        return [VpdqFeature.from_str(s) for s in json.loads(json_str or "[]")]
