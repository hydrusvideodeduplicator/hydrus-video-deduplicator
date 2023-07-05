from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import ffmpeg

# import vpdq  # VPDQ CPP IMPLEMENTATION
from PIL import Image

from pdqhashing.hasher.pdq_hasher import PDQHasher

if TYPE_CHECKING:
    from typing import Annotated

    from .typing_utils import ValueRange

    from pdqhashing.types.containers import HashAndQuality

from pdqhashing.types.hash256 import Hash256


@dataclass(slots=True)
class VpdqFeature:
    pdq_hash: Hash256  # 64 char hex string
    quality: float  # 0 to 100
    frame_number: int

    # ONLY FOR VPDQ CPP IMPLEMENTATION
    @classmethod
    def from_vpdq_feature(cls, feature: vpdq.VpdqFeature) -> VpdqFeature:
        return cls(feature.hex, feature.quality, int(feature.frameNumber))

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
    @staticmethod
    def get_vid_info(file: bytes) -> dict:
        # ffprobe command to get info. ffmpeg-python requires a file name, this does not.
        ffprobe_process = subprocess.Popen(
            ["ffprobe", "-show_streams", "-select_streams", "v:0", "-print_format", "json", "-", "-sexagesimal"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        # Retrieve the output and error streams from ffprobe
        stdout, stderr = ffprobe_process.communicate(input=file)

        # Decode the output stream as json
        output = json.loads(stdout.decode("utf-8"))
        error = stderr.decode("utf-8")
        if "streams" not in output:
            print(output)
            print(error)
            print(stderr)

        video_info = next(
            (stream for stream in output["streams"] if stream["codec_type"] == "video"),
            None,
        )

        if not video_info:
            raise ValueError("No video stream found in the input file.")
        return video_info

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

    # TODO: move this to util class
    @staticmethod
    def convert_seconds_to_seek_time(seconds):
        hours, remainder = divmod(seconds, 3600)
        minutes, seconds = divmod(remainder, 60)

        return f'{hours:02d}:{minutes:02d}:{seconds:02d}'

    # Perceptually hash video from a file path or the bytes
    @staticmethod
    def computeHash(
        video_file: Path | str | bytes,
        ffmpeg_path: str = "ffmpeg",
        seconds_per_hash: float = 1,
        verbose: bool = False,
        downsample_width: int = 0,
        downsample_height: int = 0,
    ) -> list[VpdqFeature]:
        video = Vpdq.get_video_bytes(video_file)
        if video is None:
            raise ValueError

        try:
            video_info = Vpdq.get_vid_info(video)
            width = int(video_info["width"])
            height = int(video_info["height"])
        except KeyError as exc:
            raise ValueError from exc

        pdq = PDQHasher()
        features: list[VpdqFeature] = []

        second = 0
        while True:
            out, _ = (
                ffmpeg.input(ss=f"{Vpdq.convert_seconds_to_seek_time(second)}", filename="pipe:")
                .output(
                    "pipe:",
                    loglevel="error",
                    format="rawvideo",
                    pix_fmt="rgb24",
                    avoid_negative_ts=1,
                    map="0:v",  # Get only first video stream
                    frames=1,  # Extract frame
                )
                .run(input=video, capture_stdout=True)
            )

            # Create image or if end of video is reached then return
            try:
                image = Image.frombytes("RGB", tuple([width, height]), out)
            except ValueError as exc:
                if str(exc) != "not enough image data":
                    raise ValueError from exc
                else:
                    break

            pdq_hash_and_quality = pdq.fromBufferedImage(image)
            pdq_frame = VpdqFeature(pdq_hash_and_quality.getHash(), pdq_hash_and_quality.getQuality(), second)
            features.append(pdq_frame)
            second += seconds_per_hash

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
