from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import ffmpeg
from PIL import Image

from pdqhashing.hasher.pdq_hasher import PDQHasher

if TYPE_CHECKING:
    from typing import Annotated

    from pdqhashing.types.containers import HashAndQuality
    from pdqhashing.types.hash256 import Hash256
    from typing_utils import ValueRange


@dataclass(slots=True)
class VpdqFeature:
    pdq_hash: Hash256  # 64 char hex string
    quality: float  # 0 to 100
    frame_number: int


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
        # error = stderr.decode("utf-8")

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
            except OSError:
                raise ValueError("Failed to get video file bytes. Invalid object type.")
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

    # quality tolerance from [0,100]
    @staticmethod
    def filter_features(vpdq_features: list[VpdqFeature], quality_tolerance: float) -> list[VpdqFeature]:
        return [feature for feature in vpdq_features if feature.quality >= quality_tolerance]

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

    # Perceptually hash video from a file path or the bytes
    @staticmethod
    def computeHash(video_file: Path | str | bytes, seconds_per_hash: float = 1) -> list[VpdqFeature]:
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
                ffmpeg.input("pipe:")
                .output(
                    "pipe:",
                    loglevel="error",
                    format="rawvideo",
                    pix_fmt="rgb24",
                    avoid_negative_ts=1,
                    ss=f"{second}",  # Seek second
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
