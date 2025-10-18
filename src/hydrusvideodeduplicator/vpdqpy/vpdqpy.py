from __future__ import annotations

import io
import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING

import av
from hvdaccelerators import vpdq
from PIL import Image

if TYPE_CHECKING:
    from collections.abc import Iterator
    from fractions import Fraction
    from typing import Annotated, TypeAlias

    from .typing_utils import ValueRange

log = logging.getLogger(__name__)
log.setLevel(logging.CRITICAL)

# The dimensions of the image after downscaling for pdq
DOWNSCALE_DIMENSIONS = 512

VpdqHash: TypeAlias = list[vpdq.vpdqFeature]


class Vpdq:
    @staticmethod
    def get_video_bytes(video_file: Path | str | bytes) -> bytes:
        """Get the bytes of a video"""
        video = bytes()
        if isinstance(video_file, (Path, str)):
            if not Path(video_file).is_file():
                raise ValueError("Failed to get video file bytes. Video does not exist")

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

    @staticmethod
    def match_hash(
        query_features: VpdqHash,
        target_features: VpdqHash,
        quality_tolerance: float = 50.0,
        distance_tolerance: float = 31.0,
    ):
        """Get the similarity of two videos by comparing their list of features"""
        return vpdq.matchHash(query_features, target_features, int(distance_tolerance), int(quality_tolerance))

    @staticmethod
    def frame_extract_pyav(video_bytes: bytes) -> Iterator[Image.Image]:
        """Extract frames from video"""
        with av.open(io.BytesIO(video_bytes), metadata_encoding="utf-8", metadata_errors="ignore") as container:
            # Check for video in video container
            video_streams = container.streams.video
            if video_streams is None or len(video_streams) < 1:
                log.error("Video stream not found.")
                raise ValueError("Video stream not found.")

            video = container.streams.video[0]
            video.thread_type = "AUTO"

            raw_average_fps: Fraction = video.average_rate
            average_fps: int = 1
            # Some videos, like small GIFs, will have a NoneType FPS
            if raw_average_fps is None or raw_average_fps < 1:
                log.warning("Average FPS is None or less than 1. Every frame will be hashed.")
            else:
                average_fps = round(raw_average_fps)

            # The following is a very overly complex "for loop" in order to decode frames from the video.
            # Why not use a for loop? Because for some reason av>=12 will sometimes throw an
            # av.error.InvalidDataError error in container.decode(). I'm not sure why this happens.
            # Normally this loop would be written as `for frame_index, frame in container.decode(video)`,
            # but to catch the exception I need to wrap the decode call with try/catch and iterate using next().
            frame_generator = container.decode(video)
            frame_index = 0
            while True:
                try:
                    frame = next(frame_generator)
                    if frame_index % average_fps == 0:
                        yield frame.reformat(
                            width=DOWNSCALE_DIMENSIONS,
                            height=DOWNSCALE_DIMENSIONS,
                            format="rgb24",
                            interpolation=av.video.reformatter.Interpolation.POINT,
                        )
                    frame_index += 1
                except StopIteration:
                    break
                except av.error.InvalidDataError as exc:
                    log.error(f"Skipping bad frame at index {frame_index}: {exc}")
                    frame_index += 1

    @staticmethod
    def computeHash(video_file: Path | str | bytes, num_threads: int = 0) -> VpdqHash:
        """Perceptually hash video from a file path or the bytes"""
        video = Vpdq.get_video_bytes(video_file)
        if video is None:
            raise ValueError

        features: VpdqHash = []

        # Average FPS is used by vpdq to calculate the timestamp, but we completely discard
        # the timestamp so this value doesn't matter.
        average_fps = 1
        hasher = vpdq.VideoHasher(average_fps, DOWNSCALE_DIMENSIONS, DOWNSCALE_DIMENSIONS, num_threads)
        for frame in Vpdq.frame_extract_pyav(video):
            # Note: hash_frame will block if vpdq's internal frame queue is full. This is necessary,
            # otherwise if hashing gets too far behind decoding there will be an insane amount of memory
            # used to hold the raw frames.
            hasher.hash_frame(bytes(frame.planes[0]))
        features = hasher.finish()
        return features

    @staticmethod
    def is_similar(
        vpdq_features1: VpdqHash,
        vpdq_features2: VpdqHash,
        threshold: Annotated[float, ValueRange(0.0, 100.0)] = 75.0,
    ) -> tuple[bool, float]:
        """Check if video is similar by comparing their list of features
        Threshold is minimum similarity to be considered similar
        """
        similarity = Vpdq.match_hash(query_features=vpdq_features1, target_features=vpdq_features2)
        return similarity >= threshold, similarity

    @staticmethod
    def vpdq_to_json(vpdq_features: VpdqHash, *, indent: int | None = None) -> str:
        """Convert from VPDQ features to json object and return the json object as a str"""
        return json.dumps([str(f) for f in vpdq_features], indent=indent)

    @staticmethod
    def json_to_vpdq(json_str: str) -> VpdqHash:
        """Load a str as a json object and convert from json object to VPDQ features"""
        return [vpdq.vpdqFeature.from_str(s) for s in json.loads(json_str or "[]")]
