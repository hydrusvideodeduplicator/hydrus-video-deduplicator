from __future__ import annotations

import logging
from itertools import islice
from typing import TYPE_CHECKING

from hydrusvideodeduplicator.hydrus_api import Client

if TYPE_CHECKING:
    from collections.abc import Generator, Iterable
    from typing import Any


def batched(iterable: Iterable, batch_size: int) -> Generator[tuple, Any, None]:
    """
    Batch data into tuples of length batch_size. The last batch may be shorter."
    batched('ABCDEFG', 3) --> ABC DEF G
    """
    assert batch_size >= 1
    it = iter(iterable)
    while batch := tuple(islice(it, batch_size)):
        yield batch


# Given a lexicographically SORTED list of tags, find the tag given a namespace
# TODO: Do binary search since the tags are sorted
def find_tag_in_tags(target_tag_namespace: str, tags: list) -> str:
    namespace_len = len(target_tag_namespace)
    for tag in tags:
        if tag[0:namespace_len] == target_tag_namespace:
            return tag[namespace_len:]
    return ""


# Get the filename from the filename tag if it exists in Hydrus
# This is just used for debugging.
# Note: Hydrus tags and NTFS are case insensitive, but ext4 is not
#       But, this shouldn't really matter since Hydrus stores files with lower case names
# TODO: Clean this up it's a mess
def get_file_names_hydrus(client: Client, file_hashes: list[str]) -> list[str]:
    err_msg = "Cannot get file name from Hydrus."
    result = []
    files_metadata = client.get_file_metadata(hashes=file_hashes, only_return_basic_information=False)
    all_known_tags = "all known tags".encode("utf-8").hex()
    for file_metadata in files_metadata.get("metadata", []):
        # Try to get file extension
        try:
            ext = file_metadata["ext"]
        except KeyError:
            ext = ""

        # Try to get the file name
        tag = ""
        try:
            tag_services = file_metadata["tags"]
            tags = tag_services[all_known_tags]["storage_tags"]["0"]
            tag = find_tag_in_tags(target_tag_namespace="filename:", tags=tags)
            # Don't show extension if filename doesn't exist
            if tag != "":
                tag = f"{tag}{ext}"
        except Exception as exc:
            logging.error(exc)
            logging.error(f"{err_msg} Hash: {file_metadata['hash']}")

        result.append(tag)

    return result


# Get the oldest file by import time in list of file_metadata
def get_oldest_imported_file_time(all_files_metadata: list) -> int:
    file_import_times = []
    for video_metadata in all_files_metadata:
        try:
            file_import_times.append(get_file_import_time(video_metadata))
        except KeyError:
            continue
    return min(file_import_times)


# Get the import time of a file from file_metadata request from Hydrus
def get_file_import_time(file_metadata: dict):
    for service in file_metadata["file_services"]["current"].values():
        try:
            if service["name"] == "all local files":
                return service["time_imported"]
        except KeyError:
            continue
    raise KeyError
