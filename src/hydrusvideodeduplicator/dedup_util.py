import logging

from hydrus_api import Client
 
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
# TODO: Clean this up it's a mess
def get_file_names_hydrus(client: Client, file_hashes: list[str]) -> list[str]:
    err_msg = "Cannot get file name from Hydrus."
    result = []
    files_metadata = client.get_file_metadata(hashes=file_hashes, only_return_basic_information=False)
    all_known_tags = "all known tags".encode("utf-8").hex()
    for file_metadata in files_metadata["metadata"]:
        # Try to get file extension
        try:
            ext = file_metadata["ext"]
        except KeyError:
            ext = ""
            
        # Try to get the file name
        try:
            tag_services = file_metadata["tags"]
        except KeyError:
            logging.warning(f"{err_msg} Hash: {file_metadata['hash']}")
        else:
            tags = tag_services[all_known_tags]["storage_tags"]["0"]
            tag = find_tag_in_tags(target_tag_namespace="filename:", tags=tags)
            # Don't show extension if filename doesn't exist
            if tag != "":
                tag = f"{tag}{ext}"
            else:
                logging.warning(f"{err_msg} Hash: {file_metadata['hash']}")

        result.append(tag)

    return result