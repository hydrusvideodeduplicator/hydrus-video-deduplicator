from itertools import islice
import sys, enum, pprint

from videohash import VideoHash
import hydrus_api
import hydrus_api.utils
import tempfile
from termcolor import colored, cprint
from numpy import base_repr, binary_repr, bitwise_xor
from io import IOBase
from tqdm import tqdm
import logging

from secret import *

"""
TODO: LIST

- CLI
- Rollback option to remove potential duplicates after they're added
- Option to remove all perceptual hash tags
- Option to add phash tag on specific tag service (default is my tags)
"""

"""
Overall process:
1. GET video file hashes for videos WITHOUT perceptual hashes
2. For each video from the query, GET the file 
   and calculate the perceptual hash.
3. Store perceptual hash for a file in a tag
   e.g. a_perceptual_hash: 0b01010100010100010100101010
        - I don't really like this. There is already
        the perceptual hash built into Hydrus for images.
4. POST perceptual hash tag to video
5. GET video file hashes for videos WITH perceptual hashes
5. For each video from the query, check if similar to every 
   other video and if it is mark them as potential duplicates.
"""

DEBUGGING = False

loglevel = logging.INFO

if DEBUGGING:
    loglevel = logging.DEBUG

hydlog = logging.getLogger("hydlog")

logging.basicConfig(format=' %(asctime)s - %(name)s: %(message)s',
                    datefmt='%H:%M:%S',
                    level=loglevel)

HYDRUS_HOST="http://localhost:45869"
# Service name of where to store perceptual hash tag for video files
LOCAL_TAG_SERVICE_NAME = "my tags"
PHASH_TAG = "phashv1"

NAME = "Basic Test"
REQUIRED_PERMISSIONS = (
    hydrus_api.Permission.IMPORT_URLS,
    hydrus_api.Permission.IMPORT_FILES,
    hydrus_api.Permission.ADD_TAGS,
    hydrus_api.Permission.SEARCH_FILES,
    hydrus_api.Permission.MANAGE_PAGES,
)

class ExitCode(enum.IntEnum):
    SUCCESS = 0
    FAILURE = 1

"""
The default value for VideoHash class method is_similar is
15% of bit length in VideoHash and it's not a parameter.
That is way too high!
"""
def is_similar(a: str, b: str, hamming_distance_threshold: int = 4) -> bool:
    _bitlist_a = list(map(int, a.replace("0b", "")))
    _bitlist_b = list(map(int, b.replace("0b", "")))
    h_distance = len(bitwise_xor(_bitlist_a,_bitlist_b,).nonzero()[0])
    if h_distance <= hamming_distance_threshold:
        return True
    else:
        return False

client = hydrus_api.Client(api_url=HYDRUS_HOST, access_key=HYDRUS_API_KEY)
print(f"Client API version: v{client.VERSION} | Endpoint API version: v{client.get_api_version()['version']}")

client = hydrus_api.Client(HYDRUS_API_KEY)
if not hydrus_api.utils.verify_permissions(client, REQUIRED_PERMISSIONS):
    print("The API key does not grant all required permissions:", REQUIRED_PERMISSIONS)
    sys.exit(ExitCode.FAILURE)

# Get the perceptual hash of a video.
def calc_perceptual_hash(video_file: IOBase):
    # TODO: Can I do higher frame_intervals for shorter videos? It should work...
    return VideoHash(video_file=video_file, frame_interval=2)

# Calculate the video perceptual hash of a file from the response of a hydrus_api client.get_file() request
def calc_perceptual_hash_hydrus(video: IOBase, video_hash: str, hc: hydrus_api.Client) -> VideoHash:
    with tempfile.TemporaryDirectory() as tmp_dir_name:
        # TODO: Do I need to check for valid request here?
        video_ext = hc.get_file_metadata(hashes=[video_hash])["metadata"][0]["ext"]
        video_file_name = video_hash + video_ext
        # spooled file stays in RAM unless it's too big
        with tempfile.SpooledTemporaryFile(mode="w+b", suffix=video_ext, prefix=video_hash, dir=tmp_dir_name) as tmp_vid_file:
            tmp_vid_file.write(video.content)
            tmp_vid_file.seek(0)
            video_perceptual_hash = calc_perceptual_hash(video_file=tmp_vid_file)
    return video_perceptual_hash

local_tag_services = client.get_services()["local_tags"]
local_tag_service = [service for service in local_tag_services if service["name"] == LOCAL_TAG_SERVICE_NAME]
# TODO: Add error message for user if local_tag_service doesn't exist
assert(len(local_tag_service) > 0)
local_tag_service = local_tag_service[0]

# Update perceptual hash for videos
# By default only adds missing phashes
def add_perceptual_hash_to_videos(add_missing: bool = True, overwrite: bool = False) -> None:
    search_tags = ["system:filetype=video"]
    
    # Add phash tag to files without one
    if add_missing and not overwrite:
        search_tags.append(f"-{PHASH_TAG}:*")
        print("Adding perceptual hash tags to files without one...")
    # Overwrite existing phash tag but don't add any to files without one
    elif not add_missing and overwrite:
        search_tags.append(f"+{PHASH_TAG}:*")
        print("Updating existing perceptual hash tags...")
    # Add phash tags to all files and overwrite existing ones
    elif add_missing and overwrite:
        print("Updating perceptual hash tags for ALL files...")
        pass
    else:
        print("Both add new and overwrite perceptual tags are false. Skipping add tags...")
        return None
    
    # GET video files SHA256 with no perceptual hash tag and store for later
    print(f"Retrieving video file hashes from {HYDRUS_HOST}")
    percep_tagged_video_hashes = client.search_files(search_tags, return_hashes=True)["hashes"]
    
    i = 1
    with tempfile.TemporaryDirectory() as tmp_dir_name:
        for video_hash in percep_tagged_video_hashes:
            print(f"Calculating hash {i}/{len(percep_tagged_video_hashes)}")
            i+=1
            try:
                video = client.get_file(hash_=video_hash)
                # TODO: Do I need to check for valid request here?
                # spooled file means it stays in RAM unless it's too big
                with tempfile.SpooledTemporaryFile(mode="w+b", dir=tmp_dir_name) as tmp_vid_file:
                    tmp_vid_file.write(video.content)
                    tmp_vid_file.seek(0)
                    video_percep_hash = calc_perceptual_hash(video_file=tmp_vid_file)
            except Exception as err:
                print(err)
                cprint(f"Failed to calculate perceptual hash {i-1}/{len(percep_tagged_video_hashes)}.\n", "red", attrs=["bold"], file=sys.stderr)
                continue
            print("Perceptual hash calculated!")
            # Store the perceptual hash in base 36 because it's really long
            short_video_percep_hash = base_repr(int(video_percep_hash.hash, base=2), base=36)
            assert(f"0b{binary_repr(int(short_video_percep_hash, base=36), width=len(video_percep_hash.hash)-2)}" == video_percep_hash.hash)
            percep_hash_tag = f'{PHASH_TAG}:{short_video_percep_hash}'
            print(percep_hash_tag)

            print("Uploading perceptual hash tag to Hydrus...")
            d = {}
            d[local_tag_service["service_key"]] = [percep_hash_tag]
            # TODO: Batch this call (?)
            client.add_tags(hashes=[video_hash], service_keys_to_tags=d)
    print("All perceptual hash tags have been added to video files.")

add_perceptual_hash_to_videos()

def decode_tag(coded_hash: str):
    decoded_hash = f"0b{binary_repr(int(coded_hash, base=36), width=64)}" 
    return decoded_hash

print("Checking for duplicates among all video files with perceptual tags...")

# Given a lexicographically SORTED list of tags, find the tag given a namespace
# TODO: Do binary search since the tags are sorted
def find_tag_in_tags(target_tag_namespace: str, tags: list) -> str:
    namespace_len = len(target_tag_namespace)
    for tag in tags:
        if tag[0:namespace_len] == target_tag_namespace:
            return tag[namespace_len:]

    return ""

# SHA256 hashes of videos with perceptual hash tag 
video_hashes = client.search_files(["system:filetype=video", f"{PHASH_TAG}:*"],
                                   return_file_ids=False, return_hashes=True)["hashes"]
# Store video_hash in list of tuples for the key phash
# tuple is (phash, sha256)
# Sliding window duplicate comparisons
# i = 0
# Iterate over list starting at i
    # if is_similar() then mark as duplicate (TODO: Defer this call to the API for speed)
    # i++

# Returns perceptual hashes from Hydrus converted back to hex
# Tuple is (sha256, phash) 
def get_stored_video_perceptual_hashes(video_hashes: list[str]) -> list[tuple[str, str]]:
    phashes = []
    i = 0
    for video_hash in video_hashes:
        #print(f"Checking {i+1}/{len(video_hashes)}")
        i+=1
        # TODO: batch this api call in batches of 256 (that's what Hydrus Client does)
        video_metadata = client.get_file_metadata(hashes=[video_hash], only_return_basic_information=False)
        # Why does video_tag_services contain tuples as the values?
        # The tuple is just the service as the key with value dict...
        video_tag_services = video_metadata["metadata"][0]["tags"]
        
        # tag_services are hex strings
        all_known_tags = "all known tags".encode("utf-8").hex()
        video_tags = video_tag_services[all_known_tags]["storage_tags"]["0"]
        
        # add : to PHASH_TAG in case it is not the complete namespace value for searching e.g. phasv15:
        phash = find_tag_in_tags(target_tag_namespace=f"{PHASH_TAG}:", tags=video_tags)
        # TODO: I think VideoHash works with hex which will probably be faster
        phashes.append(decode_tag(phash))

    return list(zip(video_hashes, phashes))

# Get the filename from the filename tag if it exists in Hydrus
# This is just used for debugging.
def get_file_names_hydrus(file_hashes: list[str]) -> list[str]:
    err_msg = "Cannot retrieve file name from Hydrus. Bad file name."
    result = []
    files_metadata = client.get_file_metadata(hashes=file_hashes, only_return_basic_information=False)
    all_known_tags = "all known tags".encode("utf-8").hex()
    for file_metadata in files_metadata["metadata"]:
        try:
            tag_services = file_metadata["tags"]
        except KeyError:
            logging.error(err_msg)
            tag = ""
        else:
            tags = tag_services[all_known_tags]["storage_tags"]["0"]
            tag = find_tag_in_tags(target_tag_namespace="filename:", tags=tags)
        result.append(tag)
    return result

video_hash_phash = get_stored_video_perceptual_hashes(video_hashes)

# TODO: Split this into two functions, one checks and one uploads
def find_potential_duplicates(video_hash_phash: list[tuple[str, str]]):
    for i, video in enumerate(video_hash_phash):
        video_hash, video_phash = video[0], video[1]
        for video2 in islice(video_hash_phash, i+1, None):
            video2_hash, video2_phash = video2[0], video2[1]
            if is_similar(video_phash, video2_phash):
                hydlog.info(f" \x1b[6;30;42m Similar files found. {i+1}/{len(video_hash_phash)+1} \033[0;0m")
                if DEBUGGING:
                    file_names = get_file_names_hydrus([video_hash, video2_hash])
                    hydlog.info(f"\"{file_names[0]}\" and \"{file_names[1]}\"")
                d = {
                    "hash_a": str(video_hash),
                    "hash_b": str(video2_hash),
                    "relationship": 0,
                    "do_default_content_merge": True,
                }
                # TODO: Defer this API call to speed up processing
                try:
                    client.set_file_relationships([d])
                except Exception as e:
                    pass

find_potential_duplicates(video_hash_phash)
print("All files have been processed.")