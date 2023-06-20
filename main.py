import hashlib
from collections import defaultdict
import pickle
import glob
from itertools import islice
import sys, enum, pprint

import mgzip
from videohash import VideoHash
import hydrus_api
import hydrus_api.utils
import tempfile
from termcolor import colored, cprint
from numpy import base_repr, binary_repr

from secret import *

"""
Note: I'm using a dictionary just to test stuff. It's really bad.
But, when I figure out how to use the sqlite Hydrus DB/API I will use that.
"""

VIDSDICTFILE = "saved_dictionary.pkl"
VIDSPATH = "../hydrus/vids/"

HYDRUS_HOST="http://localhost:45869"

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
def is_similar(a: VideoHash, b: VideoHash, hamming_distance_threshold: int = 4) -> bool:
    try:
        if VideoHash.hamming_distance(self=VideoHash, bitlist_a=a.bitlist, bitlist_b=b.bitlist) <= hamming_distance_threshold:
            return True
        else:
            return False
    except Exception as e:
        print(e)
        return False

"""
Overall process:
1. GET video file hashes
2. For each video, GET the file
   and then calculate the perceptual hash if it doesn't already exist.
3. Store perceptual hash for a file in a tag
   e.g. a_perceptual_hash: 0b01010100010100010100101010
        * I don't really like this. There is already
        the perceptual hash built into Hydrus for images.
4. POST perceptual hash tag to video
5. For each video, check if similar to every other video
   and if it is mark them as potential duplicates.
"""

client = hydrus_api.Client(api_url=HYDRUS_HOST, access_key=HYDRUS_API_KEY)
print(f"Client API version: v{client.VERSION} | Endpoint API version: v{client.get_api_version()['version']}")

client = hydrus_api.Client(HYDRUS_API_KEY)
if not hydrus_api.utils.verify_permissions(client, REQUIRED_PERMISSIONS):
    print("The API key does not grant all required permissions:", REQUIRED_PERMISSIONS)
    sys.exit(ExitCode.FAILURE)

# calculate the video perceptual hash of a file from the response of a hydrus_api client.get_file() request
def calc_video_perceptual_hash(video, hc: hydrus_api.Client) -> VideoHash:
    with tempfile.TemporaryDirectory() as tmp_dir_name:
        # TODO: Do I need to check for valid request here?
        video_ext = hc.get_file_metadata(hashes=[video_hash])["metadata"][0]["ext"]
        video_file_name = video_hash + video_ext
        with tempfile.SpooledTemporaryFile(mode="w+b", suffix=video_ext, prefix=video_hash, dir=tmp_dir_name) as tmp_vid_file:
            tmp_vid_file.write(video.content)
            tmp_vid_file.seek(0)
            vidurl = video.url+"&Hydrus-Client-API-Access-Key="+hc.access_key
            
            # TODO: Can I do higher frame_intervals for shorter videos? It should work...
            video_perceptual_hash = VideoHash(video_file=tmp_vid_file, frame_interval=2)
    return video_perceptual_hash

local_tag_services = client.get_services()["local_tags"]
# This won't work if your local tag service is not called my tags. Maybe I should make a new tag service?
local_tag_service = [service for service in local_tag_services if service["name"] == "my tags"]
# TODO: Add error message for user if local_tag_service doesn't exist
assert(len(local_tag_service) > 0)
local_tag_service = local_tag_service[0]

# GET video files with no perceptual hash tag and store their SHA256 hash to retrieve later
print(f"Retrieving video file hashes from {HYDRUS_HOST}")
all_video_hashes = client.search_files(["system:filetype=video", "-phashv1:*"], return_hashes=True)["hashes"]
i = 1
for video_hashes in hydrus_api.utils.yield_chunks(all_video_hashes[::-1], 100):
    for video_hash in video_hashes:
        video = client.get_file(hash_=video_hash)
        print(f"Calculating hash {i}/{len(all_video_hashes)}")
        i+=1
        try:
            video_percep_hash = calc_video_perceptual_hash(video, client)
        except Exception as err:
            print(err)
            video_percep_hash = None
            cprint("Failed to calculate perceptual hash.\n", "red", attrs=["bold"], file=sys.stderr)
            continue
        print(video_percep_hash)
        # Store the perceptual hash in base 36 because it's really long
        short_video_percep_hash = base_repr(int(video_percep_hash.hash, base=2), base=36)
        assert(f"0b{binary_repr(int(short_video_percep_hash, base=36), width=len(video_percep_hash.hash)-2)}" == video_percep_hash.hash)
        percep_hash_tag = f'phashv1:{short_video_percep_hash}'
        print(percep_hash_tag)

        print("Uploading perceptual hash tag to Hydrus.")
        d = {}
        d[local_tag_service["service_key"]] = [percep_hash_tag]
        client.add_tags(hashes=[video_hash], service_keys_to_tags=d)

print("All perceptual hash tags have been added to video files.")

exit()

vids = {}
try:
    with mgzip.open(VIDSDICTFILE, 'rb') as f:
        vids = pickle.load(f)
except FileNotFoundError as e:
    print("Dictionary does not exist. Creating Dictionary...")

# Add video to map where key:filehash, value: dict
# TODO: Only parse video files
for vid in glob.iglob(f'{VIDSPATH}/*.mp4'):
    print("Loaded:",vid)
    filehash = gen_sha_hash(vid)
    print(filehash)
    if filehash in vids: continue

    print("Adding new video to dictionary")
    print(filehash)
    vids[filehash] = defaultdict(dict)
    vh = vids[filehash]
    vh["path"] = vid

# Calculate perceptual hash and store in dict
for i, vidD in enumerate(vids):
    vid = vids[vidD]
    if("perceptual_hash" in vid.keys()): continue
    print("Calculating perceptual hash")
    try:
        vidHash = VideoHash(path=vid["path"], frame_interval=2)
        vid["perceptual_hash"] = vidHash
        print(vidHash)
    except Exception as e:
        print("Could not generate perceptual hash.")
        continue

# Write vids dict to file to speed up next processing
with mgzip.open(VIDSDICTFILE, 'wb') as f:
    pickle.dump(vids, f)

vidPHashDict = {}
for sha, vid in vids.items():
    try:
        vidPHashDict.setdefault(vid["perceptual_hash"].hash, []).append(sha)
    except Exception as err:
        print(err)


# Comparing using sliding window. This is not efficient AT ALL, but it does work!
print(f"\n \x1b[6;30;42m Comparing {len(vidPHashDict)} videos \033[0;0m")
for i, vida in enumerate(vids.items()):
    vidpa = vida[1]
    vHashA = vidpa["perceptual_hash"]
    for vidb in islice(vids.items(), i+1, None):
        vidpb = vidb[1]
        vHashB = vidpb["perceptual_hash"]
        if i+1 < len(vids):
            if is_similar(vHashA, vHashB):
                print(" \x1b[6;30;42m Similar:", is_similar(vHashB, vHashA), " \033[0;0m")
                print(f"Comparing {i+1}/{len(vidPHashDict)}", vidpa["path"], "and", vidpb["path"])
                print("H-Distance:", VideoHash.hamming_distance(VideoHash, bitlist_a=vHashA.bitlist, bitlist_b=vHashB.bitlist))
                d = {
                    "hash_a": str(vida[0]),
                    "hash_b": str(vidb[0]),
                    "relationship": 0,
                    "do_default_content_merge": True,
                }
                try:
                    client.set_file_relationships([d])
                except Exception as e:
                    print(e)
                print("")