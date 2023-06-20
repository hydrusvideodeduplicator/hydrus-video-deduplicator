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
from tqdm import tqdm

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


# GET video files and store their SHA256 hash to retrieve later
all_video_hashes = client.search_files(["system:filetype=video"], return_hashes=True)["hashes"]
for video_hashes in hydrus_api.utils.yield_chunks(all_video_hashes, 100):
    for video_hash in video_hashes:
        # TODO: Do I need to check for valid request here?
        video = client.get_file(hash_=video_hash)
        vidurl = video.url+"&Hydrus-Client-API-Access-Key="+client.access_key
        print(VideoHash(url=vidurl, frame_interval=2))
    #pprint.pprint(client.get_file_metadata(hashes=video_hashes, only_return_basic_information=True))

exit()

def gen_sha_hash(filename: str) -> str:
    sha256_hash = hashlib.sha256()
    with open(filename, "rb") as f:
        # Read and update hash string value in blocks of 4K
        for byte_block in iter(lambda: f.read(4096),b""):
            sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

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
for i, vidD in enumerate(tqdm(vids)):
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