import hashlib
from collections import defaultdict
import pickle
import glob
from itertools import islice

import mgzip
from videohash import VideoHash
"""
Note: I'm using a dictionary just to test stuff. It's really bad.
But, when I figure out how to use the sqlite Hydrus DB/API I will use that.
"""

VIDSDICTFILE = "saved_dictionary.pkl"
VIDSPATH = "../hydrus/vids/"

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
for vid in glob.iglob(f'{VIDSPATH}/*'):
    print("Loaded:",vid)
    filehash = gen_sha_hash(vid)
    if filehash in vids: continue

    print("Adding new video to dictionary")
    print(filehash)
    vids[filehash] = defaultdict(dict)
    vh = vids[filehash]
    vh["path"] = vid
    print(type(vh))

# Calculate perceptual hash and store in dict
for i, vidD in enumerate(vids):
    vid = vids[vidD]
    if("perceptual_hash" in vid.keys()): continue
    print("Calculating perceptual hash")
    vidHash = VideoHash(path=vid["path"], frame_interval=2)
    vid["perceptual_hash"] = vidHash
    print(vidHash)

# Write vids dict to file to speed up next processing
with mgzip.open(VIDSDICTFILE, 'wb') as f:
    pickle.dump(vids, f)

vidPHashDict = {}
[vidPHashDict.setdefault(vid["perceptual_hash"].hash, []).append(sha) for sha, vid in vids.items()]

"""
The default value for VideoHash class method is_similar is
15% of bit length in VideoHash and it's not a parameter.
That is way too high!
"""
def is_similar(a: VideoHash, b: VideoHash, hamming_distance_threshold: int = 4) -> bool:
    if VideoHash.hamming_distance(self=VideoHash, bitlist_a=a.bitlist, bitlist_b=b.bitlist) <= hamming_distance_threshold:
        return True
    else:
        return False

# Comparing using sliding window. This is not efficient or effective.
print(f"\n \x1b[6;30;42m Comparing {len(vidPHashDict)} videos \033[0;0m")
for i, vid in enumerate(vids.values()):
    vHash = vid.get("perceptual_hash")
    for vid2 in islice(vids.values(), i+1, None):
        vHash2 = vid2.get("perceptual_hash")
        if i+1 < len(vids):
            #if not is_similar(vHash, vHash2): break
            print(f"Comparing {i+1}/{len(vidPHashDict)}", vid["path"], "and", vid2["path"])
            print("Similar:", is_similar(vHash, vHash2))
            print("H-Distance:", VideoHash.hamming_distance(VideoHash, bitlist_a=vHash.bitlist, bitlist_b=vHash2.bitlist))
            print("")