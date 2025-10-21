import json


# Convert <0.9.0 full vpdq hash (json) to 0.10.0 hash
def convert_old_vpdq_to_new(old_vpdq_phash_json: str) -> bytes:
    # The old vpdq converted the hashes in reverse order. This has no effect on the similarity
    # or anything, but it complicates code in the vpdq implementation, so I changed the order
    # to the native byte order of the PDQ hash.
    def reverse_byte_order(s: str):
        bytes_obj = bytes.fromhex(s)
        reversed_bytes = bytes_obj[::-1]
        return reversed_bytes.hex()

    j = json.loads(old_vpdq_phash_json)
    new_vpdq_hash_str = ""
    for vpdq_feature in j:
        phash, feature_quality, frame_num = vpdq_feature.split(",")
        # Filtering hash quality is done during hashing starting in 0.10.0 instead of during similarity
        # comparison because there's no reason to store hashes which will never be used.
        if int(feature_quality) >= 31:
            new_vpdq_hash_str += reverse_byte_order(phash)

    # In the uncommon scenario that all hashes have all been filtered out due to bad quality, then videos
    # will compare as not similar to any other video (including itself, naturally). This matches the same
    # behavior as the old algorithm where low quality hashes were filtered before similarity was calculated.

    return bytes.fromhex(new_vpdq_hash_str)
