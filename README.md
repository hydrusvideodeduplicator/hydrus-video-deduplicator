<div align="center">
  
  <img src="https://github.com/appleappleapplenanner/hydrus-video-deduplicator/assets/104981058/968603d3-5a11-4a05-bbb4-7b91b71fb61d">

  Hydrus Video Deduplicator finds similar video files and marks them as potential duplicates through the Hydrus API.
  
</div>

---

Currently it works! However, it does add a namespaced tag to all of your video files in the `my tags` service and may not work well on large libraries.

The only database modifications are tagging video files and marking them as duplicates. It only interacts through the Hydrus API so it should be harmless.

Only tested on Linux. I will test it on Windows soon.

## How It Works:
The deduplicator works by using a library, [VideoHash](https://github.com/akamhy/videohash), to create a perceptual hash of a video. [I modified it](https://github.com/appleappleapplenanner/videohash) to allow direct video objects to avoid excessive writes.

A perceptual hash is just a binary string for a video based on characteristics of the videos frames at a regular interval e.g. `0b01010100010100010100101010`

Once all perceptual hashes for all the videos in your database are computed and tagged, they are compared against each other to detect if they're similar. If they are similar, they will be marked as potential duplicates in Hydrus.

It's not as accurate as the image duplicate detector in Hydrus, but it's WAY better than nothing. It should easily detect the same videos with different resolutions and possible cropping.

Currently, it doesn't use the search distance function of Hydrus because I can't set that through the API. The search distance for images is calculated internally.

## Instructions:
0. Backup your database and files unless you trust a stranger on an anonymous Github alternate account to not ruin your hard work. 

1. Clone or download the repository.
2. Create Python virtual environment and install dependencies (MUST BE PYTHON >=3.11).

`hydrus-video-deduplicator/`
```sh
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```
3. [Enable the Hydrus Client API](https://hydrusnetwork.github.io/hydrus/client_api.html#enabling_the_api) and create an access key with all permissions.
4. Fill in your access key under `HYDRUS_API_KEY` in `secret.py`. If you have a remote instance or different port, also change the `HYDRUS_HOST`.
5. Run `main.py` and let it process the duplicates.
```sh
python main.py
```
6. Your video files should now have a perceptual hash tag and any similar files should be marked as potential duplicates.

Please create an issue on Github if you have any problems or questions! Pull requests also welcome on this or my VideoHash fork. 

There is a lot to fix and cleanup and I'm more experienced in C than Python, so fix stuff please.

## System Dependencies:
- FFmpeg
- Python >=3.11
- Linux (?)
    - I haven't tested it on Windows yet, but I can't see a reason why it wouldn't work.

## TODO:
- CLI
- Rollback option to remove potential duplicates after they're added
- Option to remove all perceptual hash tags
- Option to add phash tag on specific tag service (default is my tags)

## Credits:
[Hydrus Network](https://github.com/hydrusnetwork/hydrus) by dev

[Hydrus API Library](https://gitlab.com/cryzed/hydrus-api) by Cryzed

[VideoHash](https://github.com/akamhy/videohash) by Akash Mahanty
