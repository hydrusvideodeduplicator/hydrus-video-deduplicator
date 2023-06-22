<div align="center">
  
 # Hydrus Video Deduplicator
  <img src="https://github.com/appleappleapplenanner/hydrus-video-deduplicator/assets/104981058/968603d3-5a11-4a05-bbb4-7b91b71fb61d">

  
Hydrus Video Deduplicator detects similar video files and marks them as potential duplicates through the Hydrus API

</div>
 
---

## How It Works:
The deduplicator works by using a library, [VideoHash](https://github.com/akamhy/videohash), to create a perceptual hash of a video. [I modified it](https://github.com/appleappleapplenanner/videohash) to allow direct video objects to avoid excessive writes.

A perceptual hash is just a binary string for a video based on characteristics of the videos frames at a regular interval e.g. `0b01010100010100010100101010`

Once all perceptual hashes for all the videos in your database are computed and tagged, they are compared against each other to detect if they're similar. If they are similar, they will be marked as potential duplicates in Hydrus.

It's not as accurate as the image duplicate detector in Hydrus, but it's WAY better than nothing. It should easily detect the same videos with different resolutions and possible cropping.

Currently, it doesn't use the search distance function of Hydrus because I can't set that through the API. The search distance for images is calculated internally.

The only database modifications are tagging video files and marking them as duplicates. It only interacts through the Hydrus API so it should be harmless.

---

## Instructions:
0. Backup your database and files unless you trust a stranger on an anonymous Github alternate account to not ruin your hard work. 

1. Install system requirements (see below).

2. Clone or download the repository.
3. Create Python virtual environment and install (MUST BE PYTHON >=3.11):

### Windows (Powershell)

`hydrus-video-deduplicator/`
```sh
py -m venv venv
venv\Scripts\Activate.ps1
pip install .
```

### Linux

`hydrus-video-deduplicator/`
```sh
python -m venv venv
source venv/bin/activate
pip install .
```

4. [Enable the Hydrus Client API](https://hydrusnetwork.github.io/hydrus/client_api.html#enabling_the_api) and create an access key with all permissions.

5. Run and enter your access key as a parameter
```sh
python -m hydrus_video_deduplicator --api-key="<your key>"
```

6. Your video files should now have a perceptual hash tag, and any similar files should be marked as potential duplicates.

7. Optional: Set your API key using an environment variable.

### Windows (Powershell)
```Powershell
$env:HYDRUS_API_KEY="<your key>" 
```

### Linux
```sh
HYDRUS_API_KEY="<your key>"
```

Other environment variables:

`HYDRUS_API_URL` is the Hydrus URL e.g. `http://localhost:45869`

`LOCAL_TAG_SERVICE_NAME` is the service used to add tags e.g. `my tags`

You can also set these in a .env file with each env variable on their own line.

## System Requirements:
- FFmpeg
- Python >=3.11
- Windows or Linux. macos is untested.

---

## TODO:
- [ ] Option to rollback and remove potential duplicates after they're added
- [ ] Option to only generate phashes or only search for duplicates
- [ ] Option to remove all perceptual hash tags
- [x] Option to add phash tag on specific tag service (default is my tags)
- [ ] Option to enter custom Hydrus tag search parameters
- [ ] Async and multiprocessing
- [ ] Automatically generate access key with Hydrus API
- [ ] Upload to PyPI

Please create an issue on Github if you have any problems or questions! Pull requests also welcome on this or my VideoHash fork. 

There is a lot to fix and cleanup and I'm more experienced in C than Python, so fix stuff please.

---

## Credits:
[Hydrus Network](https://github.com/hydrusnetwork/hydrus) by dev

[Hydrus API Library](https://gitlab.com/cryzed/hydrus-api) by Cryzed

[VideoHash](https://github.com/akamhy/videohash) by Akash Mahanty