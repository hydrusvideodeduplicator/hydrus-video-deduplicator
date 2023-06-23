<div align="center">
  
 # Hydrus Video Deduplicator
  <img src="https://github.com/appleappleapplenanner/hydrus-video-deduplicator/assets/104981058/968603d3-5a11-4a05-bbb4-7b91b71fb61d">

  
Hydrus Video Deduplicator detects similar video files and marks them as potential duplicates through the Hydrus API

</div>
 
---

## How It Works:
The deduplicator works by using a library, [VideoHash](https://github.com/akamhy/videohash), to create a perceptual hash of a video.

A perceptual hash is just a binary string for a video based on frames at a regular interval e.g. `0b01010100010100010100101010`

Once all perceptual hashes for all the videos in your database are computed and tagged, they are compared against each other to detect if they're similar. If they are similar, they will be marked as potential duplicates in Hydrus.

---

## Installation:

### System Requirements:
- FFmpeg
- Python >=3.11
- python-dev for Python.h
- Windows or Linux. macos is untested.

1. Install python headers for your system.

Ubuntu Linux
```sh
sudo apt-get python-dev 
# or python3.11-dev if needed
```

1. Clone or download the repository.

2. Install with pip

`hydrus-video-deduplicator/`
```sh
pip install -U .
```

---

## Usage:

0. Backup your database and files unless you trust a stranger on an anonymous Github alternate account to not ruin your hard work. 

1. [Enable the Hydrus Client API](https://hydrusnetwork.github.io/hydrus/client_api.html#enabling_the_api) and create an access key with all permissions.

2. Run and enter your access key as a parameter

```sh
python -m hydrus_video_deduplicator --api-key="<your key>"
```

##### See full list of options with `--help`

<br>

Your video files should now have a perceptual hash tag, and any similar files should be marked as potential duplicates.

---

## TODO:
- [ ] Option to rollback and remove potential duplicates after they're added
- [x] Option to only generate phashes or only search for duplicates
- [ ] Option to remove all perceptual hash tags
- [x] Option to enter custom Hydrus tag search parameters
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

vpdq by Meta

various other files from threatexchange by Meta