<div align="center">
  
 # Hydrus Video Deduplicator
  <img src="https://github.com/appleappleapplenanner/hydrus-video-deduplicator/assets/104981058/e65383e8-1978-46aa-88b6-6fdda9767367">
  
Hydrus Video Deduplicator finds potential duplicate videos through the Hydrus API


[![PyPI - Version](https://img.shields.io/pypi/v/hydrusvideodeduplicator.svg)](https://pypi.org/project/hydrusvideodeduplicator)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/hydrusvideodeduplicator.svg)](https://pypi.org/project/hydrusvideodeduplicator)
[![GitHub Repo stars](https://img.shields.io/github/stars/appleappleapplenanner/hydrus-video-deduplicator)](https://github.com/appleappleapplenanner/hydrus-video-deduplicator/stargazers)

</div>

---

## How It Works:
The deduplicator works by comparing videos similarity by their [perceptual hash](https://en.wikipedia.org/wiki/Perceptual_hashing).

Potential duplicates can be processed through the Hydrus duplicates processing page just like images.

You can choose to process only a subset of videos with `--query` using Hydrus tags, e.g. `--query="character:edward"` will only process videos with the tag `character:edward`.

For more information check out the [wiki](https://github.com/appleappleapplenanner/hydrus-video-deduplicator/wiki) and the [FAQ](https://github.com/appleappleapplenanner/hydrus-video-deduplicator/wiki/faq)

---

## Installation:
#### Dependencies:
- Python >=3.10
- FFmpeg

```sh
python3 -m pip install hydrusvideodeduplicator
```

---

## [Usage:](https://github.com/appleappleapplenanner/hydrus-video-deduplicator/wiki/Usage)

```sh
python3 -m hydrusvideodeduplicator --api-key="<your key>"
```

For full list of options see `--help` or the [usage page.](https://github.com/appleappleapplenanner/hydrus-video-deduplicator/wiki/Usage)

---

## TODO:
- [ ] Option to rollback and remove potential duplicates
- [ ] OR predicates for --query
- [ ] Parallelize hashing and duplicate search
- [ ] Automatically generate access key with Hydrus API
- [x] Docker container
- [ ] Upload Docker container to Docker Hub (GitHub Action)
- [x] Pure Python port of vpdq
- [x] Windows compatibility without WSL or Docker

---

## Contact:

Message me @applenanner on the [Hydrus Discord](https://discord.gg/wPHPCUZ) for general questions/concerns

Create an issue on GitHub if there's a problem with the program. Provide as much info as possible in your issue.

---

## Credits:
[Hydrus Network](https://github.com/hydrusnetwork/hydrus) by dev

[Hydrus API Library](https://gitlab.com/cryzed/hydrus-api) by Cryzed

[pdq](https://github.com/facebook/ThreatExchange/tree/main/pdq) by Meta

vpdq by Meta, ported to Python by me.

[Big Buck Bunny](https://peach.blender.org/about) clips by Blender Foundation (CC BY 3.0)
