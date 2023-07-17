<div align="center">
  
 # Hydrus Video Deduplicator
  <img src="https://github.com/hydrusvideodeduplicator/hydrus-video-deduplicator/assets/104981058/e65383e8-1978-46aa-88b6-6fdda9767367">
  
Hydrus Video Deduplicator finds potential duplicate videos through the Hydrus API


[![PyPI - Version](https://img.shields.io/pypi/v/hydrusvideodeduplicator.svg)](https://pypi.org/project/hydrusvideodeduplicator)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/hydrusvideodeduplicator.svg)](https://pypi.org/project/hydrusvideodeduplicator)
[![PyPI downloads](https://img.shields.io/pypi/dm/hydrusvideodeduplicator.svg)](https://pypistats.org/packages/hydrusvideodeduplicator)
[![GitHub Repo stars](https://img.shields.io/github/stars/hydrusvideodeduplicator/hydrus-video-deduplicator)](https://github.com/hydrusvideodeduplicator/hydrus-video-deduplicator/stargazers)

</div>

---

## How It Works:
The deduplicator works by comparing videos similarity by their [perceptual hash](https://en.wikipedia.org/wiki/Perceptual_hashing).

Potential duplicates can be processed through the Hydrus duplicates processing page just like images.

You can choose to process only a subset of videos with `--query` using Hydrus tags, e.g. `--query="character:edward"` will only process videos with the tag `character:edward`.

For more information check out the [wiki](https://github.com/hydrusvideodeduplicator/hydrus-video-deduplicator/wiki) and the [FAQ](https://github.com/hydrusvideodeduplicator/hydrus-video-deduplicator/wiki/faq)

---

## [Installation:](https://github.com/hydrusvideodeduplicator/hydrus-video-deduplicator/wiki/Installation)
#### Dependencies:
- [Python](https://www.python.org/downloads/) >=3.10

```sh
python3 -m pip install hydrusvideodeduplicator
```

---

## [Usage:](https://github.com/hydrusvideodeduplicator/hydrus-video-deduplicator/wiki/Usage)

```sh
python3 -m hydrusvideodeduplicator --api-key="<your key>"
```

For full list of options see `--help` or the [usage page.](https://github.com/hydrusvideodeduplicator/hydrus-video-deduplicator/wiki/Usage)

---

## TODO:
- [ ] Option to rollback and remove potential duplicates
- [ ] OR predicates for --query
- [x] Parallelize hashing and duplicate search
- [ ] Automatically generate access key with Hydrus API
- [x] Docker container
- [ ] Upload Docker container to Docker Hub (GitHub Action)
- [x] Pure Python port of vpdq
- [x] Windows compatibility without WSL or Docker

---

## Contact:

Create an issue on GitHub for any problems/concerns. Provide as much detail as possible in your issue.

Message @applenanner on the [Hydrus Discord](https://discord.gg/wPHPCUZ) for other general questions/concerns

---

## Attribution:
[Hydrus Network](https://github.com/hydrusnetwork/hydrus) (DWTFYWTPL)

[Hydrus API Library](https://gitlab.com/cryzed/hydrus-api) (GNU AGPLv3) by cryzed

[pdq](https://github.com/facebook/ThreatExchange/tree/main/pdq) (BSD) by Meta

[vpdq](https://github.com/facebook/ThreatExchange/tree/main/vpdq) (BSD) by Meta

[Big Buck Bunny](https://peach.blender.org/about), [Sintel](https://durian.blender.org/about/)  (CC BY 3.0) clips by Blender Foundation
