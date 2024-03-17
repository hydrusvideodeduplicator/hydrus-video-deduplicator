<div align="center">
  
 # Hydrus Video Deduplicator
  <img src="https://github.com/hydrusvideodeduplicator/hydrus-video-deduplicator/assets/104981058/e65383e8-1978-46aa-88b6-6fdda9767367">
  
Hydrus Video Deduplicator finds potential duplicate videos through the Hydrus API and marks them as potential duplicates to allow manual filtering through the Hydrus Client GUI.


[![PyPI - Version](https://img.shields.io/pypi/v/hydrusvideodeduplicator.svg)](https://pypi.org/project/hydrusvideodeduplicator)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/hydrusvideodeduplicator.svg)](https://pypi.org/project/hydrusvideodeduplicator)
[![PyPI downloads](https://img.shields.io/pypi/dm/hydrusvideodeduplicator.svg)](https://pypistats.org/packages/hydrusvideodeduplicator)
[![GitHub Repo stars](https://img.shields.io/github/stars/hydrusvideodeduplicator/hydrus-video-deduplicator)](https://github.com/hydrusvideodeduplicator/hydrus-video-deduplicator/stargazers)

</div>

---

Hydrus Video Deduplicator **does not modify your files**. It only marks videos as "potential duplicates" through the Hydrus API so that you can filter them manually in the duplicates processing page.

[See the Hydrus documentation for how duplicates are managed in Hydrus](https://hydrusnetwork.github.io/hydrus/duplicates.html).

This program contains no telemetry. It only makes requests to the Hydrus API URL.

## [Installation:](https://github.com/hydrusvideodeduplicator/hydrus-video-deduplicator/wiki/Installation)
#### Dependencies:
- [Python](https://www.python.org/downloads/) >=3.10

```sh
python3 -m pip install hydrusvideodeduplicator
```

---

## [Usage:](https://github.com/hydrusvideodeduplicator/hydrus-video-deduplicator/wiki/Usage)

Simplest usage:

```sh
python3 -m hydrusvideodeduplicator --api-key="put your Hydrus api key in these quotes here"
```

You should now see all potential video duplicates in the Hydrus duplicates processing page.

For many users, it should be as simple as the Usage command above.

For more information, see the [Usage](https://github.com/hydrusvideodeduplicator/hydrus-video-deduplicator/wiki/Usage) and [FAQ](https://github.com/hydrusvideodeduplicator/hydrus-video-deduplicator/wiki/faq). 

---

## Contact:

Create an issue on GitHub for any problems/concerns. Provide as much detail as possible in your issue.

Message @applenanner on the [Hydrus Discord](https://discord.gg/wPHPCUZ) for other general questions/concerns.

---

## Attribution:
[Hydrus Network](https://github.com/hydrusnetwork/hydrus) (DWTFYWTPL)

[Hydrus API Library](https://gitlab.com/cryzed/hydrus-api) (GNU AGPLv3) by cryzed

[pdq](https://github.com/facebook/ThreatExchange/tree/main/pdq) (BSD) by Meta

[vpdq](https://github.com/facebook/ThreatExchange/tree/main/vpdq) (BSD) by Meta

[Big Buck Bunny](https://peach.blender.org/about), [Sintel](https://durian.blender.org/about/)  (CC BY 3.0) clips by Blender Foundation
