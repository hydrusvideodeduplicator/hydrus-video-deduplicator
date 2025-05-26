# Hydrus Video Deduplicator

<div align="center">

![hydrus video deduplicator running in terminal](./docs/img/preview.png)

Hydrus Video Deduplicator finds potential duplicate videos through the Hydrus API and marks them as potential duplicates to allow manual filtering through the Hydrus Client GUI.

[![PyPI - Version](https://img.shields.io/pypi/v/hydrusvideodeduplicator.svg)](https://pypi.org/project/hydrusvideodeduplicator)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/hydrusvideodeduplicator.svg)](https://pypi.org/project/hydrusvideodeduplicator)
[![PyPI downloads](https://img.shields.io/pypi/dm/hydrusvideodeduplicator.svg)](https://pypistats.org/packages/hydrusvideodeduplicator)
[![GitHub Repo stars](https://img.shields.io/github/stars/hydrusvideodeduplicator/hydrus-video-deduplicator)](https://github.com/hydrusvideodeduplicator/hydrus-video-deduplicator/stargazers)

</div>

---

Hydrus Video Deduplicator **does not modify your files**. It only marks videos as `potential duplicates` through the Hydrus API so that you can filter them manually in the duplicates processing page.

[See the Hydrus documentation for how duplicates are managed in Hydrus](https://hydrusnetwork.github.io/hydrus/duplicates.html).

This program contains no telemetry. It only makes requests to the Hydrus API URL which you specify.

## [Installation](./docs/installation.md)

For Windows, you can get the [latest release directly from the github releases page](https://github.com/hydrusvideodeduplicator/hydrus-video-deduplicator/releases).

For Linux and macos, see the [Python Install](#python-install).

> **Note**: Many instructions in this repo are written for the Python install instead of the Windows exe. If you're using the Windows exe, simply replace `python3 -m hydrusvideodeduplicator` with `hydrusvideodeduplicator.exe` and they will work the same.

## [Usage](./docs/usage.md)

See [Usage](./docs/usage.md) for how to run the program, and [FAQ](./docs/faq.md) for more information.

## Python Install

> **Note**: Windows users do not need the Python install. Download the exe as described in [Installation](#installation) instead.

### Dependencies

- [Python](https://www.python.org/downloads/) >=3.10

```sh
python3 -m pip install hydrusvideodeduplicator
```

Then continue to [Usage](./docs/usage.md).

---

## [Contact](./docs/contact.md)

Create an issue on GitHub for any problems/concerns. Provide as much detail as possible in your issue.

Email `hydrusvideodeduplicator@gmail.com` for other general questions/concerns.

Message `@applenanner` on the [Hydrus Discord](https://discord.gg/wPHPCUZ) for other general questions/concerns.
