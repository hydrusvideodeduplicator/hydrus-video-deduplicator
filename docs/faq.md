# FAQ

## How to update

I highly recommend backing up your dedupe database folder if it took a long time to hash/search your Hydrus files.

I will do my best to preserve your database across upgrades, but there are no guarantees. Just back it up.

See [How to backup my dedupe database](#how-to-backup-my-dedupe-database) for instructions on backups.

To upgrade:

### Windows

Download the [latest release directly from the github releases page](https://github.com/hydrusvideodeduplicator/hydrus-video-deduplicator/releases).

### Linux and macOS

```sh
# activate your venv first
pip install hydrusvideodeduplicator --upgrade
```

---

## Can I safely cancel a dedupe in progress?

Yes. You can safely skip any or all of the dedupe steps in progress by pressing `CTRL+C`. The next time you launch the
program it will continue where you left off.

---

## How to backup my dedupe database?

You can backup your dedupe database by copying the database directory somewhere safe.

See [Where are the video hashes stored?](#where-are-the-video-hashes-stored) location for the database directory.

---

## How does this work?

1. First, the program will perceptually hash all your video files and store them in a database.

    - Initial hashing takes longer than searching for duplicates. It will also probably get slower as it progresses because the API requests are sorted by file size.

1. Then, the perceptual hashes are put into a data structure to make it fast to search for duplicates.

    - Note: Initial search tree building may take a while, but it should be very fast on subsequent runs when new files are added.

1. Finally, it will search the database for potential duplicates and mark them as potential duplicates in Hydrus.

You can run the program again when you add more files to find more duplicates.

> **Note**: You can skip any of the steps to find duplicates for only a few videos at a time. The next time you
> launch the program it will continue where you left off.

---

## Where are the video hashes stored?

Hashes are stored in an sqlite database created in an app dir to speed up processing on subsequent runs.

On Linux, this directory is likely `~/.local/share/hydrusvideodeduplicator`

On Windows, this directory is likely `%USERPROFILE%\AppData\Local\hydrusvideodeduplicator` or `%USERPROFILE%\AppData\Roaming\hydrusvideodeduplicator`

On macos, this directory is likely `/Users/<yourusername>/Library/Application Support/hydrusvideodeduplicator`

The database directory can be set with `DEDUP_DATABASE_DIR` environment variable.

---

## I have a big library. How do I test this on just a few files?

You can use [system predicates](https://hydrusnetwork.github.io/hydrus/developer_api.html#get_files_search_files) and [queries](https://hydrusnetwork.github.io/hydrus/getting_started_searching.html) to limit your search.

Each query will reduce the number of files you process. By default all videos/animated are processed.

For example:

You want to deduplicate files with these requirements:

- Max of 1000 files

- <50 MB filesize
- In `system:archive`

- Has the tags `character:jacob`

- Imported < 1 hour ago

Then the arguments for the query would be:

`--query="system:filesize > 10MB" --query="system:limit 1000" --query="system:archive" --query="character:jacob" --query="system:import time < 1 hour"`

These are the same queries as would be used in Hydrus.

---

## I want to search for duplicates without hashing new video files

You can either use `--skip-hashing`, press CTRL+C while perceptual hashing is running, or use a query limiting when files were imported.

<details>
<summary>Example query that limits import time</summary>
<br>

```sh
--query="system:import time > 1 day"
```

</details>

---

## What kind of files does it support?

Almost all video and animated files e.g. mp4, gif, apng, etc. are supported if they are supported in Hydrus.

If you find a video that fails to perceptually hash, please create an issue on GitHub with some information about the video or message `@applenanner` on the [Hydrus Discord](https://discord.gg/wPHPCUZ).

If a bad file crashes the whole program also create an issue. Skipping files is fine, but crashing is not.

---

## Why does processing slow down the longer it runs?

The files are retrieved from Hydrus in increasing file size order. Naturally, this would also affect searching because the database is also ordered.

If this is an issue for you and think this should be changed, please create an issue and explain why.

---

## I set the threshold too low and now I have too many potential duplicates

While the perceptual hasher should have very few false-positives, you may accidently get too many if you change your search threshold too low using `--threshold`.

You can reset your potential duplicates in Hydrus in duplicates processing:

![Demonstration of how to reset potential duplicates in Hydrus](./img/reset_duplicates.png)

Then, you should also reset your search cache with `--clear-search-cache` to search for duplicates from scratch.

---

## My file failed to hash

If you find a video that fails to perceptually hash, please create an issue on GitHub with some information about the video or message `@applenanner` on the [Hydrus Discord](https://discord.gg/wPHPCUZ).

---

## I have some "weird" messages while perceptual hashing

If you get messages like this:

> VPS 0 does not exist
>
> SPS 0 does not exist.

or

> deprecated pixel format used, make sure you did set range correctly

Don't worry. These are FFmpeg logs from file decoding. They can be safely ignored.

These messages are prevalent  with certain video codecs, namely AV1 and H.265.

Unfortunately these obnoxious messages are not able to be silenced yet.

But if your messages mention perceptual hashing failing then see [My file failed to hash](#my-file-failed-to-hash).
