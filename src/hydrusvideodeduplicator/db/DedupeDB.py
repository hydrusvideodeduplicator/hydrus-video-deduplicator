from __future__ import annotations

import logging
import os
import sqlite3
from dataclasses import dataclass
from itertools import islice
from pathlib import Path
from typing import TYPE_CHECKING

from rich import print
from sqlitedict import SqliteDict
from tqdm import tqdm

from ..__about__ import __version__
from .vptree import VpTreeManager

if TYPE_CHECKING:
    from collections.abc import Generator, Iterable
    from typing import Any, TypeAlias

    FileServiceKeys: TypeAlias = list[str]
    FileHashes: TypeAlias = Iterable[str]

    from hydrusvideodeduplicator.client import HVDClient

dedupedblog = logging.getLogger("db")
dedupedblog.setLevel(logging.INFO)

_db_dir: Path = Path()
_DB_FILE_NAME: str = "videohashes.sqlite"


class DedupeDbException(Exception):
    """Base class for DedupeDb exceptions."""


def does_db_exist() -> bool:
    """
    Check if the database exists.
    """
    db_path = get_db_file_path()
    try:
        _ = db_path.resolve(strict=True)
        return True
    except FileNotFoundError:
        return False


def database_accessible(db_file: Path | str, tablename: str, verbose: bool = False):
    try:
        with SqliteDict(str(db_file), tablename=tablename, flag="r"):
            return True
    except OSError:
        if verbose:
            print("[red] Database does not exist.")
    except RuntimeError:  # SqliteDict error when trying to create a table for a DB in read-only mode
        if verbose:
            print("[red] Database does not exist.")
    except Exception as exc:
        if verbose:
            print(f"[red] Could not access database. Exception: {exc}")
    return False


def is_db_accessible(verbose: bool = False) -> bool:
    """
    Check DB exists and is accessible.

    Return DB exists and is accessible.
    """
    return database_accessible(get_db_file_path(), tablename="videos", verbose=verbose)


def clear_search_cache() -> None:
    """Delete cache search index value for each video in database"""
    if not is_db_accessible():
        return

    with SqliteDict(str(get_db_file_path()), tablename="videos", flag="c") as hashdb:
        for key in hashdb:
            row = hashdb[key]
            if "farthest_search_index" in row:
                del row["farthest_search_index"]
                hashdb[key] = row
                hashdb.commit()
    print("[green] Cleared search cache.")


def update_search_cache(new_total: int | None = None) -> None:
    """
    Update the search cache to clamp the farthest_search_index to the current length of the database.
    """
    assert new_total is None or new_total >= 0

    if not is_db_accessible():
        return

    BATCH_SIZE = 256
    with SqliteDict(str(get_db_file_path()), tablename="videos", flag="c", outer_stack=False) as hashdb:
        if new_total is None:
            new_total = len(hashdb)
        for batched_items in batched_and_save_db(hashdb, BATCH_SIZE):
            for video_hash, _ in batched_items.items():
                row = hashdb[video_hash]
                if "farthest_search_index" in row and row["farthest_search_index"] > new_total:
                    row["farthest_search_index"] = new_total
                    hashdb[video_hash] = row


def batched_and_save_db(
    db: SqliteDict,
    batch_size: int = 1,
    chunk_size: int = 1,
) -> Generator[dict[str, dict[str, Any]], Any, None]:
    """
    Batch rows into rows of length n and save changes after each batch or after chunk_size batches.
    """
    assert batch_size >= 1 and chunk_size >= 1
    it = iter(db.items())
    chunk_counter = 0
    while batch_items := dict(islice(it, batch_size)):
        yield batch_items
        chunk_counter += 1

        # Save changes after chunk_size batches
        if chunk_counter % chunk_size == 0:
            db.commit()


def are_files_deleted_hydrus(client: HVDClient, file_hashes: FileHashes) -> dict[str, bool]:
    """
    Check if files are trashed or deleted in Hydrus

    Returns a dictionary of {hash, trashed_or_not}
    """
    videos_metadata = client.client.get_file_metadata(hashes=file_hashes, only_return_basic_information=False)[
        "metadata"
    ]

    result: dict[str, bool] = {}
    for video_metadata in videos_metadata:
        # This should never happen, but it shouldn't break the program if it does
        if "hash" not in video_metadata:
            logging.error("Hash not found for potentially trashed file.")
            continue
        video_hash = video_metadata["hash"]
        is_deleted: bool = video_metadata.get("is_deleted", False)
        result[video_hash] = is_deleted

    return result


def clear_trashed_files_from_db(client: HVDClient) -> None:
    """
    Delete trashed and deleted Hydrus files from the database.
    """
    try:
        with SqliteDict(str(get_db_file_path()), tablename="videos", flag="c", outer_stack=False) as hashdb:
            # This is EXPENSIVE. sqlitedict gets len by iterating over the entire database!
            if (total := len(hashdb)) < 1:
                return

            delete_count = 0
            try:
                with tqdm(
                    dynamic_ncols=True,
                    total=total,
                    desc="Searching for trashed files to prune",
                    unit="video",
                    colour="BLUE",
                ) as pbar:
                    BATCH_SIZE = 32
                    for batched_items in batched_and_save_db(hashdb, BATCH_SIZE):
                        is_trashed_result = are_files_deleted_hydrus(client, batched_items.keys())
                        for video_hash, is_trashed in is_trashed_result.items():
                            if is_trashed is True:
                                del hashdb[video_hash]
                                delete_count += 1
                        pbar.update(min(BATCH_SIZE, total - pbar.n))
            except Exception as exc:
                print("[red] Failed to clear trashed videos cache.")
                print(exc)
                dedupedblog.error(exc)
            finally:
                if delete_count > 0:
                    print(f"Cleared {delete_count} trashed videos from the database.")
                update_search_cache(total - delete_count)

    except OSError as exc:
        dedupedblog.info(exc)


def create_db_dir() -> None:
    """
    Create the database folder if it doesn't already exist.
    """
    try:
        db_dir = get_db_file_path().parent
        os.makedirs(db_dir, exist_ok=False)
        # Exception before this log if directory already exists
        dedupedblog.info(f"Created DB dir {db_dir}")
    except OSError:
        pass


@dataclass
class DatabaseStats:
    num_videos: int
    file_size: int  # in bytes


def get_db_stats_old() -> DatabaseStats:
    """OLD Get some database stats."""
    con = sqlite3.connect(get_db_file_path())
    (num_videos,) = con.execute("SELECT COUNT(*) FROM videos").fetchone()
    con.close()
    file_size = os.path.getsize(get_db_file_path())
    return DatabaseStats(num_videos, file_size)


def get_db_stats(db: DedupeDb) -> DatabaseStats:
    """Get some database stats."""
    num_videos = len(
        db.execute(
            "SELECT hash_id FROM files WHERE hash_id IN (SELECT hash_id FROM shape_perceptual_hash_map)"
        ).fetchall()
    )
    file_size = os.path.getsize(get_db_file_path())
    return DatabaseStats(num_videos, file_size)


def create_db():
    """
    Create the database files.
    """
    if not get_db_dir().exists():
        create_db_dir()
    db = DedupeDb(get_db_dir(), get_db_name())
    db.init_connection()
    db.create_tables()
    db.commit()
    db.close()


def set_db_dir(dir: Path):
    """Set the directory for the database."""
    global _db_dir
    _db_dir = dir


def get_db_dir():
    """Get the directory of the database."""
    return _db_dir


def get_db_name() -> str:
    """Get the db file name, e.g. videohashes.sqlite."""
    return _DB_FILE_NAME


def get_db_file_path() -> Path:
    """
    Get database file path.

    Return the database file path.
    """
    return get_db_dir() / get_db_name()


class DedupeDb:
    def __init__(self, db_dir: Path, db_name: str):
        self.db_dir = db_dir
        self.db_name = db_name

        self.conn = None
        self.cur = None

    """
    Main functions
    """

    def execute(self, query, *query_args) -> sqlite3.Cursor:
        return self.cur.execute(query, *query_args)

    def set_cursor(self, cur: sqlite3.Cursor):
        self.cur = cur

    def close_cursor(self):
        if self.cur is not None:
            self.cur.close()
            del self.cur
            self.cur = None

    def init_connection(self):
        db_path = self.db_dir / self.db_name
        self.conn = sqlite3.connect(db_path)
        cur = self.conn.cursor()
        self.set_cursor(cur)

    def commit(self):
        self.conn.commit()

    def close(self):
        self.conn.close()

    """
    Tables
    """

    def create_tables(self):
        # old:

        # videos table (this is the sqlitedict schema)
        self.execute("CREATE TABLE IF NOT EXISTS videos (key TEXT PRIMARY KEY, value BLOB)")

        # new:

        # version table
        self.execute("CREATE TABLE IF NOT EXISTS version (version TEXT)")
        self.execute("INSERT INTO version (version) VALUES (:version)", {"version": __version__})

        # files table
        self.execute("CREATE TABLE IF NOT EXISTS files ( hash_id INTEGER PRIMARY KEY, file_hash BLOB_BYTES UNIQUE )")

        # this is straight up copied from Hydrus
        self.execute(
            "CREATE TABLE IF NOT EXISTS shape_perceptual_hashes ( phash_id INTEGER PRIMARY KEY, phash BLOB_BYTES UNIQUE )"  # noqa: E501
        )
        self.execute(
            "CREATE TABLE IF NOT EXISTS shape_perceptual_hash_map ( phash_id INTEGER, hash_id INTEGER, PRIMARY KEY ( phash_id, hash_id ) )"  # noqa: E501
        )
        self.execute(
            "CREATE TABLE IF NOT EXISTS shape_vptree ( phash_id INTEGER PRIMARY KEY, parent_id INTEGER, radius INTEGER, inner_id INTEGER, inner_population INTEGER, outer_id INTEGER, outer_population INTEGER )"  # noqa: E501
        )
        self.execute(
            "CREATE TABLE IF NOT EXISTS shape_maintenance_branch_regen ( phash_id INTEGER PRIMARY KEY )"
        )  # noqa: E501
        self.execute(
            "CREATE TABLE IF NOT EXISTS shape_search_cache ( hash_id INTEGER PRIMARY KEY, searched_distance INTEGER )"
        )
        # TODO: We don't need this I don't think.
        # self.conn.execute(
        #     "CREATE TABLE IF NOT EXISTS pixel_hash_map ( hash_id INTEGER, pixel_hash_id INTEGER, PRIMARY KEY ( hash_id, pixel_hash_id ) )"  # noqa: E501
        # )

    """
    Utility
    """

    def clear_search_cache(self):
        """Clear the search cache for all files."""
        tree = VpTreeManager(self)
        result = self.execute("SELECT hash_id FROM shape_search_cache").fetchall()
        if result:
            hash_ids = [hash_id[0] for hash_id in result]
            tree.reset_search(hash_ids)

    def add_file(self, file_hash: str):
        """Add a file to the db. If it already exists, do nothing."""
        self.execute("INSERT OR IGNORE INTO files ( file_hash ) VALUES ( :file_hash )", {"file_hash": file_hash})

    def add_perceptual_hash(self, perceptual_hash: str) -> int:
        """
        Add a perceptual hash to the db.
        If it already exists, do nothing.

        Returns the phash_id of the perceptual hash.
        """
        result = self.execute(
            "SELECT phash_id FROM shape_perceptual_hashes WHERE phash = :phash;", {"phash": perceptual_hash}
        ).fetchone()

        if result is None:
            self.execute(
                "INSERT INTO shape_perceptual_hashes ( phash ) VALUES ( :phash )",
                {"phash": perceptual_hash},
            )
            result = self.execute(
                "SELECT phash_id FROM shape_perceptual_hashes WHERE phash = :phash;", {"phash": perceptual_hash}
            ).fetchone()
            result = result[0]
            assert isinstance(result, int)
        else:
            result = result[0]
        # TODO: Double check that the return value here is actually there if the result is not None.
        # Remove the assert below if it is.
        assert isinstance(result, int)
        return result

    def associate_file_with_perceptual_hash(self, file_hash: str, perceptual_hash: str):
        """
        Associate a file with a perceptual hash in the database. If the file already has a perceptual hash, it will be
        overwritten.

        Note:
        Perceptual hashes are not unique for each file.
        Files can have identical perceptual hashes.
        This is not even that rare, e.g. a video that is all the same color.
        """

        # new
        hash_id = self.get_hash_id(file_hash)

        perceptual_hash_id = self.get_phash_id(perceptual_hash)
        assert perceptual_hash_id is not None

        tree = VpTreeManager(self)
        tree.add_leaf(perceptual_hash_id, perceptual_hash)

        already_exists = self.execute(
            "SELECT hash_id FROM shape_perceptual_hash_map WHERE hash_id = :hash_id", {"hash_id": hash_id}
        ).fetchone()

        if already_exists:
            self.execute("DELETE FROM shape_perceptual_hash_map WHERE hash_id = :hash_id", {"hash_id": hash_id})

        res = self.execute(
            "INSERT INTO shape_perceptual_hash_map ( phash_id, hash_id ) VALUES ( :phash_id, :hash_id )",
            {"phash_id": perceptual_hash_id, "hash_id": hash_id},
        )

        # NOTE: We must fetchone here so that the rowcount is updated.
        res.fetchone()
        if res.rowcount > 0:
            self.execute(
                "REPLACE INTO shape_search_cache ( hash_id, searched_distance ) VALUES ( :hash_id, :searched_distance );",  # noqa: E501
                {"hash_id": hash_id, "searched_distance": None},
            )

        # old
        # if not is_db_accessible():
        #    raise DedupeDbException("db is not accessible while trying to associate file with perceptual hash.")
        # with SqliteDict(
        #    get_db_file_path(), tablename="videos", flag="c", autocommit=True, outer_stack=False
        # ) as videos_table:
        #    row = videos_table[file_hash] if file_hash in videos_table else {}
        #    row["perceptual_hash"] = perceptual_hash
        #    videos_table[file_hash] = row

    def get_version(self) -> str:
        if self.does_table_exist("version"):
            (version,) = self.execute("SELECT version FROM version;").fetchone()
        else:
            # Old versions of the database did not have a version table. We will assume it's the most recent version
            # that had no version table.
            version = "0.6.0"
        return version

    def set_version(self, version: str):
        self.execute("UPDATE version SET version = :version", {"version": version})

    def does_table_exist(self, table: str) -> bool:
        # pls no injection. named placeholders don't work for tables.
        res = self.execute(f"SELECT * FROM pragma_table_list WHERE name='{table}'")
        return bool(res.fetchall())

    def get_phash_id(self, perceptual_hash: str) -> str | None:
        """Get the perceptual hash id from the phash, or None if not found."""
        result = self.execute(
            "SELECT phash_id FROM shape_perceptual_hashes WHERE phash = :phash", {"phash": perceptual_hash}
        ).fetchone()
        perceptual_hash_id = None
        if result is not None:
            (perceptual_hash_id,) = result
        return perceptual_hash_id

    def get_phash_id_from_hash_id(self, hash_id: str) -> str | None:
        """Get the phash id from the hash_id, or None if not found."""
        result = self.execute(
            "SELECT phash_id FROM shape_perceptual_hash_map WHERE hash_id = :hash_id", {"hash_id": hash_id}
        ).fetchone()
        perceptual_hash_id = None
        if result is not None:
            (perceptual_hash_id,) = result
        return perceptual_hash_id

    def get_hash_id(self, file_hash: str) -> str | None:
        """Get the hash id from the file hash, or None if not found."""
        result = self.execute(
            "SELECT hash_id FROM files WHERE file_hash = :file_hash", {"file_hash": file_hash}
        ).fetchone()
        hash_id = None
        if result is not None:
            (hash_id,) = result
        return hash_id

    def get_phash(self, phash_id: str) -> str | None:
        """Get the perceptual hash from its phash_id, or None if not found."""
        result = self.execute(
            "SELECT phash FROM shape_perceptual_hashes WHERE phash_id = :phash_id", {"phash_id": phash_id}
        ).fetchone()
        phash = None
        if result is not None:
            (phash,) = result
        return phash

    def get_file_hash(self, hash_id: str) -> str | None:
        """Get the file hash from its hash_id, or None if not found."""
        result = self.execute("SELECT file_hash FROM files WHERE hash_id = :hash_id", {"hash_id": hash_id}).fetchone()
        file_hash = None
        if result is not None:
            (file_hash,) = result
        return file_hash

    """
    Misc
    """

    def does_need_upgrade(self) -> bool:
        db_version = SemanticVersion(self.get_version())
        dedupe_version = SemanticVersion(__version__)
        return db_version < dedupe_version

    def upgrade_db(self):
        """Upgrade the db."""

        def print_upgrade(version: str, new_version: str):
            print(f"Upgrading db from {version} to version {new_version}")

        version = self.get_version()
        if __version__ < version:
            raise DedupeDbException(
                f"""
Database version {version} is newer than the installed hydrusvideodeduplicator version {__version__}.\
\nPlease upgrade and try again. \
\nSee documentation for how to upgrade: https://github.com/hydrusvideodeduplicator/hydrus-video-deduplicator/blob/main/docs/faq.md#how-to-update
                """
            )

        if not self.does_need_upgrade():
            return

        # Note: We need to keep re-running get_version so that we can progressively upgrade.
        if SemanticVersion(version) < SemanticVersion("0.6.0"):
            print_upgrade(version, "0.6.0")
            self.set_version("0.6.0")
            version = self.get_version()

        if SemanticVersion(version) < SemanticVersion("0.7.0"):
            print_upgrade(version, "0.7.0")
            self.set_version("0.7.0")
            version = self.get_version()

        self.set_version(__version__)


class SemanticVersion:
    """Simple semantic version class. Supports MAJOR.MINOR.PATCH, e.g. 1.2.3"""

    def __init__(self, version: str):
        self.version = version
        try:
            self.parts = list(map(int, version.split(".")))
            if len(self.parts) != 3:
                raise DedupeDbException("len != 3")
        except Exception as exc:
            raise DedupeDbException(f"Bad semantic version: {self.version}.\nFull exception: {exc}")

    def __eq__(self, other):
        return self.parts == other.parts

    def __lt__(self, other):
        return self.parts < other.parts

    def __le__(self, other):
        return self.parts <= other.parts

    def __gt__(self, other):
        return self.parts > other.parts

    def __ge__(self, other):
        return self.parts >= other.parts

    def __repr__(self):
        return f"SemanticVersion('{self.version}')"
