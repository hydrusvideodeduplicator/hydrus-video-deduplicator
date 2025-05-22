from __future__ import annotations

import logging
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from rich import print

from ..__about__ import __version__
from .vptree import VpTreeManager

if TYPE_CHECKING:
    from collections.abc import Iterable
    from typing import TypeAlias

    FileServiceKeys: TypeAlias = list[str]
    FileHashes: TypeAlias = Iterable[str]


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


def get_db_stats(db: DedupeDb) -> DatabaseStats:
    """Get some database stats."""
    # TODO: We don't need to get the file hashes. We just need the length.
    num_videos = len(db.get_phashed_files())
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

    def begin_transaction(self):
        self.execute("BEGIN TRANSACTION")

    def close(self):
        self.conn.close()

    """
    Tables
    """

    def create_tables(self):
        # version table
        self.execute("CREATE TABLE IF NOT EXISTS version (version TEXT)")
        self.execute("INSERT INTO version (version) VALUES (:version)", {"version": __version__})

        # files table
        self.execute("CREATE TABLE IF NOT EXISTS files ( hash_id INTEGER PRIMARY KEY, file_hash BLOB_BYTES UNIQUE )")

        # phash tables
        self.execute(
            "CREATE TABLE IF NOT EXISTS shape_perceptual_hashes ( phash_id INTEGER PRIMARY KEY, phash BLOB_BYTES UNIQUE )"  # noqa: E501
        )
        self.execute(
            "CREATE TABLE IF NOT EXISTS shape_perceptual_hash_map ( phash_id INTEGER, hash_id INTEGER, PRIMARY KEY ( phash_id, hash_id ) )"  # noqa: E501
        )

        # vptree tables
        self.execute(
            "CREATE TABLE IF NOT EXISTS shape_vptree ( phash_id INTEGER PRIMARY KEY, parent_id INTEGER, radius INTEGER, inner_id INTEGER, inner_population INTEGER, outer_id INTEGER, outer_population INTEGER )"  # noqa: E501
        )
        # fmt: off
        self.execute(
            "CREATE TABLE IF NOT EXISTS shape_maintenance_branch_regen ( phash_id INTEGER PRIMARY KEY )"
        )  # noqa: E501
        # fmt: on
        self.execute(
            "CREATE TABLE IF NOT EXISTS shape_search_cache ( hash_id INTEGER PRIMARY KEY, searched_distance INTEGER )"
        )

        # vptree insert queue. this is the list of files and their phashes that need to be inserted into the vptree.
        # when entries are added to this queue they don't exist at all in the other tables. they don't have a hash_id
        # or phash_id yet, unless those already exist from other files.
        # this is just a table to store the phashes until they are properly inserted into the vptree, since inserting
        # can take a while.
        self.execute(
            "CREATE TABLE IF NOT EXISTS phashed_file_queue ( file_hash BLOB_BYTES NOT NULL UNIQUE, phash BLOB_BYTES NOT NULL, PRIMARY KEY ( file_hash, phash ) )"  # noqa: E501
        )

    """
    Utility
    """

    def clear_search_tree(self):
        """Clear the search tree. The search cache will also be cleared. Does not clear the perceptual hash map."""
        # Note: Need a separate cursor here since we're running queries in the loop that overwrite this cursor.
        cur = self.conn.cursor()
        cur.execute("SELECT phash_id, hash_id FROM shape_perceptual_hash_map")

        # Move the files back into the queue so that the tree can be rebuilt.
        for phash_id, hash_id in cur:
            phash_result = self.execute(
                "SELECT phash FROM shape_perceptual_hashes WHERE phash_id = :phash_id", {"phash_id": phash_id}
            ).fetchone()
            if not phash_result:
                # This should not happen. Perceptual hashes should always be in the database if there is a phash_id,
                # otherwise we have no idea what perceptual hash it is.
                print(
                    f"ERROR clearing search tree while to get perceptual_hash from phash_id {phash_id}. perceptual_hash not found. Your DB may be corrupt."  # noqa: E501
                )
                continue
            perceptual_hash = phash_result[0]

            file_hash_result = self.execute(
                "SELECT file_hash FROM files WHERE hash_id = :hash_id", {"hash_id": hash_id}
            ).fetchone()
            if not file_hash_result:
                # This should not happen. File hashes should always be in the database if there is a hash_id,
                # otherwise we have no idea what file it is in Hydrus.
                print(
                    f"ERROR clearing search tree while to get file_hash from hash_id {hash_id}. file_hash not found. Your DB may be corrupt."  # noqa: E501
                )
                continue
            file_hash = file_hash_result[0]

            self.add_to_phashed_files_queue(file_hash, perceptual_hash)

        self.execute("DELETE FROM shape_vptree")
        self.execute("DELETE FROM shape_search_cache")
        self.execute("DELETE FROM shape_maintenance_branch_regen")

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

    def add_to_phashed_files_queue(self, file_hash: str, perceptual_hash: str):
        """
        Add a file and its corresponding perceptual hash to the queue to be inserted into the vptree.

        We keep the queue of files to be inserted in the vptree in a separate table to avoid any potential issues
        with assumptions of what needs to exist when/where for vptree operations.

        If the file hash is already in the queue, it will be replaced with the new perceptual hash.
        """
        self.execute(
            "REPLACE INTO phashed_file_queue ( file_hash, phash ) VALUES ( :file_hash, :phash )",
            {"file_hash": file_hash, "phash": perceptual_hash},
        )

    def associate_file_with_perceptual_hash(self, file_hash: str, perceptual_hash: str):
        """
        Associate a file with a perceptual hash in the database.
        This will insert the file into the VpTree.
        If the file already has a perceptual hash, it will be overwritten.

        Note:
        Perceptual hashes are not unique for each file.
        Files can have identical perceptual hashes.
        This is not even that rare, e.g. a video that is all the same color.
        """
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

    def get_phashed_files(self) -> list[str]:
        """Get the file hashes of all files that are phashed. This includes the files in the phashed_file_queue."""
        all_phashed_files_query = (
            "SELECT file_hash FROM files "
            "WHERE hash_id IN (SELECT hash_id FROM shape_perceptual_hash_map) "
            "UNION "
            "SELECT file_hash FROM phashed_file_queue"
        )
        all_phashed_files = self.execute(all_phashed_files_query)
        all_phashed_files = [row[0] for row in all_phashed_files]
        return all_phashed_files

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

        if SemanticVersion(version) < SemanticVersion("0.7.0"):
            print_upgrade(version, "0.7.0")

            # Create version table
            self.execute("CREATE TABLE IF NOT EXISTS version (version TEXT)")
            self.execute("INSERT INTO version (version) VALUES (:version)", {"version": "0.6.0"})

            # Create the vptree tables
            self.execute(
                "CREATE TABLE IF NOT EXISTS files ( hash_id INTEGER PRIMARY KEY, file_hash BLOB_BYTES UNIQUE )"
            )
            self.execute(
                "CREATE TABLE IF NOT EXISTS shape_perceptual_hashes ( phash_id INTEGER PRIMARY KEY, phash BLOB_BYTES UNIQUE )"  # noqa: E501
            )
            self.execute(
                "CREATE TABLE IF NOT EXISTS shape_perceptual_hash_map ( phash_id INTEGER, hash_id INTEGER, PRIMARY KEY ( phash_id, hash_id ) )"  # noqa: E501
            )
            self.execute(
                "CREATE TABLE IF NOT EXISTS shape_vptree ( phash_id INTEGER PRIMARY KEY, parent_id INTEGER, radius INTEGER, inner_id INTEGER, inner_population INTEGER, outer_id INTEGER, outer_population INTEGER )"  # noqa: E501
            )
            # fmt: off
            self.execute(
                "CREATE TABLE IF NOT EXISTS shape_maintenance_branch_regen ( phash_id INTEGER PRIMARY KEY )"
            )  # noqa: E501
            # fmt: on
            self.execute(
                "CREATE TABLE IF NOT EXISTS shape_search_cache ( hash_id INTEGER PRIMARY KEY, searched_distance INTEGER )"  # noqa: E501
            )

            self.execute(
                "CREATE TABLE IF NOT EXISTS phashed_file_queue ( file_hash BLOB_BYTES NOT NULL UNIQUE, phash BLOB_BYTES NOT NULL, PRIMARY KEY ( file_hash, phash ) )"  # noqa: E501
            )

            # Insert the files from the SqliteDict videos table into the hash queue.
            old_videos_data = []
            print(
                "Migrating perceptually hashed videos from the old table.\n"
                "This may take a bit, depending your db length."
            )

            from pickle import loads

            for key, value in self.execute("SELECT key, value FROM videos"):
                # I don't see why value could be None, but if it happens for whatever reason
                # we just want to continue.
                if value is None:
                    continue
                row = loads(bytes(value))  # this is decode function in SqliteDict
                if "perceptual_hash" in row:
                    video_hash = key
                    old_videos_data.append((video_hash, row["perceptual_hash"]))
                    # The farthest search index will not be moved.

            for video_hash, perceptual_hash in old_videos_data:
                # TODO: If these functions change this upgrade may not work! We need to be careful about updating them. # noqa: E501
                #       An upgrade cutoff at some point to prevent bitrot is a good idea, which is what Hydrus does.
                self.add_to_phashed_files_queue(video_hash, perceptual_hash)

            self.set_version("0.7.0")
            # Note: We need to keep re-running get_version so that we can progressively upgrade.
            version = self.get_version()

        # No db changes in this case, just print a nice message that your DB is upgraded.
        if SemanticVersion(version) < SemanticVersion(__version__):
            print_upgrade(version, __version__)

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
