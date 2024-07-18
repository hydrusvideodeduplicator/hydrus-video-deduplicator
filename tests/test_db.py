from __future__ import annotations

import logging
import unittest
from typing import TYPE_CHECKING

from hydrusvideodeduplicator.db import DedupeDB

if TYPE_CHECKING:
    pass

import sqlite3
from pathlib import Path
from tempfile import TemporaryDirectory


class TestDedupeDB(unittest.TestCase):
    log = logging.getLogger(__name__)
    log.setLevel(logging.WARNING)
    logging.basicConfig()

    def setUp(self):
        pass

    def test_set_get_db_dir(self):
        with TemporaryDirectory() as tmpdir:
            db_dir = Path(tmpdir) / "somedbdir"
            DedupeDB.set_db_dir(db_dir)
            result = DedupeDB.get_db_dir()
            self.assertEqual(result, db_dir)

    def test_get_db_file_path(self):
        expected = DedupeDB.get_db_dir() / "videohashes.sqlite"
        result = DedupeDB.get_db_file_path()
        self.assertEqual(result, expected)

    def test_create_db(self):
        with TemporaryDirectory() as tmpdir:
            db_dir = Path(tmpdir) / "somedbdir"
            DedupeDB.set_db_dir(db_dir)

            DedupeDB.create_db()

            # Check file exists
            expected = DedupeDB.get_db_dir() / "videohashes.sqlite"
            self.assertTrue(Path.exists(expected))
            self.assertTrue(Path.is_file(expected))

            # Check database

            con = sqlite3.connect(DedupeDB.get_db_file_path(), uri=True)  # uri is read-only
            cur = con.cursor()
            # Check tables
            res = cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
            self.assertEqual(res.fetchall(), [("videos",)])

            # Check the database is queryable (this may be overkill)
            with self.assertRaises(sqlite3.OperationalError):
                _ = cur.execute("SELECT key1 FROM videos")

            # Check videos table
            res = cur.execute("SELECT key FROM videos")
            self.assertEqual(len(res.fetchall()), 0)
            res = cur.execute("SELECT value FROM videos")
            self.assertEqual(len(res.fetchall()), 0)


if __name__ == "__main__":
    unittest.main(module="test_db")
