from __future__ import annotations

import logging
import unittest
from typing import TYPE_CHECKING

from hydrusvideodeduplicator import __about__
from hydrusvideodeduplicator.db import DedupeDB

if TYPE_CHECKING:
    pass

import sqlite3
from pathlib import Path
from tempfile import TemporaryDirectory
import uuid


def somedbdir():
    return str(uuid.uuid4().hex)


class TestDedupeDB(unittest.TestCase):
    log = logging.getLogger(__name__)
    log.setLevel(logging.WARNING)
    logging.basicConfig()

    def setUp(self):
        pass

    def test_set_get_db_dir(self):
        with TemporaryDirectory() as tmpdir:
            db_dir = Path(tmpdir) / somedbdir()
            DedupeDB.set_db_dir(db_dir)
            result = DedupeDB.get_db_dir()
            self.assertEqual(result, db_dir)

    def test_get_db_file_path(self):
        expected = DedupeDB.get_db_dir() / "videohashes.sqlite"
        result = DedupeDB.get_db_file_path()
        self.assertEqual(result, expected)

    def test_create_db(self):
        with TemporaryDirectory() as tmpdir:
            db_dir = Path(tmpdir) / somedbdir()
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
            self.assertEqual(
                set(res.fetchall()),
                set(
                    [
                        ("version",),
                        ("files",),
                        ("phashed_file_queue",),
                        ("shape_maintenance_branch_regen",),
                        ("shape_perceptual_hash_map",),
                        ("shape_perceptual_hashes",),
                        ("shape_search_cache",),
                        ("shape_vptree",),
                    ]
                ),
            )

            # Check the database is queryable (this may be overkill)
            with self.assertRaises(sqlite3.OperationalError):
                _ = cur.execute("SELECT foo FROM files")

            def check_table_columns(table: str, expected_columns: list[str]):
                for column in expected_columns:
                    res = cur.execute(f"SELECT {column} FROM {table}")
                    self.assertEqual(len(res.fetchall()), 0)

            expected_tables = {
                "files": ["hash_id", "file_hash"],
                "phashed_file_queue": ["file_hash", "phash"],
                "shape_maintenance_branch_regen": ["phash_id"],
                "shape_perceptual_hash_map": ["phash_id", "hash_id"],
                "shape_perceptual_hashes": ["phash_id", "phash"],
                "shape_search_cache": ["hash_id", "searched_distance"],
                "shape_vptree": [
                    "phash_id",
                    "parent_id",
                    "radius",
                    "inner_id",
                    "inner_population",
                    "outer_id",
                    "outer_population",
                ],
            }
            for table, cols in expected_tables.items():
                check_table_columns(table, cols)

            # Check version table
            res = cur.execute("SELECT version FROM version")
            expected_version = __about__.__version__
            self.log.info(f"Expected version: {expected_version}")
            self.assertIsNotNone(expected_version)
            self.assertEqual(
                res.fetchall(),
                [
                    (expected_version,),
                ],
            )

            con.close()

    def test_get_version(self):
        with TemporaryDirectory() as tmpdir:
            db_dir = Path(tmpdir) / somedbdir()
            DedupeDB.set_db_dir(db_dir)

            DedupeDB.create_db()

            db = DedupeDB.DedupeDb(db_dir, DedupeDB.get_db_name())
            db.init_connection()
            db.set_version("1.2.3")
            version = db.get_version()
            self.assertEqual(version, "1.2.3")

            db.conn.close()

    def test_semantic_version(self):
        pairs = [("0.1.0", "0.2.0"), ("1.0.1", "1.1.0"), ("1.0.10", "1.1.0")]
        for lhs, rhs in pairs:
            self.assertLess(DedupeDB.SemanticVersion(lhs), DedupeDB.SemanticVersion(rhs), f"{lhs} not less than {rhs}")

        pairs = [("0.0.0", "0.0.0"), ("1.0.0", "1.0.0"), ("0.1.0", "0.1.0")]
        for lhs, rhs in pairs:
            self.assertLessEqual(
                DedupeDB.SemanticVersion(lhs), DedupeDB.SemanticVersion(rhs), f"{lhs} not less than {rhs}"
            )

        pairs = [("1.0.0", "0.0.100"), ("10.0.0", "1.100.0"), ("0.0.1", "0.0.0")]
        for lhs, rhs in pairs:
            self.assertGreaterEqual(
                DedupeDB.SemanticVersion(lhs), DedupeDB.SemanticVersion(rhs), f"{lhs} not less than {rhs}"
            )


if __name__ == "__main__":
    unittest.main(module="test_db")
