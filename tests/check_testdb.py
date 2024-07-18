import os
from pathlib import Path


def check_testdb_exists():
    """
    Check if the testdb submodule is pulled.
    Throws RuntimeError if it's not updated.
    """
    testdb_dir = Path(__file__).parent / "testdb"
    if len(os.listdir(testdb_dir)) == 0:
        raise RuntimeError("Video hashes dir is missing. Is the testdb submodule pulled?")
