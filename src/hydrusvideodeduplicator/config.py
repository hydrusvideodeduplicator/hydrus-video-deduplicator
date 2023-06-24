import os
from pathlib import Path

from dotenv import load_dotenv
from appdirs import AppDirs

load_dotenv()
HYDRUS_API_KEY=os.getenv("HYDRUS_API_KEY")
HYDRUS_API_URL=os.getenv("HYDRUS_API_URL", "http://localhost:45869")
# Service name of where to store perceptual hash tag for video files
HYDRUS_LOCAL_TAG_SERVICE_NAME=os.getenv("HYDRUS_LOCAL_TAG_SERVICE_NAME", "my tags")

# ~/.local/share/hydrusvideodeduplicator/ on Linux
DEDUP_DATABASE_DIR=AppDirs("hydrusvideodeduplicator").user_data_dir
DEDUP_DATABASE_DIR=os.getenv("DEDUP_DATABASE_DIR", DEDUP_DATABASE_DIR)
DEDUP_DATABASE_DIR=Path(DEDUP_DATABASE_DIR)

DEDUP_DATABASE_NAME=os.getenv("DEDUP_DATABASE_NAME", "hashdb")
DEDUP_DATABASE_FILE=Path(DEDUP_DATABASE_DIR, DEDUP_DATABASE_NAME)
