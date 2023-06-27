import os
from pathlib import Path
from platform import uname

from dotenv import load_dotenv
from appdirs import AppDirs

load_dotenv()
HYDRUS_API_KEY=os.getenv("HYDRUS_API_KEY")

def in_wsl() -> bool:
    return 'microsoft-standard' in uname().release

_DEFAULT_IP = "localhost"
_DEFAULT_PORT = "45869"
# If you're in WSL you probably want to connect to your Windows Hydrus Client by default
if in_wsl():
    from socket import gethostname
    _DEFAULT_IP = f"{gethostname()}.local"

HYDRUS_API_URL=os.getenv("HYDRUS_API_URL", f"https://{_DEFAULT_IP}:{_DEFAULT_PORT}")

# Service name of where to store perceptual hash tag for video files
HYDRUS_LOCAL_TAG_SERVICE_NAME=os.getenv("HYDRUS_LOCAL_TAG_SERVICE_NAME", "my tags")

# ~/.local/share/hydrusvideodeduplicator/ on Linux
DEDUP_DATABASE_DIR=AppDirs("hydrusvideodeduplicator").user_data_dir
DEDUP_DATABASE_DIR=os.getenv("DEDUP_DATABASE_DIR", DEDUP_DATABASE_DIR)
DEDUP_DATABASE_DIR=Path(DEDUP_DATABASE_DIR)

DEDUP_DATABASE_NAME=os.getenv("DEDUP_DATABASE_NAME", "videohashes")
DEDUP_DATABASE_FILE=Path(DEDUP_DATABASE_DIR, f"{DEDUP_DATABASE_NAME}.sqlite")

REQUESTS_CA_BUNDLE=os.getenv("REQUESTS_CA_BUNDLE")