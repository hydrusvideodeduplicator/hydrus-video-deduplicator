import os
from dotenv import load_dotenv

load_dotenv()
HYDRUS_API_KEY=os.getenv("HYDRUS_API_KEY")
HYDRUS_API_URL=os.getenv("HYDRUS_API_URL", "http://localhost:45869")
# Service name of where to store perceptual hash tag for video files
HYDRUS_LOCAL_TAG_SERVICE_NAME=os.getenv("HYDRUS_LOCAL_TAG_SERVICE_NAME", "my tags")
# Perceptual hash tag namespace
DEDUP_DATABASE_NAME=os.getenv("DEDUP_DATABASE_NAME", "thedb")