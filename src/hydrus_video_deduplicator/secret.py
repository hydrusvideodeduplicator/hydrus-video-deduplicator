from dotenv import load_dotenv
import os 

load_dotenv()
HYDRUS_API_KEY=os.getenv("HYDRUS_API_KEY")
HYDRUS_HOST=os.getenv("HYDRUS_HOST", "http://localhost:45869")
LOCAL_TAG_SERVICE_NAME=os.getenv("LOCAL_TAG_SERVICE_NAME", "my tags")