import logging
from datetime import datetime
import time
import os
import contextlib
from pathlib import Path
from sqlitedict import SqliteDict
from rich import print as rprint

from hydrusvideodeduplicator.hydrus_api import Client
 
# Given a lexicographically SORTED list of tags, find the tag given a namespace
# TODO: Do binary search since the tags are sorted
def find_tag_in_tags(target_tag_namespace: str, tags: list) -> str:
    namespace_len = len(target_tag_namespace)
    for tag in tags:
        if tag[0:namespace_len] == target_tag_namespace:
            return tag[namespace_len:]
    return ""

# Get the filename from the filename tag if it exists in Hydrus
# This is just used for debugging.
# Note: Hydrus tags and NTFS are case insensitive, but ext4 is not
#       But, this shouldn't really matter since Hydrus stores files with lower case names
# TODO: Clean this up it's a mess
def get_file_names_hydrus(client: Client, file_hashes: list[str]) -> list[str]:
    err_msg = "Cannot get file name from Hydrus."
    result = []
    files_metadata = client.get_file_metadata(hashes=file_hashes, only_return_basic_information=False)
    all_known_tags = "all known tags".encode("utf-8").hex()
    for file_metadata in files_metadata.get("metadata", []):
        # Try to get file extension
        try:
            ext = file_metadata["ext"]
        except KeyError:
            ext = ""
            
        # Try to get the file name
        tag = ""
        try:
            tag_services = file_metadata["tags"]
            tags = tag_services[all_known_tags]["storage_tags"]["0"]
            tag = find_tag_in_tags(target_tag_namespace="filename:", tags=tags)
            # Don't show extension if filename doesn't exist
            if tag != "":
                tag = f"{tag}{ext}"
        except:
            logging.error(f"{err_msg} Hash: {file_metadata['hash']}")

        result.append(tag)

    return result

# Get the oldest file by import time in list of file_metadata 
def get_oldest_imported_file_time(all_files_metadata: list) -> int:
    file_import_times = []
    for video_metadata in all_files_metadata:
        try:
            file_import_times.append(get_file_import_time(video_metadata))
        except KeyError:
            continue
    return min(file_import_times)

# Get the import time of a file from file_metadata request from Hydrus
def get_file_import_time(file_metadata: dict):
    for service in file_metadata["file_services"]["current"].values():
        try:
            if service["name"] == "all local files":
                return service["time_imported"]
        except KeyError:
            continue
    raise KeyError

# From Stack Overflow
# Returns a duration as specified by variable interval
# Functions, except totalDuration, returns [quotient, remainder]
def getDuration(then, now = datetime.now(), interval = "default"):

    if now < then:
        duration = now
        duration_in_s = time.mktime(duration.timetuple())
    else:
        duration = now - then
        duration_in_s = duration.total_seconds() 
    
    def years():
        return divmod(duration_in_s, 31536000) # Seconds in a year=31536000.

    def days(seconds = None):
        return divmod(seconds if seconds != None else duration_in_s, 86400) # Seconds in a day = 86400

    def hours(seconds = None):
        return divmod(seconds if seconds != None else duration_in_s, 3600) # Seconds in an hour = 3600

    def minutes(seconds = None):
        return divmod(seconds if seconds != None else duration_in_s, 60) # Seconds in a minute = 60

    def seconds(seconds = None):
        if seconds != None:
            return divmod(seconds, 1)   
        return duration_in_s

    def totalDuration():
        y = years()
        d = days(y[1]) # Use remainder to calculate next variable
        h = hours(d[1])
        m = minutes(h[1])
        s = seconds(m[1])

        return f"{int(y[0])} years {int(d[0])} days {int(h[0])} hours"

    return {
        'years': int(years()[0]),
        'days': int(days()[0]),
        'hours': int(hours()[0]),
        'minutes': int(minutes()[0]),
        'seconds': int(seconds()),
        'default': totalDuration()
    }[interval]        

from threading import Thread
from threading import Lock
 
# thread safe counter class
class ThreadSafeCounter():
    # constructor
    def __init__(self):
        # initialize counter
        self._counter = 0
        # initialize lock
        self._lock = Lock()
 
    # increment the counter
    def increment(self):
        with self._lock:
            self._counter += 1
 
    # get the counter value
    def value(self):
        with self._lock:
            return self._counter

def database_accessible(db_file: Path | str, tablename: str):
    try:
        with SqliteDict(str(db_file), tablename=tablename, flag="r"):
            return True
    except OSError:
        rprint(f"[red] Database does not exist. Cannot search for duplicates.")
    except RuntimeError: # SqliteDict error when trying to create a table for a DB in read-only mode
        rprint(f"[red] Database does not exist. Cannot search for duplicates.")
    except Exception as exc:
        rprint(f"[red] Could not access database.")
        logging.error(str(exc))
    return False