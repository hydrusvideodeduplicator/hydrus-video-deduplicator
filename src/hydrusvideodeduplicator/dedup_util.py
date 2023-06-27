import logging
from datetime import datetime
import time
import os
import contextlib

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
            logging.warning(f"{err_msg} Hash: {file_metadata['hash']}")

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

def cleanup_defunct_processes():
    while True:
        try:
            pid, status = os.waitpid(-1, os.WNOHANG)
            if pid == 0:  # No more defunct processes
                break
        except ChildProcessError:
            break  # No more child processes

import os
import shutil
import subprocess

def get_open_fds() -> int:
    """Get the number of open file descriptors for the current process."""
    lsof_path = shutil.which("lsof")
    if lsof_path is None:
        raise NotImplementedError("Didn't handle unavailable lsof.")
    raw_procs = subprocess.check_output(
        [lsof_path, "-w", "-Ff", "-p", str(os.getpid())]
    )

    def filter_fds(lsof_entry: str) -> bool:
        return lsof_entry.startswith("f") and lsof_entry[1:].isdigit()

    fds = list(filter(filter_fds, raw_procs.decode().split(os.linesep)))
    return len(fds)

@contextlib.contextmanager
def temporary_filename(suffix=None):
  """Context that introduces a temporary file.

  Creates a temporary file, yields its name, and upon context exit, deletes it.
  (In contrast, tempfile.NamedTemporaryFile() provides a 'file' object and
  deletes the file as soon as that file object is closed, so the temporary file
  cannot be safely re-opened by another library or process.)

  Args:
    suffix: desired filename extension (e.g. '.mp4').

  Yields:
    The name of the temporary file.
  """
  import tempfile
  try:
    f = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
    tmp_name = f.name
    f.close()
    yield tmp_name
  finally:
    os.unlink(tmp_name)