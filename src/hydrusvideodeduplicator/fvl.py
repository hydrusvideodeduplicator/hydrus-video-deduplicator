from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass

from logging import Logger
from datetime import datetime
from .config import FAILED_VIDEOS_LOG_FILE

import hydrusvideodeduplicator.hydrus_api as hydrus_api


class FailedVideoLogger:
    """Utility object for logging any videos that can't be perceptually hashed for whatever reason. Always logs
    failed videos (with exception info) to .txt file, and optionally sends failed videos to a specified page in the
    Hydrus Client."""

    def __init__(self, client: hydrus_api, hydlog: Logger, page_name: str | None):
        self.client = client
        self.hydlog = hydlog
        self.page_name = page_name
        self.page_key = self._get_page_key() if page_name is not None else None
        self.failed_video_list = []
        self._init_log_file()

    def log(self, video_hash: str, exception: Exception) -> None:
        """Logs provided video hash to .txt log file, and also to Hydrus page if one was provided"""
        self.failed_video_list.append(video_hash)
        self._add_to_hydrus_page(video_hash)
        self._add_to_log_file(video_hash, exception)

    def finish(self) -> None:
        """Appends the final information to the .txt log file"""
        with open(FAILED_VIDEOS_LOG_FILE, "a", encoding="utf-8") as log_file:
            if len(self.failed_video_list) == 0:
                log_file.write("No videos failed during the phashing process.")
            else:
                log_file.write("List of all failed video hashes (can be pasted into Hydrus):\n")
                for video_hash in self.failed_video_list:
                    log_file.write(video_hash + "\n")

    def _add_to_hydrus_page(self, video_hash: str) -> None:
        """Adds provided video to Hydrus Client file page, if a valid file page name was provided via the CLI"""
        if self.page_key is None:
            return

        try:
            self.client.add_files_to_page(page_key=self.page_key, hashes=[video_hash])
        except Exception as e:
            self.hydlog.debug(
                f"Error when trying to add file {video_hash} to client page {self.page_name} (key='{self.page_key}')"
            )
            self.hydlog.debug(e)

    @staticmethod
    def _add_to_log_file(video_hash: str, exception: Exception) -> None:
        """Adds provided video hash to the .txt log file, along with any information provided by the Exception thrown
        when the video failed phashing"""
        with open(FAILED_VIDEOS_LOG_FILE, "a", encoding="utf-8") as log_file:
            log_file.writelines([f"video hash: {video_hash}\n", "Failed with exception:\n", str(exception) + "\n\n"])

    def _get_page_key(self) -> str | None:
        """Takes the provided page name, and searches through all the pages in the Hydrus client for an appropriate
        page with that name. If there are multiple pages with the same name, one of those pages is chosen
        pseudo-randomly."""
        response = self.client.get_pages()
        page_key = self._find_page_key_from_name(response["pages"])

        if page_key is None:
            self.hydlog.info(
                f"Warning: could not find file search page for name matching '{self.page_name}'. "
                f"Failed files will not be sent to Hydrus client page"
            )

        return page_key

    def _find_page_key_from_name(self, page: dict[str, any]) -> str | None:
        """Recursive function to search the response JSON provided by the Hydrus API's get_pages call. Because every
        page can potentially contain other pages, a recursive search through the object is necessary. As soon as a
        page is found with the correct page name and page type, that page's page_key is returned."""
        if page["name"].lower() == self.page_name.lower() and page["page_type"] == 6:
            return page["page_key"]
        elif "pages" in page:
            for subpage in page["pages"]:
                result = self._find_page_key_from_name(subpage)
                if result is not None:
                    return result
        return None

    @staticmethod
    def _init_log_file() -> None:
        """Initializes the .txt log file, overwriting any leftover contents from prior runs"""
        with open(FAILED_VIDEOS_LOG_FILE, "w", encoding="utf-8") as log_file:
            log_file.write("===== Log of Videos That Failed PHashing Process =====\n")
            log_file.write(f"Runtime start: {datetime.now()}\n\n")
