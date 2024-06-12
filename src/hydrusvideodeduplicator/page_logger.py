from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any

from .client import HVDClient
from .dedup_util import print_and_log

import logging


def get_page_key(client: HVDClient, page_name: str) -> str | None:
    """Takes the provided page name, and searches through all the pages in the Hydrus client for an appropriate
    page with that name. If there are multiple pages with the same name, one of those pages is chosen randomly."""
    response = client.client.get_pages()
    page_key = find_page_key_from_name(response["pages"], page_name)
    return page_key


def find_page_key_from_name(page: dict[str, Any], page_name: str) -> str | None:
    """Recursive function to search the response JSON provided by the Hydrus API's get_pages call. Because every
    page can potentially contain other pages, a recursive search through the object is necessary. As soon as a
    page is found with the correct page name and page type, that page's page_key is returned."""
    if page["name"].lower() == page_name.lower() and page["page_type"] == 6:
        return page["page_key"]
    elif "pages" in page:
        for subpage in page["pages"]:
            result = find_page_key_from_name(subpage, page_name)
            if result is not None:
                return result
    return None


class HydrusPageLogger:
    """Class to add files to pages in Hydrus."""

    _log = logging.getLogger("HydrusPageLogger")
    _log.setLevel(logging.INFO)

    def __init__(self, client: HVDClient, page_name: str):
        """Page name must exist in Hydrus or an error will occur."""
        self.client = client
        self.page_name = page_name

    def add_failed_video(self, video_hash: str) -> None:
        """Try to add a failed video to the Hydrus page."""
        try:
            page_key = get_page_key(self.client, self.page_name)
            if page_key is None:
                raise Exception("page_key is None.")
        except Exception as e:
            print_and_log(self._log, str(e), logging.ERROR)
            print_and_log(self._log, f"Error when trying to get page key for page name {self.page_name}", logging.ERROR)
            return None

        try:
            self.client.client.add_files_to_page(page_key=page_key, hashes=[video_hash])
        except Exception as e:
            print_and_log(self._log, str(e), logging.ERROR)
            print_and_log(
                self._log,
                f"""Error when trying to add file: '{video_hash}' \
                \nto client page: '{self.page_name}' \
                \nwith page_key: '{page_key}' \
                \nEnsure there is a page in Hydrus named '{self.page_name}' \
                """,
                logging.ERROR,
            )
