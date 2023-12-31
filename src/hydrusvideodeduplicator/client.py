from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable
    from typing import TypeAlias

    FileServiceKeys: TypeAlias = list[str]

import hydrusvideodeduplicator.hydrus_api as hydrus_api
import hydrusvideodeduplicator.hydrus_api.utils as hydrus_api_utils

hvdclientlog = logging.getLogger("hvdclient")
hvdclientlog.setLevel(logging.INFO)


class HVDClient:
    def __init__(
        self,
        file_service_keys: FileServiceKeys,
        api_url: str,
        access_key: str,
        verify_cert: bool = False,
    ):
        self.client = hydrus_api.Client(access_key=access_key, api_url=api_url, verify_cert=verify_cert)
        self.file_service_keys = (
            [key for key in file_service_keys if key.strip()]
            if (file_service_keys and file_service_keys is not None)
            else self.get_default_file_service_keys()
        )
        self.verify_file_service_keys()

    def get_video(self, video_hash: str) -> bytes:
        """
        Retrieves a video from Hydrus by the videos hash.

        Returns the video bytes.
        """
        video_response = self.client.get_file(hash_=video_hash)
        video = video_response.content
        return video

    def get_potential_duplicate_count_hydrus(self) -> int:
        return self.client.get_potentials_count(file_service_keys=self.file_service_keys)["potential_duplicates_count"]

    def get_default_file_service_keys(self) -> FileServiceKeys:
        services = self.client.get_services()

        # Set the file service keys to be used for hashing
        # Default is "all local files"
        file_service_keys = [services["all_local_files"][0]["service_key"]]
        return file_service_keys

    def verify_file_service_keys(self) -> None:
        """Verify that the supplied file_service_key is a valid key for a local file service."""
        VALID_SERVICE_TYPES = [hydrus_api.ServiceType.ALL_LOCAL_FILES, hydrus_api.ServiceType.FILE_DOMAIN]
        services = self.client.get_services()

        for file_service_key in self.file_service_keys:
            file_service = services['services'].get(file_service_key)
            if file_service is None:
                raise KeyError(f"Invalid file service key: '{file_service_key}'")

            service_type = file_service.get('type')
            if service_type not in VALID_SERVICE_TYPES:
                raise KeyError("File service key must be a local file service")

    def verify_api_connection(self) -> bool:
        """
        Verify client connection and permissions.

        Throws hydrus_api.APIError if something is wrong.
        """
        hvdclientlog.info(
            (
                f"Client API version: v{self.client.VERSION} "
                f"| Endpoint API version: v{self.client.get_api_version()['version']}"
            )
        )
        return hydrus_api_utils.verify_permissions(self.client, hydrus_api.utils.Permission)

    def retrieve_video_hashes(self, search_tags: Iterable[str]) -> Iterable[str]:
        """
        Retrieve video hashes from Hydrus from a list of search tags.

        Returns video hashes that have those tags.
        """
        all_video_hashes = self.client.search_files(
            tags=search_tags,
            file_service_keys=self.file_service_keys,
            file_sort_type=hydrus_api.FileSortType.FILE_SIZE,
            return_hashes=True,
            file_sort_asc=True,
            return_file_ids=False,
        )["hashes"]
        return all_video_hashes
