from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable
    from typing import TypeAlias

    FileServiceKeys: TypeAlias = list[str]
    FileHashes: TypeAlias = Iterable[str]

import hydrusvideodeduplicator.hydrus_api as hydrus_api
import hydrusvideodeduplicator.hydrus_api.utils as hydrus_api_utils


class HVDClient:
    _log = logging.getLogger("HVDClient")
    _log.setLevel(logging.INFO)

    def __init__(
        self,
        file_service_keys: FileServiceKeys | None,
        api_url: str,
        access_key: str,
        verify_cert: str | None,  # None means do not verify SSL.
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
        valid_service_types = [
            hydrus_api.ServiceType.ALL_LOCAL_FILES,
            hydrus_api.ServiceType.FILE_DOMAIN,
        ]
        services = self.client.get_services()

        for file_service_key in self.file_service_keys:
            file_service = services['services'].get(file_service_key)
            if file_service is None:
                raise KeyError(f"Invalid file service key: '{file_service_key}'")

            service_type = file_service.get('type')
            if service_type not in valid_service_types:
                raise KeyError("File service key must be a local file service")

    def verify_api_connection(self) -> bool:
        """
        Verify client connection and permissions.

        Throws hydrus_api.APIError if something is wrong.
        """
        self._log.info(
            (
                f"Client API version: v{self.client.VERSION} "
                f"| Endpoint API version: v{self.client.get_api_version()['version']}"
            )
        )
        return hydrus_api_utils.verify_permissions(self.client, hydrus_api.utils.Permission)

    def get_video_hashes(self, search_tags: Iterable[str]) -> Iterable[str]:
        """
        Get video hashes from Hydrus from a list of search tags.

        Get video hashes that have the given search tags.
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

    def are_files_deleted_hydrus(self, file_hashes: FileHashes) -> dict[str, bool]:
        """
        Check if files are trashed or deleted in Hydrus

        Returns a dictionary of {hash, trashed_or_not}
        """
        videos_metadata = self.client.get_file_metadata(hashes=file_hashes, only_return_basic_information=False)[
            "metadata"
        ]

        result: dict[str, bool] = {}
        for video_metadata in videos_metadata:
            # This should never happen, but it shouldn't break the program if it does
            if "hash" not in video_metadata:
                self._log.error("Hash not found for potentially trashed file.")
                continue
            video_hash = video_metadata["hash"]
            is_deleted: bool = video_metadata.get("is_deleted", False)
            result[video_hash] = is_deleted

        return result
