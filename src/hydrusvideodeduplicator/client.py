from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable
    from typing import TypeAlias

    FileServiceKeys: TypeAlias = list[str]
    FileHashes: TypeAlias = Iterable[str]

from urllib3.connection import NewConnectionError

import hydrusvideodeduplicator.hydrus_api as hydrus_api
import hydrusvideodeduplicator.hydrus_api.utils as hydrus_api_utils


class ClientAPIException(Exception):
    """Base exception for HVDClient failures."""

    def __init__(self, pretty_msg: str = "", real_msg: str = ""):
        super().__init__(real_msg)
        self.pretty_msg = pretty_msg


class FailedHVDClientConnection(ClientAPIException):
    """Raise for when HVDClient fails to connect."""


class InsufficientPermissions(ClientAPIException):
    """Raise for when Hydrus API key permissions are insufficient."""


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
            file_service = services["services"].get(file_service_key)
            if file_service is None:
                raise KeyError(f"Invalid file service key: '{file_service_key}'")

            service_type = file_service.get("type")
            if service_type not in valid_service_types:
                raise KeyError("File service key must be a local file service")

    def get_hydrus_api_version(self) -> str:
        api_version_req = self.client.get_api_version()
        if "version" not in api_version_req:
            raise ClientAPIException(
                "'version' is not in the Hydrus API version response. Something is terribly wrong."
            )
        return api_version_req["version"]

    def get_api_version(self) -> int:
        """Get the API version of the API module used to connect to Hydrus."""
        return self.client.VERSION

    def verify_permissions(self):
        """
        Verify API permissions. Throws InsufficientPermissions if permissions are insufficient, otherwise nothing.

        Throws ClientAPIException on Hydrus API failure.
        """
        try:
            permissions = hydrus_api_utils.verify_permissions(self.client, hydrus_api.utils.Permission)
        except hydrus_api.HydrusAPIException as exc:
            raise ClientAPIException("An error has occurred while trying to verify permissions.", str(exc))

        if not permissions:
            raise ClientAPIException("Insufficient Hydrus permissions.")

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


def create_client(*args) -> HVDClient:
    """
    Try to create a client and connect to Hydrus.

    Throws FailedHVDClientConnection on failure.

    TODO: Try to connect with https first and then fallback to http with a strong warning (GH #58)
    """
    connection_failed = True
    try:
        hvdclient = HVDClient(*args)
    except hydrus_api.InsufficientAccess as exc:
        pretty_msg = "Invalid Hydrus API key."
        real_msg = str(exc)
    except hydrus_api.DatabaseLocked as exc:
        pretty_msg = "Hydrus database is locked. Try again later."
        real_msg = str(exc)
    except hydrus_api.ServerError as exc:
        pretty_msg = "Unknown Server Error."
        real_msg = str(exc)
    except hydrus_api.APIError as exc:
        pretty_msg = "API Error"
        real_msg = str(exc)
    except (NewConnectionError, hydrus_api.ConnectionError, hydrus_api.HydrusAPIException) as exc:
        # Probably SSL error
        if "SSL" in str(exc):
            pretty_msg = "Failed to connect to Hydrus. SSL certificate verification failed."
        # Probably tried using http instead of https when client is https
        elif "Connection aborted" in str(exc):
            pretty_msg = (
                "Failed to connect to Hydrus.\nDoes your Hydrus Client API 'http/https' setting match your API URL?"
            )
        elif "Connection refused" in str(exc):
            pretty_msg = """Failed to connect to Hydrus.
Is your Hydrus instance running?
Is the client API enabled? (hint: services -> manage services -> client api)
Is your port correct? (hint: default is 45869)
            """
        else:
            pretty_msg = "Failed to connect to Hydrus. Unknown exception occurred."
        real_msg = str(exc)
    else:
        connection_failed = False

    if connection_failed:
        raise FailedHVDClientConnection(pretty_msg, real_msg)

    return hvdclient
