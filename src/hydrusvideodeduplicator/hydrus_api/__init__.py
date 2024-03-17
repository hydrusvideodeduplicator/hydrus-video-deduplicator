# Copyright (C) 2023 cryzed
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import enum
import json
import os
import typing as T
import collections.abc as abc
import warnings

import requests

__version__ = "5.0.1"

DEFAULT_API_URL = "http://127.0.0.1:45869/"
HYDRUS_METADATA_ENCODING = "utf-8"
AUTHENTICATION_TIMEOUT_CODE = 419


# Customize IntEnum, so we can just do str(Enum.member) to get the string representation of its value unmodified,
# without users having to access .value explicitly
class _StringableIntEnum(enum.IntEnum):
    def __str__(self) -> str:
        return str(self.value)


# The client should accept all objects that either support the iterable or mapping protocol. We must ensure that objects
# are either lists or dicts, so Python's json module can handle them
class _ABCJSONEncoder(json.JSONEncoder):
    def default(self, object_: T.Any) -> T.Any:
        if isinstance(object_, abc.Mapping):
            return dict(object_)
        if isinstance(object_, abc.Iterable):
            return list(object_)
        return super().default(object_)


# This is public so other code can import it to annotate their own types
class BinaryFileLike(T.Protocol):
    def read(self) -> bytes: ...


class HydrusAPIException(Exception):
    pass


class ConnectionError(HydrusAPIException, requests.ConnectTimeout):
    pass


class APIError(HydrusAPIException):
    def __init__(self, response: requests.Response) -> None:
        super().__init__(response.text)
        self.response = response


class MissingParameter(APIError):
    pass


class InsufficientAccess(APIError):
    pass


class DatabaseLocked(APIError):
    pass


class ServerError(APIError):
    pass


class DeleteLocked(APIError):
    pass


@enum.unique
class Permission(_StringableIntEnum):
    IMPORT_URLS = 0
    IMPORT_FILES = 1
    ADD_TAGS = 2
    SEARCH_FILES = 3
    MANAGE_PAGES = 4
    MANAGE_COOKIES = 5
    MANAGE_DATABASE = 6
    ADD_NOTES = 7
    MANAGE_FILE_RELATIONSHIPS = 8
    EDIT_FILE_RATINGS = 9


@enum.unique
class URLType(_StringableIntEnum):
    POST_URL = 0
    FILE_URL = 2
    GALLERY_URL = 3
    WATCHABLE_URL = 4
    UNKNOWN_URL = 5


@enum.unique
class ImportStatus(_StringableIntEnum):
    IMPORTABLE = 0
    SUCCESS = 1
    EXISTS = 2
    PREVIOUSLY_DELETED = 3
    FAILED = 4
    VETOED = 7


@enum.unique
class TagAction(_StringableIntEnum):
    ADD = 0
    DELETE = 1
    PEND = 2
    RESCIND_PENDING = 3
    PETITION = 4
    RESCIND_PETITION = 5


@enum.unique
class TagStatus(_StringableIntEnum):
    CURRENT = 0
    PENDING = 1
    DELETED = 2
    PETITIONED = 3


@enum.unique
class PageType(_StringableIntEnum):
    GALLERY_DOWNLOADER = 1
    SIMPLE_DOWNLOADER = 2
    HARD_DRIVE_IMPORT = 3
    PETITIONS = 5
    FILE_SEARCH = 6
    URL_DOWNLOADER = 7
    DUPLICATES = 8
    THREAD_WATCHER = 9
    PAGE_OF_PAGES = 10


@enum.unique
class PageState(_StringableIntEnum):
    READY = 0
    INITIALIZING = 1
    SEARCHING = 2
    SEARCH_CANCELLED = 3


@enum.unique
class FileSortType(_StringableIntEnum):
    FILE_SIZE = 0
    DURATION = 1
    IMPORT_TIME = 2
    FILE_TYPE = 3
    RANDOM = 4
    WIDTH = 5
    HEIGHT = 6
    RATIO = 7
    NUMBER_OF_PIXELS = 8
    NUMBER_OF_TAGS = 9
    NUMBER_OF_MEDIA_VIEWS = 10
    TOTAL_MEDIA_VIEWTIME = 11
    APPROXIMATE_BITRATE = 12
    HAS_AUDIO = 13
    MODIFIED_TIME = 14
    FRAMERATE = 15
    NUMBER_OF_FRAMES = 16


@enum.unique
class ServiceType(_StringableIntEnum):
    TAG_REPOSITORY = 0
    FILE_REPOSITORY = 1
    FILE_DOMAIN = 2
    TAG_DOMAIN = 5
    NUMBERICAL_RATING = 6
    LIKE_DISLIKE_RATING = 7
    ALL_KNOWN_TAGS = 10
    ALL_KNOWN_FILES = 11
    LOCAL_BOORU = 12
    IPFS = 13
    TRASH = 14
    ALL_LOCAL_FILES = 15
    FILE_NOTES = 17
    CLIENT_API = 18
    ALL_DELETED_FILES = 19
    LOCAL_UPDATES = 20
    ALL_MY_FILES = 21
    SERVER_ADMINISTRATION = 99


@enum.unique
class NoteConflictResolution(_StringableIntEnum):
    REPLACE = 0
    IGNORE = 1
    APPEND = 2
    RENAME = 3


@enum.unique
class DuplicateStatus(_StringableIntEnum):
    POTENTIAL_DUPLICATES = 0
    FALSE_POSITIVES = 1
    ALTERNATES = 3
    DUPLICATES = 8


class Client:
    VERSION = 56

    # Access Management
    _GET_API_VERSION_PATH = "/api_version"
    _REQUEST_NEW_PERMISSIONS_PATH = "/request_new_permissions"
    _GET_SESSION_KEY_PATH = "/session_key"
    _VERIFY_ACCESS_KEY_PATH = "/verify_access_key"
    _GET_SERVICE_PATH = "/get_service"
    _GET_SERVICES_PATH = "/get_services"

    # Adding Files
    _ADD_FILE_PATH = "/add_files/add_file"
    _DELETE_FILES_PATH = "/add_files/delete_files"
    _UNDELETE_FILES_PATH = "/add_files/undelete_files"
    _ARCHIVE_FILES_PATH = "/add_files/archive_files"
    _UNARCHIVE_FILES_PATH = "/add_files/unarchive_files"
    _GENERATE_HASHES_PATH = "/add_files/generate_hashes"

    # Adding Tags
    _CLEAN_TAGS_PATH = "/add_tags/clean_tags"
    _SEARCH_TAGS_PATH = "/add_tags/search_tags"
    _ADD_TAGS_PATH = "/add_tags/add_tags"
    _GET_SIBLINGS_AND_PARENTS_PATH = "/add_tags/get_siblings_and_parents"

    # Adding URLs
    _GET_URL_FILES_PATH = "/add_urls/get_url_files"
    _GET_URL_INFO_PATH = "/add_urls/get_url_info"
    _ADD_URL_PATH = "/add_urls/add_url"
    _ASSOCIATE_URL_PATH = "/add_urls/associate_url"

    # Adding Notes
    _SET_NOTES_PATH = "/add_notes/set_notes"
    _DELETE_NOTES_PATH = "/add_notes/delete_notes"

    # Managing Cookies and HTTP Headers
    _GET_COOKIES_PATH = "/manage_cookies/get_cookies"
    _SET_COOKIES_PATH = "/manage_cookies/set_cookies"
    _SET_HEADERS_PATH = "/manage_headers/set_headers"
    _SET_USER_AGENT_PATH = "/manage_headers/set_user_agent"  # Deprecated

    # Managing Pages
    _GET_PAGES_PATH = "/manage_pages/get_pages"
    _GET_PAGE_INFO_PATH = "/manage_pages/get_page_info"
    _ADD_FILES_TO_PAGE_PATH = "/manage_pages/add_files"
    _FOCUS_PAGE_PATH = "/manage_pages/focus_page"
    _REFRESH_PAGE_PATH = "/manage_pages/refresh_page"

    # Searching and Fetching Files
    _SEARCH_FILES_PATH = "/get_files/search_files"
    _FILE_HASHES_PATH = "/get_files/file_hashes"
    _GET_FILE_METADATA_PATH = "/get_files/file_metadata"
    _GET_FILE_PATH = "/get_files/file"
    _GET_THUMBNAIL_PATH = "/get_files/thumbnail"
    _GET_RENDER_PATH = "/get_files/render"

    # Managing the Database
    _LOCK_DATABASE_PATH = "/manage_database/lock_on"
    _UNLOCK_DATABASE_PATH = "/manage_database/lock_off"
    _MR_BONES_PATH = "/manage_database/mr_bones"
    _GET_CLIENT_OPTIONS_PATH = "/manage_database/get_client_options"

    # Managing File Relationships
    _GET_FILE_RELATIONSHIPS_PATH = "/manage_file_relationships/get_file_relationships"
    _GET_POTENTIALS_COUNT_PATH = "/manage_file_relationships/get_potentials_count"
    _GET_POTENTIAL_PAIRS_PATH = "/manage_file_relationships/get_potential_pairs"
    _GET_RANDOM_POTENTIALS_PATH = "/manage_file_relationships/get_random_potentials"
    _SET_FILE_RELATIONSHIPS_PATH = "/manage_file_relationships/set_file_relationships"
    _SET_KINGS_PATH = "/manage_file_relationships/set_kings"

    # Editing File Ratings
    _SET_RATING_PATH = "/edit_ratings/set_rating"

    def __init__(
        self,
        access_key: T.Optional[str] = None,
        api_url: str = DEFAULT_API_URL,
        session: T.Optional[requests.Session] = None,
        verify_cert: T.Optional[str] = None,  # Path to cert
    ) -> None:
        """
        See https://hydrusnetwork.github.io/hydrus/client_api.html for documentation.
        """

        self.access_key = access_key
        self.api_url = api_url.rstrip("/")
        self._verify_cert = verify_cert
        self.session = session or requests.Session()

    def _api_request(self, method: str, path: str, **kwargs: T.Any) -> requests.Response:
        if self.access_key is not None:
            kwargs.setdefault("headers", {}).update({"Hydrus-Client-API-Access-Key": self.access_key})

        # Make sure we use our custom JSONEncoder that can serialize all objects that implement the iterable or mapping
        # protocol
        json_data = kwargs.pop("json", None)
        if json_data is not None:
            kwargs["data"] = json.dumps(json_data, cls=_ABCJSONEncoder)
            # Since we aren't using the json keyword-argument, we have to set the Content-Type manually
            kwargs["headers"]["Content-Type"] = "application/json"

        if self._verify_cert is None:
            kwargs["verify"] = False
            requests.packages.urllib3.disable_warnings()
        else:
            kwargs["verify"] = self._verify_cert

        try:
            response = self.session.request(method, self.api_url + path, **kwargs)
        except requests.RequestException as error:
            # Re-raise connection and timeout errors as hydrus.ConnectionErrors so these are more easy to handle for
            # client applications
            raise ConnectionError(*error.args)

        try:
            response.raise_for_status()
        except requests.HTTPError:
            if response.status_code == requests.codes.bad_request:
                raise MissingParameter(response)
            elif response.status_code in {
                requests.codes.unauthorized,
                requests.codes.forbidden,
                AUTHENTICATION_TIMEOUT_CODE,
            }:
                raise InsufficientAccess(response)
            elif response.status_code == requests.codes.service_unavailable:
                raise DatabaseLocked(response)
            elif response.status_code == requests.codes.server_error:
                raise ServerError(response)
            elif response.status_code == requests.codes.conflict:
                raise DeleteLocked(response)

            raise APIError(response)

        return response

    def get_api_version(self) -> dict[str, T.Any]:
        response = self._api_request("GET", self._GET_API_VERSION_PATH)
        return response.json()

    def request_new_permissions(
        self, name: str, permissions: abc.Iterable[T.Union[int, Permission]]
    ) -> dict[str, T.Any]:
        response = self._api_request(
            "GET",
            self._REQUEST_NEW_PERMISSIONS_PATH,
            params={"name": name, "basic_permissions": json.dumps(permissions, cls=_ABCJSONEncoder)},
        )
        return response.json()

    def get_session_key(self) -> dict[str, T.Any]:
        response = self._api_request("GET", self._GET_SESSION_KEY_PATH)
        return response.json()

    def verify_access_key(self) -> dict[str, T.Any]:
        response = self._api_request("GET", self._VERIFY_ACCESS_KEY_PATH)
        return response.json()

    def get_service(
        self, service_name: T.Optional[str] = None, service_key: T.Optional[str] = None
    ) -> dict[str, T.Any]:
        if service_name is None and service_key is None:
            raise ValueError("At least one of service_name, service_key is required")

        payload = {}
        if service_name is not None:
            payload["service_name"] = service_name
        elif service_key is not None:
            payload["service_key"] = service_key

        response = self._api_request("GET", self._GET_SERVICE_PATH, params=payload)
        return response.json()

    def get_services(self) -> dict[str, T.Any]:
        response = self._api_request("GET", self._GET_SERVICES_PATH)
        return response.json()

    def add_file(self, path_or_file: T.Union[str, os.PathLike, BinaryFileLike]) -> dict[str, T.Any]:
        if isinstance(path_or_file, (str, os.PathLike)):
            response = self._api_request("POST", self._ADD_FILE_PATH, json={"path": os.fspath(path_or_file)})
        else:
            response = self._api_request(
                "POST",
                self._ADD_FILE_PATH,
                data=path_or_file.read(),
                headers={"Content-Type": "application/octet-stream"},
            )

        return response.json()

    def delete_files(
        self,
        hashes: T.Optional[abc.Iterable[str]] = None,
        file_ids: T.Optional[abc.Iterable[int]] = None,
        file_service_keys: T.Optional[abc.Iterable[str]] = None,
        deleted_file_service_keys: T.Optional[abc.Iterable[str]] = None,
        reason: T.Optional[str] = None,
    ) -> None:
        if hashes is None and file_ids is None:
            raise ValueError("At least one of hashes, file_ids is required")

        payload: dict[str, T.Any] = {}
        if hashes is not None:
            payload["hashes"] = hashes
        if file_ids is not None:
            payload["file_ids"] = file_ids
        if file_service_keys is not None:
            payload["file_service_keys"] = file_service_keys
        if deleted_file_service_keys is not None:
            payload["deleted_file_service_keys"] = deleted_file_service_keys
        if reason is not None:
            payload["reason"] = reason

        self._api_request("POST", self._DELETE_FILES_PATH, json=payload)

    def undelete_files(
        self,
        hashes: T.Optional[abc.Iterable[str]] = None,
        file_ids: T.Optional[abc.Iterable[int]] = None,
        file_service_keys: T.Optional[abc.Iterable[str]] = None,
        deleted_file_service_keys: T.Optional[abc.Iterable[str]] = None,
    ) -> None:
        if hashes is None and file_ids is None:
            raise ValueError("At least one of hashes, file_ids is required")

        payload: dict[str, T.Any] = {}
        if hashes is not None:
            payload["hashes"] = hashes
        if file_ids is not None:
            payload["file_ids"] = file_ids
        if file_service_keys is not None:
            payload["file_service_keys"] = file_service_keys
        if deleted_file_service_keys is not None:
            payload["deleted_file_service_keys"] = deleted_file_service_keys

        self._api_request("POST", self._UNDELETE_FILES_PATH, json=payload)

    def archive_files(
        self, hashes: T.Optional[abc.Iterable[str]] = None, file_ids: T.Optional[abc.Iterable[int]] = None
    ) -> None:
        if hashes is None and file_ids is None:
            raise ValueError("At least one of hashes, file_ids is required")

        payload: dict[str, T.Any] = {}
        if hashes is not None:
            payload["hashes"] = hashes
        if file_ids is not None:
            payload["file_ids"] = file_ids

        self._api_request("POST", self._ARCHIVE_FILES_PATH, json=payload)

    def unarchive_files(
        self, hashes: T.Optional[abc.Iterable[str]] = None, file_ids: T.Optional[abc.Iterable[int]] = None
    ) -> None:
        if hashes is None and file_ids is None:
            raise ValueError("At least one of hashes, file_ids is required")

        payload: dict[str, T.Any] = {}
        if hashes is not None:
            payload["hashes"] = hashes
        if file_ids is not None:
            payload["file_ids"] = file_ids

        self._api_request("POST", self._UNARCHIVE_FILES_PATH, json=payload)

    def generate_hashes(self, path: str | os.PathLike) -> dict[str, T.Any]:
        if isinstance(path, os.PathLike):
            path = str(path)

        response = self._api_request("POST", self._GENERATE_HASHES_PATH, json={"path": path})
        return response.json()

    def get_url_files(self, url: str, doublecheck_file_system: T.Optional[bool] = None) -> dict[str, T.Any]:
        payload = {"url": url}
        if doublecheck_file_system is not None:
            payload["doublecheck_file_system"] = json.dumps(doublecheck_file_system)

        response = self._api_request("GET", self._GET_URL_FILES_PATH, params=payload)
        return response.json()

    def get_url_info(self, url: str) -> dict[str, T.Any]:
        response = self._api_request("GET", self._GET_URL_INFO_PATH, params={"url": url})
        return response.json()

    def add_url(
        self,
        url: str,
        destination_page_key: T.Optional[str] = None,
        destination_page_name: T.Optional[str] = None,
        show_destination_page: T.Optional[bool] = None,
        service_keys_to_additional_tags: T.Optional[abc.Mapping[str, abc.Iterable[str]]] = None,
        filterable_tags: T.Optional[abc.Iterable[str]] = None,
    ) -> dict[str, str]:
        if destination_page_key is not None and destination_page_name is not None:
            raise ValueError("Exactly one of destination_page_key, destination_page_name is required")

        payload: dict[str, T.Any] = {"url": url}
        if destination_page_key is not None:
            payload["destination_page_key"] = destination_page_key
        if destination_page_name is not None:
            payload["destination_page_name"] = destination_page_name
        if show_destination_page is not None:
            payload["show_destination_page"] = show_destination_page
        if service_keys_to_additional_tags is not None:
            payload["service_keys_to_additional_tags"] = service_keys_to_additional_tags
        if filterable_tags is not None:
            payload["filterable_tags"] = filterable_tags

        response = self._api_request("POST", self._ADD_URL_PATH, json=payload)
        return response.json()

    def associate_url(
        self,
        hashes: T.Optional[abc.Iterable[str]] = None,
        file_ids: T.Optional[abc.Iterable[int]] = None,
        urls_to_add: T.Optional[abc.Iterable[str]] = None,
        urls_to_delete: T.Optional[abc.Iterable[str]] = None,
    ) -> None:
        if hashes is None and file_ids is None:
            raise ValueError("At least one of hashes, file_ids is required")
        if urls_to_add is None and urls_to_delete is None:
            raise ValueError("At least one of urls_to_add, urls_to_delete is required")

        payload: dict[str, T.Any] = {}
        if hashes is not None:
            payload["hashes"] = hashes
        if file_ids is not None:
            payload["file_ids"] = file_ids
        if urls_to_add is not None:
            urls_to_add = urls_to_add
            payload["urls_to_add"] = urls_to_add
        if urls_to_delete is not None:
            urls_to_delete = urls_to_delete
            payload["urls_to_delete"] = urls_to_delete

        self._api_request("POST", self._ASSOCIATE_URL_PATH, json=payload)

    def clean_tags(self, tags: abc.Iterable[str]) -> list[str]:
        response = self._api_request(
            "GET", self._CLEAN_TAGS_PATH, params={"tags": json.dumps(tags, cls=_ABCJSONEncoder)}
        )
        return response.json()

    def search_tags(
        self,
        search: str,
        tag_service_key: str,
        file_service_keys: T.Optional[abc.Iterable[str]] = None,
        deleted_file_service_keys: T.Optional[abc.Iterable[str]] = None,
        tag_display_type: T.Optional[str] = None,
    ) -> list[dict[str, T.Union[str, int]]]:
        payload = {"search": search, "tag_service_key": tag_service_key}
        if file_service_keys is not None:
            payload["file_service_keys"] = json.dumps(file_service_keys, cls=_ABCJSONEncoder)
        if deleted_file_service_keys is not None:
            payload["deleted_file_service_keys"] = json.dumps(deleted_file_service_keys, cls=_ABCJSONEncoder)
        if tag_display_type is not None:
            payload["tag_display_type"] = tag_display_type

        response = self._api_request("GET", self._SEARCH_TAGS_PATH, params=payload)
        return response.json()

    def add_tags(
        self,
        hashes: T.Optional[abc.Iterable[str]] = None,
        file_ids: T.Optional[abc.Iterable[int]] = None,
        service_keys_to_tags: T.Optional[abc.Mapping[str, abc.Iterable[str]]] = None,
        service_keys_to_actions_to_tags: T.Optional[
            abc.Mapping[str, abc.Mapping[T.Union[int, TagAction], abc.Iterable[str]]]
        ] = None,
    ) -> None:
        if hashes is None and file_ids is None:
            raise ValueError("At least one of hashes, file_ids is required")
        if service_keys_to_tags is None and service_keys_to_actions_to_tags is None:
            raise ValueError("At least one of service_keys_to_tags, service_keys_to_actions_to_tags is required")

        payload: dict[str, T.Any] = {}
        if hashes is not None:
            payload["hashes"] = hashes
        if file_ids is not None:
            payload["file_ids"] = file_ids
        if service_keys_to_tags is not None:
            payload["service_keys_to_tags"] = service_keys_to_tags
        if service_keys_to_actions_to_tags is not None:
            payload["service_keys_to_actions_to_tags"] = service_keys_to_actions_to_tags

        self._api_request("POST", self._ADD_TAGS_PATH, json=payload)

    def set_rating(
        self,
        rating_service_key: str,
        rating: bool | int | None,
        hashes: T.Optional[abc.Iterable[str]] = None,
        file_ids: T.Optional[abc.Iterable[int]] = None,
    ) -> None:
        if hashes is None and file_ids is None:
            raise ValueError("At least one of hashes, file_ids is required")

        payload: dict[str, T.Any] = {"rating_service_key": rating_service_key, "rating": rating}
        if hashes is not None:
            payload["hashes"] = hashes
        if file_ids is not None:
            payload["file_ids"] = file_ids

        self._api_request("POST", self._SET_RATING_PATH, json=payload)

    def get_siblings_and_parents(self, tags: abc.Iterable[str]) -> dict[str, T.Any]:
        params = {"tags": json.dumps(tags, cls=_ABCJSONEncoder)}
        response = self._api_request("GET", self._GET_SIBLINGS_AND_PARENTS_PATH, params=params)
        return response.json()

    def set_notes(
        self,
        notes: dict[str, str],
        hash_: T.Optional[str] = None,
        file_id: T.Optional[int] = None,
        merge_cleverly: T.Optional[bool] = None,
        extend_existing_note_if_possible: T.Optional[bool] = None,
        conflict_resolution: T.Optional[T.Union[int, NoteConflictResolution]] = None,
    ) -> None:
        if (hash_ is None and file_id is None) or (hash_ is not None and file_id is not None):
            raise ValueError("Exactly one of hash_, file_id is required")

        payload: dict[str, T.Any] = {"notes": notes}
        if hash_ is not None:
            payload["hash"] = hash_
        if file_id is not None:
            payload["file_id"] = file_id
        if merge_cleverly is not None:
            payload["merge_cleverly"] = merge_cleverly
        if extend_existing_note_if_possible is not None:
            payload["extend_existing_note_if_possible"] = extend_existing_note_if_possible
        if conflict_resolution is not None:
            payload["conflict_resolution"] = conflict_resolution

        self._api_request("POST", self._SET_NOTES_PATH, json=payload)

    def delete_notes(
        self, note_names: abc.Iterable[str], hash_: T.Optional[str] = None, file_id: T.Optional[int] = None
    ) -> None:
        if (hash_ is None and file_id is None) or (hash_ is not None and file_id is not None):
            raise ValueError("Exactly one of hash_, file_id is required")

        payload: dict[str, T.Any] = {"note_names": note_names}
        if hash_ is not None:
            payload["hash"] = hash_
        if file_id is not None:
            payload["file_id"] = file_id

        self._api_request("POST", self._DELETE_NOTES_PATH, json=payload)

    def search_files(
        self,
        tags: abc.Iterable[str],
        file_service_keys: T.Optional[abc.Iterable[T.Union[str, abc.Iterable[str]]]] = None,
        deleted_file_service_keys: T.Optional[abc.Iterable[str]] = None,
        tag_service_key: T.Optional[str] = None,
        file_sort_type: T.Optional[T.Union[int, FileSortType]] = None,
        file_sort_asc: T.Optional[bool] = None,
        return_file_ids: T.Optional[bool] = None,
        return_hashes: T.Optional[bool] = None,
    ) -> dict[str, T.Any]:
        params: dict[str, T.Any] = {"tags": json.dumps(tags, cls=_ABCJSONEncoder)}
        if file_service_keys is not None:
            params["file_service_keys"] = json.dumps(file_service_keys, cls=_ABCJSONEncoder)
        if deleted_file_service_keys is not None:
            params["deleted_file_service_keys"] = json.dumps(deleted_file_service_keys, cls=_ABCJSONEncoder)
        if tag_service_key is not None:
            params["tag_service_key"] = tag_service_key
        if file_sort_type is not None:
            params["file_sort_type"] = file_sort_type
        if file_sort_asc is not None:
            params["file_sort_asc"] = json.dumps(file_sort_asc)
        if return_file_ids is not None:
            params["return_file_ids"] = json.dumps(return_file_ids)
        if return_hashes is not None:
            params["return_hashes"] = json.dumps(return_hashes)

        response = self._api_request("GET", self._SEARCH_FILES_PATH, params=params)
        return response.json()

    def get_file_hashes(
        self, hashes: abc.Iterable[str], desired_hash_type: str, source_hash_type: T.Optional[str] = None
    ) -> dict[str, T.Any]:
        params = {"hashes": json.dumps(hashes, cls=_ABCJSONEncoder), "desired_hash_type": desired_hash_type}
        if source_hash_type is not None:
            params["source_hash_type"] = source_hash_type

        response = self._api_request("GET", self._FILE_HASHES_PATH, params=params)
        return response.json()

    def get_file_metadata(
        self,
        hashes: T.Optional[abc.Iterable[str]] = None,
        file_ids: T.Optional[abc.Iterable[int]] = None,
        create_new_file_ids: T.Optional[bool] = None,
        only_return_identifiers: T.Optional[bool] = None,
        only_return_basic_information: T.Optional[bool] = None,
        detailed_url_information: T.Optional[bool] = None,
        include_notes: T.Optional[bool] = None,
        include_services_object: T.Optional[bool] = None,
        include_blurhash: T.Optional[bool] = None,
    ) -> dict[str, T.Any]:
        if hashes is None and file_ids is None:
            raise ValueError("At least one of hashes, file_ids is required")

        params = {}
        if hashes is not None:
            params["hashes"] = json.dumps(hashes, cls=_ABCJSONEncoder)
        if file_ids is not None:
            params["file_ids"] = json.dumps(file_ids, cls=_ABCJSONEncoder)
        if create_new_file_ids is not None:
            params["create_new_file_ids"] = json.dumps(create_new_file_ids)
        if only_return_identifiers is not None:
            params["only_return_identifiers"] = json.dumps(only_return_identifiers)
        if only_return_basic_information is not None:
            params["only_return_basic_information"] = json.dumps(only_return_basic_information)
        if detailed_url_information is not None:
            params["detailed_url_information"] = json.dumps(detailed_url_information)
        if include_notes is not None:
            params["include_notes"] = json.dumps(include_notes)
        if include_services_object is not None:
            params["include_services_object"] = json.dumps(include_services_object)
        if include_blurhash is not None:
            params["include_blurhash"] = json.dumps(include_blurhash)

        response = self._api_request("GET", self._GET_FILE_METADATA_PATH, params=params)
        return response.json()

    def get_file(
        self, hash_: T.Optional[str] = None, file_id: T.Optional[int] = None, download: T.Optional[bool] = None
    ) -> requests.Response:
        if (hash_ is None and file_id is None) or (hash_ is not None and file_id is not None):
            raise ValueError("Exactly one of hash_, file_id is required")

        params: dict[str, T.Union[str, int]] = {}
        if hash_ is not None:
            params["hash"] = hash_
        if file_id is not None:
            params["file_id"] = file_id
        if download is not None:
            params["download"] = download

        return self._api_request("GET", self._GET_FILE_PATH, params=params, stream=True)

    def get_file_relationships(
        self,
        file_ids: T.Optional[abc.Iterable[int]] = None,
        hashes: T.Optional[abc.Iterable[str]] = None,
        file_service_keys: T.Optional[abc.Iterable[str]] = None,
        deleted_file_service_keys: T.Optional[abc.Iterable[str]] = None,
    ) -> dict[str, T.Any]:
        if hashes is None and file_ids is None:
            raise ValueError("At least one of hashes, file_ids is required")

        params = {}
        if file_ids is not None:
            params["file_ids"] = json.dumps(file_ids, cls=_ABCJSONEncoder)
        if hashes is not None:
            params["hashes"] = json.dumps(hashes, cls=_ABCJSONEncoder)
        if file_service_keys is not None:
            params["file_service_keys"] = json.dumps(file_service_keys, cls=_ABCJSONEncoder)
        if deleted_file_service_keys is not None:
            params["deleted_file_service_keys"] = json.dumps(deleted_file_service_keys, cls=_ABCJSONEncoder)

        response = self._api_request("GET", self._GET_FILE_RELATIONSHIPS_PATH, params=params)
        return response.json()

    def get_potentials_count(
        self,
        file_service_keys: T.Optional[abc.Iterable[str]] = None,
        deleted_file_service_keys: T.Optional[abc.Iterable[str]] = None,
        tag_service_key_1: T.Optional[str] = None,
        tags_1: T.Optional[abc.Iterable[str]] = None,
        tag_service_key_2: T.Optional[str] = None,
        tags_2: T.Optional[abc.Iterable[str]] = None,
        potentials_search_type: T.Optional[int] = None,
        pixel_duplicates: T.Optional[int] = None,
        max_hamming_distance: T.Optional[int] = None,
    ) -> dict[str, T.Any]:
        if file_service_keys is None and deleted_file_service_keys is None:
            raise ValueError("At least one of file_service_keys, deleted_file_service_keys is required")

        params: dict[str, T.Any] = {}
        if file_service_keys is not None:
            params["file_service_keys"] = json.dumps(file_service_keys, cls=_ABCJSONEncoder)
        if deleted_file_service_keys is not None:
            params["deleted_file_service_keys"] = json.dumps(deleted_file_service_keys, cls=_ABCJSONEncoder)
        if tag_service_key_1 is not None:
            params["tag_service_key_1"] = tag_service_key_1
        if tags_1 is not None:
            params["tags_1"] = json.dumps(tags_1, cls=_ABCJSONEncoder)
        if tag_service_key_2 is not None:
            params["tag_service_key_2"] = tag_service_key_2
        if tags_2 is not None:
            params["tags_2"] = json.dumps(tags_2, cls=_ABCJSONEncoder)
        if potentials_search_type is not None:
            params["potentials_search_type"] = potentials_search_type
        if pixel_duplicates is not None:
            params["pixel_duplicates"] = pixel_duplicates
        if max_hamming_distance is not None:
            params["max_hamming_distance"] = max_hamming_distance

        response = self._api_request("GET", self._GET_POTENTIALS_COUNT_PATH, params=params)
        return response.json()

    def get_potential_pairs(
        self,
        file_service_keys: T.Optional[abc.Iterable[str]] = None,
        deleted_file_service_keys: T.Optional[abc.Iterable[str]] = None,
        tag_service_key_1: T.Optional[str] = None,
        tags_1: T.Optional[abc.Iterable[str]] = None,
        tag_service_key_2: T.Optional[str] = None,
        tags_2: T.Optional[abc.Iterable[str]] = None,
        potentials_search_type: T.Optional[int] = None,
        pixel_duplicates: T.Optional[int] = None,
        max_hamming_distance: T.Optional[int] = None,
        max_num_pairs: T.Optional[int] = None,
    ) -> dict[str, T.Any]:
        if file_service_keys is None and deleted_file_service_keys is None:
            raise ValueError("At least one of file_service_keys, deleted_file_service_keys is required")

        params: dict[str, T.Any] = {}
        if file_service_keys is not None:
            params["file_service_keys"] = json.dumps(file_service_keys, cls=_ABCJSONEncoder)
        if deleted_file_service_keys is not None:
            params["deleted_file_service_keys"] = json.dumps(deleted_file_service_keys, cls=_ABCJSONEncoder)
        if tag_service_key_1 is not None:
            params["tag_service_key_1"] = tag_service_key_1
        if tags_1 is not None:
            params["tags_1"] = json.dumps(tags_1, cls=_ABCJSONEncoder)
        if tag_service_key_2 is not None:
            params["tag_service_key_2"] = tag_service_key_2
        if tags_2 is not None:
            params["tags_2"] = json.dumps(tags_2, cls=_ABCJSONEncoder)
        if potentials_search_type is not None:
            params["potentials_search_type"] = potentials_search_type
        if pixel_duplicates is not None:
            params["pixel_duplicates"] = pixel_duplicates
        if max_hamming_distance is not None:
            params["max_hamming_distance"] = max_hamming_distance
        if max_num_pairs is not None:
            params["max_num_pairs"] = max_num_pairs

        response = self._api_request("GET", self._GET_POTENTIAL_PAIRS_PATH, params=params)
        return response.json()

    def get_random_potentials(
        self,
        file_service_keys: T.Optional[abc.Iterable[str]] = None,
        deleted_file_service_keys: T.Optional[abc.Iterable[str]] = None,
        tag_service_key_1: T.Optional[str] = None,
        tags_1: T.Optional[abc.Iterable[str]] = None,
        tag_service_key_2: T.Optional[str] = None,
        tags_2: T.Optional[abc.Iterable[str]] = None,
        potentials_search_type: T.Optional[int] = None,
        pixel_duplicates: T.Optional[int] = None,
        max_hamming_distance: T.Optional[int] = None,
    ) -> dict[str, T.Any]:
        if file_service_keys is None and deleted_file_service_keys is None:
            raise ValueError("At least one of file_service_keys, deleted_file_service_keys is required")

        params: dict[str, T.Any] = {}
        if file_service_keys is not None:
            params["file_service_keys"] = json.dumps(file_service_keys, cls=_ABCJSONEncoder)
        if deleted_file_service_keys is not None:
            params["deleted_file_service_keys"] = json.dumps(deleted_file_service_keys, cls=_ABCJSONEncoder)
        if tag_service_key_1 is not None:
            params["tag_service_key_1"] = tag_service_key_1
        if tags_1 is not None:
            params["tags_1"] = json.dumps(tags_1, cls=_ABCJSONEncoder)
        if tag_service_key_2 is not None:
            params["tag_service_key_2"] = tag_service_key_2
        if tags_2 is not None:
            params["tags_2"] = json.dumps(tags_2, cls=_ABCJSONEncoder)
        if potentials_search_type is not None:
            params["potentials_search_type"] = potentials_search_type
        if pixel_duplicates is not None:
            params["pixel_duplicates"] = pixel_duplicates
        if max_hamming_distance is not None:
            params["max_hamming_distance"] = max_hamming_distance

        response = self._api_request("GET", self._GET_RANDOM_POTENTIALS_PATH, params=params)
        return response.json()

    def set_file_relationships(self, relationships: abc.Iterable[abc.Mapping[str, T.Any]]) -> None:
        payload = {"relationships": relationships}
        self._api_request("POST", self._SET_FILE_RELATIONSHIPS_PATH, json=payload)

    def set_kings(
        self, file_ids: T.Optional[abc.Iterable[int]] = None, hashes: T.Optional[abc.Iterable[str]] = None
    ) -> dict[str, T.Any]:
        if file_ids is None and hashes is None:
            raise ValueError("At least one of file_ids, hashes is required")

        payload: dict[str, T.Any] = {}
        if file_ids is not None:
            payload["file_ids"] = file_ids
        if hashes is not None:
            payload["hashes"] = hashes

        response = self._api_request("POST", self._SET_KINGS_PATH, json=payload)
        return response.json()

    def get_thumbnail(self, hash_: T.Optional[str] = None, file_id: T.Optional[int] = None) -> requests.Response:
        if (hash_ is None and file_id is None) or (hash_ is not None and file_id is not None):
            raise ValueError("Exactly one of hash_, file_id is required")

        params: dict[str, T.Union[str, int]] = {}
        if hash_ is not None:
            params["hash"] = hash_
        if file_id is not None:
            params["file_id"] = file_id

        return self._api_request("GET", self._GET_THUMBNAIL_PATH, params=params, stream=True)

    def get_render(
        self, hash_: T.Optional[str] = None, file_id: T.Optional[int] = None, download: T.Optional[bool] = None
    ) -> requests.Response:
        if (hash_ is None and file_id is None) or (hash_ is not None and file_id is not None):
            raise ValueError("Exactly one of hash_, file_id is required")

        params: dict[str, T.Union[str, int]] = {}
        if hash_ is not None:
            params["hash"] = hash_
        if file_id is not None:
            params["file_id"] = file_id
        if download is not None:
            params["download"] = download

        return self._api_request("GET", self._GET_RENDER_PATH, params=params, stream=True)

    def get_cookies(self, domain: str) -> dict[str, T.Any]:
        response = self._api_request("GET", self._GET_COOKIES_PATH, params={"domain": domain})
        return response.json()

    def set_cookies(self, cookies: abc.Iterable[abc.Iterable[T.Union[str, int]]]) -> None:
        self._api_request("POST", self._SET_COOKIES_PATH, json={"cookies": cookies})

    def set_headers(self, headers: T.Mapping[str, T.Mapping[str, str | None]], domain: str | None = None) -> None:
        payload: dict[str, T.Any] = {"headers": headers}
        if domain is not None:
            payload["domain"] = domain

        self._api_request("POST", self._SET_HEADERS_PATH, json=payload)

    def set_user_agent(self, user_agent: str) -> None:
        # https://hydrusnetwork.github.io/hydrus/developer_api.html#manage_headers_set_user_agent
        warnings.warn("set_user_agent() is deprecated, please use set_headers() instead", DeprecationWarning)
        self._api_request("POST", self._SET_USER_AGENT_PATH, json={"user-agent": user_agent})

    def get_pages(self) -> dict[str, T.Any]:
        response = self._api_request("GET", self._GET_PAGES_PATH)
        return response.json()

    def get_page_info(self, page_key: str, simple: T.Optional[bool] = None) -> dict[str, T.Any]:
        params = {"page_key": page_key}
        if simple is not None:
            params["simple"] = json.dumps(simple)

        response = self._api_request("GET", self._GET_PAGE_INFO_PATH, params=params)
        return response.json()

    def add_files_to_page(
        self,
        page_key: str,
        file_ids: T.Optional[abc.Iterable[int]] = None,
        hashes: T.Optional[abc.Iterable[str]] = None,
    ) -> None:
        if file_ids is None and hashes is None:
            raise ValueError("At least one of file_ids, hashes is required")

        payload: dict[str, T.Any] = {"page_key": page_key}
        if file_ids is not None:
            payload["file_ids"] = file_ids
        if hashes is not None:
            payload["hashes"] = hashes

        self._api_request("POST", self._ADD_FILES_TO_PAGE_PATH, json=payload)

    def focus_page(self, page_key: str) -> None:
        self._api_request("POST", self._FOCUS_PAGE_PATH, json={"page_key": page_key})

    def refresh_page(self, page_key: str) -> None:
        self._api_request("POST", self._REFRESH_PAGE_PATH, json={"page_key": page_key})

    def lock_database(self) -> None:
        self._api_request("POST", self._LOCK_DATABASE_PATH)

    def unlock_database(self) -> None:
        self._api_request("POST", self._UNLOCK_DATABASE_PATH)

    def get_mr_bones(
        self,
        tags: T.Optional[abc.Iterable[str]] = None,
        file_service_keys: T.Optional[abc.Iterable[str]] = None,
        deleted_file_service_keys: T.Optional[abc.Iterable[str]] = None,
        tag_service_key: T.Optional[str] = None,
    ) -> dict[str, T.Any]:
        params: dict[str, str] = {}
        if tags is not None:
            params["tags"] = json.dumps(tags, cls=_ABCJSONEncoder)
        if file_service_keys is not None:
            params["file_service_keys"] = json.dumps(file_service_keys, cls=_ABCJSONEncoder)
        if deleted_file_service_keys is not None:
            params["deleted_file_service_keys"] = json.dumps(deleted_file_service_keys, cls=_ABCJSONEncoder)
        if tag_service_key is not None:
            params["tag_service_key"] = tag_service_key

        return self._api_request("GET", self._MR_BONES_PATH, params=params).json()

    def get_client_options(self) -> dict[str, T.Any]:
        return self._api_request("GET", self._GET_CLIENT_OPTIONS_PATH).json()
