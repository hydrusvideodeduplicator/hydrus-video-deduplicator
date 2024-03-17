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

import collections
import os
import typing as T
import collections.abc as abc

from hydrusvideodeduplicator.hydrus_api import (
    DEFAULT_API_URL,
    HYDRUS_METADATA_ENCODING,
    BinaryFileLike,
    Client,
    ImportStatus,
    Permission,
)

_X = T.TypeVar("_X")


# This is public so other code can import it to annotate their own types
class TextFileLike(T.Protocol):
    def read(self) -> str: ...


def verify_permissions(
    client: Client, permissions: abc.Iterable[T.Union[int, Permission]], exact: bool = False
) -> bool:
    granted_permissions = set(client.verify_access_key()["basic_permissions"])
    return granted_permissions == set(permissions) if exact else granted_permissions.issuperset(permissions)


def cli_request_api_key(
    name: str,
    permissions: abc.Iterable[T.Union[int, Permission]],
    verify: bool = True,
    exact: bool = False,
    api_url: str = DEFAULT_API_URL,
) -> str:
    while True:
        input(
            'Navigate to "services->review services->local->client api" in the Hydrus client and click "add->from api '
            'request". Then press enter to continue...'
        )
        access_key = Client(api_url=api_url).request_new_permissions(name, permissions)["access_key"]
        input("Press OK and then apply in the Hydrus client dialog. Then press enter to continue...")

        client = Client(access_key, api_url)
        if verify and not verify_permissions(client, permissions, exact):
            granted = client.verify_access_key()["basic_permissions"]
            print(
                f"The granted permissions ({granted}) differ from the requested permissions ({permissions}), please "
                "grant all requested permissions."
            )
            continue

        return access_key


def parse_hydrus_metadata(text: str) -> dict[T.Optional[str], set[str]]:
    namespaces = collections.defaultdict(set)
    for line in (line.strip() for line in text.splitlines()):
        if not line:
            continue

        parts = line.split(":", 1)
        namespace, tag = (None, line) if len(parts) == 1 else parts
        namespaces[namespace].add(tag)

    # Ignore type, mypy has trouble figuring out that tag isn't optional
    return namespaces  # type: ignore


def parse_hydrus_metadata_file(
    path_or_file: T.Union[str, os.PathLike, TextFileLike]
) -> dict[T.Optional[str], set[str]]:
    if isinstance(path_or_file, (str, os.PathLike)):
        with open(path_or_file, encoding=HYDRUS_METADATA_ENCODING) as file:
            return parse_hydrus_metadata(file.read())

    return parse_hydrus_metadata(path_or_file.read())


# Useful for splitting up requests to get_file_metadata()
def yield_chunks(sequence: T.Sequence[_X], chunk_size: int, offset: int = 0) -> T.Generator[T.Sequence[_X], None, None]:
    while offset < len(sequence):
        yield sequence[offset : offset + chunk_size]
        offset += chunk_size


def add_and_tag_files(
    client: Client,
    paths_or_files: abc.Iterable[T.Union[str, os.PathLike, BinaryFileLike]],
    tags: abc.Iterable[str],
    tag_service_keys: abc.Iterable[str],
) -> list[dict[str, T.Any]]:
    """Convenience function to add and tag multiple files at the same time.

    Returns:
        Returns results of all `Client.add_file()` calls, matching the order of the paths_or_files iterable
    """
    results = []
    hashes = set()
    for path_or_file in paths_or_files:
        result = client.add_file(path_or_file)
        results.append(result)
        if result["status"] != ImportStatus.FAILED:
            hashes.add(result["hash"])

    client.add_tags(hashes, service_keys_to_tags={key: tags for key in tag_service_keys})
    return results


def get_page_list(client: Client) -> list[dict[str, T.Any]]:
    """Convenience function that returns a flattened version of the page tree from `Client.get_pages()`.

    Returns:
        A list of every "pages" value in the page tree in pre-order (NLR)
    """
    tree = client.get_pages()["pages"]
    pages = []

    def walk_tree(page: dict[str, T.Any]) -> None:
        pages.append(page)
        for sub_page in page.get("pages", ()):
            walk_tree(sub_page)

    walk_tree(tree)
    return pages


def get_service_mapping(client: Client) -> dict[str, list[str]]:
    mapping = collections.defaultdict(list)

    # Ignore the keys in the JSON which for some reason replaces spaces with underscores
    for services in client.get_services().values():
        for service in services:
            mapping[service["name"]].append(service["service_key"])

    return mapping
