from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Annotated, List, Optional

if TYPE_CHECKING:
    from typing import NoReturn

from pathlib import Path

import typer
from rich import print

from .__about__ import __version__
from .client import ClientAPIException, FailedHVDClientConnection, HVDClient, create_client
from .config import (
    DEDUP_DATABASE_DIR,
    FAILED_PAGE_NAME,
    HYDRUS_API_KEY,
    HYDRUS_API_URL,
    HYDRUS_LOCAL_FILE_SERVICE_KEYS,
    HYDRUS_QUERY,
    REQUESTS_CA_BUNDLE,
)
from .db import DedupeDB
from .dedup import HydrusVideoDeduplicator
from .dedup_util import print_and_log

"""
Parameters:
- api_key will be read from env var $HYDRUS_API_KEY or .env file
- api_url will be read from env var $HYDRUS_API_URL or .env file
- to add custom queries, do
  --custom-query="series:twilight" --custom-query="character:edward" ... etc for each query
- threshold is the min % matching to be considered similar. 100% is identical.
- verbose turns on logging
- debug turns on logging and sets the logging level to debug
"""
print(f"[blue] Hydrus Video Deduplicator {__version__} [/]")


def main(
    api_key: Annotated[Optional[str], typer.Option(help="Hydrus API Key")] = None,
    api_url: Annotated[Optional[str], typer.Option(help="Hydrus API URL")] = HYDRUS_API_URL,
    overwrite: Annotated[Optional[bool], typer.Option(help="Overwrite existing perceptual hashes")] = False,
    query: Annotated[Optional[List[str]], typer.Option(help="Custom Hydrus tag query")] = HYDRUS_QUERY,
    threshold: Annotated[
        Optional[float], typer.Option(help="Similarity threshold for a pair of videos where 100 is identical")
    ] = 75.0,
    skip_hashing: Annotated[
        Optional[bool], typer.Option(help="Skip perceptual hashing and just search for duplicates")
    ] = False,
    file_service_key: Annotated[
        Optional[List[str]], typer.Option(help="Local file service key")
    ] = HYDRUS_LOCAL_FILE_SERVICE_KEYS,
    verify_cert: Annotated[
        Optional[str], typer.Option(help="Path to TLS cert. This forces verification.")
    ] = REQUESTS_CA_BUNDLE,
    clear_search_cache: Annotated[
        Optional[bool], typer.Option(help="Clear the cache that tracks what files have already been compared")
    ] = False,
    failed_page_name: Annotated[
        Optional[str], typer.Option(help="The name of the Hydrus page to add failed files to.")
    ] = FAILED_PAGE_NAME,
    job_count: Annotated[
        Optional[int], typer.Option(help="Number of CPU threads to use. Default is all but one core.")
    ] = -2,
    dedup_database_dir: Annotated[
        Optional[Path], typer.Option(help="The directory to store the database used for dedupe.")
    ] = DEDUP_DATABASE_DIR,
    verbose: Annotated[Optional[bool], typer.Option(help="Verbose logging")] = False,
    debug: Annotated[Optional[bool], typer.Option(hidden=True)] = False,
):
    # Fix mypy errors from optional parameters
    assert overwrite is not None and threshold is not None and skip_hashing is not None and job_count is not None
    if job_count != 1:
        print(f"[yellow] Job count was {job_count} but was overriden to '1' for development right now.")
        print("Don't worry. Multithreaded hashing will be added back soon before the next release.")
        job_count = 1

    # CLI debug parameter sets log level to info or debug
    loglevel = logging.INFO
    if debug:
        loglevel = logging.DEBUG
        verbose = True

    logging.basicConfig(format=" %(asctime)s - %(name)s: %(message)s", datefmt="%H:%M:%S", level=loglevel)
    logger = logging.getLogger("main")
    logger.debug("Starting Hydrus Video Deduplicator.")

    def exit_from_failure() -> NoReturn:
        print_and_log(logger, "Exiting due to failure...")
        raise typer.Exit(code=1)

    # Verbose sets whether logs are shown to the user at all.
    # Logs are separate from printing in this program.
    if not verbose:
        logging.disable()

    DedupeDB.set_db_dir(dedup_database_dir)

    # CLI overwrites env vars with no default value
    if not api_key:
        api_key = HYDRUS_API_KEY

    # Check for necessary variables
    if not api_key:
        print_and_log(logger, "Hydrus API key is not set. Please set with '--api-key'.")
        exit_from_failure()
    # This should not happen because there's a default val in config.py
    if not api_url:
        print_and_log(logger, "Hydrus API URL is not set. Please set with '--api-url'.")
        exit_from_failure()

    # Client connection
    print_and_log(logger, f"Connecting to Hydrus at {api_url}")
    try:
        hvdclient = create_client(
            file_service_key,
            api_url,
            api_key,
            verify_cert,
        )
        api_version = hvdclient.get_api_version()
        hydrus_api_version = hvdclient.get_hydrus_api_version()
        print_and_log(logger, f"Dedupe API version: 'v{api_version}'")
        print_and_log(logger, f"Hydrus API version: 'v{hydrus_api_version}'")
        hvdclient.verify_permissions()
    except (FailedHVDClientConnection, ClientAPIException) as exc:
        print_and_log(logger, str(exc), logging.FATAL)
        print_and_log(logger, exc.pretty_msg, logging.FATAL)
        exit_from_failure()

    if debug:
        HVDClient._log.setLevel(logging.DEBUG)

    # Deduplication

    if DedupeDB.does_db_exist():
        print_and_log(logger, f"Found existing database at '{DedupeDB.get_db_file_path()}'")
        db = DedupeDB.DedupeDb(DedupeDB.get_db_dir(), DedupeDB.get_db_name())
        db.init_connection()
        # Upgrade the database before doing anything.
        db.upgrade_db()
        db_stats = DedupeDB.get_db_stats(db)

        print_and_log(
            logger,
            f"Database has {db_stats.num_videos} videos already perceptually hashed.",
        )
        print_and_log(
            logger,
            f"Database filesize: {db_stats.file_size} bytes.",
        )
        db.commit()

        if clear_search_cache:
            db.clear_search_cache()
            print("[green] Cleared the search cache.")

        # DedupeDB.clear_trashed_files_from_db(hvdclient)
    else:
        print_and_log(logger, f"Database not found. Creating one at '{DedupeDB.get_db_file_path()}'", logging.INFO)
        if not DedupeDB.get_db_dir().exists():
            DedupeDB.create_db_dir()
        db = DedupeDB.DedupeDb(DedupeDB.get_db_dir(), DedupeDB.get_db_name())
        db.init_connection()
        db.create_tables()
        db.commit()
        db_stats = DedupeDB.get_db_stats(db)

    deduper = HydrusVideoDeduplicator(db, client=hvdclient, job_count=job_count, failed_page_name=failed_page_name)

    if debug:
        deduper.hydlog.setLevel(logging.DEBUG)
        deduper._DEBUG = True

    if threshold < 0.0 or threshold > 100.0:
        print("[red] ERROR: Invalid similarity threshold. Must be between 0 and 100.")
        raise typer.Exit(code=1)
    HydrusVideoDeduplicator.threshold = threshold

    if overwrite:
        print(f"[yellow] Overwriting {db_stats.num_videos} existing hashes.")

    deduper.deduplicate(
        overwrite=overwrite,
        custom_query=query,
        skip_hashing=skip_hashing,
    )

    raise typer.Exit()


try:
    typer.run(main)
except KeyboardInterrupt as exc:
    raise typer.Exit(-1) from exc
