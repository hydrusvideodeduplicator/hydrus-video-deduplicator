import logging
from typing import Optional, Annotated, List

import typer
import hydrus_api
from rich import print as rprint

from .__about__ import __version__
from .config import HYDRUS_API_KEY, HYDRUS_API_URL
from .dedup import HydrusVideoDeduplicator

"""
Parameters:
- api_key will be read from env var $HYDRUS_API_KEY or .env file
- api_url will be read from env var $HYDRUS_API_URL or .env file
- overwrite is false, add_missing is true, so only files without phashes will be hashed
- custom_query is empty. to add custom queries, do
  --custom-query="series:twilight" --custom-query="character:edward" ... etc for each query
- search_distance is the max threshold for a pair to be considered similar. 0 is identical.
- verbose turns on logging
- debug turns on logging and sets the logging level to debug
"""
rprint(f"[blue] Hydrus Video Deduplicator {__version__} [/]")

def main(api_key: Annotated[Optional[str], typer.Option(help="Hydrus API Key")] = None,
        api_url: Annotated[Optional[str], typer.Option(help="Hydrus API URL")] = None,
        add_missing:  Annotated[Optional[bool], typer.Option(help="Add perceptual hashes to files without one")] = True,
        overwrite:  Annotated[Optional[bool], typer.Option(help="Overwrite existing perceptual hashes")] = False,
        verbose:  Annotated[Optional[bool], typer.Option(hidden=True)] = False,
        debug: Annotated[Optional[bool], typer.Option(hidden=True)] = False,
        custom_query: Annotated[Optional[List[str]], typer.Option(help="Custom Hydrus tag query")] = None,
        search_distance: Annotated[Optional[int], typer.Option(help="Similarity threshold. 0 is the strictest")] = 4,
        skip_hashing: Annotated[Optional[bool], typer.Option(help="Skip perceptual hashing and just search for duplicates")] = False,
        ):

    # CLI debug parameter sets log level to info or debug
    loglevel: logging._Level = logging.WARNING
    if debug:
        loglevel = logging.DEBUG
        verbose = True

    logging.basicConfig(format=' %(asctime)s - %(name)s: %(message)s',
                        datefmt='%H:%M:%S',
                        level=loglevel)
    logging.info("Starting Hydrus Video Deduplicator")
    
    # Verbose sets whether logs are shown to the user at all.
    # Logs are separate from printing in this program.
    if not verbose:
        logging.disable()

    # CLI overwrites env vars
    if not api_key:
        api_key = HYDRUS_API_KEY
    if not api_url:
        api_url = HYDRUS_API_URL

    # Check for necessary variables
    if not api_key:
        print("API key not set. Exiting...")
        raise typer.Exit(code=1)
    # This should not happen because there's a default val in secret.py
    if not api_url:
        print("Hydrus URL not set. Exiting...")
        raise typer.Exit(code=1)

    # Client connection
    _client = hydrus_api.Client(api_url=api_url,
                                access_key=api_key)

    error_connecting = True
    error_connecting_exception_msg = ""
    error_connecting_exception = ""
    try:
        superdeduper = HydrusVideoDeduplicator(_client)
    except hydrus_api.InsufficientAccess as exc:
        error_connecting_exception_msg = "Invalid Hydrus API key."
        error_connecting_exception = exc
    except hydrus_api.DatabaseLocked as exc:
        error_connecting_exception_msg = "Hydrus database is locked. Try again later."
        error_connecting_exception = exc
    except hydrus_api.ServerError as exc:
        error_connecting_exception_msg = "Unknown Server Error."
        error_connecting_exception = exc
    except hydrus_api.APIError as exc:
        error_connecting_exception_msg = "API Error"
        error_connecting_exception = exc
    except hydrus_api.ConnectionError as exc:
        error_connecting_exception_msg = "Failed to connect to Hydrus. Is your Hydrus instance running?"
        error_connecting_exception = exc
    else:
        error_connecting = False
    
    if error_connecting:
        logging.fatal("FATAL ERROR HAS OCCURRED")
        logging.fatal(error_connecting_exception)
        rprint(f"[red] {error_connecting_exception_msg} ")
        raise typer.Exit(code=1)

    # Deduplication parameters

    if debug:
        superdeduper.hydlog.setLevel(logging.DEBUG)
        superdeduper._DEBUG = True

    # This is not a hard limit but you don't want to set it higher than this.
    MAX_SEARCH_DISTANCE = 15
    if search_distance < 0 or search_distance > MAX_SEARCH_DISTANCE:
        rprint(f"[red] ERROR: Invalid search_distance {search_distance}. Must be between 0 and {MAX_SEARCH_DISTANCE} ")
        raise typer.Exit(code=1)
    superdeduper.search_distance = search_distance

    # Run all deduplicate functionality
    superdeduper.deduplicate(add_missing=add_missing,
                             overwrite=overwrite,
                             custom_query=custom_query,
                             skip_hashing=skip_hashing)

    typer.Exit()


try:
    typer.run(main)
except KeyboardInterrupt:
    typer.Exit()