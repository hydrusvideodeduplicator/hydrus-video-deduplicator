import logging
from typing import Optional, Annotated, List

import typer
import hydrus_api
from rich import print as rprint

from .__about__ import __version__
from .config import HYDRUS_API_KEY, HYDRUS_API_URL
from .dedup import HydrusVideoDeduplicator

from .vpdq_util import VPDQ_QUERY_MATCH_THRESHOLD_PERCENT

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
rprint(f"[blue] Hydrus Video Deduplicator {__version__} [/]")

def main(api_key: Annotated[Optional[str], typer.Option(help="Hydrus API Key")] = None,
        api_url: Annotated[Optional[str], typer.Option(help="Hydrus API URL")] = None,
        overwrite:  Annotated[Optional[bool], typer.Option(help="Overwrite existing perceptual hashes")] = False,
        query: Annotated[Optional[List[str]], typer.Option(help="Custom Hydrus tag query")] = None,
        threshold: Annotated[Optional[float], typer.Option(help="Similarity threshold for a pair of videos where 100 is identical")] = VPDQ_QUERY_MATCH_THRESHOLD_PERCENT,
        skip_hashing: Annotated[Optional[bool], typer.Option(help="Skip perceptual hashing and just search for duplicates")] = False,
        verbose:  Annotated[Optional[bool], typer.Option(hidden=True)] = False,
        debug: Annotated[Optional[bool], typer.Option(hidden=True)] = False,
        ):

    threshold = threshold/100.

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

    if threshold < 0:
        rprint(f"[red] ERROR: Invalid similarity threshold. Must be between 0 and 100.")
        raise typer.Exit(code=1)
    superdeduper.threshold = threshold

    # Run all deduplicate functionality
    superdeduper.deduplicate(overwrite=overwrite,
                             custom_query=query,
                             skip_hashing=skip_hashing)

    typer.Exit()


try:
    typer.run(main)
except KeyboardInterrupt:
    typer.Exit()