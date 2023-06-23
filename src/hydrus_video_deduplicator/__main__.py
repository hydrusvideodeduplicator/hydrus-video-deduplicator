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

- verbose turns on logging
- debug turns on logging and sets the logging level to debug
"""
def main(api_key: Annotated[Optional[str], typer.Option()] = None,
        api_url: Annotated[Optional[str], typer.Option()] = None,
        add_missing:  Annotated[Optional[bool], typer.Option()] = True,
        overwrite:  Annotated[Optional[bool], typer.Option()] = False,
        verbose:  Annotated[Optional[bool], typer.Option()] = False,
        debug: Annotated[Optional[bool], typer.Option()] = False,
        custom_query: Annotated[Optional[List[str]], typer.Option()] = None,
        ):
    rprint(f"[blue] Hydrus Video Deduplicator {__version__} [/]")

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
        print(error_connecting_exception_msg)
        raise typer.Exit(code=1)

    # Run all deduplicate functionality
    superdeduper.deduplicate(add_missing=add_missing,
                             overwrite=overwrite,
                             custom_query=custom_query)

    typer.Exit()


try:
    typer.run(main)
except KeyboardInterrupt:
    typer.Exit()