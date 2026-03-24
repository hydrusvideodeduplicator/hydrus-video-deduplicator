from __future__ import annotations

import json
import os
from pathlib import Path
from platform import uname

from dotenv import dotenv_values
from platformdirs import PlatformDirs


class InvalidEnvironmentVariable(Exception):
    """Raise for when environment variables are invalid."""

    def __init__(self, msg):
        super().__init__(msg)
        print("Exiting due to invalid environment variable.")


# Validates that a env var is a valid JSON array
# Program will exit if invalid
# Returns the parsed json array as a list
def validate_json_array_env_var(env_var: str | None, err_msg: str) -> list | None:
    if env_var is None:
        return None

    try:
        env_var_list = json.loads(env_var)
        if not isinstance(env_var_list, list):
            raise InvalidEnvironmentVariable(f"ERROR: {err_msg}")
    except json.decoder.JSONDecodeError as exc:
        raise InvalidEnvironmentVariable(f"ERROR: {err_msg}") from exc

    return env_var_list


class Config:
    def __init__(self):
        self.hydrus_api_key = None
        self.hydrus_api_url = None
        self.dedupe_database_dir = None
        self.failed_page_name = None
        self.requests_ca_bundle = None
        self.hydrus_query = None
        self.hydrus_local_file_service_keys = None
        self.hvd_gui = None

    @staticmethod
    def _load(config_map: dict[str, str | None]):
        config = Config()

        def _get_default_ip():
            default_ip = "localhost"

            # If you're in WSL you probably want to connect to your Windows Hydrus Client by default
            def in_wsl() -> bool:
                return "microsoft-standard" in uname().release

            if in_wsl():
                from socket import gethostname

                default_ip = f"{gethostname()}.local"
            return default_ip

        config.hydrus_api_url = config_map.get("HYDRUS_API_URL", f"http://{_get_default_ip()}:45869")
        config.hydrus_api_key = config_map.get("HYDRUS_API_KEY", "")

        # Default is ~/.local/share/hydrusvideodeduplicator/ on Linux
        config.dedupe_database_dir = Path(
            config_map.get("DEDUP_DATABASE_DIR", PlatformDirs("hydrusvideodeduplicator").user_data_dir)
        )

        config.failed_page_name = config_map.get("FAILED_PAGE_NAME", None)

        config.requests_ca_bundle = config_map.get("REQUESTS_CA_BUNDLE", None)

        # Optional query for selecting files to process
        # TODO: Should validation really be done here? What other config options can/should be validated?
        config.hydrus_query = validate_json_array_env_var(
            config_map.get("HYDRUS_QUERY", None), err_msg="Ensure HYDRUS_QUERY is a JSON formatted array."
        )

        # Optional service key of local file service/s to fetch files from
        config.hydrus_local_file_service_keys = validate_json_array_env_var(
            config_map.get("HYDRUS_LOCAL_FILE_SERVICE_KEYS"),
            err_msg="Ensure HYDRUS_LOCAL_FILE_SERVICE_KEYS is a JSON formatted array",
        )

        config.hvd_gui = config_map.get("HVD_GUI", False)

        return config

    @staticmethod
    def load_from_dotenv():
        """Load config options from dotenv file. This does not affect environment variables."""
        dotenv_config = dotenv_values()
        return Config._load(dotenv_config)

    @staticmethod
    def load_from_env():
        """Load config options from environment variables."""
        config_map = {}
        config_options = [
            "HYDRUS_API_URL",
            "HYDRUS_API_KEY",
            "DEDUP_DATABASE_DIR",
            "FAILED_PAGE_NAME",
            "REQUESTS_CA_BUNDLE",
            "HYDRUS_QUERY",
            "HYDRUS_LOCAL_FILE_SERVICE_KEYS",
            "HVD_GUI",
        ]
        for option in config_options:
            config_map[option] = os.getenv(option)

        # Remove all items so that _load() can populate them with defaults if they are missing.
        filtered_config_map = {k: v for k, v in config_map.items() if v is not None}
        return Config._load(filtered_config_map)
