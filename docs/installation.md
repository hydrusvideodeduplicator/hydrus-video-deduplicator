# Installation

## Windows

For Windows, you can get the [latest release directly from the github releases page](https://github.com/hydrusvideodeduplicator/hydrus-video-deduplicator/releases).

You should now be good to go. Proceed to [usage.](./usage.md)

---

## Linux

The following instructions are written for Ubuntu, but should be similar for most distros.

> **Note:** [uv](https://docs.astral.sh/uv/) is the preferred package manager instead of pip for a development install due to its speed and ease of use. If it isn't supported by your distribution or you are using another OS like FreeBSD, then follow the steps below but use the usual Python venv and pip instead of uv.

### Dependencies

- Python >=3.10

### Steps

1. Update system packages:

    ```sh
    sudo apt-get update && sudo apt-get upgrade
    ```

1. Install system dependencies (just pip):

    ```sh
    sudo apt-get install -y python3-pip
    ```

1. Create and activate a virtual environment:

    ```sh
    pip install uv 
    uv venv # Create a virtual environment somewhere to avoid system dependency conflicts
    .venv/bin/activate # Activate the virtual environment (run the command uv suggests after running uv venv)
    ```

1. Install the program:

    ```sh
    uv pip install hydrusvideodeduplicator
    ```

You should now be good to go. Proceed to [usage.](./usage.md)

> **Note:** Any time you want to run the program again you will have to run the command to activate the virtual environment first.

## macos

Same directions as Linux but using your preferred package manager for system dependencies e.g. [brew](https://brew.sh/).
