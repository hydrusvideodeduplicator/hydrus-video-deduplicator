# Installation

## Windows

### Dependencies

- [Python](https://www.python.org/downloads/) >=3.10

Run in PowerShell:

```Powershell
python3 -m venv venv         # Create a virtual environment somewhere to avoid system dependency conflicts
.\venv\Scripts\Activate.ps1  # Activate the virtual environment
python3 -m pip install hydrusvideodeduplicator
```

You should now be good to go. Proceed to [usage.](./usage.md)

> Note: Any time you want to run the program again you will have to run the command to activate the virtual environment first.

---

## Linux

The following instructions are written for Ubuntu, but should be similar for most distros.

### Dependencies

- Python >=3.10

### Steps

1. Update system packages:

    ```sh
    sudo apt-get update && sudo apt-get upgrade
    ```

1. Install dependencies:

    ```sh
    sudo apt-get install -y python3-pip
    ```

1. Create and activate a virtual environment:

    ```sh
    python3 -m venv venv      # Create a virtual environment to avoid system dependency conflicts
    source venv/bin/activate  # Activate the virtual environment
    ```

1. Install the program:

    ```sh
    pip install hydrusvideodeduplicator
    ```

You should now be good to go. Proceed to [usage.](./usage.md)

> Note: Any time you want to run the program again you will have to run the command to activate the virtual environment first.

## macos

Same directions as Linux and Windows but using your preferred package manager for dependencies e.g. [brew](https://brew.sh/).
