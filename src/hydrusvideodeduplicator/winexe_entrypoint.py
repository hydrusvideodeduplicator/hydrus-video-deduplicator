from hydrusvideodeduplicator import config
from hydrusvideodeduplicator.entrypoint import run_main

if __name__ == "__main__":
    config.set_windows_exe()
    run_main()
