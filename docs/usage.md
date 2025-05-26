# Usage

## Creating a Hydrus API Key

1. [Enable the Hydrus Client API service](https://hydrusnetwork.github.io/hydrus/client_api.html#enabling_the_api). Ensure `use https` is enabled for the client api service.

    - If you need http for some reason instead of https, you must specify your Hydrus API URL directly with `--api-url`
        - Ex. `python -m hydrusvideodeduplicator --api-url=http://localhost:45869`

    - Enable `allow non-local connections` in `manage services->client api` if you are running under [Windows Subsystem for Linux](https://learn.microsoft.com/en-us/windows/wsl/), or the connection will fail.

    - ⚠️ For https, SSL cert is **not verified** by default unless you enter the cert's file path with `--verify-cert`

2. Create a client api service key with `permits everything` enabled.

    - Do NOT set blacklist/whitelist filters for the API token.

> **Example**: The API key should look something vaguely like `78d2fcc9fe1f43c5008959ed1abfe38ffedcfa127d4f051a1038e068d3e32656`

> **Note**: You will need this API key in the next step, so you should probably copy this API key to your clipboard.

After getting your API key, continue to [Running Video Dedupe](#running-video-dedupe).

## Running Video Dedupe

<details open>
<summary>Windows</summary>
<br>

Run **hydrusvideodeduplicator.exe** and enter the Hydrus API key you created previously when prompted.

</details>

<br>

<details>
<summary>Linux and macos</summary>
<br>

Run the program and enter the Hydrus API key you created previously.

<br>

Example:

```sh
python3 -m hydrusvideodeduplicator --api-key="78d2fcc9fe1f43c5008959ed1abfe38ffedcfa127d4f051a1038e068d3e32656"
```

</details>

<br>

To cancel any stage of processing at any time, press CTRL+C.

You may want to cancel the first step, perceptually hashing, before it's finished in order to search for duplicates on that subset of videos that have been hashed. The next time you run the program it will continue where you left off. This may be useful for large databases that may take forever to perceptually hash.

See the full list of options with `--help`.

See the [FAQ](./faq.md) for more information.

### Advanced Usage

You can select certain files with queries just like Hydrus. e.g. `--query="character:batman"`

<details>
<summary>Example</summary>
<br>

```sh
python3 -m hydrusvideodeduplicator --api-key="..." --query="character:batman"
```

</details>
