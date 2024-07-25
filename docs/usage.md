# Usage

## Basic usage

1. Backup your Hydrus database to be safe.

1. [Enable the Hydrus Client API](https://hydrusnetwork.github.io/hydrus/client_api.html#enabling_the_api) and create an access key with all permissions.

    - The program needs access to your _entire_ database. Do NOT use blacklist/whitelist filters for the API token.

    - https is the default for the client API URL.
        - Specify your Hydrus API URL with `--api-url` if you need http
            - Ex. `python -m hydrusvideodeduplicator --api-url=http://localhost:45869`
        - ⚠️ SSL cert is **not verified** by default unless you enter the cert's file path with `--verify-cert`

    - Enable `allow non-local connections` in `manage services->client api` if you are using [WSL](https://learn.microsoft.com/en-us/windows/wsl/) or the connection will fail.

1. Run and enter your access key as a parameter

```sh
python3 -m hydrusvideodeduplicator --api-key="put your Hydrus api key here"
```

<details>
<summary>Example</summary>
<br>

```sh
python3 -m hydrusvideodeduplicator --api-key="78d2fcc9fe1f43c5008959ed1abfe38ffedcfa127d4f051a1038e068d3e32656"
```

</details>

You can select certain files with queries just like Hydrus. e.g. `--query="character:batman"`

<details>
<summary>Example</summary>
<br>

```sh
python3 -m hydrusvideodeduplicator --api-key="78d2fcc9fe1f43c5008959ed1abfe38ffedcfa127d4f051a1038e068d3e32656" --query="character:batman"
```

</details>

To cancel any stage of processing, press CTRL+C.

You can cancel the first step, perceptual hashing, at any time to only search the duplicates that are currently hashed. You don't need to perceptually hash your entire database before you start searching.

See the full list of options with `--help`.

See the [FAQ](./faq.md) for more information.
