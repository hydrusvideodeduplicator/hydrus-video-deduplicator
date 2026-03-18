# Development

## Setup

[Hatch](https://hatch.pypa.io/latest/) is the Python project manager for this project.

It is used for packaging and environment management for development.

There are hatch commands for this project are defined in the [pyproject.toml]. They are just aliases for other commands that run in specific environments.

For example, to run the command to generate the documentation and serve it locally:

```sh
hatch run docs:serve
```

`docs` is the environment and `build` is the command to run.

For more information, see the Hatch [environment documentation](https://hatch.pypa.io/latest/environment/)

### Useful commands

Run all tests:

```sh
hatch run test:all
```

Serve documentation locally:

```sh
hatch run docs:serve
```

Build documentation:

```sh
hatch run docs:build
```

Check code formatting (doesn't actually run formatting):

```sh
hatch run lint:format
```

Lint code:

```sh
hatch run lint:lint
```

Format code:

```sh
hatch run format:format
```

Benchmark vpdq:

```sh
hatch run benchmark:vpdq
```

### Developing without Hatch

Alternatively, if you don't want to use hatch and you know what you're doing, you can install and do development the general way.

1. Clone or download the repository:

    ```sh
    git clone https://github.com/hydrusvideodeduplicator/hydrus-video-deduplicator.git
    ```

1. Install local editable package with [uv](https://docs.astral.sh/uv):

    ```sh
    cd hydrus-video-deduplicator
    pip install uv
    uv venv
    source .venv/bin/activate # or .venv\Scripts\activate on Windows
    uv pip install -e .
    ```

    To install the GUI, run `uv pip install -e ".[gui]"`.

1. Now if you run `python -m hydrusvideodeduplicator` there should be no errors.

---

### Testing

#### testdb

testdb is a submodule that contains the Hydrus DB and videos for integration testing purposes.

It is a submodule to avoid bloating this repo with large media files used for testing.

To checkout the testdb submodule:

```sh
git submodule update --init --recursive
```

Run tests with `hatch run test:all`

TODO: Explain how to run Hydrus using this DB.

## git workflow

`main` is for releases. It must be committed to through a PR from `develop` by the project maintainer.

`develop` should have the same history as main unless it has newer commits that have not been merged. When a PR is approved to develop, changes should be squash-merged.

Create PRs for `develop` if you want to submit changes.

## Maintainer Release Process

This release process section is for the maintainer.

Use the `develop` branch for development. Do not rewrite history on `develop` because others may be branched from it.
During development, if you think you might need to rewrite history, then create a branch off of `develop` and use that.
Commits to `develop` should be "clean"; do not add "WIP" commits to develop, because they will not be squashed when
released to main.

Once `develop` has all the changes you want to publish, follow the steps below.

```sh
git switch main
git rebase develop
```

Then, following semantic versioning rules, increment set the new version in `src/hydrusvideodeduplicator/__about__.py`.

After incrementing the version, do another test run locally on the CLI and GUI to ensure the DB upgrades as expected.
There should be no problem if the DB hasn't had changes since the last version and DB migration code wasn't touched.
There is no automated tests for testing DB migrations, so this **must** be done manually.

Add a commit with only the new version:

```sh
git add src/hydrusvideodeduplicator/__about__.py
git commit -m "Release X.Y.Z"
```

Add a git tag to the new commit:

```sh
git tag -a "vX.Y.Z" -m "vX.Y.Z"
```

Push the changes from develop and the tag:

```sh
git push
git push origin tag "vX.Y.Z"
```

Once the GitHub Actions for main are done running and everything passes, a release needs to be made on the GitHub Release page.
Follow the formatting from previous releases. A GitHub release will trigger a PyPI publish for the new package build and a GitHub
publish for the built executables to the new release.
