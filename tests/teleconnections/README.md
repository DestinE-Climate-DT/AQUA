# How to locally run the teleconnections tests

## Requirements

- `teleconnections` environment installed (see [README.md](../../diagnostics/teleconnectionsREADME.md))
- an internet connection

## Run the tests

You should be located in the main `AQUA` directory.

- download the test data:

```bash
source tests/teleconnections/download_data_for_tests.sh
```

- run the tests:

```bash
pytest -m teleconnections
```