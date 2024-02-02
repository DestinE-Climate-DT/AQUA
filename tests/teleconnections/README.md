# How to locally run the teleconnections tests

## Requirements

- the `AQUA` framework and diagnostics installed, use the `conda` environment `aqua` in the main `AQUA` directory.
- an internet connection

## Run the tests

You should be located in the main `AQUA` directory.

- download the test data, both the framework and the teleconnections data:

```bash
source download_data_for_tests.sh
source tests/teleconnections/download_data_for_tests.sh
```

- run the tests:

```bash
pytest -m teleconnections
```