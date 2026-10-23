# AQUA local STAC catalog

This directory contains a generator for a static STAC 1.0.0 catalog for the NetCDF fixtures in
`AQUA_tests/models` at the AQUA repository root. The fixture directory is
downloaded separately in CI and is not stored in Git.

Pytest generates the JSON files once per test session using `generate_catalog.py`,
before test collection or catalog discovery. All generated JSON files, including
`catalog.json`, are ignored by Git and remain in this directory after the tests
finish for inspection or reuse. Only the generator and this README are version controlled.

- `catalog.json`: root catalog
- `collections/*.json`: one collection per model family
- `items/<collection>/*.json`: one item per NetCDF file
- `generate_catalog.py`: generator used to rebuild the JSON files

Each item has a `data` asset with media type `application/x-netcdf`. Its `href`
is relative to the item JSON file, so the catalog and `AQUA_tests` directory can
move together. The path relative to `AQUA_tests/models` is also stored in the
item property `aqua:relative_path`.

## Regenerate

Install the `ncdump` command, place `AQUA_tests` at the repository root, and run
from the root:

```sh
python3 tests/stac_catalogue/generate_catalog.py
```

The generator finds `AQUA_tests/models` from its location and writes the JSON
files next to itself. It fails if the fixture directory or NetCDF files are
missing.

## Reuse in tests

The session-scoped `stac_catalogue_dir` fixture in `tests/conftest.py` is available
to every test module. Request it to obtain the directory prepared before collection:

```python
def test_collection(stac_catalogue_dir):
    collection_path = stac_catalogue_dir / "collections" / "ifs.json"
    assert collection_path.is_file()
```

Generation runs in the controller's `pytest_configure` hook, before pytest-xdist
starts workers. A shared lock protects generation, and workers only read the
completed catalog. Generation requires `ncdump` and is skipped if
`AQUA_tests/models` is unavailable; the fixture then skips dependent tests.
Generated files are retained after the session for inspection.

Reader integration tests use the STAC Intake examples in the installed `ci`
catalog from [Climate-DT-catalog](https://github.com/DestinE-Climate-DT/Climate-DT-catalog).
Install `ci` before running the tests, as in the standard AQUA test setup. The
tests generate only the STAC JSON files; they reuse the installed Intake YAML
entries under `stac` (`ifs`, `nemo`, and `ifs-items`).

## PySTAC

```python
from pathlib import Path

import pystac
import xarray as xr

catalog_path = Path("tests/stac_catalogue/catalog.json").resolve()
catalog = pystac.Catalog.from_file(str(catalog_path))

print([collection.id for collection in catalog.get_collections()])
item = next(catalog.get_all_items())
asset_path = item.assets["data"].get_absolute_href()
dataset = xr.open_dataset(asset_path)
print(dataset)
```

## AQUA Intake STAC driver

```python
from pathlib import Path

from aqua.core.intake_drivers import IntakeSTACSource

item_path = Path("tests/stac_catalogue/items/ifs/ifs-short-ifs2d-tco79.json").resolve()
source = IntakeSTACSource(urlpath=str(item_path), asset="data")
dataset = source.read()
print(dataset)
```

Reading NetCDF4 files requires an xarray-compatible backend such as `netCDF4`
or `h5netcdf`.

## Notes

- Geometry is `null` because several fixtures use unstructured or non-spatial
  result layouts; collection extents use a global bounding box.
- Time extents come from a NetCDF `time` or `time_counter` coordinate when one
  exists. Files without a usable time coordinate fall back to a year in the
  filename and then to file modification time. The choice is recorded in
  `aqua:datetime_source`.
