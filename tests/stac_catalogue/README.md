# AQUA local STAC catalog

This directory contains a static STAC 1.0.0 catalog for the NetCDF fixtures in
`AQUA_tests/models` at the AQUA repository root. The fixture directory is
downloaded separately in CI and is not stored in Git.

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
