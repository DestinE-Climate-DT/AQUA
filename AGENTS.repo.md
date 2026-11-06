# AQUA-core Repository Instructions

These instructions apply only to the AQUA-core repository. Read them after
`AGENTS.common.md`, as directed by `AGENTS.md`.

## Before implementing anything

1. Search `aqua/core/` for an existing class handling similar logic
2. Extend it if a clean extension point exists
3. Create a new module only if no viable extension point exists

## Core Architecture

- **Modularity over monoliths**: when not possible to delegate to specialized (`FixerConfigure`, `FixerOperator`, `FixerDataModel`) sub-classes, use mixins (`InstallMixin`, `CatalogMixin`, `FilesMixin`)
- **Console logging exception**: existing user-facing `print()` calls in `aqua/core/console/` may remain; update only in files already being modified for functional changes
- **Legacy configuration exception**: some legacy install/config flows still use explicit defaults — route new logic through `ConfigContext` but do not refactor existing flows unless already touching them
- At write/output boundaries, use `_compute_data()` in the writer to materialise data.
- CLI commands go in `aqua/core/console/` and register in `AquaConsole.command_map`; parser definitions in `aqua/core/console/parser.py`

## Intake Drivers

- Extend an Intake driver to support new data formats (see `aqua/core/intake_drivers/` — `xarray/` for the netcdf/zarr sources, `fdb/` for FDB/GSV/Polytope/z3fdb, `icechunk/` for IceChunk); do not bypass Intake
- AQUA uses intake ≥2 (pinned in `pyproject.toml`) with v1-style YAML catalogs; the `netcdf`, `zarr`, `gsv` and `icechunk` drivers are registered by AQUA itself (see `aqua/core/intake_drivers/__init__.py`)

## Test Assets and CI Selection

- Tests live in `tests/`; fixtures and reference assets come from `AQUA_tests/` plus the repository-level `cell_area_*.nc` / `weights_*.nc`
- Mark every new test with an appropriate marker registered in `pyproject.toml` (for example, `pytestmark = pytest.mark.aqua` at module level or `@pytest.mark.console` on a test). CI runs pytest with `-m` filters and skips unmarked tests; verify collection with the marker expression used by the relevant workflow.
- Never create new test data files — parametrize over existing grids (`test-r2b0`, `test-pi-2d`, `test-healpix`, `test-hpz3-nested`)

## Integration Checklist

- [ ] `DataModel.apply()` used for coordinate transformations
