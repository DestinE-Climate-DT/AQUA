# AQUA Agent Guidelines

## Core Directives

**Code reuse, modularity, minimal dependencies, lazy evaluation, and performance are non-negotiable.**

### Before implementing anything

1. Search `aqua/core/` for an existing class handling similar logic
2. Extend it if a clean extension point exists
3. Create a new module only if no viable extension point exists

---

## Processing Pipeline (Mental Model)

Data always flows as:

```
Catalog → Reader → Fixer → DataModel → Regridder → Statistics → Output
```

- **`Reader` is the sole entry point** for all data workflows — it orchestrates `Fixer`, `DataModel`, `Regridder`, and statistics via internal collaborator objects
- Do not call `Fixer`, `DataModel`, or `Regridder` directly in user-facing code
- Each step is independent and composable; no step should bypass or duplicate another

---

## Reuse Map

> Verify import paths before use — this table reflects the stable core API but may lag refactors.

| Need | Import |
|------|--------|
| Logging | `from aqua.core.logger import log_configure` |
| Provenance / history | `from aqua.core.logger import log_history` |
| List coercion | `from aqua.core.util import to_list` |
| Env vars, attrs, username | `from aqua.core.util import expand_env_vars, extract_attrs, username` |
| Paths & catalog config | `from aqua.core.configurer import ConfigPath` |
| YAML load/merge/dump | `from aqua.core.util import load_yaml, load_multi_yaml, dump_yaml` |
| Lock shared config writes | `from aqua.core.lock import SafeFileLock` |
| Coordinate standardization | `from aqua.core.data_model import DataModel` |
| Variable/unit fixing | `from aqua.core.fixer import Fixer` |
| Regridding | `from aqua.core.regridder import Regridder` |
| Field statistics | `from aqua.core.fldstat import FldStat` |
| Data access | `from aqua.core.reader import Reader, Streaming` |
| Errors | `aqua.core.exceptions` |

---

## Architecture Rules

- **Modularity over monoliths**: when not possible to delegate to specialized (`FixerConfigure`, `FixerOperator`, `FixerDataModel`) sub-classes, use mixins (`InstallMixin`, `CatalogMixin`, `FilesMixin`)
- **No `print()`**: use `self.logger = log_configure(log_level=loglevel, log_name="ClassName")` and pass `loglevel` through every constructor *(existing user-facing `print()` calls in `aqua/core/console/` may remain; update only in files already being modified for functional changes)*
- **No hardcoded paths**: route installation/catalog/machine resolution through `ConfigPath` and `ConfigLocator`; respect `AQUA_CONFIG` and `$HOME/.aqua` *(some legacy install/config flows still use explicit defaults — route new logic through `ConfigPath` but do not refactor existing flows unless already touching them)*
- **Configuration is declarative**: define grids, fixes, data models, catalogs, and analyses in YAML first; add Python `if/else` only when config cannot express the behaviour
- **Protect shared YAML writes**: use `SafeFileLock` + `dump_yaml()`; never use raw `open(..., "w")` on shared metadata files
- **No new external dependencies** without discussion; check the Reuse Map and External Dependencies section first
- **Accessor contract**: if a method returns an xarray object intended for chained AQUA operations, call `data.aqua.set_default(self)` before returning
- **Backward compatibility**: public API changes require a CHANGELOG entry; new parameters must have defaults so existing call sites keep working
- **Prefer imports at the top of the file**: avoid importing inside functions or methods unless strictly necessary

---

## Intake Usage Rules

All data *read paths* must go through Intake catalogs unless explicitly justified:

- Do not hardcode file access patterns already representable in catalog YAML
- Extend an Intake driver to support new data formats (see `aqua/core/gsv/` for a reference implementation); do not bypass Intake
- Use Intake 0.7.x syntax (pinned in `pyproject.toml`)

---

## Dask & HPC Guidelines

- Do not trigger computation (`compute`, `persist`) inside transformation or processing code — leave execution control to the caller
- **Exception**: materialisation at write/output boundaries is correct and expected (e.g. `to_netcdf()` requires in-memory data; use `_compute_data()` in the writer)
- Chunking strategy must be defined at `Reader` level via the `chunks` argument, not downstream
- Never assume a specific scheduler (threads/processes/distributed); let the caller configure Dask

---

## Data Handling

- Set `xr.set_options(keep_attrs=True)` at the top of each module performing xarray operations *(currently set per-module; no global consolidation exists yet)*
- All xarray operations are lazy by default — do not force `.compute()` early (see Dask & HPC)
- Preserve provenance: after any transform that changes variables, coordinates, grids, or statistics, call `log_history(data, msg)` and keep `AQUA_*` attrs updated via `set_attrs()` / `update_metadata()`
- **DataArray → Dataset conversion silently drops Dataset-level attrs** — always copy them explicitly:
  ```python
  attrs = da.attrs.copy()
  ds = da.to_dataset(name=var_name)
  ds.attrs.update(attrs)
  ```
- **Attribute loss through slicing is a common silent failure** — verify that Dataset-level attrs (especially `history`) survive `.sel()` in tests
- Cache expensive results (areas, weights) with a `hasattr` guard:
  ```python
  if not hasattr(self, '_cached_areas'):
      self._cached_areas = compute_areas(...)
  return self._cached_areas
  ```

---

## Error Handling

Custom exceptions live in `aqua.core.exceptions`. Raise them in core modules; catch them in user-facing layers (CLI, diagnostics entry points).

| Exception | Raise when |
|---|---|
| `NoDataError` | data retrieval from catalog/source finds nothing |
| `NoRegridError` | requested regridding weights or grid are unavailable |
| `NotEnoughDataError` | data exists but is insufficient for the requested operation |
| `NoObservationError` | observation dataset is missing for a comparison/validation |
| `NoEcCodesShortNameError` | requested GRIB short name not found in eccodes |

---

## Forbidden Patterns

- Calling `.compute()` or `.values` in transformation/processing code (materialise only at write boundaries)
- Looping over xarray dimensions instead of using vectorized operations
- Re-reading the same dataset multiple times — retrieve once and pass the object
- Calling `Fixer`, `DataModel`, or `Regridder` directly in user-facing code (use `Reader`)
- Using `open(..., "w")` on shared catalog/config YAML files (use `SafeFileLock` + `dump_yaml()`)
- Building a new Dataset from scratch when `assign()` / `assign_coords()` on an existing one would preserve metadata

---

## New Module Template

```python
import xarray as xr
from aqua.core.logger import log_configure, log_history
from aqua.core.util import to_list

xr.set_options(keep_attrs=True)


class MyProcessor:
    def __init__(self, loglevel="WARNING"):
        self.logger = log_configure(log_level=loglevel, log_name="MyProcessor")
        self.loglevel = loglevel

    def transform(self, data, variables=None):
        variables = to_list(variables)
        self.logger.info("Transforming %d variables", len(variables))
        # ... transformation logic ...
        log_history(data, "transformation applied by MyProcessor")
        return data  # keep lazy
```

- Use `%`-style formatting in all logger calls (`self.logger.info("val: %s", x)`) — never f-strings, to avoid eager string construction
- CLI commands go in `aqua/core/console/` and register in `AquaConsole.command_map`; parser definitions in `aqua/core/console/parser.py`

---

## Code Style

All code is linted with **Ruff**: no unused imports, no bare `except`, consistent quoting, line length ≤ 127. Run `ruff check --fix` before submitting.

---

## External Dependencies

Prefer in this order before adding anything new: `xarray`, `numpy`, `pandas`, `metpy`, `smmregrid`, `intake_xarray`.

---

## Docstrings & Documentation

Use Google-style docstrings. Minimum required:

```python
def method(self, arg1, arg2=None):
    """One-line description.

    Args:
        arg1 (type): Description.
        arg2 (type, optional): Description. Defaults to None.

    Returns:
        type: Description.

    Raises:
        NoDataError: When no data is found.
    """
```

- Private/internal methods (`_name`) need only a one-liner
- Always add a usage example in the module-level docstring for public classes

---

## Testing

- Tests live in `tests/`; fixtures and reference assets come from `AQUA_tests/` plus the repository-level `cell_area_*.nc` / `weights_*.nc`
- Never create new test data files — parametrize over existing grids (`test-r2b0`, `test-pi-2d`, `test-healpix`, `test-hpz3-nested`)
- Use `Reader(model=..., exp=..., source=...)` to load test data; avoid raw `xr.open_dataset` unless testing I/O directly
- Prefer shared fixtures in `tests/conftest.py` with `session` or `module` scope to avoid repeated heavy retrievals
- Favour `pytest.mark.parametrize` over duplicated test bodies
- When testing write paths, verify that Dataset-level attributes (especially `history`) are present in the output file — attribute loss through slicing or DataArray→Dataset conversion is a common silent failure

---

## Integration Checklist

- [ ] `DataModel.apply()` used for coordinate transformations
- [ ] Semantic data transforms call `log_history()` and preserve `AQUA_*` attrs
- [ ] xarray outputs for AQUA chaining call `data.aqua.set_default(self)`
- [ ] Operations remain lazy until write/output boundaries
- [ ] No new dependency added unless explicitly requested
- [ ] Public API changes have a CHANGELOG entry and new parameters have defaults
