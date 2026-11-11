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
- Mark every new test with an appropriate marker registered in `pyproject.toml` (for example, `pytestmark = pytest.mark.aqua` at module level or `@pytest.mark.console` on a test; add `network` to the few tests that need internet access, together with the usual marker so that CI selects them; an offline run deselects them with `-m "(aqua or console or graphics or catgen) and not network"`). CI runs pytest with `-m` filters and skips unmarked tests; verify collection with the marker expression used by the relevant workflow.
- Never create new test data files — parametrize over existing grids (`test-r2b0`, `test-pi-2d`, `test-healpix`, `test-hpz3-nested`)

## Test Design Principles

New and refactored tests follow six principles (rationale and batch plan: issue #3309). A test must fail when the code is wrong; raising coverage is not enough.

1. **Easy to review, minimal mocks**
   - Mock only the network, FDB and clock. Never mock the unit under test or its internal methods; use `monkeypatch`, not global `patch`.
   - Every assertion checks a value (hard-coded reference or independent oracle). Errors use `pytest.raises(..., match=)`.
   - The test name states the behaviour; a one-line docstring says what wrong code would make it fail.
   - Tests write only under `tmp_path` and never touch the installed `~/.aqua`, the current directory or the network. The session runs on a private `AQUA_CONFIG` (`tests/utils_tests/config.py`) and fails if new files appear in the repository root.
   - One batch of changes is one PR, tests only, of about 5 modules and 400 lines at most.
2. **Time maintained or reduced**
   - Retrieve once, in a session fixture. No new heavy retrieval or `rebuild=True` for a single assertion.
   - No `sleep`, no unseeded random data: each test passes alone, in random order and under `-n --dist loadgroup`.
3. **Coverage maintained or increased**
   - Per-module branch coverage must not go down. Delete or merge a test only if the coverage diff shows nothing is lost, and say which test covers it now.
   - A branch that no public path reaches gets a small unit test, or is reported as dead code.
   - A missing optional dependency (FDB, network) gives an explicit skip with a reason, never a silent pass.
4. **No change to scientific results**
   - Reference values come from `main` before the refactoring and are never regenerated from the new code. Tolerances are tight and never widened to make a test pass. Compare numbers and arrays with `pytest.approx` and xarray objects with `xr.testing.assert_equal` (or `assert_allclose` with its defaults); no `np.allclose`, `np.isclose`, `np.testing.assert_allclose` or hand-written `abs(a - b) < tol`. Use only three tolerances, defined in `tests/utils_tests`: the pytest default (`pytest.approx(value)`, rel=1e-6) first; `rel=APPROX_REL` (1e-4) only when 1e-6 is not achievable (regridding weights, dask reductions, platform floating point); `abs=APPROX_ABS` (1e-3) for machine-dependent fractions such as land fractions. Never use a tolerance that covers the difference between the values the test must distinguish; any other value is an exception that needs a comment explaining why. Compute the oracle in the dtype of the code under test (e.g. float64 when it promotes float32 data), instead of widening the tolerance.
   - Source fixes go in separate PRs; a bug exposed by a test is left failing (never edited to pass, never `xfail`), with its docstring pointing to an issue that documents the problem, never hidden by a weaker assertion.
   - Check a new assertion by breaking the source line it covers by hand and seeing the test fail.
5. **Integration over unit**
   - Default to the public entry points (`Reader`, the accessor, `Drop`, the console in-process) with real collaborators on the existing test data; assert the result and its provenance (`history`, `units`, `AQUA_*` attributes).
   - Unit tests are for pure functions with many cheap cases, error branches the pipeline cannot reach, and exact synthetic oracles. Do not test `_private` methods.
   - Graphics is the exception: call the plot function and assert on the `Axes`.
6. **Reduce the number of tests and avoid redundancy**
   - One behaviour, one owning test: before adding a test, search for an existing one and extend or parametrize it.
   - If an integration test already covers a branch, do not add a unit test for it. A test never re-implements the source logic to compute its expected value.
   - Shared setup lives once in a session fixture in `tests/conftest.py`: no alias fixtures, no second copy of the same dataset or `Reader`.

## Integration Checklist

- [ ] `DataModel.apply()` used for coordinate transformations
