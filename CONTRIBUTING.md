# Contributing guide

In this guide you will get an overview of the contribution workflow in AQUA, from opening an *issue*, creating a *Pull Request* (PR), reviewing, and merging the PR.

We welcome contributions to the AQUA project in many forms, and there's always plenty to do!

## Reporting issues

Before opening an issue, please [search the if the issue already exists](https://docs.github.com/en/github/searching-for-information-on-github/searching-on-github/searching-issues-and-pull-requests#search-by-the-title-body-or-comments). If it does, please add a comment to the existing issue instead of opening a new one.

As a general rule, if you are unsure, please open an issue anyway and we will help you.
There is no automatic assignement of issues to anyone. If it is a bug, please fill all the cases and verify that reproducible code is reported.
If you open a pull request to close some issues, please reference the issue it solves in the description.

### Reporting bugs

If you find a bug in the source code, you can help us by opening an issue in the AQUA repository.
If you have a solution to it, you can submit a Pull Request with a fix.
Please use the `bug` label for bugs issue and the `fix` label for Pull Requests with a fix.

When describing the bug, please include as much information as possible. This includes:
- short description of the bug
- steps to reproduce the bug
- catalog you are using
- machine you are using

## Pull Requests

The contribution workflow is based on Pull Requests (PR).
A Pull Request is a request to merge a set of changes into the `main` branch of the repository.
It is the main way to contribute to AQUA.

### Creating a Pull Request

Pull requests can be created directly in the AQUA repository, creating your own fork of the repository is not mandatory.

When creating a Pull Request, please make sure to:
- add a meaningful title and description
- use square bracker to identify the topic (e.g. [Console], [CI/CD])
- reference the issue it solves in the description, if any
- start from the `main` branch if your Pull Request wants to be merged in the `main` branch
- point to the correct branch

If your Pull Request is adding some new dependencies, please make sure to state it in the description. Also check the conda-lock instructions below.

### Conda-lock

We use conda-lock files to guarantee a fully reproducible environment. Tests use these files to reproduce always the exact same
environment.

If a new dependency has been added, please make sure to update the `environment.yml` and `pyproject.toml` files.
In this case you will also need to regenerate the conda-lock files. To do so, add the `conda-lock` label to your Pull Request:
this triggers the `conda-relock` workflow on your branch, which regenerates the lock files and opens a new PR
(`[Conda-Lock] Relock AQUA-core for <your-branch>`) targeting your branch, with the `run tests` label already set.
If tests in that PR pass, merge it into your branch to update the lockfiles. Remember to remove the label to avoid further triggering of relocking. 

The relock workflow also runs every Monday on `main`, and can be launched manually from the GitHub Actions tab (`workflow_dispatch`), in which case it relocks `main`.

### Finalizing a Pull Request

If your Pull Request is a work in progress, please set it to "Draft" mode.
When your Pull Request is ready to be reviewed, please remove the "Draft" mode.
A label `run tests` can be added to the Pull Request to activate the CI tests (see [Testing](#testing) for all the test labels).
A label `ready for review` can be added to the Pull Request to indicate that it is ready to be reviewed and hopefully merged in the opinion of the author.

Before asking for a review, please make sure to:
- Be up to date with the `main` branch
- Run the tests successfully
- If a new dependency has been added to the framework, please make sure to update the `environment.yml` and `pyproject.toml` files and relock the environment via the `conda-lock` label (see above).
- Please notice that a lumi_install.sh script is available to install the framework on a LUMI machine.
  It is located in the `cli/lumi-install` folder.
- If a new feature has been added, please make sure to update the documentation accordingly
- Add docstrings to your code
- Remove any file unrelated to your Pull Request

When you are ready to merge:
- Make sure that all items in your checklist have been completed. Do not forget to update the `CHANGELOG.md` file

Do not merge your Pull Request yourself, it will be merged by the AQUA team.

### Suggesting enhancements

Enhancements of existing features or new features may be suggested by opening an issue in the AQUA repository. Please use the `improvements` label for existing features and the `enhancement` label for new features.

### Coding style checks with ruff and pre-commit in a Pull Request

This project uses pre-commit hooks and ruff as linter and formatter to enforce the coding style.
The coding style is defined by the `pyproject.toml` file and the `pre-commit` configuration file in `.pre-commit-config.yaml`.

The pre-commit and ruff dependencies are installed automatically when setting up
the dev environment through the `environment-dev.yml` file.
The pre-commit configuration enforces two groups of checks:

- **General file cleaning hooks** from `pre-commit-hooks`: large-file check, trailing whitespace trimming (except `.md` and `Makefile`), end-of-file newline fix, YAML validation, and overall Python syntax validation.
- **Ruff hooks**: `ruff-check` and `ruff-format`.

Ruff has two complementary roles in this workflow:

- `ruff check` runs linting rules and reports code-quality/style violations; with `--fix` (and optionally `--unsafe-fixes`) it can also auto-fix part of them.
- `ruff format` only applies formatting (layout/style normalization), similar to a code formatter, without running lint-rule diagnostics.

To check and align code changes introduced in a PR with the coding style, developers can choose between the following options:

1. [Recommended option] Use `pre-commit` hooks to check and align code changes at commit time.


This requires installing the `pre-commit` hooks first. From the root folder of the repository, run:

```bash
pre-commit install
```

From this moment on, every time a commit is made, the `pre-commit` hooks will be run automatically to check and align
the code changes with the coding style. If the code changes are not aligned, the commit will be rejected, and the
proposed changes will appear as unstaged changes in the developer’s local repository.
The developer can then review the changes, stage them again, and commit successfully.

If, for any reason, the developer wants to disable the `pre-commit` hooks, they can run:
```bash
pre-commit uninstall
```

2. Alternatively, developers can manually run the `pre-commit` hooks to check the coding style of the code changes.
To trigger all the pre-commit hooks manually, from the root folder of the repository, run:

```bash
pre-commit run -a
```

If the developer wants to run the pre-commit hooks only for specific file(s) or folder(s), they can run:
```bash
pre-commit run --files <file_or_folder_to_target>
```

Side note:
During the manual run of the pre-commit hooks, some errors can be fixed automatically
by ruff. However, in some cases, ruff may require the extra flag `--unsafe-fixes`.
This flag allows ruff to apply fixes that might change the behavior of your code, even when it is not safe to do so.  
Use it with caution and review the diff!
To run manually ruff linter with the `--unsafe-fixes` flag:
```bash
ruff check --fix <file_or_folder_to_target> --no-cache --unsafe-fixes
```
(to run over all files, set "." as the target, from the root folder of the repository)

Then, to run the formatter manually:

```bash
ruff format <file_or_folder_to_target> --no-cache
```

This manual run will also format the code according to the formatting rules defined by the `pyproject.toml` file.

## Testing

AQUA is tested with `pytest`, both locally and through GitHub Actions.
CI is organised in **suites**: each suite tests a different way of installing AQUA, so that a broken
dependency or packaging problem is caught by the suite that owns it, not by the PR tests.

### Test suites

| Workflow | Suite | When it runs | Platforms | Python | Install mode | FDB | Coverage |
|---|---|---|---|---|---|---|---|
| `aqua.yml` | PR tests | PR with a test label, Dependabot PRs, push to `main`, manual | `ubuntu-latest` in FDB container | 3.14 | `local` | yes | yes (not on Dependabot) |
| `aqua-nightly.yml` | Nightly | every night (03:00 UTC), PR with `nightly` label, Dependabot pip/conda PRs, manual | `ubuntu-26.04`, `xcode-27`, `ubuntu-latest` in FDB container | 3.14 | `environment` | container only | no |
| `aqua-release.yml` | Release | every Monday (05:00 UTC), PR with `release` label, manual | `ubuntu-22.04`, `ubuntu-26.04`, `macos-15`, `xcode-27` | 3.12, 3.13, 3.14 | `release` | no | no |
| `aqua-pypi.yml` | PyPI | every Tuesday (02:00 UTC), push to `pypi-test`, manual | `ubuntu-latest`, `macos-latest` | 3.12, 3.13, 3.14 | `pypi` | no | no |
| `aqua-conda.yml` | Conda | every Tuesday (03:00 UTC), push to `conda-test`, manual | `ubuntu-latest`, `macos-latest` | 3.12, 3.13, 3.14 | `conda` | no | no |
| `aqua-ruff.yml` | Style | every push | `ubuntu-latest` | 3.14 | `pre-commit` only | - | - |

Runner labels are the GitHub-hosted ones: `xcode-27` is the latest macOS image,
`macos-15` the previous one; `*-latest` labels move automatically to the newest stable image.

The install modes are:

- `local`: checkout + `conda-linux-64.lock` (with FDB), i.e. the exact locked environment. Linux only.
- `environment`: checkout + `environment.yml`, unpinned: catches breakages from new upstream releases.
- `release`: `environment-pypi.yml` (conda dependencies only) + `pip install -e .[all]` of the checkout: what a user gets when installing from source.
- `pypi`: `environment-pypi.yml` + `aqua-core[all]` from PyPI, tested against the matching release tag.
- `conda`: `aqua-core` from conda-forge, tested against the matching release tag.

`aqua-tests / aqua_test (3.14)` is a **required check** for merging into `main`.

### Triggering tests on a Pull Request

Tests are not run on every commit, since GitHub Actions hours are limited. They are controlled by PR labels:

| Label (contains) | Effect |
|---|---|
| `run tests`, `ready for review`, `ready to merge` | run `aqua.yml` |
| `nightly` | run `aqua-nightly.yml` (use it when changing dependencies or platform-specific code) |
| `release` | run the full `aqua-release.yml` matrix (use it on release PRs) |
| `conda-lock` | regenerate the lock file (see [Conda-lock](#conda-lock)) |

Once a label is set, the corresponding suite runs again at every push.
Adding the label triggers a run only for the label just added: adding an unrelated label does not restart running tests.
A new push cancels the runs of the previous commit on the same PR.

Dependabot PRs always run `aqua.yml`; pip/conda bumps also run the nightly suite, since they change the environment.

### How the CI is built

All suites call one reusable workflow, `aqua-test-reusable.yml`, which runs **one job** (one platform, one Python);
callers only define the matrix and the inputs. The job checks out the repositories, creates the environment for the
`install-mode` with micromamba, installs AQUA, then relies on the composite actions in `.github/actions/`
(reusable by other repositories, e.g. diagnostics):

- `setup-test-data`: test data into `AQUA/AQUA_tests` from DVC, HTTP, or DVC with HTTP fallback (`auto`, default);
- `setup-fdb`: loads the sample GRIB files into FDB and runs a smoke test;
- `setup-aqua`: `aqua install github`, adds the `climatedt-gen2` and `ci` catalogs, sets test paths and Polytope key;
- `pytest-reports`: coverage to Codecov, xdist worker log on failure, artifact upload.

AQUA callers pass `aqua-actions-ref: ${{ github.sha }}`, so a PR is tested with its own version of the actions
(external repositories default to `main`). `AQUA_TOKEN` is used for all GitHub access (fallback: `github.token`).

Pytest is run with:

- **markers** (`pyproject.toml`): `aqua`, `graphics`, `catgen`, `console`, `fdb` (FDB suites only), `polytope` (not in Conda).
  A test without a marker is never run in CI;
- **parallelism**: `-n 4 --dist loadgroup`; tests that cannot run concurrently share a `pytest.mark.xdist_group`;
  each worker gets its own `TMPDIR` (`tests/conftest.py`);
- **timeouts**: 300 s per test (`pytest-timeout-seconds`), 40 min per run; hung tests are found from
  `.pytest_cache/xdist_worker_tests.log`, printed on failure;
- **coverage**: only in `aqua.yml`, sent to Codecov (`codecov.yml`).

### Artifacts and failure reports

Every job uploads an artifact `aqua-test-artifacts-<install-mode>-<platform>-fdb<bool>-py<version>` with
`junit.xml`, `coverage.xml`, `aqua_pytest.log`, `.pytest_cache` (including the xdist worker log), `micromamba_list.txt` and `pip_freeze.txt`.
Download it from the run page to debug a failure or to compare environments.

When a scheduled or manual run of the Nightly, Release, PyPI or Conda suite fails, `report-failure-reusable.yml`
opens an issue labelled `ci-failure:<suite> :clown_face:` listing the failed jobs and the broken tests (parsed from `junit.xml`).
If an open issue with that label already exists, the report is added as a comment instead. Close the issue once the suite is green again.

### Running tests locally

1. Install the development environment (`environment.yml`, which includes the `tests` extra).
2. Get the test data into `AQUA_tests/` in the repository root from the HTTP mirror (the CI fallback):
   ```bash
   mkdir -p AQUA_tests && cd AQUA_tests
   wget -nv -r -np -nH -R "index.html*" --cut-dirs=3 http://wilma.to.isac.cnr.it/paolo/aqua-tests/data-tests/
   ```
   The same data is versioned with DVC in the `aqua-dvc` repository (the CI default), but pulling it requires
   access to that repository and credentials for the DVC remote.
   `./download_data_for_tests.sh` downloads an older archive and is **deprecated**.
3. Configure AQUA as CI does: `aqua install github`, `aqua add ci`, and set the `grids`, `weights` and `areas` paths in `~/.aqua/config-aqua.yaml` to `./AQUA_tests/...`.
4. Run the tests from the repository root, e.g.:
   ```bash
   pytest -m aqua                                        # core tests
   pytest -n 4 --dist loadgroup -m "aqua or graphics"    # in parallel, as in CI
   ```

### Changing the CI

- Changes to `aqua.yml`, the reusable workflow or the composite actions are tested by the PR itself (add `run tests`).
  Add `nightly` and `release` when the change affects other suites.
- To test changes to `aqua-pypi.yml` or `aqua-conda.yml`, push to a branch named `pypi-test` or `conda-test`.
- Keep logic shared across suites in the reusable workflow or in a composite action, not in the callers:
  callers should only define the matrix and the inputs.

## 🤖 AI Contribution Guidelines

If you are an AI agent, please read AGENTS.md in the root directory
