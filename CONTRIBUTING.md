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
- reference the issue it solves in the description, if any
- start from the `main` branch if your Pull Request wants to be merged in the `main` branch
- point to the correct branch

If your Pull Request is adding some new dependencies, please make sure to state it in the description.

### Finalizing a Pull Request

If your Pull Request is a work in progress, please add `[WIP]` to the title.
When your Pull Request is ready to be reviewed, please remove `[WIP]` from the title. A label `run tests` can be added to the Pull Request to activate the CI tests.
A label `ready to merge` can be added to the Pull Request to indicate that it is ready to be reviewed and hopefully merged in the opinion of the author.

Before asking for a review, please make sure to:
- be up to date with the `main` branch
- run the tests successfully
- if a new dependency has been added to the framework, please make sure to update the `environment.yml` and `pyproject.toml` files
- please notice that a lumi_install.sh script is available to install the framework on a LUMI machine.
  It is located in the `cli/lumi-install` folder.
  If the environment has been modified, please make sure to update the files `environment_lumi.yml` and `pip_lumi.txt` accordingly.
- if a new feature has been added, please make sure to update the documentation accordingly
- add docstrings to your code
- remove any file unrelated to your Pull Request

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
