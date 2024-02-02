# Contributing guide

In this guide you will get an overview of the contribution workflow in AQUA, from opening an *issue*, creating a *Pull Request* (PR), reviewing, and merging the PR.

We welcome contributions to the AQUA project in many forms, and there's always plenty to do!

## Pull Requests

The contribution workflow is based on Pull Requests (PR). 
A Pull Request is a request to merge a set of changes into the `main` branch of the repository. 
It is the main way to contribute to AQUA.

Please note that the `main` branch is *not* protected. 
Anyway, pushing directly to `main` is forbidden and if not agreed in advance, your changes will be reverted.
Please use Pull Requests instead.

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
- please notice that a installation_lumi.sh script is available to install the framework on a LUMI machine.
  It is located in the `config/machines/lumi/installation` folder.
  If the environment has been modified, please make sure to update the files `environment_lumi.yml` and `pip_lumi.txt` accordingly.
- if a new feature has been added, please make sure to update the documentation accordingly
- add docstrings to your code
- remove any file unrelated to your Pull Request

Do not merge your Pull Request yourself, it will be merged by the AQUA team.

## Reporting issues

Before opening an issue, please [search the if the issue already exists](https://docs.github.com/en/github/searching-for-information-on-github/searching-on-github/searching-issues-and-pull-requests#search-by-the-title-body-or-comments). If it does, please add a comment to the existing issue instead of opening a new one.

As a general rule, if you are unsure, please open an issue anyway and we will help you.
There is no automatic assignement of issues to anyone. 
If you want to work on an issue, please comment on it and we will assign it to you.
If you open a pull request to close some issues, please reference the issue it solves in the description.

### Reporting bugs

If you find a bug in the source code, you can help us by opening an issue in the AQUA repository. 
If you have a solution to it, you can submit a Pull Request with a fix. 
Please use the `bug` label for bugs issue and the `fix` label for Pull Requests with a fix.

When describing the bug, please include as much information as possible. This includes:
- short description of the bug
- steps to reproduce the bug
- machine you are using

### Suggesting enhancements

Enhancements of existing features or new features may be suggested by opening an issue in the AQUA repository. Please use the `improvements` label for existing features and the `enhancement` label for new features.

### Coding style

Please follow the [PEP8](https://www.python.org/dev/peps/pep-0008/) coding style.
We use Flake8 to check the coding style, with a length limit of 127 characters per line.
We run the Flake8 check with the following command:

```bash
flake8 . --count --select=E9,F63,F7,F82
```
