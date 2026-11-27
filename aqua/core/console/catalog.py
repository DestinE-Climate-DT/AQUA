#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AQUA catalog operations mixin
"""

import os
import sys
from contextlib import nullcontext

import fsspec
from requests.exceptions import HTTPError as RequestsHTTPError

from aqua.core.configurer import ConfigCatalog
from aqua.core.configurer.context import CATALOG_KEY

from .github import GitHubAPIRequestTracker, GitHubCatalogFetcher

# folder used for reading/storing catalogs
CATPATH = "catalogs"


class CatalogMixin:
    """Mixin for AQUA catalog operations"""

    def _log_github_api_error(self, error, context):
        """Log GitHub response details that distinguish quota and permission failures."""
        response = error.response
        if response is None:
            self.logger.error("GitHub API request failed during %s: %s", context, error)
            return

        headers = response.headers
        rate_info = {
            key: headers.get(header)
            for key, header in (
                ("resource", "X-RateLimit-Resource"),
                ("remaining", "X-RateLimit-Remaining"),
                ("limit", "X-RateLimit-Limit"),
                ("reset", "X-RateLimit-Reset"),
                ("retry_after", "Retry-After"),
                ("sso", "X-GitHub-SSO"),
            )
            if headers.get(header) is not None
        }
        try:
            message = response.json().get("message", "")
        except (ValueError, AttributeError):
            message = response.text[:300]

        self.logger.error(
            "GitHub API request failed during %s (HTTP %s): %s; response headers: %s",
            context,
            response.status_code,
            message,
            rate_info,
        )

    def catalog_set(self, args):
        """Set an installed catalog as the active one in config-aqua.yaml

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        self._check_installed_catalog(args.catalog)
        self._catalog_key_set(args.catalog)

    def catalog_add(self, args):
        """Add a catalog, linked with -e or downloaded from GitHub, and make it the active one

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        self._catalog_add(args.catalog, editable=args.editable, repository=args.repository)

    def _catalog_add(self, name, editable=None, repository=None):
        """Add a catalog to an existing installation and make it the active one.

        Args:
            name (str): catalog name
            editable (str): path of a local catalog folder to link, instead of downloading it
            repository (str): GitHub repository to download from, as user/repo: the default one if None
        """
        print("Adding the AQUA catalog", name)
        self._check_new_catalog(name)
        source = self._catalog_source(name, editable, repository)
        try:
            self._catalog_install(name, source, link=editable is not None)
            self._catalog_key_set(name)
        except BaseException as e:
            # nothing was there before: what was linked or downloaded is removed, also after an interruption
            self._remove_target(self._catalog_path(name))
            if not isinstance(e, Exception):
                raise
            self.logger.error("Cannot add catalog %s, what was linked or downloaded has been removed. Error: %s", name, e)
            sys.exit(1)

    def _catalog_source(self, name, editable=None, repository=None):
        """Where to take a catalog from, checked before any change: the local folder given for the editable mode,
        which must exist, or the GitHub repository as user/repo (None for the default one)."""
        if editable is None:
            self._github_repository_parts(repository)
            return repository
        editable = os.path.abspath(editable)
        if not os.path.exists(editable):
            self.logger.error(
                "Catalog in editable mode %s cannot be found at %s. "
                "Using the Climate-DT catalog repository, likely need a path like: "
                "/full/path/to/Climate-DT-catalog/catalogs/%s",
                name,
                editable,
                name,
            )
            sys.exit(1)
        return editable

    def _catalog_install(self, name, source, link=False):
        """Link, or download, a catalog and check that it loads: shared by add and update.

        Args:
            name (str): catalog name
            source (str): the local folder to link, or the GitHub repository as user/repo (None for the default one)
            link (bool): link the folder instead of downloading from the repository
        """
        if link:
            print("Installing catalog in editable mode from", source, "to", self.configpath)
            os.symlink(source, self._catalog_path(name))
        else:
            with GitHubAPIRequestTracker(self.logger, f"catalog {name}") as request_tracker:
                self._catalog_download(name, source, request_tracker=request_tracker)
        # a catalog can be validated only once it is there: it raises if its catalog or machine file is missing
        ConfigCatalog(catalog=name, loglevel=self.loglevel)

    def _github_explore(self, repository=None, request_tracker=None):
        """
        Explore the remote GitHub repository

        Args:
            repository (str): the repository to explore, if None it uses the default
                              DestinE-Climate-DT/Climate-DT-catalog

        Returns:
            fsspec.filesystem: the filesystem object for the GitHub repository
        """
        org, repo = self._github_repository_parts(repository)
        try:
            username, token = self._github_credentials()
            if token and username:
                auth_kwargs = {"username": username, "token": token}
                self.logger.info("Using authenticated access to GitHub API.")
            else:
                auth_kwargs = {}
                self.logger.info("Running without authentication. Rate limits may apply.")
                self.logger.info(
                    "Consider setting GITHUB_TOKEN and GITHUB_USER environment variables for authenticated access."
                )

            stage = request_tracker.stage("GitHub filesystem initialization") if request_tracker else nullcontext()
            with stage:
                fs = fsspec.filesystem(
                    "github",
                    org=org,
                    repo=repo,
                    **auth_kwargs,  # Apply authentication if available
                )
            self.logger.info("Accessed remote repository https://github.com/%s/%s", org, repo)
            if token and username:
                self.logger.debug(
                    "fsspec GitHub filesystem auth configured: username_present=%s token_present=%s",
                    bool(fs.username),
                    bool(fs.token),
                )
        except RequestsHTTPError as exc:
            self._log_github_api_error(exc, f"opening {org}/{repo}")
            sys.exit(1)

        return fs

    @staticmethod
    def _github_credentials():
        """Return the selected GitHub username and configured token."""
        is_github_actions = os.getenv("GITHUB_ACTIONS") == "true"
        username = "github-actions" if is_github_actions else os.getenv("GITHUB_USER")
        token = os.getenv("GITHUB_TOKEN")
        return username, token

    def _github_repository_parts(self, repository=None):
        """Return the owner and repository, using AQUA's standard catalog repository by default."""
        if repository is None:
            return "DestinE-Climate-DT", "Climate-DT-catalog"
        parts = repository.split("/")
        if len(parts) != 2 or not all(parts):
            self.logger.error("Repository should be in the format user/repo, got %s", repository)
            sys.exit(1)
        return parts[0], parts[1]

    def catalog_avail(self, args):
        """
        Return the catalog available on the Github website

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        with GitHubAPIRequestTracker(self.logger, "catalog availability") as request_tracker:
            fs = self._github_explore(repository=args.repository, request_tracker=request_tracker)
            with request_tracker.stage("catalog listing"):
                available_catalog = [os.path.basename(x) for x in fs.ls(f"{CATPATH}/")]
        print("Available ClimateDT catalogs at are:")
        print(available_catalog)

    def _catalog_download(self, catalog, repository=None, request_tracker=None):
        """
        Download a catalog from a GitHub repository into its folder of the installation.

        The callers make sure that the folder is not there: a new catalog, or one set aside by the update.

        Args:
            catalog (str): the catalog to be downloaded
            repository (str): the repository, as user/repo: DestinE-Climate-DT/Climate-DT-catalog if None
            request_tracker (GitHubAPIRequestTracker): tracker of the GitHub API requests, optional
        """
        cdir = self._catalog_path(catalog)
        try:
            username, token = self._github_credentials()
            self.logger.info("Fetching remote catalog %s from github to %s", catalog, cdir)
            if token and username:
                self.logger.info("Using authenticated batched access to GitHub API.")
                org, repo = self._github_repository_parts(repository)
                available_catalog = GitHubCatalogFetcher(
                    org, repo, username, token, request_tracker=request_tracker
                ).fetch_catalog(catalog, cdir)
            else:
                fs = self._github_explore(repository, request_tracker=request_tracker)
                stage = request_tracker.stage("catalog listing") if request_tracker else nullcontext()
                with stage:
                    available_catalog = [os.path.basename(x) for x in fs.ls(f"{CATPATH}/")]
                if catalog in available_catalog:
                    os.makedirs(cdir, exist_ok=True)
                    source_dir = f"{CATPATH}/{catalog}"
                    self._fsspec_get_single_call(fs, source_dir, cdir, request_tracker=request_tracker)

            if catalog not in available_catalog:
                self.logger.error(
                    "Cannot find on %s the requested catalog %s, available are %s",
                    repository if repository else "DestinE-Climate-DT/Climate-DT-catalog",
                    catalog,
                    available_catalog,
                )
                sys.exit(1)
            self.logger.info("Download complete!")
        except RequestsHTTPError as e:
            self._log_github_api_error(e, f"fetching catalog {catalog}")
            sys.exit(1)
        except SystemExit:
            raise
        except Exception as e:
            self.logger.error("Error occurred while fetching catalog: %s", e)
            sys.exit(1)

    def catalog_update(self, args):
        """Update an installed catalog, or all of them, by downloading it again if not installed in editable mode

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        self._check()
        catalogs = self._installed_catalogs() if args.catalog == "all" else [args.catalog]
        # every catalog is checked before the first one is downloaded again
        for catalog in catalogs:
            self._check_installed_catalog(catalog)
        for catalog in catalogs:
            self._catalog_update(catalog, repository=args.repository)

    def catalog_list(self, args):
        """List the installed catalogs

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        self._check()
        rows = []
        for name in sorted(self._installed_catalogs()):
            self._check_installed_catalog(name)
            path = self._catalog_path(name)
            rows.append((name, f"editable, {os.readlink(path)}" if self._catalog_editable(name) else "standard"))
        self._print_installed(f"AQUA catalogs installed in {self.configpath}/{CATPATH}:", rows)

    def _catalog_update(self, name, repository=None):
        """Update an installed catalog from GitHub, if not in editable mode: the active catalog is unchanged.

        Args:
            name (str): the catalog to be updated
            repository (str): GitHub repository to download from, as user/repo: the default one if None
        """
        self._check_installed_catalog(name)
        source = self._catalog_source(name, repository=repository)
        if self._catalog_editable(name):
            self._warn_editable(name)
            return
        self._catalog_refresh(name, source)

    def _catalog_refresh(self, name, source):
        """Download the catalog again in place of the installed folder, which is put back if the download fails
        or the new catalog does not load."""
        cdir = self._catalog_path(name)
        backup = cdir + ".backup"
        print(f"Updating catalog {cdir} ..")
        if os.path.lexists(backup):
            # left by an update which was killed: the backup is the good copy, the folder a partial download
            self.logger.warning("Restoring catalog %s from %s, left by an interrupted update", name, backup)
            self._remove_target(cdir)
            os.rename(backup, cdir)
        os.rename(cdir, backup)
        try:
            # as for add: downloaded, then it must load
            self._catalog_install(name, source)
        except BaseException as e:
            # also an interruption: a long download is the most likely thing to be stopped by hand
            self._remove_target(cdir)
            os.rename(backup, cdir)
            self.logger.error(
                "Catalog %s could not be updated, the installed one is kept. Error: %s", name, e or type(e).__name__
            )
            if not isinstance(e, Exception):
                raise
            sys.exit(1)
        self._remove_target(backup)

    def catalog_remove(self, args):
        """Remove a catalog

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        name = os.path.basename(args.catalog)
        # only listed, not on disk: removing is the way out for a catalog without its folder
        self._check_listed_catalog(name)
        print("Remove the AQUA catalog", name, "from", self._catalog_path(name))
        self._remove_target(self._catalog_path(name))
        self._catalog_key_remove(name)

    def _check_new_catalog(self, name):
        """Check the installation and exit if the catalog to be added is listed in config-aqua.yaml or is on disk."""
        self._check()
        self._check_new(CATALOG_KEY, name, [self._catalog_path(name)])

    def _check_listed_catalog(self, name):
        """Check the installation and exit if the catalog is not listed in config-aqua.yaml."""
        self._check()
        self._check_listed(CATALOG_KEY, name, [self._catalog_path(name)])

    def _check_installed_catalog(self, name):
        """Exit if the catalog is not listed in config-aqua.yaml or its folder is not in the installation."""
        self._check_listed_catalog(name)
        self._check_on_disk(CATALOG_KEY, name, [self._catalog_path(name)])

    def _installed_catalogs(self):
        """Catalogs listed in config-aqua.yaml, the active one first."""
        return self._config_list(CATALOG_KEY)

    def _catalog_key_set(self, name):
        """Make a catalog the active one in config-aqua.yaml, adding it to the installed ones if not listed."""
        self._config_list_set(CATALOG_KEY, name)

    def _catalog_key_remove(self, name):
        """Take a catalog out of the installed ones of config-aqua.yaml."""
        self._config_list_remove(CATALOG_KEY, name)

    def _catalog_path(self, name):
        """Folder of a catalog in the installation."""
        return f"{self.configpath}/{CATPATH}/{name}"

    def _catalog_editable(self, name):
        """True if the catalog is installed in editable mode."""
        return self._is_editable([self._catalog_path(name)])

    def _fsspec_get_single_call(self, fs, src_dir, dest_dir, request_tracker=None):
        """
        Recursively download a directory and report GitHub API usage after each phase.

        Args:
            fs: fsspec filesystem object, as github instance
            src_dir (str): source directory
            dest_dir (str): target directory

        Returns:
            Remotely copy data from source to dest directory
        """
        # find() may traverse multiple directories, and get() may request each file separately.
        stage = request_tracker.stage("catalog file discovery") if request_tracker else nullcontext()
        with stage:
            all_files = fs.find(src_dir, withdirs=False, detail=False)

        if not all_files:
            self.logger.debug("No files found under %s", src_dir)
            return

        # Prepare destination paths and create directory structure
        dest_paths = []
        for src_path in all_files:
            relative_path = os.path.relpath(src_path, src_dir)
            dest_path = os.path.join(dest_dir, relative_path)
            dest_paths.append(dest_path)
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)

        stage = request_tracker.stage("catalog file download") if request_tracker else nullcontext()
        with stage:
            fs.get(all_files, dest_paths)
