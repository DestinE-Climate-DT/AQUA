#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AQUA catalog operations mixin
"""

import os
import shutil
import sys
from contextlib import nullcontext

import fsspec
from requests.exceptions import HTTPError as RequestsHTTPError

from aqua.core.configurer import ConfigCatalog
from aqua.core.lock import SafeFileLock
from aqua.core.util import dump_yaml, load_yaml
from aqua.core.util.util import to_list

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
        """Set an installed catalog as the one used in the config-aqua.yaml

        Args:
            args (argparse.Namespace): arguments from the command line
        """

        self._check()
        self._check_installed_catalog(args.catalog)
        self._catalog_set(args.catalog)

    def _installed_catalogs(self):
        """Catalogs listed in config-aqua.yaml, the active one first: as for the projects, these are the installed ones."""
        return to_list(load_yaml(self.configfile)["catalog"])

    def _check_installed_catalog(self, catalog):
        """Exit if the catalog is not listed in config-aqua.yaml or its folder is missing: return its folder."""
        cdir = f"{self.configpath}/{CATPATH}/{catalog}"
        if catalog not in self._installed_catalogs():
            self.logger.error("Catalog %s is not installed: add it with `aqua catalog add %s`", catalog, catalog)
            sys.exit(1)
        # lexists: an editable catalog whose source is gone is a dangling link, which can still be removed
        if not os.path.lexists(cdir):
            self.logger.error(
                "Catalog %s is listed in %s but %s is missing: add it again with `aqua catalog add %s`",
                catalog,
                self.configfile,
                cdir,
                catalog,
            )
            sys.exit(1)
        return cdir

    def catalog_add(self, args):
        """Add a catalog and set it as a default in config-aqua.yaml

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        print("Adding the AQUA catalog", args.catalog)
        self._check(silent=True)

        cdir = f"{self.configpath}/{CATPATH}/{args.catalog}"
        is_new_catalog_dir = not os.path.exists(cdir)

        try:
            if args.editable is not None:
                self._catalog_add_editable(args.catalog, args.editable)
            else:
                with GitHubAPIRequestTracker(self.logger, f"catalog {args.catalog}") as request_tracker:
                    self._catalog_add_github(args.catalog, args.repository, request_tracker=request_tracker)

            # validate only the new catalog: raises if its catalog or machine file is missing
            ConfigCatalog(catalog=args.catalog, loglevel=self.loglevel)

            self._catalog_set(args.catalog)

        except (Exception, SystemExit) as e:
            if is_new_catalog_dir and os.path.exists(cdir):
                if os.path.islink(cdir):
                    os.unlink(cdir)
                else:
                    shutil.rmtree(cdir)

            if isinstance(e, SystemExit):
                raise

            self.logger.error(
                "Current catalog %s is not compatible with AQUA, removing it for safety! Error: %s", args.catalog, e
            )
            sys.exit(1)

    def _catalog_add_editable(self, catalog, editable):
        """Add a catalog in editable mode (i.e. link)"""

        cdir = f"{self.configpath}/{CATPATH}/{catalog}"
        editable = os.path.abspath(editable)
        print("Installing catalog in editable mode from", editable, "to", self.configpath)
        if os.path.exists(editable):
            if os.path.exists(cdir):
                self.logger.error("Catalog %s already installed in %s, please consider `aqua catalog remove`", catalog, cdir)
                sys.exit(1)
            else:
                os.symlink(editable, cdir)
        else:
            self.logger.error(
                "Catalog in editable mode %s cannot be found at %s. "
                "Using the Climate-DT catalog repository, likely need a path like: "
                "/full/path/to/Climate-DT-catalog/catalogs/%s",
                catalog,
                editable,
                catalog,
            )
            sys.exit(1)

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

    def _catalog_add_github(self, catalog, repository=None, request_tracker=None):
        """
        Add a catalog from a remote Github repository.
        Default repository is the Climate-DT repository
        DestinE-Climate-DT/Climate-DT-catalog

        Args:
            catalog (str): the catalog to be added
            repository (str): the repository from which to fetch the catalog, if None it uses the default
        """
        # recursive copy
        cdir = f"{self.configpath}/{CATPATH}/{catalog}"
        if not os.path.exists(cdir):
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
        else:
            self.logger.error("Catalog %s already installed in %s, please consider `aqua catalog update`.", catalog, cdir)
            sys.exit(1)

    def catalog_update(self, args):
        """Update an installed catalog, or all of them, by downloading it again if not installed in editable mode

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        self._check()
        if args.catalog == "all":
            self.logger.info("Updating all AQUA catalogs")
            # every folder is checked before the first catalog is downloaded again
            for catalog in self._installed_catalogs():
                self._check_installed_catalog(catalog)
            for catalog in self._installed_catalogs():
                print(f"Updating catalog {self.configpath}/{CATPATH}/{catalog} ..")
                self._catalog_update(catalog)
        else:
            self._catalog_update(args.catalog)

    def catalog_list(self, args):
        """List the installed catalogs

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        self._check()
        folders = [self._check_installed_catalog(catalog) for catalog in sorted(self._installed_catalogs())]
        print("AQUA current installed catalogs in", f"{self.configpath}/{CATPATH}", ":")
        for folder in folders:
            editable = f" (editable from {os.readlink(folder)})" if os.path.islink(folder) else ""
            print(f"\t - {folder}{editable}")

    def _catalog_update(self, catalog):
        """Update a catalog by downloading it again if not installed in editable mode: the active catalog is unchanged

        Args:
            catalog (str): the catalog to be updated
        """
        cdir = self._check_installed_catalog(catalog)
        if os.path.islink(cdir):
            self.logger.warning("%s is installed in editable mode: it follows its repository, nothing to update", catalog)
            return
        self._remove_target(cdir)
        self._catalog_add_github(catalog)

    def _catalog_set(self, catalog):
        """Modify the config-aqua.yaml with the proper catalog

        Args:
            catalog (str): the catalog to be set as the default in the config-aqua.yaml
        """

        self.logger.info("Setting catalog name to %s", catalog)
        with SafeFileLock(self.configfile + ".lock", loglevel=self.loglevel):
            cfg = load_yaml(self.configfile)
            if cfg["catalog"] is None:
                self.logger.debug("No catalog previously installed: setting catalog name to %s", catalog)
                cfg["catalog"] = catalog
            else:
                if catalog not in to_list(cfg["catalog"]):
                    self.logger.debug("Adding catalog %s to the existing list %s", catalog, cfg["catalog"])
                    cfg["catalog"] = [catalog] + to_list(cfg["catalog"])
                else:
                    if isinstance(cfg["catalog"], list):
                        other_catalogs = [x for x in cfg["catalog"] if x != catalog]
                        self.logger.debug(
                            "Catalog %s is already there, setting it as first entry before %s", catalog, other_catalogs
                        )
                        cfg["catalog"] = [catalog] + other_catalogs
                    else:
                        self.logger.debug("Catalog %s is already there, but is the only installed", catalog)
                        cfg["catalog"] = catalog

            dump_yaml(self.configfile, cfg)

    def catalog_remove(self, args):
        """Remove a catalog

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        self._check()
        if "/" in args.catalog:
            args.catalog = os.path.basename(args.catalog)
        cdir = self._check_installed_catalog(args.catalog)
        print("Remove the AQUA catalog", args.catalog, "from", cdir)
        self._remove_target(cdir)
        self._catalog_clean(args.catalog)

    def _catalog_clean(self, catalog):
        """
        Remove catalog from the configuration file
        """

        with SafeFileLock(self.configfile + ".lock", loglevel=self.loglevel):
            cfg = load_yaml(self.configfile)
            # as for the projects: a catalog not listed leaves the key as it is, the last one left is a string
            catalogs = [name for name in to_list(cfg["catalog"]) if name != catalog]
            cfg["catalog"] = catalogs[0] if len(catalogs) == 1 else catalogs or None
            self.logger.info("Catalog %s removed, catalogs %s are available", catalog, cfg["catalog"])
            dump_yaml(self.configfile, cfg)

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
