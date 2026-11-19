#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AQUA project operations mixin: ``aqua project <verb>``.

A project is a package shipping configuration (and optionally diagnostics) installed in the
project-scoped folders ``<configdir>/<folder>/<project>`` (see ``ConfigContext.get_project_folder``).
The name "project" is provisional: its spellings live in ``aqua.core.configurer.context``.
"""

import os
import shutil
import sys

from aqua.core.configurer.context import PROJECT_FOLDERS, PROJECT_KEY
from aqua.core.lock import SafeFileLock
from aqua.core.util import dump_yaml, load_yaml

from .components import component_version

# active project after the active one is removed, as in config-aqua.tmpl
DEFAULT_PROJECT = "diagnostics"


class ProjectMixin:
    """Mixin for AQUA project operations"""

    def project_set(self, args):
        """Set an installed project as the active one in config-aqua.yaml

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        self._check_project_layout(args.project)

        if args.project not in self._installed_projects():
            self.logger.error(
                "Project %s is not installed: none of %s has a %s subfolder in %s",
                args.project,
                ", ".join(PROJECT_FOLDERS),
                args.project,
                self.configpath,
            )
            sys.exit(1)

        self.logger.info("Setting project to %s", args.project)
        self._write_project(args.project)

    def project_install(self, args):
        """Add a project to an existing installation, copied or linked with -e, and make it the active one

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        mode = {"mode": "editable", "path": args.editable} if args.editable else {"mode": "standard", "path": None}
        self._project_add(args.project, mode)

    def _project_add(self, name, mode):
        """Add a project to an existing installation and make it the active one.

        Args:
            name (str): project name
            mode (dict): installation mode with 'mode' ('standard' or 'editable') and 'path' keys
        """
        self._check_project_layout(name)
        self._check_project_package(name)
        if name in self._installed_projects():
            self.logger.error(
                "Project %s is already installed: use `aqua project update %s` or `aqua project remove %s`",
                name,
                name,
                name,
            )
            sys.exit(1)

        self._install_component(name, mode)
        self.logger.info("Project %s installed and set as the active project", name)
        self._write_project(name)

    def project_update(self, args):
        """Copy again the folders and extra files of an installed project, skipping editable ones

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        self._check_installed_project(args.project)
        self._check_project_package(args.project)
        self.logger.info("Updating project %s", args.project)
        self._update_component(args.project)

    def project_remove(self, args):
        """Remove the folders and extra files of an installed project; if it was active, activate the default one

        The extra files are found by listing the package: they are kept if the package is not available.

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        name = args.project
        self._check_installed_project(name)

        targets = [os.path.join(self.configpath, folder, name) for folder in PROJECT_FOLDERS]
        files = []
        if self.components.get(name, {}).get("path"):
            targets += [target for _, target in self._component_targets(name) if target not in targets]
            files = [self._flat_file(kind, file) for kind, file in self._project_files(name, self.components[name]["path"])]
        else:
            self.logger.warning("aqua.%s package not found: its extra files, if any, are not removed", name)

        for target in targets + files:
            if os.path.islink(target) or os.path.isfile(target):
                self.logger.info("Removing %s", target)
                os.remove(target)
            elif os.path.isdir(target):
                self.logger.info("Removing %s", target)
                shutil.rmtree(target)

        if self.configurer.config_dict.get(PROJECT_KEY) == name:
            self.logger.warning("Removed the active project %s, %s is now the active project", name, DEFAULT_PROJECT)
            self._write_project(DEFAULT_PROJECT)

    def project_list(self, args):
        """List the installed projects, marking the active one

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        self._check_project_layout()
        self._print_projects()

    def _print_projects(self):
        """Print the installed projects with their mode and package version, marking the active one with *."""
        print(f"AQUA projects installed in {self.configpath}:")
        for name in self._installed_projects():
            links = [os.path.islink(os.path.join(self.configpath, folder, name)) for folder in PROJECT_FOLDERS]
            mode = "editable" if any(links) else "standard"
            version = component_version(name)
            package = f"aqua-{name} {version}" if version else f"aqua-{name} package not found"
            print(f"{'*' if name == self.configurer.project else ' '} {name} ({mode}, {package})")

    def _write_project(self, name):
        """Write the active project in config-aqua.yaml."""
        configfile = os.path.join(self.configpath, "config-aqua.yaml")
        with SafeFileLock(configfile + ".lock", loglevel=self.loglevel):
            cfg = load_yaml(configfile)
            cfg[PROJECT_KEY] = name
            dump_yaml(configfile, cfg)

    def _check_installed_project(self, name):
        """Check the installation and exit if the project is not installed."""
        self._check_project_layout(name)
        if name not in self._installed_projects():
            self.logger.error("Project %s is not installed", name)
            sys.exit(1)

    def _check_project_package(self, name):
        """Exit if the package of a project is not available or breaks the plugin contract."""
        info = self.components.get(name)
        if info is not None and info["error"]:
            self.logger.error(info["error"])
            sys.exit(1)
        if info is None or info["path"] is None:
            self.logger.error("aqua.%s package not found", name)
            sys.exit(1)

    def _refuse_core(self, name):
        """Exit if the name is core, which is not a project."""
        if name == "core":
            self.logger.error("core is not a project")
            sys.exit(1)

    def _check_project_layout(self, name=None):
        """Check the installation and exit if it predates project support (no project key)."""
        if name is not None:
            self._refuse_core(name)
        self._check()
        if not self.configurer.config_dict.get(PROJECT_KEY):
            self.logger.error(
                "This installation predates project support (no %s key in %s): run `aqua install` to upgrade it",
                PROJECT_KEY,
                self.configfile,
            )
            sys.exit(1)

    def _installed_projects(self):
        """Names of the subfolders of the project-scoped folders, empty for a pre-project installation."""
        if not self.configurer.config_dict.get(PROJECT_KEY):
            return []
        names = set()
        for folder in PROJECT_FOLDERS:
            path = os.path.join(self.configpath, folder)
            if os.path.isdir(path):
                names.update(name for name in os.listdir(path) if os.path.isdir(os.path.join(path, name)))
        return sorted(names)
