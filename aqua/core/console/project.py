#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AQUA project operations mixin: ``aqua project <verb>``.

A project is a package shipping configuration (and optionally diagnostics) installed in the
project-scoped folders ``<configdir>/<folder>/<project>`` (see ``ConfigContext.get_project_folder``).
The project key and folder names live in ``aqua.core.configurer.context``.
"""

import os
import shutil
import sys

from aqua.core.configurer.context import PRE_PROJECT_MESSAGE, PROJECT_FOLDERS, PROJECT_KEY
from aqua.core.lock import SafeFileLock
from aqua.core.util import dump_yaml, load_yaml

from .components import component_version


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

        self._project_set(args.project)

    def _project_set(self, project):
        """Modify the config-aqua.yaml with the active project

        Args:
            project (str): the project to be set as the active one in the config-aqua.yaml
        """
        self.logger.info("Setting project to %s", project)
        with SafeFileLock(self.configfile + ".lock", loglevel=self.loglevel):
            cfg = load_yaml(self.configfile)
            cfg[PROJECT_KEY] = project
            dump_yaml(self.configfile, cfg)

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
            self.logger.error("Project %s is already installed: use `aqua project update %s`", name, name)
            sys.exit(1)
        self._install_component(name, mode)
        self._project_set(name)

    def project_update(self, args):
        """Copy again the folders and extra files of an installed project, skipping editable ones

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        self._check_installed_project(args.project)
        self._check_project_package(args.project)
        self._copy_component(args.project, self.components[args.project]["path"], update=True)

    def project_remove(self, args):
        """Remove the project-scoped folders and the extra files of an installed project

        Its ``flat`` folders are removed too, while ``templates`` folders, shared with other projects, are kept.
        The flat folders and extra files are found from the package: without the package they are kept, with a warning.
        If the project was active, another installed project becomes active, if any.

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        name = args.project
        self._check_installed_project(name)

        targets = [os.path.join(self.configpath, folder, name) for folder in PROJECT_FOLDERS]
        path = self.components.get(name, {}).get("path")
        if path:
            targets += self._project_flat_dirs(name) + [target for _, target in self._component_files(name, path)]
        else:
            self.logger.warning("aqua.%s package not found: its flat folders and extra files, if any, are kept", name)

        for target in targets:
            if os.path.islink(target) or os.path.isfile(target):
                self.logger.info("Removing %s", target)
                os.remove(target)
            elif os.path.isdir(target):
                self.logger.info("Removing %s", target)
                shutil.rmtree(target)

        if self.configurer.config_dict.get(PROJECT_KEY) == name:
            others = self._installed_projects()
            if others:
                self.logger.warning("Removed the active project %s, %s is now the active project", name, others[0])
            else:
                self.logger.warning("Removed the active project %s, no project is installed", name)
            self._project_set(others[0] if others else None)

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
        if not self.configurer.has_project_layout():
            self.logger.error(PRE_PROJECT_MESSAGE.format(config_file=self.configfile))
            sys.exit(1)

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

    def _installed_projects(self):
        """Names of the subfolders of the project-scoped folders, empty for a pre-project installation."""
        if not self.configurer.has_project_layout():
            return []
        names = set()
        for folder in PROJECT_FOLDERS:
            path = os.path.join(self.configpath, folder)
            if os.path.isdir(path):
                names.update(name for name in os.listdir(path) if os.path.isdir(os.path.join(path, name)))
        return sorted(names)
