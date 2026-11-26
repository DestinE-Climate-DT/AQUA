#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AQUA project operations mixin: ``aqua project <verb>``.

A project is a package shipping configuration (and optionally diagnostics) installed in the
project-scoped folders ``<configdir>/<folder>/<project>`` (see ``ConfigContext.get_project_folder``).
The installed projects are listed by the project key of ``config-aqua.yaml``, the active one first.
The project key and folder names live in ``aqua.core.configurer.context``.
"""

import os
import sys

from aqua.core.configurer.context import PRE_PROJECT_MESSAGE, PROJECT_FOLDERS, PROJECT_KEY
from aqua.core.lock import SafeFileLock
from aqua.core.util import dump_yaml, load_yaml, to_list

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
            self.logger.error("Project %s is not installed: add it with `aqua project add %s`", args.project, args.project)
            sys.exit(1)
        self._check_project_folders(args.project)

        self._project_set(args.project)

    def _project_set(self, project, remove=False):
        """Modify the list of installed projects of the config-aqua.yaml, as done for the catalogs

        The project becomes the first of the list, i.e. the active one, or is removed from it.
        A single project is written as a string, and the key is kept, empty, when no project is left.

        Args:
            project (str): the project to be set as the active one in the config-aqua.yaml
            remove (bool): remove the project from the list instead
        """
        self.logger.info("%s project %s", "Removing" if remove else "Setting", project)
        with SafeFileLock(self.configfile + ".lock", loglevel=self.loglevel):
            cfg = load_yaml(self.configfile)
            projects = [name for name in to_list(cfg.get(PROJECT_KEY)) if name != project]
            if not remove:
                projects.insert(0, project)
            cfg[PROJECT_KEY] = projects[0] if len(projects) == 1 else projects or None
            dump_yaml(self.configfile, cfg)

    def project_list(self, args):
        """List the installed projects

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        self._check_project_layout()
        self._print_projects()

    def _print_projects(self):
        """Print the installed projects in alphabetical order, with their mode and package version."""
        for name in self._installed_projects():
            self._check_project_folders(name)
        print(f"AQUA projects installed in {self.configpath}:")
        for name in sorted(self._installed_projects()):
            links = [os.path.islink(os.path.join(self.configpath, folder, name)) for folder in PROJECT_FOLDERS]
            mode = "editable" if any(links) else "standard"
            version = component_version(name)
            package = f"aqua-{name} {version}" if version else f"aqua-{name} package not found"
            print(f"\t - {name} ({mode}, {package})")

    def project_add(self, args):
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
        self._check_project_folders(args.project)
        self._check_project_package(args.project)
        self._copy_component(args.project, self.components[args.project]["path"], update=True)

    def project_remove(self, args):
        """Remove the project-scoped folders and the extra files of an installed project

        Its ``flat`` and ``templates`` folders are removed too.
        These folders and the extra files are found from the package: without the package nothing is removed.
        If the project was active, another installed project becomes active, if any.

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        name = args.project
        self._check_installed_project(name)
        self._check_project_package(name)

        targets = [os.path.join(self.configpath, folder, name) for folder in PROJECT_FOLDERS]
        targets += self._project_owned_dirs(name)
        targets += [target for _, target in self._component_files(name, self.components[name]["path"])]

        for target in targets:
            self._remove_target(target)

        active = self._installed_projects()[0] == name
        self._project_set(name, remove=True)
        others = self._installed_projects()
        if active and others:
            self.logger.warning("Removed the active project %s, %s is now the active project", name, others[0])
        elif active:
            self.logger.warning("Removed the active project %s, no project is installed", name)

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

    def _check_project_folders(self, name):
        """Exit if none of the project-scoped folders of a listed project is in the installation, as for a catalog
        without its folder. `aqua project remove` does not check it: it is the way out."""
        folders = self.components.get(name, {}).get("config") or PROJECT_FOLDERS
        if not any(os.path.lexists(os.path.join(self.configpath, folder, name)) for folder in folders):
            self.logger.error(
                "Project %s is listed in %s but none of its folders (%s) is installed: "
                "use `aqua project remove %s` and add it again",
                name,
                self.configfile,
                ", ".join(folders),
                name,
            )
            sys.exit(1)

    def _check_project_package(self, name):
        """Exit if the package of a project is not available or breaks the plugin contract."""
        info = self.components.get(name)
        if info is not None and info["error"]:
            self.logger.error(info["error"])
            sys.exit(1)
        if info is None or info["path"] is None:
            self.logger.error("aqua.%s package not found: install it in the current environment first", name)
            sys.exit(1)

    def _installed_projects(self):
        """Projects listed in config-aqua.yaml, the active one first: empty for a pre-project installation."""
        return to_list(load_yaml(self.configfile).get(PROJECT_KEY))
