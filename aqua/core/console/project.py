#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AQUA project operations mixin: ``aqua project <verb>``.

A project is a package shipping configuration (and optionally diagnostics) installed in the
project-scoped folders ``<configdir>/<folder>/<project>`` (see ``ConfigContext.get_project_folder``).
The project key and folder names live in ``aqua.core.configurer.context``.
"""

import os
import sys

from aqua.core.configurer.context import PROJECT_FOLDERS, PROJECT_KEY
from aqua.core.lock import SafeFileLock
from aqua.core.util import dump_yaml, load_yaml


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
        with SafeFileLock(self.configfile + ".lock", loglevel=self.loglevel):
            cfg = load_yaml(self.configfile)
            cfg[PROJECT_KEY] = args.project
            dump_yaml(self.configfile, cfg)

    def project_list(self, args):
        """List the installed projects, marking the active one

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        self._check_project_layout()
        print(f"AQUA projects installed in {self.configpath}:")
        for name in self._installed_projects():
            print(f"{'*' if name == self.configurer.project else ' '} {name}")

    def project_not_implemented(self, args):
        """Placeholder of the project verbs not available yet (install, update, remove)

        Args:
            args (argparse.Namespace): arguments from the command line

        Raises:
            NotImplementedError: always, after checking the project name.
        """
        self._refuse_core(args.project)
        raise NotImplementedError(f"`aqua project {args.nested_command}` is available in a later PR")

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
