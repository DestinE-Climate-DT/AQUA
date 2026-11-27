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

from .components import component_version


class ProjectMixin:
    """Mixin for AQUA project operations"""

    def project_set(self, args):
        """Set an installed project as the active one in config-aqua.yaml

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        self._check_installed_project(args.project)
        self._project_key_set(args.project)

    def project_list(self, args):
        """List the installed projects

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        self._check_project_layout()
        self._print_projects()

    def _print_projects(self):
        """Print the installed projects in alphabetical order, with their mode and package version."""
        rows = []
        for name in sorted(self._installed_projects()):
            self._check_installed_project(name)
            mode = "editable" if self._project_editable(name) else "standard"
            version = component_version(name)
            rows.append((name, f"{mode}, aqua-{name} {version}" if version else f"{mode}, aqua-{name} package not found"))
        self._print_installed(f"AQUA projects installed in {self.configpath}:", rows)

    def project_add(self, args):
        """Add a project to an existing installation, copied or linked with -e, and make it the active one

        diagnostics is refused: it is installed with ``aqua install -d``, which runs the same steps.

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        if args.project == "diagnostics":
            # one way to install diagnostics: the worker below is what `aqua install -d` runs
            self.logger.error("diagnostics is installed with `aqua install -d`, also in an existing installation")
            sys.exit(1)
        self._project_add(args.project, editable=args.editable)

    def _project_add(self, name, editable=None):
        """Add a project to an existing installation and make it the active one.

        Args:
            name (str): project name
            editable (str): path of a local repository of the project to link, instead of copying from the package
        """
        self._check_new_project(name)
        source = self._project_source(name, editable)
        # the install stops before any change if a target exists, and removes what it created if a copy fails
        self._project_install(name, source, link=editable is not None)
        try:
            self._project_key_set(name)
        except BaseException as e:
            # as for a catalog: a project which cannot be listed is not left on disk
            for target in self._project_targets(name):
                self._remove_target(target)
            if not isinstance(e, Exception):
                raise
            self.logger.error("Cannot add project %s, what was installed has been removed. Error: %s", name, e)
            sys.exit(1)

    def _project_source(self, name, editable=None):
        """Config folder to take a project from, checked before any change: the package must be available and follow
        the plugin contract, and the local repository given for the editable mode must hold the project."""
        self._check_project_package(name)
        if editable is not None:
            return self._validate_component_path(name, editable)
        return self.components[name]["path"]

    def _project_install(self, name, source, link=False):
        """Copy, or link, the folders and extra files of a project: what was created is removed if a copy fails."""
        self.logger.info("Installing %s in %s mode", name, "editable" if link else "standard")
        self._copy_component(name, source, link=link)

    def project_update(self, args):
        """Copy again the folders and extra files of an installed project, skipping the linked ones

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        self._project_update(args.project)

    def _project_update(self, name):
        """Update an installed project from its package, but for what is linked: the active project is unchanged."""
        self._check_installed_project(name)
        source = self._project_source(name)
        # no early return for an editable project: the refresh skips each link, and still copies what is not one
        self._project_refresh(name, source)

    def _project_refresh(self, name, source):
        """Copy the package over the installed folders and extra files, skipping the linked ones (with a warning if
        all are): user files with other names are kept."""
        self._copy_component(name, source, update=True)

    def project_remove(self, args):
        """Remove the project-scoped folders and the extra files of an installed project

        Its ``flat`` and ``templates`` folders are removed too.
        These folders and the extra files are found from the package: without the package nothing is removed.
        If the project was active, another installed project becomes active, if any.

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        name = args.project
        # only listed, not on disk: removing is the way out for a project without its folders
        self._check_listed_project(name)
        self._check_project_package(name)

        for target in self._project_targets(name):
            self._remove_target(target)

        active = self._installed_projects()[0] == name
        self._project_key_remove(name)
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

    def _check_new_project(self, name):
        """Check the installation and exit if the project to be added is listed in config-aqua.yaml or is on disk.

        Only the project folders are looked at here, without the package: its flat folders and extra files, known
        from the package, are checked when it is installed.
        """
        self._check_project_layout(name)
        self._check_new(PROJECT_KEY, name, self._project_paths(name))

    def _check_listed_project(self, name):
        """Check the installation and exit if the project is not listed in config-aqua.yaml."""
        self._check_project_layout(name)
        self._check_listed(PROJECT_KEY, name, self._project_paths(name))

    def _check_installed_project(self, name):
        """Exit if the project is not listed in config-aqua.yaml or none of its folders is in the installation."""
        self._check_listed_project(name)
        self._check_on_disk(PROJECT_KEY, name, self._project_paths(name))

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
        return self._config_list(PROJECT_KEY)

    def _project_key_set(self, name):
        """Make a project the active one in config-aqua.yaml, adding it to the installed ones if not listed."""
        self._config_list_set(PROJECT_KEY, name)

    def _project_key_remove(self, name):
        """Take a project out of the installed ones of config-aqua.yaml."""
        self._config_list_remove(PROJECT_KEY, name)

    def _project_paths(self, name):
        """Project-scoped folders of a project in the installation: those of its package, if available."""
        folders = self.components.get(name, {}).get("config") or PROJECT_FOLDERS
        return [os.path.join(self.configpath, folder, name) for folder in folders]

    def _project_editable(self, name):
        """True if the project is installed in editable mode."""
        return self._is_editable(self._project_paths(name))

    def _project_targets(self, name):
        """Everything a project installs: its project folders, its flat and templates folders and its extra files.

        The last two are known from the package, which must be available.
        """
        targets = [os.path.join(self.configpath, folder, name) for folder in PROJECT_FOLDERS]
        targets += self._project_owned_dirs(name)
        targets += [target for _, target in self._component_files(name, self.components[name]["path"])]
        return targets
