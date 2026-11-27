#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AQUA installation operations mixin - includes also listing
"""

import os
import shutil
import sys

from aqua.core.configurer import ConfigContext, ConfigLocator, ConfigPackages
from aqua.core.configurer.context import PRE_PROJECT_MESSAGE, PROJECT_FOLDERS
from aqua.core.lock import SafeFileLock
from aqua.core.util import dump_yaml, load_yaml, to_list

from .catalog import CATPATH
from .util import query_yes_no

# folder used for reading/storing catalogs


class InstallMixin:
    """Mixin for AQUA installation operations"""

    def _check(self, silent=False, return_info=False):
        """
        Check installation and optionally return detailed information

        Args:
            silent (bool): If True, suppress errors and use ERROR loglevel
            return_info (bool): If True, return installation info dict instead of exiting

        Returns:
            dict or None: If return_info=True, returns {'installed': bool, 'configpath': str or None}
        """
        checklevel = "ERROR" if silent else self.loglevel

        try:
            self.configurer = ConfigContext(loglevel=checklevel)
            self.configpath = self.configurer.get_config_dir()
            self.configfile = os.path.join(self.configpath, "config-aqua.yaml")
            self.templatepath = os.path.join(self.configpath, "templates")
            self.logger.debug("AQUA found in %s", self.configpath)

            if return_info:
                return {"installed": True, "configpath": self.configpath}

        except FileNotFoundError:
            if return_info:
                return {"installed": False, "configpath": None}

            locator = ConfigLocator(logger=self.logger)
            try:
                if os.path.exists(locator.config_file):
                    self.logger.error(
                        "AQUA configuration found at %s but failed to load. "
                        "A configured catalog might be missing or corrupted.",
                        locator.config_file,
                    )
                    sys.exit(1)
            except FileNotFoundError:
                self.logger.error(
                    "No AQUA configuration found (config-aqua.yaml). Use `aqua install` to initialize a new installation, "
                    "or manually set the AQUA_CONFIG environment variable to an existing AQUA installation directory."
                )
                sys.exit(1)

    def _component_targets(self, component, source=None):
        """Folders installed by a component, as (source, target) pairs.

        Project-scoped folders go to ``<configpath>/<dir>/<component>``, flat folders to ``<configpath>/<dir>``,
        templates to ``<configpath>/templates/<dir>``. The ``files`` of a plugin are not whole folders and are not
        listed here (see ``_component_files``).

        Args:
            component (str): component name, e.g. 'core', 'diagnostics', 'claima'
            source (str, optional): config folder of the component package; if None, sources are None

        Returns:
            list: (source, target) tuples
        """
        info = self.components[component]

        def src(*parts):
            return os.path.join(source, *parts) if source else None

        targets = [(src(d), os.path.join(self.configpath, d, component)) for d in info["config"]]
        targets += [(src(d), os.path.join(self.configpath, d)) for d in info["flat"]]
        targets += [(src("..", "templates", d), os.path.join(self.configpath, "templates", d)) for d in info["templates"]]
        return targets

    def _project_owned_dirs(self, component):
        """Flat and templates folders of a project in the AQUA config folder: installed and removed with the project."""
        info = self.components[component]
        return [os.path.join(self.configpath, d) for d in info["flat"]] + [
            os.path.join(self.configpath, "templates", d) for d in info["templates"]
        ]

    def _component_files(self, component, source):
        """Extra files of a component, as (source, target) pairs: each file of its ``files`` folders goes to the
        flat ``<configpath>/<dir>``.

        Args:
            component (str): component name, e.g. 'claima'
            source (str): config folder of the component package or repository

        Returns:
            list: (source, target) tuples
        """
        files = []
        for kind in self.components[component]["files"]:
            folder = os.path.join(source, kind)
            if not os.path.isdir(folder):
                self.logger.warning("%s declared by %s does not exist, skipping it", folder, component)
                continue
            files += [(os.path.join(folder, n), os.path.join(self.configpath, kind, n)) for n in sorted(os.listdir(folder))]
        return files

    def _validate_component_path(self, component, custom_path):
        """Validate that editable install path exists and return the source path

        Args:
            component (str): component name, e.g. 'core', 'diagnostics', 'claima'
            custom_path (str): Path to the editable installation

        Returns:
            str: Absolute path to the component config directory
        """
        actual_source = os.path.join(os.path.abspath(custom_path), "aqua", component, "config")
        if not os.path.exists(actual_source):
            self.logger.error(f"{actual_source} does not exist for {component}")
            sys.exit(1)
        return actual_source

    def _copy_component(self, component, source_path, link=False, update=False):
        """
        Copy (or link) the folders and extra files of a component; when updating, those in editable mode are skipped.

        Args:
            component (str): component name, e.g. 'core', 'diagnostics', 'claima'
            source_path (str): config folder of the component package or repository
            link (bool): link the folders and files instead of copying them
            update (bool): replace the folders and files already installed
        """
        targets = self._component_targets(component, source_path) + self._component_files(component, source_path)
        if update:
            self._update_targets(component, targets)
        else:
            self._install_targets(component, targets, link)

    def _install_targets(self, component, targets, link):
        """
        First installation of the (source, target) pairs of a component.

        A project owns its targets: the installation stops before any change if one of them already exists.
        If a copy fails, the targets created so far are removed, so that no half-installed project is left.
        """
        existing = [target for _, target in targets if component != "core" and os.path.lexists(target)]
        if existing:
            self.logger.error("Cannot install %s, these files or folders already exist: %s", component, ", ".join(existing))
            sys.exit(1)

        created = []
        try:
            for source, target in targets:
                if not os.path.exists(source):
                    self.logger.warning("%s declared by %s does not exist, skipping it", source, component)
                else:
                    os.makedirs(os.path.dirname(target), exist_ok=True)
                    self._copy_update_folder_file(source, target, link=link)
                    created.append(target)
        except OSError as e:
            for target in reversed(created):
                self._remove_target(target)
            self.logger.error("Cannot install %s: %s. The files installed so far have been removed", component, e)
            sys.exit(1)

    def _update_targets(self, component, targets):
        """
        Copy again the (source, target) pairs of an installed component, skipping the editable (linked) ones.

        Core folders are replaced, but for the links they hold (extra files of editable projects, files added with
        ``-e``), which are kept. Project folders are copied over the installed ones, so user files with other names
        are kept, as well as files removed or renamed in the package.
        """
        skipped = [target for _, target in targets if os.path.islink(target)]
        if skipped and len(skipped) == len(targets):
            self._warn_editable(component)
            return
        for source, target in targets:
            if target in skipped:
                self.logger.info("Skipping %s (editable mode)", target)
            elif not os.path.exists(source):
                self.logger.warning("%s declared by %s does not exist, skipping it", source, component)
            elif os.path.isdir(source):
                self.logger.info("Updating %s ...", target)
                kept = self._clean_core_folder(target) if component == "core" else []
                # a kept link is never written through, even if the package ships a file with its name
                shutil.copytree(
                    source, target, dirs_exist_ok=True, ignore=lambda folder, _, kept=kept: kept if folder == source else []
                )
            else:
                os.makedirs(os.path.dirname(target), exist_ok=True)
                self._copy_update_folder_file(source, target, update=True)

    def _clean_core_folder(self, folder):
        """Empty an installed core folder, if present, but for its links: return the names of the links kept."""
        kept = []
        for name in os.listdir(folder) if os.path.isdir(folder) else []:
            entry = os.path.join(folder, name)
            if os.path.islink(entry):
                self.logger.info("Keeping %s (editable mode)", entry)
                kept.append(name)
            else:
                self._remove_target(entry)
        return kept

    def _core_unshipped(self, projects):
        """Entries of the core folders that an update deletes and does not copy again.

        They are neither links, nor shipped by the core package, nor owned by one of the installed projects:
        files copied with ``aqua grids|fixes|styles add``, or files that core does not ship any more.

        Args:
            projects (list): names of the installed projects

        Returns:
            list: paths about to be deleted
        """
        owned = set()
        for name in projects:
            source = self.components[name]["path"]
            owned.update(t for _, t in self._component_targets(name, source) + self._component_files(name, source))

        unshipped = []
        for source, target in self._component_targets("core", self.components["core"]["path"]):
            if os.path.islink(target) or not os.path.isdir(target) or not os.path.isdir(source):
                continue
            shipped = set(os.listdir(source))
            for name in sorted(os.listdir(target)):
                entry = os.path.join(target, name)
                if name not in shipped and not os.path.islink(entry) and entry not in owned:
                    unshipped.append(entry)
        return unshipped

    def _remove_target(self, target):
        """Remove an installed file, link or folder, if present."""
        if os.path.islink(target) or os.path.isfile(target):
            self.logger.info("Removing %s", target)
            os.remove(target)
        elif os.path.isdir(target):
            self.logger.info("Removing %s", target)
            shutil.rmtree(target)

    # Catalogs and projects are the two nouns listed by a key of config-aqua.yaml, named as the noun.
    # The helpers below are shared: CatalogMixin and ProjectMixin wrap them with the same names
    # (_installed_<noun>s, _<noun>_key_set, _<noun>_key_remove, _check_new_<noun>, _check_listed_<noun>,
    # _check_installed_<noun>, _<noun>_editable). Adding and updating follow the same steps for both:
    # _<noun>_source (where to take it from, validated), then _<noun>_install or _<noun>_refresh..

    def _config_list(self, noun):
        """Installed catalogs or projects: the names listed by the key of config-aqua.yaml, the active one first."""
        return to_list(load_yaml(self.configfile).get(noun))

    def _config_list_edit(self, noun, edit):
        """Rewrite the list of a key of config-aqua.yaml with ``edit``, a function from the old to the new list.

        A single name is written as a string, and the key is kept, empty, when no name is left.
        """
        with SafeFileLock(self.configfile + ".lock", loglevel=self.loglevel):
            cfg = load_yaml(self.configfile)
            names = edit(to_list(cfg.get(noun)))
            cfg[noun] = names[0] if len(names) == 1 else names or None
            dump_yaml(self.configfile, cfg)

    def _config_list_set(self, noun, name):
        """Make a catalog or project the first of its list, i.e. the active one, adding it if not listed."""
        self.logger.info("Setting %s %s", noun, name)
        self._config_list_edit(noun, lambda names: [name, *(entry for entry in names if entry != name)])

    def _config_list_remove(self, noun, name):
        """Take a catalog or project out of its list: its files are not touched."""
        self.logger.info("Removing %s %s from %s", noun, name, self.configfile)
        self._config_list_edit(noun, lambda names: [entry for entry in names if entry != name])

    # A name is in one of four states: listed or not in its key, with or without something on disk.
    # Each state has one message, whatever the verb, so that the hints never send from one command to another and back.

    @staticmethod
    def _add_command(noun, name):
        """The command which adds a catalog or project: diagnostics is the one project added by `aqua install`."""
        return "aqua install -d" if (noun, name) == ("project", "diagnostics") else f"aqua {noun} add {name}"

    @staticmethod
    def _on_disk(paths):
        """The paths which exist: a dangling link counts, it is an editable installation whose source is gone."""
        return [path for path in paths if os.path.lexists(path)]

    def _exit_missing(self, noun, name, paths):
        """Exit for a catalog or project listed in config-aqua.yaml with nothing on disk."""
        self.logger.error(
            "%s %s is listed in %s but %s is missing: use `aqua %s remove %s` and add it again",
            noun.capitalize(),
            name,
            self.configfile,
            ", ".join(paths),
            noun,
            name,
        )
        sys.exit(1)

    def _exit_unlisted(self, noun, name, existing):
        """Exit for a catalog or project not listed in config-aqua.yaml with something on disk: AQUA never deletes it."""
        self.logger.error(
            "%s %s is not installed but %s exists: delete it, then use `%s`",
            noun.capitalize(),
            name,
            ", ".join(existing),
            self._add_command(noun, name),
        )
        sys.exit(1)

    def _check_new(self, noun, name, paths):
        """Exit if a catalog or project cannot be added: it must not be listed in config-aqua.yaml nor be on disk."""
        listed, existing = name in self._config_list(noun), self._on_disk(paths)
        if listed and existing:
            self.logger.error("%s %s is already installed: use `aqua %s update %s`", noun.capitalize(), name, noun, name)
            sys.exit(1)
        if listed:
            self._exit_missing(noun, name, paths)
        if existing:
            self._exit_unlisted(noun, name, existing)

    def _check_listed(self, noun, name, paths):
        """Exit if a catalog or project is not listed in config-aqua.yaml, i.e. not installed."""
        if name in self._config_list(noun):
            return
        existing = self._on_disk(paths)
        if existing:
            self._exit_unlisted(noun, name, existing)
        self.logger.error("%s %s is not installed: add it with `%s`", noun.capitalize(), name, self._add_command(noun, name))
        sys.exit(1)

    def _check_on_disk(self, noun, name, paths):
        """Exit if none of the paths of a listed catalog or project exists."""
        if not self._on_disk(paths):
            self._exit_missing(noun, name, paths)

    @staticmethod
    def _is_editable(paths):
        """True if a catalog or project is installed in editable mode, i.e. one of its paths is a link."""
        return any(os.path.islink(path) for path in paths)

    def _warn_editable(self, name):
        """Tell that an editable catalog, project or core has nothing to update."""
        self.logger.warning("%s is installed in editable mode: it follows its repository, nothing to update", name)

    @staticmethod
    def _print_installed(title, rows):
        """Print the installed catalogs or projects: a title, then one line per (name, details)."""
        print(title)
        for name, details in rows:
            print(f"\t - {name} ({details})")

    def install(self, args):
        """Install AQUA: core and the diagnostics project, or only the one selected with -c or -d

        Other projects are added with ``aqua project add``, which refuses diagnostics: ``-d`` alone adds it to an existing
        installation in the same way.

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        self.logger.info("Running the AQUA install")

        # configure where to install AQUA
        if args.path is None:
            self._config_home()
        else:
            self._config_path(args.path)

        # define the template path
        self.templatepath = os.path.join(self.configpath, "templates")

        # Check current installation status
        install_info = self._check(silent=True, return_info=True)

        # -c and -d: not given, given alone (standard mode) or given with the path of a repository (editable mode)
        want_core, want_diagnostics = args.core is not None, args.diagnostics is not None
        if not want_core and not want_diagnostics:
            # bare `aqua install`: core and diagnostics, standard mode
            want_core = want_diagnostics = True
        core_path, diagnostics_path = self._editable_path(args.core), self._editable_path(args.diagnostics)

        diagnostics = self.components.get("diagnostics", {"path": None, "error": None})
        # a diagnostics package breaking the contract (e.g. an old contract) stops the installation before any change
        if want_diagnostics and diagnostics["error"]:
            self.logger.error(diagnostics["error"])
            sys.exit(1)

        if not want_core:
            if not install_info["installed"]:
                self.logger.error("Cannot install diagnostics without core. Install core first or use full installation.")
                sys.exit(1)
            self._project_add("diagnostics", editable=diagnostics_path)
            return

        if install_info["installed"]:
            self.logger.warning("AQUA installation found in %s", self.configpath)
            self.logger.warning("Proceeding will remove the existing installation, its projects and all catalogs.")
            if not self._remove_installation(confirm=True):
                sys.exit()

        self.logger.debug("Installing core component")
        # core is not a project: its folders are copied, or linked from the repository given
        core_source = self._validate_component_path("core", core_path) if core_path else self.components["core"]["path"]
        self._copy_component("core", core_source, link=core_path is not None)
        self.logger.debug("Creating config-aqua.yaml configuration file")
        self._copy_update_folder_file(
            os.path.join(self.components["core"]["path"], "config-aqua.tmpl"),
            os.path.join(self.configpath, "config-aqua.yaml"),
        )

        # Create catalogs directory
        os.makedirs(os.path.join(self.configpath, CATPATH), exist_ok=True)
        self._set_machine(args)

        # the project key of config-aqua.tmpl is empty: diagnostics becomes active only if it is installed
        # with the steps of `aqua project add`, whose checks of the installation do not apply to one being created
        if want_diagnostics and diagnostics["path"] is not None:
            source = self._project_source("diagnostics", diagnostics_path)
            self._project_install("diagnostics", source, link=diagnostics_path is not None)
            self.configfile = os.path.join(self.configpath, "config-aqua.yaml")
            self._project_key_set("diagnostics")
        elif want_diagnostics:
            self.logger.warning("aqua.diagnostics package not found. Skipping diagnostics installation.")
        self.logger.info("AQUA installation completed successfully in %s", self.configpath)

    def _config_home(self):
        """Configure the AQUA installation folder, by default inside $HOME"""

        if "HOME" in os.environ:
            path = os.path.join(os.environ["HOME"], ".aqua")
            self.configpath = path
            if not os.path.exists(path):
                os.makedirs(path, exist_ok=True)
            else:
                self.logger.debug("AQUA installation found in %s", path)
        else:
            self.logger.error(
                "$HOME not found.Please specify a path where to install AQUA and define AQUA_CONFIG as environment variable"
            )
            sys.exit(1)

    def _config_path(self, path):
        """Define the AQUA installation folder when a path is specified

        Args:
            path (str): the path where to install AQUA
        """

        self.configpath = path
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)
        else:
            if not os.path.isdir(path):
                self.logger.error("Path chosen is not a directory")
                sys.exit(1)

        check = query_yes_no(f"Do you want to create a link in the $HOME/.aqua to {path}", "yes")
        if check:
            if "HOME" in os.environ:
                link = os.path.join(os.environ["HOME"], ".aqua")
                if os.path.lexists(link):
                    self.logger.warning("Removing the content of %s", link)
                    self._remove_target(link)
                os.symlink(path, link)
            else:
                self.logger.error("$HOME not found. Cannot create a link to the installation path")
                self.logger.warning(
                    "AQUA will be installed in %s, but please remember to define AQUA_CONFIG environment variable", path
                )
        else:
            self.logger.warning(
                "AQUA will be installed in %s, but please remember to define AQUA_CONFIG environment variable", path
            )

    def _set_machine(self, args):
        """Modify the config-aqua.yaml with the identified machine"""

        if args.machine is not None:
            machine = args.machine
        else:
            machine = ConfigContext(configdir=self.configpath).get_machine()

        if machine is None:
            self.logger.info("Unknown machine!")
        else:
            self.configfile = os.path.join(self.configpath, "config-aqua.yaml")
            self.logger.info("Setting machine name to %s", machine)

            with SafeFileLock(self.configfile + ".lock", loglevel=self.loglevel):
                cfg = load_yaml(self.configfile)
                cfg["machine"] = machine
                dump_yaml(self.configfile, cfg)

    def _remove_installation(self, confirm=True):
        """
        Remove AQUA installation directory

        Args:
            confirm (bool): Whether to ask for confirmation

        Returns:
            bool: True if removed, False if user cancelled
        """
        if confirm:
            check = query_yes_no(
                f"Do you want to remove AQUA installation in {self.configpath}. You will lose all catalogs installed.", "no"
            )
            if not check:
                return False

        self.logger.warning("Removing the content of %s", self.configpath)

        self._remove_target(self.configpath)

        return True

    def uninstall(self, args):
        """Remove AQUA"""
        print("Remove the AQUA installation")
        self._check()
        if not self._remove_installation(confirm=True):
            sys.exit()

    def update(self, args):
        """Update the installation files of core and of every installed project: what is linked is left as it is.

        Each project is refreshed per ``<dir>/<project>``, so updating one project never deletes another;
        the extra files of the projects are copied again after core has replaced its flat folders.
        What core replaces and nobody ships (e.g. a file copied with ``aqua grids add``) is lost: it is listed in a
        warning. Only the installed projects are considered: one without its folders or its package, or breaking the
        plugin contract, stops the update before any change, as for ``aqua project update``.
        """
        self._check_project_layout()
        self.logger.info("Updating AQUA installation...")

        # the checks of `aqua project update`, for every installed project and before any change
        projects = self._installed_projects()
        for name in projects:
            self._check_installed_project(name)
            self._project_source(name)

        unshipped = self._core_unshipped(projects)
        if unshipped:
            self.logger.warning(
                "Removing %s: not shipped by aqua-core or by an installed project. "
                "A file added with `aqua grids|fixes|styles add -e` is kept by the update",
                ", ".join(unshipped),
            )

        self._copy_component("core", self.components["core"]["path"], update=True)
        for name in projects:
            self._project_update(name)

    def _copy_update_folder_file(self, source, target, link=False, update=False):
        """Generic function to copy or update a source to a target folder"""

        # Check if the target exists
        if os.path.exists(target):
            if os.path.islink(target):
                self.logger.error("AQUA has been installed in editable mode, no need to update")
                sys.exit(1)
            # Update case
            if update:
                self.logger.info("Updating %s ...", target)
                self._remove_target(target)

        if os.path.exists(target):
            self.logger.error("%s already exist, please consider update or uninstall", target)
            sys.exit(1)

        # Handle linking
        if link:
            self.logger.info("Linking from %s to %s", source, target)
            os.symlink(source, target)

        # Handle copying
        else:
            if os.path.isdir(source):
                self.logger.info("Copying directory from %s to %s", source, target)
                shutil.copytree(source, target)
            else:
                self.logger.info("Copying file from %s to %s", source, target)
                shutil.copy2(source, target)

    def list(self, args):
        """List installed catalogs and environment information"""

        self._check()

        print("AQUA environment information:")
        print(f"\t - AQUA installation path: {self.configpath}")
        print(f"\t - AQUA machine: {self.configurer.config_dict.get('machine')}")
        print(f"\t - AQUA available components: {', '.join(self.components)}")
        print(f"\t - AQUA available packages: {', '.join(ConfigPackages.avail_packages())}")

        if self.configurer.has_project_layout():
            self._print_projects()
        else:
            print(f"AQUA projects: none. {PRE_PROJECT_MESSAGE.format(config_file=self.configfile)}")

        self.catalog_list(args)

        if args.all:
            for name in self.components:
                for _, target in self._component_targets(name):
                    if os.path.isdir(target):
                        print(f"AQUA current installed {os.path.relpath(target, self.configpath)} in {self.configpath}:")
                        self._list_folder(target)

    def check(self, args):
        """Validate the installation: project key, installed projects against their folders, and catalogs.

        Exits with a non-zero code if a check fails.

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        self._check()
        failures = []

        if not self.configurer.has_project_layout():
            failures.append(PRE_PROJECT_MESSAGE.format(config_file=self.configfile))
        else:
            active = self.configurer.project
            installed = self._installed_projects()
            if active and active not in installed:
                failures.append(
                    f"The active project {active} is not installed (installed: {', '.join(installed) or 'none'}): "
                    "use `aqua project set` or install it"
                )
            if not installed:
                self.logger.warning("No project is installed: only aqua-core configuration is available")

            # the projects listed in config-aqua.yaml against the project folders found in the installation
            for name in installed:
                paths = self._project_paths(name)
                missing = [path for path in paths if not os.path.lexists(path)]
                if len(missing) == len(paths):
                    failures.append(
                        f"Project {name} is listed in {self.configfile} but {', '.join(paths)} is missing: "
                        f"use `aqua project remove {name}` and add it again"
                    )
                else:
                    for path in missing:
                        self.logger.warning("Project %s has no %s folder", name, os.path.relpath(path, self.configpath))
            for folder in PROJECT_FOLDERS:
                path = os.path.join(self.configpath, folder)
                for name in sorted(os.listdir(path)) if os.path.isdir(path) else []:
                    if os.path.isdir(os.path.join(path, name)) and name not in installed:
                        self.logger.warning("%s/%s does not belong to an installed project", folder, name)

        # as for the projects: the catalogs listed in config-aqua.yaml against the folders found in the installation
        catalogs = self._installed_catalogs()
        for name in catalogs:
            path = self._catalog_path(name)
            if not os.path.lexists(path):
                failures.append(
                    f"Catalog {name} is listed in {self.configfile} but {path} is missing: "
                    f"use `aqua catalog remove {name}` and add it again"
                )
                continue
            try:
                load_yaml(os.path.join(path, "catalog.yaml"))
            except Exception as e:
                failures.append(f"Catalog {name} cannot be loaded from {path}: {e}")
        catdir = os.path.join(self.configpath, CATPATH)
        for name in sorted(os.listdir(catdir)) if os.path.isdir(catdir) else []:
            if name not in catalogs:
                self.logger.warning("%s/%s does not belong to an installed catalog", CATPATH, name)

        for failure in failures:
            self.logger.error(failure)
        if failures:
            sys.exit(1)
        print(f"AQUA installation in {self.configpath} is valid ({len(catalogs)} catalogs checked)")

    @staticmethod
    def _list_folder(mydir, return_list=False, silent=False):
        """
        List all the files in a AQUA config folder and check if they are link or file/folder

        Args:
            mydir (str): the directory to be listed
            return_list (bool): if True, return the list of files for further processing
            silent (bool): if True, do not print the files, just return the list

        Returns:
            None or list: a list of files if return_list is True, otherwise nothing
        """

        list_files = []
        yaml_files = os.listdir(mydir)
        for file in yaml_files:
            file = os.path.join(mydir, file)
            if os.path.islink(file):
                orig_path = os.readlink(file)
                if not silent:
                    print(f"\t - {file} (editable from {orig_path})")
            else:
                if not silent:
                    print(f"\t - {file}")
                list_files.append(file)

        if return_list:
            return list_files

    @staticmethod
    def _editable_path(argument):
        """Path given to -c or -d for the editable mode: None if the flag has no path (standard mode) or is not given."""
        return None if argument in (None, "standard") else argument
