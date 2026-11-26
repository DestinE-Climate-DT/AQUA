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
from aqua.core.util import dump_yaml, load_yaml

from .util import query_yes_no

# folder used for reading/storing catalogs
CATPATH = "catalogs"


class InstallMixin:
    """Mixin for AQUA installation operations"""

    def _component_summary(self):
        return {
            name: {
                "installed": self._check_component_installed(name),
                "mode": self._get_component_mode(name),
            }
            for name in self.components
        }

    def _check(self, silent=False, return_info=False):
        """
        Check installation and optionally return detailed information

        Args:
            silent (bool): If True, suppress errors and use ERROR loglevel
            return_info (bool): If True, return installation info dict instead of exiting

        Returns:
            dict or None: If return_info=True, returns:
                {
                    'installed': bool,
                    'configpath': str or None,
                    'components': {
                        'core': {'installed': bool, 'mode': str},
                        'diagnostics': {'installed': bool, 'mode': str},
                        'emulators': {'installed': bool, 'mode': str},
                        ...
                    }
                }
        """
        checklevel = "ERROR" if silent else self.loglevel

        try:
            self.configurer = ConfigContext(loglevel=checklevel)
            self.configpath = self.configurer.get_config_dir()
            self.configfile = os.path.join(self.configpath, "config-aqua.yaml")
            self.templatepath = os.path.join(self.configpath, "templates")
            self.logger.debug("AQUA found in %s", self.configpath)

            if return_info:
                return {
                    "installed": True,
                    "configpath": self.configpath,
                    "components": self._component_summary(),
                }

        except FileNotFoundError:
            if return_info:
                return {
                    "installed": False,
                    "configpath": None,
                    "components": {name: {"installed": False, "mode": "not_installed"} for name in self.components},
                }

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

    def _project_flat_dirs(self, component):
        """Flat folders of a project in the AQUA config folder: installed and removed with the project."""
        return [os.path.join(self.configpath, d) for d in self.components[component]["flat"]]

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

    def _check_component_installed(self, component):
        """
        Check if a specific component is already installed

        Args:
            component (str): component name, e.g. 'core', 'diagnostics', 'claima'

        Returns:
            bool: True if any folder of the component is installed (per ``<dir>/<project>`` for a project)
        """
        return self._get_component_mode(component) is not None

    def _get_component_mode(self, component):
        """
        Check if a component is installed in editable mode

        Args:
            component (str): component name, e.g. 'core', 'diagnostics', 'claima'

        Returns:
            str or None: 'editable' or 'standard' from the first installed folder, None if not installed
        """
        for _, target in self._component_targets(component):
            if os.path.islink(target):
                return "editable"
            if os.path.exists(target):
                return "standard"
        return None

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

    def _install_component(self, component, mode):
        """
        Install a component in either standard or editable mode.

        A project (any plugin) installs its project-scoped folders in ``<configpath>/<dir>/<project>``:
        in editable mode the link is made at that level.

        Args:
            component (str): component name, e.g. 'core', 'diagnostics', 'claima'
            mode (dict): Installation mode with 'mode' and 'path' keys
        """
        self.logger.info("Installing %s in %s mode", component, mode["mode"])
        if mode["mode"] == "editable":
            self._copy_component(component, self._validate_component_path(component, mode["path"]), link=True)
        else:
            self._copy_component(component, self.components[component]["path"])

    def _copy_component(self, component, source_path, link=False, update=False):
        """
        Copy (or link) the folders and extra files of a component; when updating, those in editable mode are skipped.

        A first installation of a project stops before any change if one of its flat folders or extra files already
        exists (a project owns them, and `aqua project remove` deletes them); it keeps a templates folder already present.
        Updating a project copies its folders over the installed ones, so user files with other names are kept;
        core folders are replaced.

        Args:
            component (str): component name, e.g. 'core', 'diagnostics', 'claima'
            source_path (str): config folder of the component package or repository
            link (bool): link the folders and files instead of copying them
            update (bool): replace the folders and files already installed
        """
        files = self._component_files(component, source_path)
        owned = [target for _, target in files] + (self._project_flat_dirs(component) if component != "core" else [])
        existing = [target for target in owned if os.path.lexists(target)]
        if existing and not update:
            self.logger.error("Cannot install %s, these files or folders already exist: %s", component, ", ".join(existing))
            sys.exit(1)

        for source, target in self._component_targets(component, source_path) + files:
            if update and os.path.islink(target):
                self.logger.info("Skipping %s (editable mode)", target)
            elif not os.path.exists(source):
                self.logger.warning("%s declared by %s does not exist, skipping it", source, component)
            elif update and component != "core" and os.path.isdir(source):
                self.logger.info("Updating %s ...", target)
                shutil.copytree(source, target, dirs_exist_ok=True)
            elif not update and component != "core" and os.path.lexists(target):
                # a templates folder shared by projects, kept by `aqua project remove`
                self.logger.info("%s is already installed, keeping it", target)
            else:
                os.makedirs(os.path.dirname(target), exist_ok=True)
                self._copy_update_folder_file(source, target, link=link, update=update)

    def install(self, args):
        """Install AQUA: core and the diagnostics project, or only the one selected with -c or -d

        Other projects are added with ``aqua project install``; ``-d`` alone adds diagnostics to an existing
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

        core_mode = self._get_install_mode(args.core)
        diagnostics_mode = self._get_install_mode(args.diagnostics)
        if core_mode is None and diagnostics_mode is None:
            # bare `aqua install`: core and diagnostics, standard mode
            core_mode = diagnostics_mode = {"mode": "standard", "path": None}
        self.logger.debug("Installation modes: core %s, diagnostics %s", core_mode, diagnostics_mode)

        diagnostics = self.components.get("diagnostics", {"path": None, "error": None})
        # a diagnostics package breaking the contract (e.g. an old contract) stops the installation before any change
        if diagnostics_mode and diagnostics["error"]:
            self.logger.error(diagnostics["error"])
            sys.exit(1)

        if not core_mode:
            if not install_info["installed"]:
                self.logger.error("Cannot install diagnostics without core. Install core first or use full installation.")
                sys.exit(1)
            self._project_add("diagnostics", diagnostics_mode)
            return

        if install_info["installed"]:
            self.logger.warning("AQUA installation found in %s", self.configpath)
            self.logger.warning("Proceeding will remove the existing installation, its projects and all catalogs.")
            if not self._remove_installation(confirm=True):
                sys.exit()

        self.logger.debug("Installing core component")
        self._install_component("core", core_mode)
        self.logger.debug("Creating config-aqua.yaml configuration file")
        self._copy_update_folder_file(
            os.path.join(self.components["core"]["path"], "config-aqua.tmpl"),
            os.path.join(self.configpath, "config-aqua.yaml"),
        )

        # Create catalogs directory
        os.makedirs(os.path.join(self.configpath, CATPATH), exist_ok=True)
        self._set_machine(args)

        # the project key of config-aqua.tmpl is empty: diagnostics becomes active only if it is installed
        if diagnostics_mode and diagnostics["path"] is not None:
            self._install_component("diagnostics", diagnostics_mode)
            self.configfile = os.path.join(self.configpath, "config-aqua.yaml")
            self._project_set("diagnostics")
        elif diagnostics_mode:
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
                if os.path.exists(link) or os.path.islink(link):
                    self.logger.warning("Removing the content of %s", link)
                    if os.path.islink(link):
                        os.unlink(link)
                    elif os.path.isdir(link):
                        shutil.rmtree(link)
                    else:
                        os.remove(link)
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

        if os.path.islink(self.configpath):
            self.logger.info("Removing the link %s", self.configpath)
            os.unlink(self.configpath)
        else:
            self.logger.info("Removing directory %s", self.configpath)
            shutil.rmtree(self.configpath)

        return True

    def uninstall(self, args):
        """Remove AQUA"""
        print("Remove the AQUA installation")
        self._check()
        if not self._remove_installation(confirm=True):
            sys.exit()

    def update(self, args):
        """Update the installation files of core and of every installed project, skipping editable folders.

        Each project is refreshed per ``<dir>/<project>``, so updating one project never deletes another;
        the extra files of the projects are copied again after core has replaced its flat folders.
        """
        self._check_project_layout()
        self.logger.info("Updating AQUA installation...")

        failed = False
        for name, info in self.components.items():
            if info["error"]:
                self.logger.error("Cannot update %s: %s", name, info["error"])
                failed = True
            elif info["path"] is not None and (name == "core" or self._check_component_installed(name)):
                self._copy_component(name, info["path"], update=True)
        if failed:
            sys.exit(1)

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
                if os.path.isdir(target):
                    shutil.rmtree(target)
                else:
                    os.remove(target)

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
        """Validate the installation: project key, active project folders and catalogs.

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
            if active in installed:
                for folder in PROJECT_FOLDERS:
                    if not os.path.isdir(os.path.join(self.configpath, folder, active)):
                        self.logger.warning("Project %s has no %s folder", active, folder)
            elif installed:
                failures.append(
                    f"The active project {active} is not installed (installed: {', '.join(installed)}): "
                    "use `aqua project set` or install it"
                )
            else:
                self.logger.warning("No project is installed: only aqua-core configuration is available")

        catdir = os.path.join(self.configpath, CATPATH)
        catalogs = sorted(os.listdir(catdir)) if os.path.isdir(catdir) else []
        for catalog in catalogs:
            catalog_file = os.path.join(catdir, catalog, "catalog.yaml")
            try:
                load_yaml(catalog_file)
            except Exception as e:
                failures.append(f"Catalog {catalog} cannot be loaded from {catalog_file}: {e}")

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
    def _get_install_mode(specific_arg):
        """
        Determine the installation mode for a component.

        Args:
            specific_arg: Value of the component's CLI argument (e.g. --core or --diagnostics):
                None if not requested, 'standard' if requested without a path, or a path string
                for an editable install.

        Returns:
            dict or None: {'mode': 'editable'|'standard', 'path': str|None},
                or None if the component was not requested at all.
        """
        # If the component was not requested at all, argparse leaves it as None
        if specific_arg is None:
            return None

        # If specific_arg is a string path (not 'standard'), use editable mode
        if isinstance(specific_arg, str) and specific_arg != "standard":
            return {"mode": "editable", "path": specific_arg}

        # If specific_arg is 'standard' (flag without value), use standard mode
        return {"mode": "standard", "path": None}
