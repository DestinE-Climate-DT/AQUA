"""Helpers to resolve installation paths of AQUA and complementary packages."""

import importlib.resources as pypath


class ConfigPackages:
    """
    Locates the installation directory of AQUA and complementary packages
    (e.g. `aqua.diagnostics`) using `importlib.resources`.

    Required packages are resolved eagerly at construction time - if one is
    missing, this raises immediately, since AQUA cannot run without it.
    Optional packages are resolved lazily on first request via
    `get_package_path(..., required=False)`, and cached after that so
    repeated lookups don't re-hit `importlib.resources`.
    """

    #: packages AQUA cannot function without; resolved eagerly at init
    REQUIRED_PACKAGES = ("aqua", "aqua.core")

    def __init__(self, logger=None):
        """
        Args:
            logger (Logger | None): optional logger for debug/error messages.
        """
        self.logger = logger
        self._paths = {}

        for package in self.REQUIRED_PACKAGES:
            self._paths[package] = self._resolve(package, required=True)

    def get_package_path(self, package: str, required: bool = False) -> str:
        """
        Return the installation path of `package`.

        Resolved on first request and cached for subsequent calls.

        Args:
            package (str): dotted package name, e.g. "aqua.diagnostics".
            required (bool): if True, raise ModuleNotFoundError when the
                package is not installed. If False, log an error and return
                "" instead.

        Returns:
            str: the package installation path, or "" if not found and
            `required` is False.
        """
        if package not in self._paths:
            self._paths[package] = self._resolve(package, required=required)
        return self._paths[package]

    def _resolve(self, package: str, required: bool) -> str:
        try:
            path = str(pypath.files(package))
            if self.logger:
                self.logger.debug("%s path: %s", package, path)
            return path
        except ModuleNotFoundError:
            if required:
                raise
            if self.logger:
                self.logger.error("%s package not found; path will be empty.", package)
            return ""

    @property
    def aqua_path(self) -> str:
        """Convenience shortcut for the required `aqua` path."""
        return self.get_package_path("aqua", required=True)

    @property
    def aqua_core_path(self) -> str:
        """Convenience shortcut for the required `aqua.core` path."""
        return self.get_package_path("aqua.core", required=True)
