"""Backward-compatible ConfigPath facade for AQUA.

`ConfigPath` is kept as a drop-in replacement for the pre-refactor class so
that existing code (`ConfigPath(...)` used across the codebase) keeps working
without any changes. Internally it is just `ConfigPaths` (path/config
resolution) plus a composed `CatalogBrowser` (intake catalog browsing).

New code should prefer using `ConfigPaths` and `CatalogBrowser` directly -
`ConfigPath` exists purely for migration purposes and could be deprecated
once call sites have been updated.
"""

from .catalogbrowser import CatalogBrowser
from .configpaths import ConfigPaths


class ConfigPath(ConfigPaths):
    """
    Backward-compatible class preserving the full original `ConfigPath` API.

    All path/machine-resolution behavior is inherited unchanged from
    `ConfigPaths`. All intake-catalog behavior (browsing, delivering,
    scanning, displaying) is delegated to an internal `CatalogBrowser`
    instance, built against `self` (since `ConfigPath` *is* a `ConfigPaths`).

    Because `CatalogBrowser` mutates `paths.catalog` / `paths.catalog_file` /
    `paths.machine_file` in place (e.g. in `deliver_intake_catalog`), and
    `self._browser.paths is self`, those mutations are still visible on the
    `ConfigPath` instance exactly as before - `config_path.catalog` reflects
    the last delivered catalog, same as in the original implementation.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._browser = CatalogBrowser(self)

    def browse_catalogs(self, model: str, exp: str, source: str):
        """See `CatalogBrowser.browse_catalogs`."""
        return self._browser.browse_catalogs(model, exp, source)

    def deliver_intake_catalog(self, model, exp, source, catalog=None):
        """See `CatalogBrowser.deliver_intake_catalog`."""
        return self._browser.deliver_intake_catalog(model, exp, source, catalog=catalog)

    def scan_catalog(self, cat, model=None, exp=None, source=None):
        """See `CatalogBrowser.scan_catalog`."""
        return self._browser.scan_catalog(cat, model=model, exp=exp, source=source)

    def show_catalog_content(self, catalog=None, model=None, exp=None, source=None, verbose=True, show_descriptions=False):
        """See `CatalogBrowser.show_catalog_content`."""
        return self._browser.show_catalog_content(
            catalog=catalog, model=model, exp=exp, source=source, verbose=verbose, show_descriptions=show_descriptions
        )

    @staticmethod
    def format_catalog_structure(structure, catalog_name, descriptions=None):
        """See `CatalogBrowser.format_catalog_structure`."""
        return CatalogBrowser.format_catalog_structure(structure, catalog_name, descriptions)
