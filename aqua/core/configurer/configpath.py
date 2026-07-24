"""Backward-compatible ConfigPath facade for AQUA.

`ConfigPath` is kept as a drop-in replacement for the pre-refactor class so
that existing code (`ConfigPath(...)` used across the codebase) keeps working
without any changes - including direct attribute access like
`config_path.catalog`, `config_path.catalog_available`, or
`config_path.catalog_file`.

Internally, `ConfigPath` is just `ConfigContext` (config dir/machine
resolution) composed with `ConfigCatalog` (all catalog handling, including
intake). Catalog-related attributes are forwarded via properties so reads
AND writes (e.g. `config_path.catalog = "some_other_catalog"`) still behave
exactly as before.

New code should use `ConfigContext` and `ConfigCatalog` directly. `ConfigPath`
exists purely for migration and could be deprecated once call sites are
updated.
"""

from .catalog import ConfigCatalog
from .context import ConfigContext


class ConfigPath(ConfigContext):
    """
    Backward-compatible class preserving the full original `ConfigPath` API.
    """

    def __init__(self, configdir=None, filename="config-aqua.yaml", catalog=None, loglevel="warning", locator=None):
        super().__init__(configdir=configdir, filename=filename, loglevel=loglevel, locator=locator)
        self._catalog = ConfigCatalog(self, catalog=catalog)

    # -- forwarded catalog-related attributes (read + write) --

    @property
    def catalog(self):
        return self._catalog.catalog

    @catalog.setter
    def catalog(self, value):
        self._catalog.catalog = value

    @property
    def catalog_available(self):
        return self._catalog.catalog_available

    @property
    def base_available(self):
        return self._catalog.base_available

    @property
    def catalog_file(self):
        return self._catalog.catalog_file

    @catalog_file.setter
    def catalog_file(self, value):
        self._catalog.catalog_file = value

    @property
    def machine_file(self):
        return self._catalog.machine_file

    @machine_file.setter
    def machine_file(self, value):
        self._catalog.machine_file = value

    # -- forwarded catalog-related methods --

    def get_catalog(self):
        """See `ConfigCatalog.get_catalog`."""
        return self._catalog.get_catalog()

    def get_base(self):
        """See `ConfigCatalog.get_base`."""
        return self._catalog.get_base()

    def get_catalog_filenames(self, catalog=None):
        """See `ConfigCatalog.get_catalog_filenames`."""
        return self._catalog.get_catalog_filenames(catalog)

    def get_reader_filenames(self, catalog=None):
        """See `ConfigCatalog.get_reader_filenames`."""
        return self._catalog.get_reader_filenames(catalog)

    def get_machine_info(self):
        """See `ConfigCatalog.get_machine_info`."""
        return self._catalog.get_machine_info()

    def browse_catalogs(self, model: str, exp: str, source: str):
        """See `ConfigCatalog.browse_catalogs`."""
        return self._catalog.browse_catalogs(model, exp, source)

    def deliver_intake_catalog(self, model, exp, source, catalog=None):
        """See `ConfigCatalog.deliver_intake_catalog`."""
        return self._catalog.deliver_intake_catalog(model, exp, source, catalog=catalog)

    def scan_catalog(self, cat, model=None, exp=None, source=None):
        """See `ConfigCatalog.scan_catalog`."""
        return self._catalog.scan_catalog(cat, model=model, exp=exp, source=source)

    def show_catalog_content(self, catalog=None, model=None, exp=None, source=None, verbose=True, show_descriptions=False):
        """See `ConfigCatalog.show_catalog_content`."""
        return self._catalog.show_catalog_content(
            catalog=catalog, model=model, exp=exp, source=source, verbose=verbose, show_descriptions=show_descriptions
        )

    @staticmethod
    def format_catalog_structure(structure, catalog_name, descriptions=None):
        """See `ConfigCatalog.format_catalog_structure`."""
        return ConfigCatalog.format_catalog_structure(structure, catalog_name, descriptions)
