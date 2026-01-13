"""Simple catalog utility"""
from aqua.core.configurer import ConfigPath


def show_catalog_content(catalog=None, model=None, exp=None, source=None, configdir=None, catalog_name=None,
                         loglevel='WARNING', verbose=True, show_descriptions=False):
    """
    Display the catalog content structure (model/exp/source) without requiring
    manual ConfigPath instantiation.

    This is a convenience wrapper around ConfigPath.show_catalog_content() that
    handles the ConfigPath initialization internally.

    Args:
        catalog (str | list | None): Specific catalog(s) to scan. If None, loops over all available catalogs.
        model (str | None): Optional model filter. If provided, only shows entries for this model.
        exp (str | None): Optional experiment filter. If provided, only shows entries for this exp.
        source (str | None): Optional source filter. If provided, only shows entries for this source.
        configdir (str, optional): The directory containing the configuration files. If not provided, ConfigPath will determine it automatically.
        catalog_name (str, optional): Override the catalog name. If not provided, uses the default catalog.
        loglevel (str, optional): Logging level. Defaults to 'WARNING'.
        verbose (bool): If True, prints the formatted catalog structure. Defaults to True.
        show_descriptions (bool): If True, also print per-source descriptions.

    Returns:
        dict: Dictionary with catalog names as keys and nested dict structure
              as values.
    """
    config = ConfigPath(configdir=configdir, catalog=catalog_name, loglevel=loglevel)
    return config.show_catalog_content(catalog=catalog, model=model, exp=exp, source=source, verbose=verbose,
                                       show_descriptions=show_descriptions)
