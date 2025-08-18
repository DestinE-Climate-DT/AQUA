"""Utility functions for the CLI."""


def load_var_config(config_dict: dict, var: str, diagnostic: str = 'timeseries'):
    """Load the variable configuration from the configuration dictionary.

    Args:
        config_dict (dict): The configuration dictionary.
        var (str): The variable to load the configuration for.
        diagnostic (str): The diagnostic to load the configuration for. Default is 'timeseries'.

    Returns:
        var_config (dict): The variable configuration dictionary
    """
    default_dict = config_dict['diagnostics'][diagnostic]['params']['default']

    if var in config_dict['diagnostics'][diagnostic]['params']:
        var_config = config_dict['diagnostics'][diagnostic]['params'][var]
    else:
        var_config = {}

    # Merge the default and variable specific configuration
    # with the variable specific configuration taking precedence
    var_config = {**default_dict, **var_config}

    # Take hourly, daily, monthly, annual and make a list of the True
    # ones, dropping the individual keys
    if diagnostic == 'timeseries':
        freq = []
        for key in ['hourly', 'daily', 'monthly', 'annual']:
            if var_config[key]:
                freq.append(key)
            if var_config[key] is not None:
                del var_config[key]
        var_config['freq'] = freq

    # Get the regions
    regions = [None]
    if var_config.get('regions') is not None:
        regions.extend([region for region in var_config['regions'] if region is not None])
        del var_config['regions']

    return var_config, regions
