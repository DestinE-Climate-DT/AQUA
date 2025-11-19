"""Utility functions for the LatLonProfiles CLI."""

def load_var_config(config_dict, var, diagnostic='lat_lon_profiles'):
    """Load variable configuration from config dictionary.
    
    Args:
        config_dict (dict): Configuration dictionary.
        var (str or dict): Variable name or variable configuration dictionary.
        diagnostic (str): Diagnostic name.

    Returns:
        tuple: (var_config dict, regions list)
    """
    if isinstance(var, dict):
        var_config = var
    else:
        default_vars = config_dict.get('diagnostics', {}).get(diagnostic, {}).get('default_variables', {})
        var_config = default_vars.get(var, {'name': var})
    
    # Ensure 'name' key exists
    if 'name' not in var_config:
        var_config['name'] = var
    
    # Get regions
    regions = var_config.get('regions', [None])
    
    return var_config, regions