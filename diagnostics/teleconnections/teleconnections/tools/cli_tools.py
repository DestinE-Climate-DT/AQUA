'''
This module contains cli tools for the teleconnections diagnostic.
- get_dataset_config, to get configuration parameters for a given
  dataset from configuration file
'''


def get_dataset_config(sources=None, dataset_source=None, config_dict=None):
    """
    Get configuration parameters for a given dataset_source

    Args:
        sources (dict): dictionary with configuration parameters
        dataset_source (str): dataset source name
        config_dict (dict, opt): dictionary to store configuration parameters

    Returns:
        config_dict (dict): dictionary with configuration parameters
                            of the individual dataset_source

    Raises:
        ValueError: if dataset_source or sources is None
    """

    if dataset_source is None:
        raise ValueError('dataset_source is None')

    if sources is None:
        raise ValueError('sources is None')

    if config_dict is None:
        config_dict = {}

    # Get configuration parameters
    # Search for entries under 'sources' key
    model = sources[dataset_source]['model']
    config_dict['model'] = model
    exp = sources[dataset_source]['exp']
    config_dict['exp'] = exp
    source = sources[dataset_source]['source']
    config_dict['source'] = source

    regrid = sources[dataset_source]['regrid']
    config_dict['regrid'] = regrid

    freq = sources[dataset_source]['freq']
    config_dict['freq'] = freq

    try:
        zoom = sources[dataset_source]['zoom']
    except KeyError:
        zoom = None
    if zoom is not (None or False):
        config_dict['zoom'] = zoom

    try:
        months_window = sources[dataset_source]['months_window']
    except KeyError:
        months_window = 3
    config_dict['months_window'] = months_window

    try:
        outputfig = sources[dataset_source]['outputfig']
    except KeyError:
        outputfig = None
    config_dict['outputfig'] = outputfig

    try:
        outputdir = sources[dataset_source]['outputdir']
    except KeyError:
        outputdir = None
    config_dict['outputdir'] = outputdir

    try:
        filename = sources[dataset_source]['filename']
    except KeyError:
        filename = None
    config_dict['filename'] = filename

    return config_dict
