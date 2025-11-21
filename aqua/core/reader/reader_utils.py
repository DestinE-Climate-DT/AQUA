"""Common functions for the Reader"""
import xarray as xr


def check_catalog_source(cat, model, exp, source, name="dictionary"):
    """
    Check the entries of a nested dictionary based on the model/exp/source structure
    and return an updated source. The name argument can be used for proper printing.

    Args:
        cat (dict): The nested dictionary containing the catalog information.
        model (str): The model ID to check in the catalog.
        exp (str): The experiment ID to check in the catalog.
        source (str): The source ID to check in the catalog.
        name (str, optional): The name used for printing. Defaults to "dictionary".

    Returns:
        str: An updated source ID. If the source is not specified, "default"
            is chosen, or, if missing, the first source.
    """

    if model not in cat:
        avail = list(cat.keys())
        raise KeyError(f"Model {model} not found in {name}. " 
                       f"Please choose between available models: {avail}")
    if exp not in cat[model]:
        avail = list(cat[model].keys())
        raise KeyError(f"Experiment {exp} not found in {name} for model {model}. "
                       f"Please choose between available exps: {avail}")
    if not cat[model][exp].keys():
        raise KeyError(f"Experiment {exp} in {name} for model {model} has no sources.")

    if source:
        if source not in cat[model][exp]:
            if "default" not in cat[model][exp]:
                avail = list(cat[model][exp].keys())
                raise KeyError(f"Source {source} of experiment {exp} "
                               f"not found in {name} for model {model}. "
                               f"Please choose between available sources: {avail}")
            source = "default"
    else:
        source = list(cat[model][exp].keys())[0]  # take first source if none provided

    return source


def check_att(da, att):
    """
    Check if a dataarray has a specific attribute.

    Arguments:
        da (xarray.DataArray): DataArray to check
        att (dict or str): Attribute to check for

    Returns:
        Boolean
    """
    if att:
        if isinstance(att, str):
            return att in da.attrs
        elif isinstance(att, dict):
            key = list(att.keys())[0]
            if key in da.attrs:
                return da.attrs[key] == list(att.values())[0]
        else:
            return False
    else:
        return False


def set_attrs(ds, attrs):
    """
    Set an attribute for all variables in an xarray.Dataset

    Args:
        ds (xarray.Dataset or xarray.DataArray): Dataset to set attributes on
        attrs (dict): Dictionary of attributes to set
    
    Returns:
        xarray.Dataset or xarray.DataArray: Updated Dataset or DataArray, or the same object if not this.
    """
    if not isinstance(attrs, dict):
        raise TypeError("The 'attrs' argument must be a dictionary.")

    if isinstance(ds, xr.Dataset):
        for var in ds.data_vars:
            ds[var].attrs.update(attrs)
    elif isinstance(ds, xr.DataArray):
        ds.attrs.update(attrs)
    return ds

