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
        raise KeyError(f"Model {model} not found in {name}.")
    if exp not in cat[model]:
        raise KeyError(f"Experiment {exp} not found in {name} for model {model}.")
    if not cat[model][exp].keys():
        raise KeyError(f"Experiment {exp} in {name} for model {model} has no sources")

    if source:
        if source not in cat[model][exp]:
            if "default" not in cat[model][exp]:
                raise KeyError(f"Source {source} of experiment {exp} "
                               f"not found in {name} for model {model}.")
            source = "default"
    else:
        source = list(cat[model][exp].keys())[0]  # take first source if none provided

    return source


def check_att(da, att):
    """
    Check if a dataarray has a specific attribute.

    Arguments:
        da (xarray.DataArray): DataArray to check
        att (str): Attribute to check for

    Returns:
        Boolean
    """
    if att:
        key = list(att.keys())[0]
        if key in da.attrs:
            return da.attrs[key] == list(att.values())[0]
        else:
            return False
    else:
        return False


def group_shared_dims(ds, shared_dims, others=None, masked=None, masked_att=None):
    """
    Groups variables in a dataset that share the same dimension.

    Arguments:
        ds (xarray.Dataset or xarray.DataArray): Input dataset or dataarray to group variables
        shared_dims (list): List of shared dimensions
        others (str, optional): Name of group for variables not in `shared_dims`.
        masked (str, optional): Name of extra group for masked variables.
                                Used only if the "others" option is set.
        masked_att (dict, optional): Dictionary of attributes to use to check if variable is masked.

    Raises:
        ValueError: If no shared dimensions are found.

    Returns:
        Dictionary containing datasets that share the same dimension
    """

    # Is this a DataArray?
    if not isinstance(ds, xr.Dataset):
        dim = [x for x in shared_dims if x in ds.dims]
        if dim:
            return {dim[0]: ds}
        else:
            if others:
                if check_att(ds, masked_att):
                    return {masked: ds}
                else:
                    return {others: ds}
            else:
                raise ValueError("No shared dimensions found.")

    shared_vars = {}
    for dim in shared_dims:
        vlist = []
        for var in ds.data_vars:
            if dim in ds[var].dims:
                vlist.append(var)
        shared_vars.update({dim: ds[vlist]})
    if others:
        vlist = []
        vlistm = []
        for var in ds.data_vars:
            if not any(x in shared_dims for x in ds[var].dims):
                if check_att(ds[var], masked_att):
                    vlistm.append(var)
                else:
                    vlist.append(var)
        if vlist:
            shared_vars.update({others: ds[vlist]})
        if vlistm:
            shared_vars.update({masked: ds[vlistm]})

    return shared_vars


def set_attrs(ds, attrs):
    """
    Set an attribute for all variables in an xarray.Dataset

    Args:
        ds (xarray.Dataset or xarray.DataArray): Dataset to set attributes on
        attrs (dict): Dictionary of attributes to set
    """
    if isinstance(ds, xr.Dataset):
        for var in ds.data_vars:
            ds[var].attrs.update(attrs)
    else:
        ds.attrs.update(attrs)
    return ds
