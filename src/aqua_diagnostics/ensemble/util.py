"""
Utility functions for the ensemble class
"""

import gc
from collections import Counter
import numpy as np
import pandas as pd
import xarray as xr
from aqua import Reader
from aqua.exceptions import NoDataError
from aqua.logger import log_configure


def retrieve_merge_ensemble_data(
    variable: str = None,
    region: str = None,
    ens_dim: str = "ensemble",
    model_names: list[str] = None,
    data_path_list: list[str] = None,
    catalog_list: list[str] = None,
    model_list: list[str] = None,
    exp_list: list[str] = None,
    source_list: list[str] = None,
    startdate: str = None,
    enddate: str = None,
    log_level: str = "WARNING",
):
    """
    Retrieves, merges, and slices datasets based on specified models, experiments,
    sources, and time boundaries.

    This function reads data for a given variable (`variable`) from multiple models, experiments,
    and sources, combines the datasets along the specified "ensemble" dimension along with their indices, and slices
    the merged dataset to the given start and end dates. The ens_dim can given any customized name for the ensemble dimension.

    There are following two ways to load the datasets with function.
    a) with xarray.open_dataset
    b) with AQUA Reader class

    Args:
        variable (str): The variable to retrieve data. Defaults to None.

        In the case a):
            data_path_list (list of str): list of paths for data to be loaded by xarray.
            model_names (list): Assign names to the variable lists. It is IMPORTANT
                to assign names when calculating multi-model mean. This variable list
                will assign a name to each memeber.

        In the case b):
            region (str): This variable is specific to the Zonal averages. Defaults to None.
            catalog_list (list): A list of AQUA catalog. Default to None.
            model_list (list): A list of model names. Each model corresponds to an
                experiment and source in the `exps` and `sources` lists, respectively.
                Defaults to None.
            exp_list (list): A list of experiment names. Each experiment corresponds
                to a model and source in the `models` and `sources` lists, respectively.
                Defaults to None.
            source_list (list): A list of data source names. Each source corresponds
                to a model and experiment in the `models` and `exps` lists, respectively.
                Defaults to None.

        Specific to the timeseries datasets:
            startdate (str or datetime): The start date for slicing the merged dataset.
                If None is provided, the ensemble members are merged w.r.t to their time-interval. Defaults to None.
            enddate (str or datetime): The end date for slicing the merged dataset.
                If None is provided, the ensemble members are merged w.r.t to their time-interval. Defaults to None.
    Returns:
            xarray.Dataset: The merged dataset containing data from all specified models,
                experiments, and sources, concatenated along `ens_dim` along with model name.
    """
    logger = log_configure(log_name="retrieve_merge_ensemble_data", log_level=log_level)
    logger.info("Loading and merging the ensemble dataset")

    # in case if the list of paths of netcdf dataset is given
    # then load via xarray.open_dataset function
    # return ensemble dataset with with ensemble dimension ens_dim with individual indexes
    # temporary list to append the datasets and later concat the list to merged dataset along ens_dim
    tmp_dataset_list = []
    tmp_min_date_list = []
    tmp_max_date_list = []

    # Method (a): To load and merge the dataset via file paths
    if data_path_list is not None:
        if model_names is not None:
            model_counts = dict(Counter(model_names))
        if model_names is None or len(model_counts.keys()) <= 1:
            logger.info("Single model ensemble memebers are given")
            if model_names is None:
                logger.info("No model name is given. Assigning it to model_name.")
                model_names = ["model_name"] * len(data_path_list)
        else:
            logger.info("Multi-model ensemble members are given")

        for i, f in enumerate(data_path_list):
            # load and assign new dimensions, namely ensemble and model name
            tmp_dataset = xr.open_dataset(
                f, drop_variables=[var for var in xr.open_dataset(f).data_vars if var != variable]
            ).expand_dims({ens_dim: [i]})
            # append to the temporary list
            tmp_dataset_list.append(tmp_dataset)

            # Check if the given data is a timeseries
            # if yes, then compute common startdate and enddate.
            if "time" in tmp_dataset.dims:
                if startdate is not None and enddate is not None:
                    tmp_dataset = tmp_dataset.sel(time=slice(startdate, enddate))
                else:
                    tmp_min_date_list.append(pd.to_datetime(tmp_dataset.time.values[0]))
                    tmp_max_date_list.append(pd.to_datetime(tmp_dataset.time.values[-1]))
        ens_dataset = xr.concat(tmp_dataset_list, dim=ens_dim)
        ens_dataset = ens_dataset.assign_coords(model=(ens_dim, model_names))
    # Method (b):
    # else if check the models, exps and sources are given from the catalog in the forms of lists
    # then use the AQUA.Reader class to load the data
    elif model_list is not None and exp_list is not None and source_list is not None:
        model_names = model_list
        if len(np.unique(model_names)) <= 1:
            logger.info("Single model ensemble memebers are given")
        else:
            logger.info("Multi-model ensemble members are given")

        # if Catalog is not Null
        if catalog_list is not None:
            for i, model in enumerate(model_list):
                # check if region variable is defined
                if region is not None:
                    tmp_reader = Reader(
                        catalog=catalog_list[i],
                        model=model,
                        exp=exp_list[i],
                        source=source_list[i],
                        areas=False,
                        region=region,
                    )
                # if region variable is not defined
                else:
                    tmp_reader = Reader(
                        catalog=catalog_list[i],
                        model=model,
                        exp=exp_list[i],
                        source=source_list[i],
                        areas=False,
                    )
                tmp_dataset = tmp_reader.retrieve(var=variable)
                tmp_dataset_expended = tmp_dataset.expand_dims(ensemble=[i])
                tmp_dataset_list.append(tmp_dataset_expended)
        # if Catalog is Null
        else:
            for i, model in enumerate(model_list):
                # check if region variable is defined
                if region is not None:
                    tmp_reader = Reader(
                        model=model,
                        exp=exp_list[i],
                        source=source_list[i],
                        areas=False,
                        region=region,
                    )
                # if region variable is not defined
                else:
                    tmp_reader = Reader(
                        model=model,
                        exp=exp_list[i],
                        source=source_list[i],
                        areas=False,
                    )
                tmp_dataset = tmp_reader.retrieve(var=variable)
                tmp_dataset_expended = tmp_dataset.expand_dims(ensemble=[i])
                tmp_dataset_list.append(tmp_dataset_expended)
        # check if the given data is timeseries
        # if yes, compute common startdate and enddate
        if "time" in tmp_dataset.dims:
            if startdate is not None and enddate is not None:
                tmp_dataset = tmp_dataset.sel(time=slice(startdate, enddate))
            else:
                tmp_min_date_list.append(pd.to_datetime(tmp_dataset.time.values[0]))
                tmp_max_date_list.append(pd.to_datetime(tmp_dataset.time.values[-1]))

        # concatenate along the ensemble dimension
        ens_dataset = xr.concat(tmp_dataset_list, dim=ens_dim)
        ens_dataset = ens_dataset.assign_coords(model=(ens_dim, model_names))
        # delete tmp varaibles
        del tmp_reader, tmp_dataset_expended
    else:
        # No data is given
        raise NoDataError("No Data is provided to retreieve and merge for ensemble")

    if tmp_min_date_list and tmp_max_date_list:
        common_startdate = max(tmp_min_date_list)
        common_enddate = min(tmp_max_date_list)
    # delete all tmp varaibles
    del tmp_dataset_list, tmp_min_date_list, tmp_max_date_list, tmp_dataset
    gc.collect()
    # check if the ensemble dataset is a timeseries dataset
    # then return enemble dataset
    if "time" in ens_dataset.dims:
        if startdate is not None and enddate is not None:
            common_startdate = startdate
            common_enddate = enddate
        logger.info("Finished loading the ensemble timeseries datasets")
        ens_dataset.attrs["description"] = f"Dataset merged along {ens_dim} for ensemble statistics"
        ens_dataset.attrs["model names"] = model_names
        return ens_dataset.sel(time=slice(common_startdate, common_enddate))
    else:
        # the ensemble dataset is not a timeseries
        logger.info("Finished loading the ensemble datasets")
        ens_dataset.attrs["description"] = f"Dataset merged along {ens_dim} for ensemble statistics"
        ens_dataset.attrs["model names"] = model_names
        return ens_dataset


def compute_statistics(
    variable: str = None, ds: xr.Dataset = None, ens_dim: str = "ensemble", log_level="WARNING"
):
    """
    A function to compute mean and STD. Moreover, it can also perform weighted mean and STD.
    Args:
        variable (str): Variable name.
        ds (xr.Dataset): xarray.Dataset merged along default dimension 'ensemble'.

    Returns:
        1) In case of Single-model ensemble:
           returns xarray.Dataset ds_mean and ds_std
        2) In case of Multi-model ensemble:
           returns xarray.Dataset weighted_mean and weighted_std
    """

    logger = log_configure(log_name="compute_statistics", log_level=log_level)
    logger.info("Computing statistics of the ensemble dataset")

    if ds is None:
        raise NoDataError("No data is given to the compute_statistics method in ensemble/util.py")
    if ds.model is not None:
        if len(np.unique(ds.model)) <= 1:
            logger.info("Given dataset in compute_statistics is Single-model ensemble")
            # unweighted mean and STD in case of Single-model ensemble
            ds_mean = ds[variable].mean(dim=ens_dim, skipna=False)
            ds_std = ds[variable].std(dim=ens_dim, skipna=False)
            return ds_mean, ds_std
        else:
            logger.info("Given dataset in compute_statistics is Multi-model ensemble")
            # weighted mean and STD in case of Multi-model ensemble.
            # The function expects that the dataset is merged with
            # each ensmble member is named with the model name
            # in the multi-model ensemble
            # Step 1: Count how many ensembles each model has
            counts = ds["model"].to_series().value_counts().to_dict()

            # Step 2: Create weights per ensemble member using the model label
            weights = xr.DataArray(
                [counts[model] for model in ds["model"].values],
                dims=ens_dim,
                coords={ens_dim: ds[ens_dim]},
            )

            # Step 3: Normalize weights (optional if you want weighted *mean*, not just rescaled sum)
            normalized_weights = weights / weights.sum()
            # Step 4: Compute weighted mean
            weighted_mean = (ds * normalized_weights).sum(dim=ens_dim, skipna=False)

            # Step 5: Compute weighted standard deviation
            # First get weighted mean (without broadcasting issues)
            broadcast_mean = weighted_mean.expand_dims({ens_dim: ds.dims[ens_dim]}).transpose(
                *ds.dims
            )

            # Compute variance: E[(x - μ)^2]
            weighted_var = (((ds - broadcast_mean) ** 2) * normalized_weights).sum(
                dim=ens_dim, skipna=False
            )

            weighted_std = np.sqrt(weighted_var)

            weighted_mean.attrs["normalized weights"] = normalized_weights
            weighted_std.attrs["normalized weights"] = normalized_weights
            weighted_mean.attrs["description"] = "Weighted mean"
            weighted_std.attrs["description"] = "Weighted standard deviation"
            return weighted_mean[variable], weighted_std[variable]
    else:
        # in case ds is not None and ds.model is not given
        logger.info(
            "Given dataset in compute_statistics is Single-model ensemble and model name not given"
        )
        ds_mean = ds[variable].mean(dim=ens_dim, skipna=False)
        ds_std = ds[variable].mean(dim=ens_dim, skipna=False)
        return ds_mean, ds_std


def center_timestamp(time: pd.Timestamp, freq: str):
    """
    Center the time value at the center of the month or year

    Args:
        time (str): The time value
        freq (str): The frequency of the time period (only 'monthly' or 'annual')

    Returns:
        pd.Timestamp: The centered time value

    Raises:
        ValueError: If the frequency is not supported
    """
    if freq == "monthly":
        center_time = time + pd.DateOffset(days=15)
    elif freq == "annual":
        center_time = time + pd.DateOffset(months=6)
    else:
        raise ValueError(f"Frequency {freq} not supported")

    return center_time
