"""
Utility functions for the ensemble class
"""

import gc
import os
from collections import Counter

import numpy as np
import pandas as pd
import xarray as xr
from aqua import Reader
from aqua.exceptions import NoDataError
from aqua.logger import log_configure
from aqua.configurer import ConfigPath

def reader_retrieve_and_merge(
    variable: str = None,
    ens_dim: str = "ensemble",
    catalog_list: list[str] = None,
    model_list: list[str] = None,
    exp_list: list[str] = None,
    source_list: list[str] = None,
    reader_kwargs: dict[str, list[str]] = None,
    realization: dict[str, list[str]] = None,
    region: str = None,
    lon_limits: float = None,
    lat_limits: float = None,
    startdate: str = None,
    enddate: str = None,
    regrid: str = None,
    areas: bool = False,
    fix: bool = False,
    loglevel: str = "WARNING",
):
    """
    Retrieve, merge, and slice datasets from multiple models, experiments, and sources.

    This function uses the AQUA Reader class to load data for a specified variable
    from multiple catalogs, models, experiments, and sources. Individual realizations
    are loaded, optionally subset by spatial (lon/lat) or temporal (start/end date)
    constraints, and concatenated along a specified ensemble dimension. The final
    merged dataset contains all requested ensemble members with appropriate metadata.

    Args:
        variable (str, optional): Name of the variable to retrieve. Defaults to None.
        ens_dim (str, optional): Name of the ensemble dimension for concatenation. Defaults to "ensemble".
        catalog_list (list[str], optional): List of AQUA catalogs to retrieve data from. Defaults to None.
        model_list (list[str], optional): List of models corresponding to catalogs and experiments. Defaults to None.
        exp_list (list[str], optional): List of experiments corresponding to models and sources. Defaults to None.
        source_list (list[str], optional): List of sources corresponding to models and experiments. Defaults to None.
        realization (dict[str, list[str]], optional): Dictionary specifying realizations per model. Defaults to None.
        region (str, optional): Region for zonal or spatial selections. Defaults to None.
        lon_limits (float, optional): Longitude limits for spatial subsetting. Defaults to None.
        lat_limits (float, optional): Latitude limits for spatial subsetting. Defaults to None.
        startdate (str, optional): Start date for temporal subsetting. Defaults to None.
        enddate (str, optional): End date for temporal subsetting. Defaults to None.
        regrid (str, optional): Grid to reproject data onto. Defaults to None.
        areas (bool, optional): Whether to calculate area-weighted values. Defaults to False.
        fix (bool, optional): Apply data fixes if necessary. Defaults to False.
        loglevel (str, optional): Logging level for messages. Defaults to "WARNING".

    Returns:
        xarray.Dataset: Merged dataset containing all requested ensemble members,
        concatenated along `ens_dim` with metadata including description, variable,
        and ensemble member labels.

    Raises:
        RuntimeError: If no datasets are successfully retrieved from AQUA Reader.

    Notes:
        - If all catalog_list, model_list, exp_list, and source_list are None or empty,
          the function returns None.
        - Handles missing or default realizations by using ["r1"].
        - Automatically frees memory after processing individual datasets.

    TODO:
        - Add support for additional spatial selections beyond lon/lat slices.
        - Improve error handling and reporting for missing datasets.
        - Add option to automatically regrid or interpolate datasets.
        - Include caching mechanism to avoid repeated reads from the same catalog/model/exp/source.
    """
    logger = log_configure(log_name="reader_retrieve_and_merge", log_level=loglevel)
    logger.info("Loading and merging the ensemble dataset using the Reader class")

    if all(not v for v in [catalog_list, model_list, exp_list, source_list]):
        logger.warning("All of catalog, model, exp, and source are None or empty. Exiting reader_retrieve_and_merge.")
        return None
    # Ensure consistent list types
    if isinstance(catalog_list, str):
        catalog_list = [catalog_list]
    if isinstance(model_list, str):
        model_list = [model_list]
    if isinstance(exp_list, str):
        exp_list = [exp_list]
    if isinstance(source_list, str):
        source_list = [source_list]

    all_datasets = []
    # Loop through each (catalog, model, exp, source) combination
    for cat_i, model_i, exp_i, src_i in zip(catalog_list, model_list, exp_list, source_list):
        logger.info(f"Processing: catalog={cat_i}, model={model_i}, exp={exp_i}, source={src_i}")

        # Get realizations and set default to ['r1'] if not provided
        if realization is not None:
            reals = realization.get(model_i)
            if reals is None:
                logger.info(f"No realizations defined for {model_i}, using default ['r1']")
                reals = ["r1"]
        else:
            logger.info(f"No realizations defined for {model_i}, using default ['r1']")
            reals = ["r1"]
            
        model_ds_list = []

        for r in reals:
            try:
                # Retrieve the data using AQUA Reader
                reader = Reader(
                    catalog=cat_i,
                    model=model_i,
                    exp=exp_i,
                    source=src_i,
                    realization=r,
                    region=region,
                    regrid=regrid,
                    #reader_kwargs=reader_kwargs[model_i],
                    areas=areas,
                    fix=fix,
                )

                ds = reader.retrieve(var=variable)
                logger.info(f"Loaded {variable} for {model_i}, {exp_i}, realization={r}")
                # Spatial selection
                if lon_limits and lat_limits:
                    if "lon" in ds.dims and "lat" in ds.dims:
                        ds = ds.sel(lon=slice(*lon_limits), lat=slice(*lat_limits))
                    else:
                        logger.debug(f"Dataset for {model_i}-{r} has no lon/lat dims, skipping spatial subset.")

                # Temporal selection (only if time dimension exists)
                if "time" in ds.dims and (startdate or enddate):
                    ds = ds.sel(time=slice(startdate, enddate))
                elif "time" not in ds.dims and (startdate or enddate):
                    logger.debug(f"Dataset for {model_i}-{r} has no time dimension.")

                # Add ensemble label
                ens_label = f"{model_i}_{exp_i}_{r}"
                ds = ds.expand_dims({ens_dim: [ens_label]})

                model_ds_list.append(ds)

            except Exception as e:
                logger.warning(f"Skipping {model_i}-{exp_i}-{r} due to error: {e}")
                continue

        if not model_ds_list:
            logger.warning(f"No realizations loaded for {model_i} ({exp_i}). Skipping...")
            continue

        # Concatenate realizations for this model
        model_ens = xr.concat(model_ds_list, dim=ens_dim, combine_attrs="override")
        all_datasets.append(model_ens)

        # Free up memory from individual realizations
        for ds in model_ds_list:
            ds.close() if hasattr(ds, "close") else None
        del model_ds_list
        del model_ens
        gc.collect()

    # Merge across all models
    if not all_datasets:
        raise RuntimeError("No datasets successfully retrieved from AQUA Reader.")

    merged_dataset = xr.concat(all_datasets, dim=ens_dim, combine_attrs="override")
    logger.info(f"Merged {len(merged_dataset[ens_dim])} ensemble members total.")

    # Adding metadata
    merged_dataset.attrs.update(
        {
            "description": "Merged data for AQUA ensemble diagnostics across models, experiments, and realizations.",
            "variable": variable,
            "ensemble_members": list(merged_dataset[ens_dim].values),
        }
    )

    for ds in all_datasets:
        ds.close() if hasattr(ds, "close") else None
    del all_datasets
    gc.collect()
    logger.info("Memory successfully freed.")

    return merged_dataset


def merge_from_data_files(
    variable: str = None,
    ens_dim: str = "ensemble",
    model_names: list[str] = None,
    data_path_list: list[str] = None,
    # region: str = None,
    # lon_limits: list[float] = None,
    # lat_limits: list[float] = None,
    startdate: str = None,
    enddate: str = None,
    loglevel: str = "WARNING",
):
    """
    Merge ensemble NetCDF files along the ensemble dimension with optional temporal selection.

    This function loads NetCDF files from the given paths, assigns an ensemble dimension,
    optionally subsets the data by start and end dates, and concatenates the datasets
    into a single xarray.Dataset along `ens_dim`. Model names are assigned to each ensemble member
    for metadata tracking.

    Args:
        variable (str, optional): Name of the variable to merge. Defaults to None.
        ens_dim (str, optional): Name of the ensemble dimension. Defaults to "ensemble".
        model_names (list[str], optional): List of model names. Must correspond to the sequence of files
            in `data_path_list`. If multiple realizations exist for a model, repeat model names accordingly.
        data_path_list (list[str], optional): List of file paths to NetCDF datasets. Mandatory.
        startdate (str, optional): Start date for temporal subsetting (YYYY-MM-DD). Defaults to None.
        enddate (str, optional): End date for temporal subsetting (YYYY-MM-DD). Defaults to None.
        loglevel (str, optional): Logging level. Defaults to "WARNING".

    Returns:
        xarray.Dataset: Merged dataset concatenated along `ens_dim`, with model names in metadata.
        If the dataset has a time dimension, the data is sliced according to startdate and enddate.

    TODO:
        - Add support for spatial subsetting via `region`, `lon_limits`, and `lat_limits`.
        - Handle datasets with multiple variables more flexibly.
        - Include additional metadata about sources or experiments if available.
        - Optimize memory usage for large numbers of NetCDF files.
    """
    logger = log_configure(log_name="merge_from_data_files", log_level=loglevel)
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
        ens_dataset = xr.concat(tmp_dataset_list, dim=ens_dim, combine_attrs="override")
        ens_dataset = ens_dataset.assign_coords(model=(ens_dim, model_names))
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
        ens_dataset.attrs["model"] = model_names
        return ens_dataset.sel(time=slice(common_startdate, common_enddate))
    else:
        # the ensemble dataset is not a timeseries
        logger.info("Finished loading the ensemble datasets")
        ens_dataset.attrs["description"] = f"Dataset merged along {ens_dim} for ensemble statistics"
        ens_dataset.attrs["model"] = model_names
        return ens_dataset

# This function is mainly for testing purposes. Not-tested
def load_premerged_ensemble_dataset(ds: xr.Dataset, ens_dim: str = "ensemble", loglevel: str = "WARNING"):
    """
    Prepares a pre-merged xarray dataset for statistical computation.
    Ensures correct ensemble dimension and model labeling.

    Args:
        ds (xr.Dataset): Pre-merged dataset.
        ens_dim (str): Name of the ensemble dimension.
        loglevel (str): Logging level.

    Returns:
        xr.Dataset: Prepared dataset ready for compute_statistics.
    """

    logger = log_configure(log_name="load_premerged_ensemble_dataset", log_level=loglevel)
    logger.info("Loading and merging the ensemble dataset by reading files")

    if ds is None:
        logger.warning("No dataset provided to load_premerged_ensemble_dataset")
        return None

    # Check ensemble dimension
    if ens_dim not in ds.dims:
        logger.info(f"Adding '{ens_dim}' dimension as it does not exist")
        # Expand dataset along ensemble dimension
        ds = ds.expand_dims({ens_dim: [0]})

    # Check for model coordinate
    if "model" not in ds.coords:
        logger.info("No 'model' coordinate found. Assuming single-model ensemble")
        ds = ds.assign_coords(model=("ensemble", ["single_model"] * ds.dims[ens_dim]))

    else:
        # Ensure model coordinate is same length as ensemble dimension
        if len(ds["model"]) != ds.dims[ens_dim]:
            logger.warning(f"'model' coordinate length {len(ds['model'])} != ensemble size {ds.dims[ens_dim]}. Adjusting...")
            # Repeat or truncate model labels as needed
            repeat_factor = ds.dims[ens_dim] // len(ds["model"])
            remainder = ds.dims[ens_dim] % len(ds["model"])
            new_model = list(ds["model"].values) * repeat_factor + list(ds["model"].values)[:remainder]
            ds = ds.assign_coords(model=("ensemble", new_model))

    # Optional: sort ensemble members by model label
    logger.info("Sorting ensemble members by model label")
    sorted_indices = np.argsort(ds["model"].values)
    ds = ds.isel({ens_dim: sorted_indices})

    # Clean memory
    gc.collect()

    return ds

def compute_statistics(variable: str = None, ds: xr.Dataset = None, ens_dim: str = "ensemble", loglevel="WARNING"):
    """
    Compute mean and standard deviation (POINT-WISE for timeseries) for single- and multi-model ensembles.

    - Single-model: computes unweighted mean and standard deviation along `ens_dim`.
    - Multi-model: computes weighted mean and standard deviation based on the number
      of realizations per model.

    Args:
        variable (str): Name of the variable to compute statistics for.
        ds (xr.Dataset): xarray.Dataset containing ensemble data along `ens_dim`.
        ens_dim (str, optional): Name of the ensemble dimension. Defaults to "ensemble".
        loglevel (str, optional): Logging level. Defaults to "WARNING".

    Returns:
        tuple:
            Single-model: (ds_mean, ds_std)
            Multi-model: (weighted_mean, weighted_std)

    Raises:
        NoDataError: If `ds` is None.

    Notes:
        - Point-wise STD for ensemble timeseries.
        - The function detects multi-model ensembles via the presence of a 'model' coordinate.
        - Weighted statistics normalize contributions by the number of realizations per model.
        - Attributes 'description' are added to weighted statistics for clarity.

    TODO:
        - Add support for additional statistics (median, percentile).
        - Allow optional masking of NaN values across models before computing statistics.
        - Optimize memory usage for very large ensembles.
        - Include option to return a combined dataset with both mean and std.
    """
    logger = log_configure(log_name="compute_statistics", log_level=loglevel)
    logger.info("Computing statistics of the ensemble dataset")

    if ds is None:
        raise NoDataError("No data is given to compute_statistics")

    # Case 1: dataset has 'model' coordinate
    if "model" in ds.coords:
        unique_models = np.unique(ds["model"].values)
        if len(unique_models) <= 1:
            logger.info("Single-model ensemble detected")
            # unweighted mean and std
            ds_mean = ds[variable].mean(dim=ens_dim, skipna=False, keep_attrs=True)
            ds_std = ds[variable].std(dim=ens_dim, skipna=False, keep_attrs=True)
            return ds_mean, ds_std
        else:
            logger.info("Multi-model ensemble detected")
            # Weighted mean/std based on realizations
            # Step 1: compute number of realizations per model in the dataset
            model_counts = {model: np.sum(ds["model"].values == model) for model in unique_models}

            # Step 2: assign weight for each ensemble member
            weights = xr.DataArray([model_counts[m] for m in ds["model"].values], dims=ens_dim, coords={ens_dim: ds[ens_dim]})

            # Step 3: normalize weights
            normalized_weights = weights / weights.sum()

            # Step 4: compute weighted mean
            weighted_mean = (ds[variable] * normalized_weights).sum(dim=ens_dim, skipna=False, keep_attrs=True)

            # Step 5: compute weighted std
            broadcast_mean = weighted_mean.expand_dims({ens_dim: ds.dims[ens_dim]}).transpose(*ds[variable].dims)
            weighted_var = (((ds[variable] - broadcast_mean) ** 2) * normalized_weights).sum(
                dim=ens_dim, skipna=False, keep_attrs=True
            )
            weighted_std = np.sqrt(weighted_var)

            weighted_mean.attrs.update(
                {
                    "description": "Weighted mean based on actual model realizations",
                }
            )
            weighted_std.attrs.update(
                {
                    "description": "Weighted std based on actual model realizations",
                }
            )

            return weighted_mean, weighted_std

    else:
        # Case 2: no model coordinate, assume single-model ensemble
        logger.info("Single-model ensemble detected (no 'model' coordinate)")
        ds_mean = ds[variable].mean(dim=ens_dim, skipna=False, keep_attrs=True)
        ds_std = ds[variable].std(dim=ens_dim, skipna=False, keep_attrs=True)
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

def extract_realizations(catalog, model, exp, source):
    """Extract the realizations available for a given catalog, model, exp and source.

    Args:
        catalog (str): Intake catalog name.
        model (str): Model name.
        exp (str): Experiment name.
        source (str): Source name.

    Returns:
        list: List of available realizations.
    """
    configurer = ConfigPath(catalog=catalog, loglevel='WARNING')
    cat, catalog_file, machine_file = configurer.deliver_intake_catalog(
        catalog=catalog, model=model, exp=exp, source=source)

    expcat = cat()[model][exp]
    esmcat = expcat[source].describe().get('user_parameters', {})

    for parameter in esmcat:
        name = parameter.get('name')

        if name == 'realization':
            realization = parameter.get('allowed')
            return realization
    return None


