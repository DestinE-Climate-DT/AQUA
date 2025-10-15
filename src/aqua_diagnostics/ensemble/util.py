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

def reader_retrieve_and_merge(
    variable: str = None,
    ens_dim: str = "ensemble",
    catalog_list: list[str] = None,
    model_list: list[str] = None,
    exp_list: list[str] = None,
    source_list: list[str] = None,
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
    Retrieves, merges, and slices datasets based on specified models, experiments,
    sources, and time boundaries.

    This function reads data for a given variable (`variable`) from multiple models, experiments,
    and sources, combines the datasets along the specified "ensemble" dimension along with their indices, and slices
    the merged dataset to the given start and end dates. The ens_dim can given any customized name for the ensemble dimension.

    This function loads the data via the AQUA Reader class

    Args:
        variable (str): The variable to retrieve data. Defaults to None.
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
        # NOTE: here one can pass a dictionay of realizations of multiple models. Default is 'None'.
        realization: dict[str, list[str]] = None, 
        Specific to the timeseries datasets:
            startdate (str or datetime): The start date for slicing the merged dataset.
                If None is provided, the ensemble members are merged w.r.t to their time-interval. Defaults to None.
            enddate (str or datetime): The end date for slicing the merged dataset.
                If None is provided, the ensemble members are merged w.r.t to their time-interval. Defaults to None.
    Returns:
            xarray.Dataset: The merged dataset containing data from all specified models,
                experiments, and sources, concatenated along `ens_dim` along with model name.
    """
    logger = log_configure(log_name="reader_retrieve_and_merge", log_level=loglevel)
    logger.info("Loading and merging the ensemble dataset using the Reader class")
   
    if all(not v for v in [catalog_list, model_list, exp_list, source_list]):
        logger.warning("All of catalog, model, exp, and source are None or empty. Exiting merge_from_data_files.")
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
                reals = ['r1']
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
        model_ens = xr.concat(model_ds_list, dim=ens_dim)
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

    
    merged_dataset = xr.concat(all_datasets, dim=ens_dim)
    logger.info(f"Merged {len(merged_dataset[ens_dim])} ensemble members total.")

    # Adding metadata
    merged_dataset.attrs.update({
        "description": "Merged data for AQUA ensemble diagnostics across models, experiments, and realizations.",
        "variable": variable,
        "ensemble_members": list(merged_dataset[ens_dim].values),
    })

    for ds in all_datasets:
        ds.close() if hasattr(ds, "close") else None
    del all_datasets
    gc.collect()
    logger.info("Memory successfully freed.")

    return merged_dataset

def merge_from_data_files(
    variable: str = None,
    ens_dim: str = "ensemble",
    data_path_list: list[str] = None,
    model_list: list[str] = None,
    realization: dict[str, list[str]] = None,
    region: str = None,
    lon_limits: list[float] = None,
    lat_limits: list[float] = None,
    startdate: str = None,
    enddate: str = None,
    loglevel: str = "WARNING",
):
    """
    Merge ensemble NetCDF files along the ensemble dimension with optional
    spatial and temporal selection.

    Parameters
    ----------
    variable (str) : Name of the variable to merge.
    ens_dim (str) : Name of the ensemble dimension. Default to 'ensemble'.
    model_names (list[str]) : List of model names. Mandatory to provide model_names.
    data_path_list (list[str]) : List of directories containing NetCDF files. Mandatory.
    realization (dict[str, list[str]]) : Realizations to include for each model.
    region (str) : Optional named region.
    lon_limits (list[float]) : Longitude limits [min_lon, max_lon].
    lat_lim (list[float]) : Latitude limits [min_lat, max_lat].
    startdate (str) : Start date for temporal subsetting (YYYY-MM-DD).
    enddate (str) : End date for temporal subsetting (YYYY-MM-DD).
    loglevel (str) : Logging level. Default is 'WARNING'.

    Returns
    -------
    xarray.Dataset
        Merged dataset along ensemble dimension.
    """

    logger = log_configure(log_name="merge_from_data_files", log_level=loglevel)
    logger.info("Loading and merging the ensemble dataset by reading files")

    if not all([data_path_list, model_list]):
        logger.warning("data_path_list and model_list must be provided.")
        return None
    
    all_datasets = []

    for model in model_list:
        logger.info(f"Processing model: {model}")

        # Determine realizations (default = ['r1'])
        reals = ["r1"] if realization is None else realization.get(model, ["r1"])
        model_ds_list = []

        for r in reals:
            # Attempt to find files for this model-realization
            files_for_real = []
            for path in data_path_list:
                pattern = f"{model}_{r}_{variable}.nc"
                file_path = os.path.join(path, pattern)
                if os.path.exists(file_path):
                    files_for_real.append(file_path)

            if not files_for_real:
                logger.warning(f"No files found for {model}-{r}-{variable}. Skipping.")
                continue

            # Open each file using xarray.open_dataset
            for file in files_for_real:
                try:
                    ds = xr.open_dataset(file)[variable]
                    logger.info(f"Loaded {variable} from {file}")

                    # Spatial selection
                    if lon_limits and lat_limits:
                        if "lon" in ds.dims and "lat" in ds.dims:
                            ds = ds.sel(lon=slice(*lon_limits), lat=slice(*lat_limits))

                    # Temporal selection
                    if "time" in ds.dims and (startdate or enddate):
                        ds = ds.sel(time=slice(startdate, enddate))

                    # Add ensemble label
                    ens_label = f"{model}_{r}"
                    ds = ds.expand_dims({ens_dim: [ens_label]})

                    model_ds_list.append(ds)

                except Exception as e:
                    logger.warning(f"Skipping file {file} due to error: {e}")
                    continue

            if not model_ds_list:
                logger.warning(f"No realizations loaded for model {model}. Skipping.")
                continue

    # Concatenate realizations for this model
    model_ens = xr.concat(model_ds_list, dim=ens_dim)
    all_datasets.append(model_ens)

    # Free memory
    for ds in model_ds_list:
        ds.close() if hasattr(ds, "close") else None
    del model_ds_list
    del model_ens
    gc.collect()

    # Merge across models
    if not all_datasets:
        raise RuntimeError("No datasets successfully loaded.")

    merged_dataset = xr.concat(all_datasets, dim=ens_dim)
    log.info(f"Merged {len(merged_dataset[ens_dim])} ensemble members total.")

    # Adding metadata
    merged_dataset.attrs.update({
        "description": "Merged NetCDF datasets across models and realizations.",
        "variable": variable,
        "ensemble_members": list(merged_dataset[ens_dim].values),
    })

    for ds in all_datasets:
        ds.close() if hasattr(ds, "close") else None
    del all_datasets
    gc.collect()
    logger.info("Memory successfully freed.")

    return merged_dataset

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
    Compute mean and standard deviation for single- and multi-model ensembles.
    - Single-model: unweighted mean/std.
    - Multi-model: weighted mean/std based on actual number of realizations per model.

    Args:
        variable (str): Variable name.
        ds (xr.Dataset): xarray.Dataset merged along ensemble dimension.
        ens_dim (str): Name of ensemble dimension.
        loglevel (str): Logging level.

    Returns:
        Single-model: ds_mean, ds_std
        Multi-model: weighted_mean, weighted_std
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
            ds_mean = ds[variable].mean(dim=ens_dim, skipna=False)
            ds_std = ds[variable].std(dim=ens_dim, skipna=False)
            return ds_mean, ds_std
        else:
            logger.info("Multi-model ensemble detected")
            # Weighted mean/std based on realizations
            # Step 1: compute number of realizations per model in the dataset
            model_counts = {model: np.sum(ds["model"].values == model) for model in unique_models}

            # Step 2: assign weight for each ensemble member
            weights = xr.DataArray(
                [model_counts[m] for m in ds["model"].values],
                dims=ens_dim,
                coords={ens_dim: ds[ens_dim]}
            )

            # Step 3: normalize weights
            normalized_weights = weights / weights.sum()

            # Step 4: compute weighted mean
            weighted_mean = (ds[variable] * normalized_weights).sum(dim=ens_dim, skipna=False)

            # Step 5: compute weighted std
            broadcast_mean = weighted_mean.expand_dims({ens_dim: ds.dims[ens_dim]}).transpose(*ds[variable].dims)
            weighted_var = (((ds[variable] - broadcast_mean) ** 2) * normalized_weights).sum(dim=ens_dim, skipna=False)
            weighted_std = np.sqrt(weighted_var)

            weighted_mean.attrs.update({
                "description": "Weighted mean based on actual model realizations",
            })
            weighted_std.attrs.update({
                "description": "Weighted std based on actual model realizations",
            })

            return weighted_mean, weighted_std

    else:
        # Case 2: no model coordinate, assume single-model ensemble
        logger.info("Single-model ensemble detected (no 'model' coordinate)")
        ds_mean = ds[variable].mean(dim=ens_dim, skipna=False)
        ds_std = ds[variable].std(dim=ens_dim, skipna=False)
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
