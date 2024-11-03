"""A module for atmospheric global biases analysis and visualization."""

import os
import xarray as xr
import numpy as np
import scipy.stats as stats
import matplotlib.pylab as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.gridspec as gridspec
from cartopy.mpl.ticker import LongitudeFormatter, LatitudeFormatter
import pandas as pd
import datetime
import warnings
from aqua.exceptions import NoDataError
from aqua.util import create_folder, add_cyclic_lon
from aqua.util import evaluate_colorbar_limits, ticks_round
from aqua.logger import log_configure
from aqua.util import OutputSaver

# set default options for xarray
xr.set_options(keep_attrs=True)

# turn off warnings
warnings.filterwarnings("ignore")


def seasonal_bias(dataset1=None, dataset2=None, var_name=None,
                  plev=None, statistic="mean",
                  model=None, exp=None, model_2=None, exp_2=None,
                  start_date1=None, end_date1=None,
                  start_date2=None, end_date2=None,
                  path_to_output=None,
                  dataset2_precomputed=None,
                  loglevel='WARNING', seasons=True, **kwargs):
    '''
    Plot the seasonal bias maps between two datasets for specific variable and time ranges.

    Args:
        dataset1 (xarray.Dataset): The first dataset.
        dataset2 (xarray.Dataset): The second dataset, that will be compared to the first dataset (reference dataset).
        var_name (str): The name of the variable to compare (e.g., '2t', 'tprate', 'mtntrf', 'mtnsrf', ...).
        plev (float or None): The desired pressure level in Pa. If None, the variable is assumed to be at surface level.
        statistic (str): The desired statistic to calculate for each season.
                         Valid options are: 'mean', 'max', 'min', 'diff', and 'std'. The default statistic is 'mean'.
        model (str): The name of the model for the first dataset.
        exp (str): The name of the experiment for the first dataset.
        model_2 (str): The name of the model for the second dataset.
        exp_2 (str): The name of the experiment for the second dataset.
        start_date1 (str): The start date of the time range for dataset1 in 'YYYY-MM-DD' format.
        end_date1 (str): The end date of the time range for dataset1 in 'YYYY-MM-DD' format.
        start_date2 (str): The start date of the time range for dataset2 in 'YYYY-MM-DD' format.
        end_date2 (str): The end date of the time range for dataset2 in 'YYYY-MM-DD' format.
        path_to_output (str): The directory to save the output files.
        dataset2_precomputed (xarray.Dataset or None): Pre-computed climatology for dataset2.
        loglevel (str): The desired level of logging.
        seasons (bool): If True, plot bias maps for individual seasons. If False, only plot climatological bias.

    Keyword Args:
        nlevels (int): The number of levels for the colorbar. Default is 12.
        vmin (float): The minimum value for the colorbar. Default is None.
        vmax (float): The maximum value for the colorbar. Default is None.

    Raises:
        ValueError: If an invalid statistic is provided.
        NoDataError: If variable or level is not available in datasets

    Returns:
        A seasonal bias plot.
    '''
    logger = log_configure(log_level=loglevel, log_name='Seasonal Bias')

    if not isinstance(dataset1, xr.Dataset) or not isinstance(dataset2, xr.Dataset):
        raise NoDataError("The provided datasets are not valid. Please try again.")

    try:
        var1 = dataset1[var_name]
        var2 = dataset2[var_name]
    except KeyError:
        raise NoDataError(f"The variable {var_name} is not present in the dataset. Please try again.")

    if var_name == 'tprate' or var_name == 'mtpr':
        logger.warning(f"Adjusting {var_name} to be in mm/day")
        var1 = var1 * 86400
        var2 = var2 * 86400
        logger.warning(f"Changing {var_name} units attribute to 'mm/day'")
        var1.attrs['units'] = 'mm/day'
        var2.attrs['units'] = 'mm/day'

    if start_date1 or end_date1:
        logger.debug(f"start_date1: {start_date1}")
        logger.debug(f"end_date1: {end_date1}")
        var1 = var1.sel(time=slice(start_date1, end_date1))
    else:
        start_date1 = str(var1["time.year"][0].values) + '-' + str(var1["time.month"][0].values) + '-' + str(var1["time.day"][0].values)
        end_date1 = str(var1["time.year"][-1].values) + '-' + str(var1["time.month"][-1].values) + '-' + str(var1["time.day"][-1].values)
    if start_date2 or end_date2:
        logger.debug(f"start_date2: {start_date2}")
        logger.debug(f"end_date2: {end_date2}")
        var2 = var2.sel(time=slice(start_date2, end_date2))
    else:
        start_date2 = str(var2["time.year"][0].values) + '-' + str(var2["time.month"][0].values) + '-' + str(var2["time.day"][0].values)
        end_date2 = str(var2["time.year"][-1].values) + '-' + str(var2["time.month"][-1].values) + '-' + str(var2["time.day"][-1].values)

    # Load in memory to speed up the calculation
    logger.info("Loading data into memory to speed up the calculation...")
    var1 = var1.load()

    # Check if pre-computed climatology is provided, otherwise compute it
    if dataset2_precomputed is None:
        # Compute climatology
        var2 = var2.load()
        var2_climatology = var2.groupby('time.month').mean(dim='time')
    else:
        logger.debug("Precomputed climatology found")
        if isinstance(dataset2_precomputed, xr.Dataset):
            var2_climatology = dataset2_precomputed[var_name]
        elif isinstance(dataset2_precomputed, xr.DataArray):
            var2_climatology = dataset2_precomputed
        else:
            logger.error("Error in precomputer climatology, recomputing")
            # Compute climatology
            var2 = var2.load()
            var2_climatology = var2.groupby('time.month').mean(dim='time')

    var1_climatology = var1.groupby('time.month').mean(dim='time')

    bias_time_range_mean = var1_climatology.mean(dim='month') - var2_climatology.mean(dim='month')

    # Select the desired pressure level if provided
    if 'plev' in var1_climatology.dims:
        if plev:
            try:
                var1_climatology = var1_climatology.sel(plev=plev)
            except KeyError:
                raise NoDataError("The provided value of pressure level is absent in the dataset. Please try again.")
        else:
            raise NoDataError(f"The dataset for {var_name} variable has a 'plev' coordinate, but None is provided. Please try again.")
    else:
        logger.debug(f"The dataset does not have a 'plev' coordinate for {var_name} variable.")

    if 'plev' in var2_climatology.dims:
        if plev:
            try:
                var2_climatology = var2_climatology.sel(plev=plev)
            except KeyError:
                raise NoDataError("The provided value of pressure level is absent in the dataset. Please try again.")
        else:
            raise NoDataError(f"The dataset for {var_name} variable has a 'plev' coordinate, but None is provided. Please try again.")
    else:
        logger.debug(f"The dataset does not have a 'plev' coordinate for {var_name} variable.")

    # Calculate the desired statistic for each season
    season_ranges = {'DJF': [12, 1, 2], 'MAM': [3, 4, 5], 'JJA': [6, 7, 8], 'SON': [9, 10, 11]}
    results = []

    for season, months in season_ranges.items():
        var1_season = var1_climatology.sel(month=months)
        var2_season = var2_climatology.sel(month=months)

        if statistic == 'mean':
            result_season = var1_season.mean(dim='month') - var2_season.mean(dim='month')
        elif statistic == 'max':
            result_season = var1_season.max(dim='month') - var2_season.max(dim='month')
        elif statistic == 'min':
            result_season = var1_season.min(dim='month') - var2_season.min(dim='month')
        elif statistic == 'diff':
            result_season = var1_season - var2_season
        elif statistic == 'std':
            result_season = var1_season.std(dim='month') - var2_season.std(dim='month')
        else:
            raise ValueError("Invalid statistic. Please choose one of 'mean', 'std', 'max', 'min', or 'diff'.")

        results.append(result_season)

    # Create a cartopy projection
    projection = ccrs.PlateCarree()
    
    if seasons:
        # Plot bias maps for individual seasons
        fig = plt.figure(figsize=(15, 15))
    else:
        # Adjust figure size for climatological bias plot only
        fig = plt.figure(figsize=(10, 6))
        
    # Create a list to store the plotted objects
    cnplots = []
    # Calculate the number of rows and columns based on the seasons
    if seasons:
        num_rows = 3
        num_cols = 2
    else:
        num_rows = 1
        num_cols = 1

    # Set the colorbar limits
    vmin, vmax = evaluate_colorbar_limits(results, sym=True)
    nlevels = kwargs.get('nlevels', 12)
    vmin = kwargs.get('vmin', vmin) if kwargs.get('vmin', vmin) is not None else vmin
    vmax = kwargs.get('vmax', vmax) if kwargs.get('vmax', vmax) is not None else vmax
    logger.debug(f"vmin: {vmin}, vmax: {vmax}")
    levels = np.linspace(vmin, vmax, nlevels)

    gs = gridspec.GridSpec(num_rows, num_cols, figure=fig)

    # Plot the whole time range mean bias in the first row
    ax = fig.add_subplot(gs[0, :], projection=projection) 
    # Add coastlines to the plot
    ax.coastlines()
    # Add other cartographic features (optional)
    ax.add_feature(cfeature.LAND, facecolor='lightgray')
    ax.add_feature(cfeature.OCEAN, facecolor='lightblue')
    # Set latitude and longitude tick labels
    #ax.set_xticks(np.arange(-180, 181, 60), crs=projection)
    #ax.set_yticks(np.arange(-90, 91, 30), crs=projection)
    ax.xaxis.set_major_formatter(LongitudeFormatter())
    ax.yaxis.set_major_formatter(LatitudeFormatter())
    ax.gridlines(draw_labels=True)

    # Plot the whole time range mean bias data
    try:
        bias_time_range_mean = add_cyclic_lon(bias_time_range_mean)
    except Exception as e:
        logger.debug(f"Error: {e}")
        logger.warning(f"Cannot add cyclic longitude for {var_name} variable.")
    else:
        cnplot = bias_time_range_mean.plot.contourf(ax=ax, cmap='RdBu_r', levels=levels, extend='both',
                                                    add_colorbar=False)
        cnplots.append(cnplot)

    if seasons:
        ax.set_title("Annual Mean Bias",  y=1.1)
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    
    if seasons:
        
        # Plot the DJF and MAM in the second subplot
        for i, (result, season) in enumerate(zip(results[:2], season_ranges.keys())):
            ax = fig.add_subplot(gs[1, i], projection=projection)
            # Add coastlines to the plot
            ax.coastlines()
            # Add other cartographic features (optional)
            ax.add_feature(cfeature.LAND, facecolor='lightgray')
            ax.add_feature(cfeature.OCEAN, facecolor='lightblue')
            # Set latitude and longitude tick labels
            #ax.set_xticks(np.arange(-180, 181, 60), crs=projection)
            #ax.set_yticks(np.arange(-90, 91, 30), crs=projection)
            ax.xaxis.set_major_formatter(LongitudeFormatter())
            ax.yaxis.set_major_formatter(LatitudeFormatter())
            ax.gridlines(draw_labels=True)

            # Plot the bias data using the corresponding cnplot object
            try:
                result = add_cyclic_lon(result)
            except Exception as e:
                logger.debug(f"Error: {e}")
                logger.warning(f"Cannot add cyclic longitude for {var_name} variable.")
                continue

            cnplot = result.plot.contourf(ax=ax, cmap='RdBu_r', levels=levels, extend='both',
                                        add_colorbar=False)
            cnplots.append(cnplot)

            ax.set_title(f'{season}', y=1.1)
            ax.set_xlabel('Longitude')
            ax.set_ylabel('Latitude')

        # Plot the SON and JJA in the third subplot
        for i, (result, season) in enumerate(zip(results[2:], list(season_ranges.keys())[2:])):
            ax = fig.add_subplot(gs[2, i], projection=projection)
            # Add coastlines to the plot
            ax.coastlines()
            # Add other cartographic features (optional)
            ax.add_feature(cfeature.LAND, facecolor='lightgray')
            ax.add_feature(cfeature.OCEAN, facecolor='lightblue')
            # Set latitude and longitude tick labels
            #ax.set_xticks(np.arange(-180, 181, 60), crs=projection)
            #ax.set_yticks(np.arange(-90, 91, 30), crs=projection)
            ax.xaxis.set_major_formatter(LongitudeFormatter())
            ax.yaxis.set_major_formatter(LatitudeFormatter())
            ax.gridlines(draw_labels=True)

            # Plot the bias data using the corresponding cnplot object
            try:
                result = add_cyclic_lon(result)
            except Exception as e:
                logger.debug(f"Error: {e}")
                logger.warning(f"Cannot add cyclic longitude for {var_name} variable.")
                continue

            cnplot = result.plot.contourf(ax=ax, cmap='RdBu_r', levels=levels, extend='both',
                                        add_colorbar=False)
            cnplots.append(cnplot)

            ax.set_title(f'{season}', y=1.1)
            ax.set_xlabel('Longitude')
            ax.set_ylabel('Latitude')

    # Add a colorbar
    if seasons:
        cbar_ax = fig.add_axes([0.25, 0.033, 0.5, 0.02])  # Adjust the position and size of the colorbar
    else:
        cbar_ax = fig.add_axes([0.25, 0.09, 0.5, 0.02])
    cbar = fig.colorbar(cnplots[0], cax=cbar_ax, orientation='horizontal')
    cbar.set_label(f'Bias [{var2.units}]')

    # We want the ticks aligned to the levels and with a reasonable number of decimal places
    cbarticks = np.linspace(vmin, vmax, nlevels)
    cbarticks = ticks_round(cbarticks)
    logger.debug(f"cbarticks: {cbarticks}")
    cbar.set_ticks(cbarticks)

    if plev:
        overall_title = (
            f'Bias / Difference of {var_name} [{var2.units}] ({statistic}) from ({start_date1} to {end_date1}) at {plev} Pa\n'
            f'Experiment {model} {exp} with respect to {model_2} {exp_2} climatology ({start_date2} to {end_date2})'
        )
    else:
        overall_title = (
            f'Bias / Difference of {var_name} [{var2.units}] ({statistic})\n'
            f'Experiment {model} {exp} from ({start_date1} to {end_date1}) with respect to {model_2} {exp_2} ({start_date2} to {end_date2})'
        )
    # Set the title above the subplots
    fig.suptitle(overall_title, fontsize=14)
    plt.subplots_adjust(hspace=0.5)

    output_names = OutputSaver(diagnostic='atmglobalmean', model=model, exp=exp, default_path=path_to_output, loglevel=loglevel)
    
    if path_to_output is not None:
        data_array = xr.concat(results, dim='season')
        data_array.attrs = var1.attrs  # Copy attributes from var1 to the data_array
        metadata = {
            'climatology_range1': f'{start_date1}-{end_date1}',
            'climatology_range2': f'{start_date2}-{end_date2}',
        }
        output_names.save_netcdf(data_array, diagnostic_product='seasonal_bias', var=var_name,
                                 statistic=statistic, model_2=model_2, exp_2=exp_2,  time_start=start_date1, time_end=end_date1,
                                 metadata=metadata)
        logger.info(f"The seasonal bias data has been saved for {var_name} variable.")

        output_names.save_pdf(fig, diagnostic_product='seasonal_bias', var=var_name,
                              statistic=statistic, model_2=model_2, exp_2=exp_2, time_start=start_date1, time_end=end_date1,
                              metadata=metadata, dpi=300)
        output_names.save_png(fig, diagnostic_product='seasonal_bias', var=var_name,
                              statistic=statistic, model_2=model_2, exp_2=exp_2, time_start=start_date1, time_end=end_date1,
                              metadata=metadata, dpi=300)
        logger.info(f"The seasonal bias plots have been saved for {var_name} variable.")
    else:
        plt.show()
        logger.info(f"The seasonal bias maps were calculated and plotted for {var_name} variable.")


def compare_datasets_plev(dataset1=None, dataset2=None, var_name=None,
                          start_date1=None, end_date1=None,
                          start_date2=None, end_date2=None,
                          model=None, exp=None, model_2=None, exp_2=None,
                          path_to_output=None, dataset2_precomputed=None, loglevel='WARNING',
                          plev_min=None, plev_max=None, **kwargs):
    """
    Compare two datasets and plot the zonal bias for a selected model time range with respect to the second dataset.

    Args:
        dataset1 (xarray.Dataset): The first dataset.
        dataset2 (xarray.Dataset): The second dataset.
        var_name (str): The variable name to compare (examples: q, u, v, t)
        start_date1 (str): The start date of the time range for dataset1 in 'YYYY-MM-DD' format.
        end_date1 (str): The end date of the time range for dataset1 in 'YYYY-MM-DD' format.
        start_date2 (str): The start date of the time range for dataset2 in 'YYYY-MM-DD' format.
        end_date2 (str): The end date of the time range for dataset2 in 'YYYY-MM-DD' format.
        model (str): The name of the model for the first dataset.
        exp (str): The name of the experiment for the first dataset.
        model_2 (str): The name of the model for the second dataset.
        exp_2 (str): The name of the experiment for the second dataset.
        path_to_output (str): The directory to save the output files.
        dataset2_precomputed (xarray.Dataset or None): Pre-computed climatology for dataset2.
        loglevel (str): The desired level of logging. Default is 'WARNING'.
        plev_min (float or None): The minimum pressure level in Pa. Default is None.
        plev_max (float or None): The maximum pressure level in Pa. Default is None.

    Keyword Args:
        nlevels (int): The number of levels for the colorbar. Default is 12.
        vmin (float): The minimum value for the colorbar. Default is None.
        vmax (float): The maximum value for the colorbar. Default is None.

    Returns:
        A zonal bias plot.
    """
    logger = log_configure(log_level=loglevel, log_name='Compare dataset plev')

    # Convert start and end dates to datetime objects
    if start_date1 or end_date1:
        start1 = datetime.datetime.strptime(start_date1, "%Y-%m-%d")
        end1 = datetime.datetime.strptime(end_date1, "%Y-%m-%d")
        dataset1 = dataset1.sel(time=slice(start1, end1))
    else:
        start_date1 = str(dataset1["time.year"][0].values) + '-' + str(dataset1["time.month"][0].values) + '-' + str(dataset1["time.day"][0].values)
        end_date1 = str(dataset1["time.year"][-1].values) + '-' + str(dataset1["time.month"][-1].values) + '-' + str(dataset1["time.day"][-1].values)
    logger.debug(f"Dataset1 time range: {start_date1} to {end_date1}")

    # Select the data for the given time ranges
    if start_date2 or end_date2:
        start2 = datetime.datetime.strptime(start_date2, "%Y-%m-%d")
        end2 = datetime.datetime.strptime(end_date2, "%Y-%m-%d")
        dataset2 = dataset2.sel(time=slice(start2, end2))
    else:
        start_date2 = str(dataset2["time.year"][0].values) + '-' + str(dataset2["time.month"][0].values) + '-' + str(dataset2["time.day"][0].values)
        end_date2 = str(dataset2["time.year"][-1].values) + '-' + str(dataset2["time.month"][-1].values) + '-' + str(dataset2["time.day"][-1].values)
    logger.debug(f"Dataset2 time range: {start_date2} to {end_date2}")

    # Check if pre-computed climatology is provided, otherwise compute it
    if dataset2_precomputed is None:
        # Compute climatology
        try:
            var2_climatology = dataset2[var_name].mean(dim='time')
        except KeyError:
            raise NoDataError(f"The variable {var_name} is not present in the dataset. Please try again.")
    else:
        if isinstance(dataset2_precomputed, xr.Dataset):
            try:
                var2_climatology = dataset2_precomputed[var_name]
            except KeyError:
                raise NoDataError(f"The variable {var_name} is not present in the dataset. Please try again.")
        elif isinstance(dataset2_precomputed, xr.DataArray):
            var2_climatology = dataset2_precomputed

    # Calculate the bias between dataset1 and dataset2
    try:
        bias = dataset1[var_name] - var2_climatology
    except KeyError:
        raise NoDataError(f"The variable {var_name} is not present in the dataset. Please try again.")

    nlevels = kwargs.get('nlevels', 18)

    if 'plev' in bias.dims:
        # Load in memory to speed up the calculation
        logger.info("Loading data into memory to speed up the calculation...")
        bias = bias.load()

        # Filter pressure levels if plev_min and plev_max are specified
        if plev_min is not None and plev_max is not None:
            bias = bias.sel(plev=slice(plev_max, plev_min))

        # Get the pressure levels and coordinate values
        lat, plev = np.meshgrid(bias['lat'], bias['plev'])

        # Calculate the mean bias along the time axis
        mean_bias = bias.mean(dim='time')

        # Create the z-values for the contour plot
        z_values = mean_bias.mean(dim='lon')

        # Check if vmin and vmax are provided by the user
        if 'vmin' in kwargs and 'vmax' in kwargs:
            vmin = kwargs['vmin']
            vmax = kwargs['vmax']
        else:
            # Calculate vmin and vmax
            vmin, vmax = evaluate_colorbar_limits(z_values)
            if vmin * vmax < 0:  # we want the colorbar to be symmetric
                vmax = max(abs(vmin), abs(vmax))
                vmin = -vmax
            logger.debug(f"vmin: {vmin}, vmax: {vmax}")
    
        levels = np.linspace(vmin, vmax, nlevels)

        # Create the plot
        fig, ax = plt.subplots(figsize=(10, 8))
        cax = ax.contourf(lat, plev, z_values, cmap='RdBu_r', levels=levels, extend='both')
        ax.set_title(
            f'Bias / Difference of {var_name} Experiment {model} {exp} with respect to {model_2} {exp_2} \n'
            f'Selected model time range: {start_date1} to {end_date1} \n Reference time range: {start_date2} to {end_date2}',
            fontsize=14
            )
        ax.set_yscale('log')
        ax.set_ylabel('Pressure Level (Pa)', fontsize=14)
        ax.set_xlabel('Latitude', fontsize=14)
        ax.tick_params(axis='both', which='major', labelsize=14)
        ax.invert_yaxis()
        ax.set_xlim(-90, 90)
        ax.grid(True)

        # Add colorbar
        cbar = fig.colorbar(cax)
        cbar.set_label(f'{var_name} [{dataset1[var_name].units}]', fontsize = 14)
        cbar.ax.tick_params(labelsize=14)
        cbar.set_ticks(np.linspace(vmin, vmax, nlevels + 1))

        output_names = OutputSaver(diagnostic='atmglobalmean', model=model, exp=exp, default_path=path_to_output, loglevel=loglevel)
        
        if path_to_output:
            metadata = {
                'climatology_range1': f'{start_date1}-{end_date1}',
                'climatology_range2': f'{start_date2}-{end_date2}',
            }
            output_names.save_netcdf(mean_bias, diagnostic_product='vertical_bias', var=var_name,
                                     model_2=model_2, exp_2=exp_2,  time_start=start_date1, time_end=end_date1,
                                     metadata=metadata)
            logger.info(f"The zonal bias / difference for a selected models has been saved for {var_name} variable.")

            output_names.save_pdf(fig, diagnostic_product='vertical_bias', var=var_name,
                                  model_2=model_2, exp_2=exp_2, time_start=start_date1, time_end=end_date1,
                                  metadata=metadata, dpi=300)
            output_names.save_png(fig, diagnostic_product='vertical_bias', var=var_name,
                                  model_2=model_2, exp_2=exp_2, time_start=start_date1, time_end=end_date1,
                                  metadata=metadata, dpi=300)
            logger.info(f"The zonal bias / difference plot for a selected models have been saved for {var_name} variable.")
        else:
            plt.show()
            logger.info(f"The comparison of the two datasets is calculated and plotted for {var_name} variable.")
            
    else:
        raise NoDataError(f"The dataset for {var_name} variable does not have a 'plev' coordinate. Please try again.")


def plot_map_with_stats(dataset=None, var_name=None, start_date=None, end_date=None,
                        model=None, exp=None, path_to_output=None, loglevel='WARNING'):
    """
    Plot a map of a chosen variable from a dataset with colorbar and statistics.

    Args:
        dataset (xarray.Dataset): The dataset containing the variable.
        var_name (str): The name of the variable to plot.
        start_date (str): The start date of the time range in 'YYYY-MM-DD' format.
        end_date (str): The end date of the time range in 'YYYY-MM-DD' format.
        model (str): The name of the model for the dataset.
        exp (str): The name of the experiment for the dataset.
        path_to_output (str): The directory to save the output files.
        loglevel (str): The desired level of logging. Default is 'WARNING'.
    """
    logger = log_configure(log_level=loglevel, log_name='Statistics map')

    if start_date is not None or end_date is not None:
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
        var_data = dataset[var_name].sel(time=slice(start_date, end_date)).mean(dim='time')
    else:
        var_data = dataset[var_name].mean(dim='time')
        start_date = str(dataset["time.year"][0].values) + '-' + str(dataset["time.month"][0].values) + '-' + str(dataset["time.day"][0].values)
        end_date = str(dataset["time.year"][-1].values) + '-' + str(dataset["time.month"][-1].values) + '-' + str(dataset["time.day"][-1].values)

    if var_name == 'tprate' or var_name == 'mtpr':
        logger.warning(f"Adjusting {var_name} to be in mm/day")
        var_data = var_data * 86400
        logger.warning(f"Changing {var_name} units attribute to 'mm/day'")
        var_data.attrs['units'] = 'mm/day'

    # Calculate statistics
    if 'plev' in var_data.dims:
        logger.error(f"The dataset for {var_name} variable has a 'plev' coordinate, but None is provided. The function 'plot_map_with_stats' is terminated.")
        return
    logger.debug(f"The dataset does not have a 'plev' coordinate for {var_name} variable.")

    weights = np.cos(np.deg2rad(dataset.lat))
    weighted_data = var_data.weighted(weights)
    var_mean = weighted_data.mean(('lon', 'lat')).values.item()
    var_std = var_data.std().values.item()
    var_min = var_data.min().values.item()
    var_max = var_data.max().values.item()

    # Add cyclic longitude
    try:
        var_data = add_cyclic_lon(var_data)
    except Exception as e:
        logger.debug(f"Error: {e}")
        logger.warning(f"Cannot add cyclic longitude for {var_name} variable.")

    # Plot the map
    fig = plt.figure(figsize=(10, 8))
    ax = plt.axes(projection=ccrs.PlateCarree())
    cmap = 'RdBu_r'  # Choose a colormap (reversed)
    levels = np.linspace(var_min, var_max, num=21)

    if var_name == 'avg_tos':
        # TODO: need to meshgrid the lat and lon and set transform_first=True in contourf
        #       in order to be able to plot it
        logger.error(f"Cannot plot {var_name} variable.")

    im = ax.contourf(var_data.lon, var_data.lat, var_data.values, cmap=cmap, transform=ccrs.PlateCarree(),
                     levels=levels, extend='both')
    # Set plot title and axis labels
    ax.set_title(f'Map of {var_name} for {model} {exp} from {pd.to_datetime(start_date).strftime("%Y-%m-%d")} to {pd.to_datetime(end_date).strftime("%Y-%m-%d")}')
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')

    # Add continents and landlines
    ax.add_feature(cfeature.LAND, edgecolor='black', facecolor='gray')
    ax.add_feature(cfeature.COASTLINE)

    # Add colorbar (reversed)
    cbar = fig.colorbar(im, ax=ax, shrink=0.6, extend='both')
    cbar.set_label(f'{var_name} ({dataset[var_name].units})')
    cbar.set_ticks(np.linspace(var_data.max(), var_data.min(), num=11))

    # Display statistics below the plot
    stat_text = f'Mean: {var_mean:.2f} {dataset[var_name].units}    Std: {var_std:.2f}    Min: {var_min:.2f}    Max: {var_max:.2f}'
    ax.text(0.5, -0.3, stat_text, transform=ax.transAxes, ha='center')

    output_names = OutputSaver(diagnostic='atmglobalmean', model=model, exp=exp, default_path=path_to_output, loglevel=loglevel)

    if path_to_output is not None:
        data_array = var_data.to_dataset(name=var_name)
        data_array.attrs = dataset[var_name].attrs
        metadata = {
                'climatology_range': f'{start_date}-{end_date}',
            }
        output_names.save_netcdf(data_array, diagnostic_product='statistics_maps', var=var_name,
                                 time_start=start_date, time_end=end_date,
                                 metadata=metadata)
        logger.info(f"A {var_name} variable has been saved to {path_to_output}.")

        output_names.save_pdf(fig, diagnostic_product='statistics_maps', var=var_name,
                              time_start=start_date, time_end=end_date,
                              metadata=metadata, dpi=300)
        output_names.save_png(fig, diagnostic_product='statistics_maps', var=var_name,
                              time_start=start_date, time_end=end_date,
                              metadata=metadata, dpi=300)
        logger.info(f"Plot a map of {var_name} variable have been saved.")
    else:
        plt.show()
        logger.info(f"The map of a chosen variable from a dataset is calculated and plotted for {var_name} variable.")