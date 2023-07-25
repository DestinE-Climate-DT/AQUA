"""A module for atmospheric global biases analysis and visualization."""

import os
import xarray as xr
import numpy as np
import matplotlib.pylab as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.gridspec as gridspec
from cartopy.mpl.ticker import LongitudeFormatter, LatitudeFormatter
from aqua import Reader
import pandas as pd
import datetime

outputfig = "./output/figs"
if not os.path.exists(outputfig):
    os.makedirs(outputfig)
outputdir = "./output/data"    
if not os.path.exists(outputdir):
    os.makedirs(outputdir)
    
    
def seasonal_bias(dataset1, dataset2, var_name, plev, statistic, model_label1, model_label2, start_date1, end_date1, start_date2, end_date2):
    '''
    Plot the seasonal bias maps between two datasets for specific variable and time ranges.

    Args:
        dataset1 (xarray.Dataset): The first dataset.
        dataset2 (xarray.Dataset): The second dataset, that will be compared to the first dataset (reference dataset).
        var_name (str): The name of the variable to compare (e.g., '2t', 'tprate', 'mtntrf', 'mtnsrf', ...).
        plev (float or None): The desired pressure level in Pa. If None, the variable is assumed to be at surface level.
        statistic (str): The desired statistic to calculate for each season. Valid options are: 'mean', 'max', 'min', 'diff', and 'std'.
        model_label1 (str): The labeling for the first dataset.
        model_label2 (str): The labeling for the second dataset.
        start_date1 (str): The start date of the time range for dataset1 in 'YYYY-MM-DD' format.
        end_date1 (str): The end date of the time range for dataset1 in 'YYYY-MM-DD' format.
        start_date2 (str): The start date of the time range for dataset2 in 'YYYY-MM-DD' format.
        end_date2 (str): The end date of the time range for dataset2 in 'YYYY-MM-DD' format.

    Raises:
        ValueError: If an invalid statistic is provided.

    Returns:
        A seasonal bias plot.
    '''

    var1 = dataset1[var_name]
    var2 = dataset2[var_name]

    var1_climatology = var1.sel(time=slice(start_date1, end_date1)).groupby('time.month').mean(dim='time')
    var2_climatology = var2.sel(time=slice(start_date2, end_date2)).groupby('time.month').mean(dim='time')

    # Select the desired pressure level if provided
    if plev is not None:
        var1_climatology = var1_climatology.sel(plev=plev)
        var2_climatology = var2_climatology.sel(plev=plev)

    # Calculate the desired statistic for each season
    season_ranges = {'DJF': [12, 1, 2], 'MAM': [3, 4, 5], 'JJA': [6, 7, 8], 'SON': [9, 10, 11]}
    results = []
    for season, months in season_ranges.items():
        if season == 'DJF':
            var1_season = var1_climatology.sel(month=months[1:])
            var2_season = var2_climatology.sel(month=months)
        else:
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

    # Calculate the number of rows and columns for the subplot grid
    num_rows = 2
    num_cols = 2

    # Plot the bias maps for each season
    fig = plt.figure(figsize=(15, 10))
    gs = gridspec.GridSpec(num_rows, num_cols, figure=fig)

    # Create a list to store the plotted objects
    cnplots = []

    for i, (result, season) in enumerate(zip(results, season_ranges.keys())):
        ax = fig.add_subplot(gs[i], projection=projection)

        # Add coastlines to the plot
        ax.add_feature(cfeature.COASTLINE)

        # Add other cartographic features (optional)
        ax.add_feature(cfeature.LAND, facecolor='lightgray')
        ax.add_feature(cfeature.OCEAN, facecolor='lightblue')
        # Set latitude and longitude tick labels
        ax.set_xticks(np.arange(-180, 181, 60), crs=projection)
        ax.set_yticks(np.arange(-90, 91, 30), crs=projection)
        ax.xaxis.set_major_formatter(LongitudeFormatter())
        ax.yaxis.set_major_formatter(LatitudeFormatter())

        # Plot the bias data using the corresponding cnplot object
        cnplot = result.plot(ax=ax, cmap='RdBu_r', add_colorbar=False)
        cnplots.append(cnplot)

        ax.set_title(f'{season}')
        ax.set_xlabel('Longitude')
        ax.set_ylabel('Latitude')

    # Remove any empty subplots if the number of results is less than the number of subplots
    if len(results) < num_rows * num_cols:
        for i in range(len(results), num_rows * num_cols):
            fig.delaxes(fig.axes[i])

    # Add a colorbar
    cbar_ax = fig.add_axes([0.25, 0.05, 0.5, 0.02])  # Adjust the position and size of the colorbar
    cbar = fig.colorbar(cnplots[0], cax=cbar_ax, orientation='horizontal')
    cbar.set_label(f'Bias [{var2.units}]')

    # Set the overall figure title
    if plev is not None:
        overall_title = f'Bias of {var_name} [{var2.units}] ({statistic}) from ({start_date1} to {end_date1}) at {plev} Pa\n Experiment {model_label1} with respect to  {model_label2} climatology ({start_date2} to {end_date2})'
    else:
        overall_title = f'Bias of {var_name} [{var2.units}] ({statistic}) from ({start_date1} to {end_date1})\n Experiment {model_label1} with respect to {model_label2} climatology ({start_date2} to {end_date2})'

    # Set the title above the subplots
    fig.suptitle(overall_title, fontsize=14, fontweight='bold')
    plt.subplots_adjust(hspace=0.5)

    # Save the figure
    filename = f"{outputfig}/Seasonal_Bias_Plot_{model_label1}_{var_name}_{statistic}_{start_date1}_{end_date1}_{start_date2}_{end_date2}.pdf"
    plt.savefig(filename, dpi=300, format='pdf')
    plt.show()


    # Write the data into a NetCDF file
    data_directory = outputdir
    data_filename = f"Seasonal_Bias_Data_{model_label1}_{var_name}_{statistic}_{start_date1}_{end_date1}_{start_date2}_{end_date2}.nc"
    data_path = os.path.join(data_directory, data_filename)

    data_array = xr.concat(results, dim='season')
    data_array.attrs = var1.attrs  # Copy attributes from var1 to the data_array
    data_array.attrs['statistic'] = statistic
    data_array.attrs['dataset1'] = model_label1
    data_array.attrs['dataset2'] = model_label2
    data_array.attrs['climatology_range1'] = f'{start_date1}-{end_date1}'
    data_array.attrs['climatology_range2'] = f'{start_date2}-{end_date2}'

    data_array.to_netcdf(data_path)

    print(f"The seasonal bias plots have been saved to {outputfig}.")
    print(f"The seasonal bias data has been saved to {outputdir}.")

#---------------------------------------------------------------------------------------------------------------------------------------
#---------------------------------------------------------------------------------------------------------------------------------------

def compare_datasets_plev(dataset1, dataset2, var_name, start_date1, end_date1, start_date2, end_date2, model_label1, model_label2):
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
        model_label (str): The label for the model.

    Returns:
        A zonal bias plot.
    """
    # Convert start and end dates to datetime objects
    start1 = datetime.datetime.strptime(start_date1, "%Y-%m-%d")
    end1 = datetime.datetime.strptime(end_date1, "%Y-%m-%d")
    start2 = datetime.datetime.strptime(start_date2, "%Y-%m-%d")
    end2 = datetime.datetime.strptime(end_date2, "%Y-%m-%d")

    # Select the data for the given time ranges
    dataset1 = dataset1.sel(time=slice(start1, end1))
    dataset2 = dataset2.sel(time=slice(start2, end2))

    # Calculate the bias between dataset1 and dataset2
    bias = dataset1[var_name] - dataset2[var_name].mean(dim='time')

    # Get the pressure levels and coordinate values
    plev = bias['plev'].values
    coord_values = bias['lat'].values

    # Calculate the mean bias along the time axis
    mean_bias = bias.mean(dim='time')

    # Create the z-values for the contour plot
    coord_values_2d, plev_2d = np.meshgrid(coord_values, plev)
    z_values = np.mean(mean_bias, axis=2)

    # Create the plot
    fig, ax = plt.subplots(figsize=(10, 8))
    cax = ax.contourf(coord_values_2d, plev_2d, z_values, cmap='RdBu_r')
    ax.set_title(f'Bias of {var_name} Experiment {model_label1} with respect to {model_label2} \n Selected model time range: {start_date1} to {end_date1}. Reference time range: {start_date2} to {end_date2}')
    ax.set_yscale('log')
    ax.set_ylabel('Pressure Level (Pa)')
    ax.set_xlabel('Latitude')
    ax.invert_yaxis()
    ax.set_xlim(-90, 90)

    # Add colorbar
    cbar = fig.colorbar(cax)
    cbar.set_label(f'{var_name} [{dataset1[var_name].units}]')
 
    # Save the plot as a PDF file
    filename = f"Vertical_biases_{model_label1}_{model_label2}_{var_name}_{start_date1}_{end_date1}_{start_date2}_{end_date2}.pdf"
    output_path = os.path.join(outputfig, filename)
    plt.savefig(output_path, dpi=300, format='pdf')

    # Save the data into a NetCDF file
    filename = f"{outputdir}/Vertical_bias_{model_label1}_{model_label2}_{var_name}_{start_date1}_{end_date1}_{start_date2}_{end_date2}.nc"
    mean_bias.to_netcdf(filename)
    
    plt.show()
    print(f"The vertical bias plots have been saved to {outputfig}.")
    print(f"The vertical bias data has been saved to {outputdir}.")

    
#---------------------------------------------------------------------------------------------------------------------------------------
#---------------------------------------------------------------------------------------------------------------------------------------



def plot_map_with_stats(dataset, var_name, start_date, end_date, model_label):
    """
    Plot a map of a chosen variable from a dataset with colorbar and statistics.

    Args:
        dataset (xarray.Dataset): The dataset containing the variable.
        var_name (str): The name of the variable to plot.
        start_date (str): The start date of the time range in 'YYYY-MM-DD' format.
        end_date (str): The end date of the time range in 'YYYY-MM-DD' format.
        model_label (str): The label for the model.
        outputdir (str): The directory to save the output files.
    """
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    
    # Calculate statistics
    var_data = dataset[var_name].sel(time=slice(start_date, end_date)).mean(dim='time')
    weights = np.cos(np.deg2rad(dataset.lat))
    weighted_data = var_data.weighted(weights)
    var_mean = weighted_data.mean(('lon', 'lat')).values.item()
    var_std = var_data.std().values.item()
    var_min = var_data.min().values.item()
    var_max = var_data.max().values.item()
    
    # Plot the map
    fig = plt.figure(figsize=(10, 8))
    ax = plt.axes(projection=ccrs.PlateCarree())
    cmap = 'RdBu_r'  # Choose a colormap (reversed)
    im = ax.pcolormesh(var_data.lon, var_data.lat, var_data.values, cmap=cmap, transform=ccrs.PlateCarree())

    # Set plot title and axis labels
    ax.set_title(f'Map of {var_name} for {model_label} from {start_date.strftime("%Y-%m-%d")} to {end_date.strftime("%Y-%m-%d")}')
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
    
    # Save the plot as a PDF file
    filename = f"Statistics_maps_{model_label}_{var_name}.pdf"
    output_path = os.path.join(outputfig, filename)
    plt.savefig(output_path, dpi=300, format='pdf')

    # Save the data into a NetCDF file
    data_filename = f"Statistics_Data_{model_label}_{var_name}.nc"
    data_path = os.path.join(outputdir, data_filename)

    data_array = var_data.to_dataset(name=var_name)
    data_array.attrs = dataset[var_name].attrs  # Copy attributes from the original dataset
    data_array.attrs['model_label'] = model_label

    data_array.to_netcdf(data_path)
     
    plt.show()
    print(f"The plot has been saved to {outputfig}.")
    print(f"The data has been saved to {outputdir}.")