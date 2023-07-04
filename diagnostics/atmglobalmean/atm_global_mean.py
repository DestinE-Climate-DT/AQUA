import sys
import os
import xarray as xr
import numpy as np
import matplotlib.pylab as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from cartopy.util import add_cyclic_point
import calendar
import math

plotdir = './plots/AtmGlobalBiases/'
if not os.path.exists(plotdir):
    os.makedirs(plotdir)

class AGM_diag:

    @staticmethod
    def compare_datasets(dataset1, dataset2, var1_name, var2_name, time_range, plev, statistic):
        
        var1 = dataset1[var1_name]
        var2 = dataset2[var2_name]

        if dataset1 == 'data_tco2559':
            dataset1_name = 'tco2559'
        elif dataset1 == 'data_era5':
            dataset1_name = 'ERA5'
        elif dataset1 == 'data_icon':
            dataset1_name = 'ICON'
        elif dataset1 == 'data_tco1279':
            dataset1_name = 'tco1279'

        if dataset2 == 'data_tco2559':
            dataset2_name = 'tco2559'
        elif dataset2 == 'data_era5':
            dataset2_name = 'ERA5'
        elif dataset2 == 'data_icon':
            dataset2_name = 'ICON'
        elif dataset2 == 'data_tco1279':
            dataset2_name = 'tco1279'

        # Convert units if necessary
        # if var1_units != var2_units:
        # Perform unit conversion here
        # ...
        #     pass

        # Select the desired time range
        var1 = var1.sel(time=time_range)
        var2 = var2.sel(time=time_range)

        # Select the desired pressure level if provided
        if plev is not None:
            var1 = var1.sel(plev=plev)
            var2 = var2.sel(plev=plev)

        # Calculate the desired statistic for each month
        results = []
        for month in var1.time.dt.month:
            var1_month = var1.sel(time=var1.time.dt.month == month)
            var2_month = var2.sel(time=var2.time.dt.month == month)
            if statistic == 'mean':
                result_month = var2_month.mean(dim='time') - var1_month.mean(dim='time')
            elif statistic == 'max':
                result_month = var2_month.max(dim='time') - var1_month.max(dim='time')
            elif statistic == 'min':
                result = (var1.min(dim='time'), var2.min(dim='time'))
            elif statistic == 'diff':
                result = (var1 - var2)
            elif statistic == 'std':
                result = (var1.std(dim='time'), var2.std(dim='time'))
            else:
                raise ValueError("Invalid statistic. Please choose one of 'mean', 'std', 'max', 'min', or 'diff'.")
            results.append(result_month)

        # Create a cartopy projection
        projection = ccrs.PlateCarree()

        # Plot the bias maps for each month
        fig, axs = plt.subplots(len(results), figsize=(7, 3 * len(results)), subplot_kw={'projection': projection})
        if len(results) == 1:
            axs = [axs]  # Convert axs to a list if only one plot
        for i, result in enumerate(results):
            ax = axs[i]

            # Add coastlines to the plot
            ax.add_feature(cfeature.COASTLINE)

            # Add other cartographic features (optional)
            ax.add_feature(cfeature.LAND, facecolor='lightgray')
            ax.add_feature(cfeature.OCEAN)

            cnplot = result.plot(ax=ax, cmap='RdBu_r', vmin=-10, vmax=10)
    
            ax.set_title(f'Bias of {var2_name}[{var2.units}] ({statistic}) at {plev} Pa\n Experiment with respect to ERA5 Month {i + 1}')
            #f'Bias of {var2_name} ({dataset2[var2_name].long_name}) [{var2.units}] ({statistic}) at {plev} Pa\n Experiment with respect to ERA5 Month {i + 1}')
            ax.set_xlabel('Longitude')
            ax.set_ylabel('Latitude')
            fig.subplots_adjust(right=0.95)

        # plotlevels=np.arange(-50,51,10)
        global small_fonts
        small_fonts = 8

        plt.tight_layout()
        plt.subplots_adjust(hspace=0.5)  # Adjust the spacing as desired

        # Save the figure as a PNG file
        plt.savefig(plotdir+ dataset1_name + '_AtmosphericGlobalBiases_' + var1_name + '.png', dpi=300,
                    facecolor='white')

        return results, axs

    @staticmethod
    def compare_datasets_plev(dataset1, dataset2, var1_name, var2_name, time_range, plot_latitude=True):
        bias = dataset1[var1_name].sel(time=time_range) - dataset2[var2_name].sel(time=time_range)

        # Get the pressure levels and coordinate values
        plev = bias['plev'].values
        if plot_latitude:
            coord_name = 'lat'
            coord_values = bias['lat'].values
        else:
            coord_name = 'lon'
            coord_values = bias['lon'].values

        # Find common coordinate values
        common_coord = np.intersect1d(dataset1[coord_name], dataset2[coord_name])

        # Get indices of common coordinate values
        coord_indices = np.where(np.isin(coord_values, common_coord))

        # Slice bias and coordinate arrays using common indices
        bias = bias.isel(**{coord_name: coord_indices[0]})
        coord_values = coord_values[coord_indices]

        # Calculate the number of months based on the sliced time range
        num_months = len(bias['time'])

        if num_months == 0:
            print("No data available for the specified time range.")
            return None, None

        # Calculate the number of rows and columns for the grid layout
        num_cols = math.ceil(math.sqrt(num_months))
        num_rows = math.ceil(num_months / num_cols)

        # Create subplots for each month in a grid layout
        fig, axes = plt.subplots(nrows=num_rows, ncols=num_cols, figsize=(15, 15))

        # Flatten the axes array for easier iteration
        axes = axes.flatten()

        # Iterate over each month and plot the bias
        for i, ax in enumerate(axes[:num_months]):
            month_bias = bias.isel(time=i)
            month_title = month_bias['time'].dt.strftime('%B %Y').item()

            z_values = np.mean(month_bias, axis=2)  # Take the mean along the time axis

            # Adjust the dimensions of coord_values and plev
            coord_values_2d, plev_2d = np.meshgrid(coord_values, plev)
            cax = ax.contourf(coord_values_2d, plev_2d, z_values, cmap='RdBu_r')  #, levels=20)
            ax.set_title(f'{month_title}')
            ax.set_yscale('log')
            ax.set_ylabel('Pressure Level (Pa)')
            ax.set_xlabel('Coordinate Value')
            ax.invert_yaxis()
            ax.set_xlim(coord_values.min(), coord_values.max())  

        # Remove empty subplots if there are any
        for ax in axes[num_months:]:
            fig.delaxes(ax)

        # Add colorbar
        cbar = fig.colorbar(cax, ax=axes, shrink=0.6)
        cbar.set_label(f'{var2_name} ({dataset2[var2_name].units})')  # Add variable name and unit to the colorbar label

        # Set plot title and axis labels
        if plot_latitude:
            fig.suptitle(f' Bias of {var2_name} ({dataset2[var2_name].long_name}).\n Experiment with respect to ERA5 \n Zonal Bias Plot for Each Month (Latitude)')
        else:
            fig.suptitle(f' Bias of {var2_name} ({dataset2[var2_name].long_name}).\n Experiment with respect to ERA5 \n Zonal Bias Plot for Each Month (Longitude)')

        # plt.tight_layout()
        # Save the figure as a PNG file
        plt.savefig(plotdir+ dataset1_name + '_plevels_AtmosphericGlobalBiases_' + var2_name + '.png', dpi=300,
                    facecolor='white')
        plt.show()
    
    @staticmethod
    def seasonal_bias(dataset1, dataset2, var1_name, var2_name, year, plev, statistic):
        var1 = dataset1[var1_name]
        var2 = dataset2[var2_name]
    
        if dataset1 == 'data_tco2559':
            dataset1_name = 'tco2559'
        elif dataset1 =='data_era5':
            dataset1_name = 'ERA5'
        elif dataset1 == 'data_icon':
            dataset1_name ='ICON'
        elif dataset1 == 'data_tco1279':
            dataset1_name = 'tco1279'
    
    
        if dataset2 == 'data_tco2559':
            dataset2_name = 'tco2559'
        elif dataset2 =='data_era5':
            dataset2_name = 'ERA5'
        elif dataset2 == 'data_icon':
            dataset2_name = 'ICON'
        elif dataset2 == 'data_tco1279':
            dataset2_name = 'tco1279'
       
        var1_year = var1.sel(time=var1.time.dt.year == year)
        var2_year = var2.sel(time=var2.time.dt.year == year)
    
        # Select the desired pressure level if provided
        if plev is not None:
            var1_year = var1_year.sel(plev=plev)
            var2_year = var2_year.sel(plev=plev)
    
        # Calculate the desired statistic for each season
        season_ranges = {'DJF': [12, 1, 2], 'MAM': [3, 4, 5], 'JJA': [6, 7, 8], 'SON': [9, 10, 11]}
        results = []
        for season, months in season_ranges.items():
            var1_season = var1_year.sel(time=var1_year.time.dt.month.isin(months))
            var2_season = var2_year.sel(time=var2_year.time.dt.month.isin(months))
            if statistic == 'mean':
                result_season = var2_season.mean(dim='time') - var1_season.mean(dim='time')
            elif statistic == 'max':
                result_season = var2_season.max(dim='time') - var1_season.max(dim='time')
            elif statistic == 'min':
                result_season = var1_season.min(dim='time') - var2_season.min(dim='time')
            elif statistic == 'diff':
                result_season = var1_season - var2_season
            elif statistic == 'std':
                result_season = var1_season.std(dim='time') - var2_season.std(dim='time')
            else:
                raise ValueError("Invalid statistic. Please choose one of 'mean', 'std', 'max', 'min', or 'diff'.")
            results.append(result_season)
    
        # Create a cartopy projection
        projection = ccrs.PlateCarree()
    
        # Plot the bias maps for each season
        fig, axs = plt.subplots(len(results), figsize=(7, 3 * len(results)), subplot_kw={'projection': projection})  
        if len(results) == 1:
            axs = [axs]  # Convert axs to a list if only one plot
        for i, result in enumerate(results):
            ax = axs[i]
                
            # Add coastlines to the plot
            ax.add_feature(cfeature.COASTLINE)
        
            # Add other cartographic features (optional)
            ax.add_feature(cfeature.LAND, facecolor='lightgray')
            ax.add_feature(cfeature.OCEAN)
        
            cnplot = result.plot(ax=ax, cmap='RdBu_r', vmin=-10, vmax=10)
        
            ax.set_title(f'Bias of {var2_name} ({dataset2[var2_name].long_name}) [{var2.units}] ({statistic}) at {plev} Pa\n Experiment {dataset1_name} with respect to {dataset2_name} \n {season}')
            ax.set_xlabel('Longitude')
            ax.set_ylabel('Latitude')
            fig.subplots_adjust(right=0.95)
        
        global small_fonts
        small_fonts = 8
   
        plt.tight_layout()
        plt.subplots_adjust(hspace=0.5)
    
        # Save the figure as a PNG file
        if plotdir:
            dataset_name = dataset1.attrs.get('name', 'dataset')
            variable_name = dataset1[var1_name].attrs.get('long_name', var1_name)
            file_name = f"{dataset_name}_{var1_name}_SeasonalBiases_{variable_name}{year}.png"
            plt.savefig(plotdir + '/' + file_name, dpi=300, facecolor='white')
        #plt.savefig(plotdir+'_'+dataset1_name+'SeasonalBias'+year+'.png', dpi=300, facecolor='white')

        return results, axs
