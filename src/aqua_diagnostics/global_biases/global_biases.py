import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from aqua.exceptions import NoDataError
from aqua.graphics import plot_single_map, plot_single_map_diff, plot_maps
from aqua.util import create_folder, add_cyclic_lon, select_season
from aqua.util import evaluate_colorbar_limits, ticks_round
from aqua.logger import log_configure

# Set default options for xarray
xr.set_options(keep_attrs=True)

class GlobalBiases:
    """
    A class to process and visualize global mean data.
    
    Attributes:
        data (xr.Dataset): Input data for analysis.
        data_ref (xr.Dataset): Reference data for comparison.
        var_name (str): Name of the variable to analyze.
        plev (float): Pressure level to select.
        outputdir (str): Directory to save output plots.
        loglevel (str): Logging level.
        model (str, optional): Model name for labeling.
        exp (str, optional): Experiment name for labeling.
        startdate_data (str, optional): Start date of data period.
        enddate_data (str, optional): End date of data period.
        model_obs (str, optional): Obs name for labeling.
        startdate_obs (str, optional): Start date of reference period.
        enddate_obs (str, optional): End date of reference period.

    """
    def __init__(self, data=None, data_ref=None, var_name=None, plev=None, outputdir=None, loglevel='WARNING', 
                 model=None, exp=None, startdate_data=None, enddate_data=None, 
                 model_obs=None, startdate_obs=None, enddate_obs=None):
        
        self.data = data
        self.data_ref = data_ref
        self.var_name = var_name
        self.plev = plev
        self.outputdir = outputdir
        self.logger = log_configure(log_level=loglevel, log_name='Global Biases')
        self.model = model
        self.exp = exp
        self.startdate_data = startdate_data or pd.to_datetime(data.time[0].values).strftime('%Y-%m-%d')
        self.enddate_data = enddate_data or pd.to_datetime(data.time[-1].values).strftime('%Y-%m-%d')
        self.model_obs = model_obs
        self.startdate_obs = startdate_obs or pd.to_datetime(data_ref.time[0].values).strftime('%Y-%m-%d')
        self.enddate_obs = enddate_obs or pd.to_datetime(data_ref.time[-1].values).strftime('%Y-%m-%d')

        self.data = self._process_data(self.data)
        if self.data_ref is not None:
            self.data_ref = self._process_data(self.data_ref)

    def _process_data(self, data):
        """
        Processes the dataset, converting precipitation units if necessary 
        and selecting the specified pressure level.

        Args:
            data (xr.Dataset): The dataset to process.
        """
        if self.var_name in ['tprate', 'mtpr']:
            self.logger.info(f'Adjusting units for precipitation variable: {self.var_name}.')
            data = self._fix_precipitation_units(data, self.var_name)

        if self.plev is not None:
            self.logger.info(f'Selecting pressure level {self.plev} for variable {self.var_name}.')
            data = self.select_pressure_level(data, self.plev, self.var_name)
        elif 'plev' in data[self.var_name].dims:
            self.logger.warning(f"Variable {self.var_name} has multiple pressure levels but none selected. Skipping 2D plotting for bias maps.")

        return data

    @staticmethod
    def select_pressure_level(data, plev, var_name):
        """
        Selects a specified pressure level from the dataset.

        Args:
            data (xr.Dataset): Dataset to select from.
            plev (float): Desired pressure level.
            var_name (str): Variable name to filter by.

        Returns:
            xr.Dataset: Filtered dataset at specified pressure level.

        Raises:
            NoDataError: If specified pressure level is not available.
        """
        if 'plev' in data[var_name].dims:
            try:
                return data.sel(plev=plev)
            except KeyError:
                raise NoDataError("The specified pressure level is not in the dataset.")
        else:
            raise NoDataError(f"{var_name} does not have a 'plev' coordinate.")

    @staticmethod
    def _fix_precipitation_units(data, var_name):
        """
        Converts precipitation units from kg m-2 s-1 to mm/day.

        Args:
            data (xr.Dataset): Dataset to adjust.
            var_name (str): Variable name for precipitation.

        Returns:
            xr.Dataset: Dataset with adjusted units.
        """
        if data[var_name].attrs['units'] != 'mm/day':
            data[var_name] *= 86400
            data[var_name].attrs['units'] = 'mm/day'
        return data

    def plot_bias(self, stat='mean', vmin=None, vmax=None):
        """
        Plots global biases or a single dataset map if reference data is unavailable.

        Args:
            stat (str): Statistic for calculation ('mean' by default).
            vmin (float, optional): Minimum colorbar value.
            vmax (float, optional): Maximum colorbar value.

        Returns:
            tuple: Matplotlib figure, axis objects, and xarray Dataset of the calculated bias if available.
        """
        self.logger.info('Plotting global biases.')

        # Check if pressure levels exist but are not specified
        if 'plev' in self.data[self.var_name].dims and self.plev is None:
            self.logger.warning(f"Variable {self.var_name} has multiple pressure levels, but no specific level was selected. Skipping 2D bias plotting.")
            return None  # Return None for both fig and ax  

        # Plot a single map if only one dataset is provided
        if self.data_ref is None:
            self.logger.warning('Plotting single dataset map since no reference dataset is provided.')

            title = (f"{self.var_name} map {self.model} {self.exp} {self.startdate_data}/{self.enddate_data}" 
                    + (f" at {int(self.plev / 100)} hPa" if self.plev else ""))

            fig, ax = plot_single_map(self.data[self.var_name].mean(dim='time'), 
                                      return_fig=True, 
                                      title=title,
                                      vmin=vmin, vmax=vmax)
            ax.set_xlabel("Longitude")
            ax.set_ylabel("Latitude")
            bias = self.data[self.var_name].mean(dim='time')
        else:
            # Plot the bias map if two datasets are provided
            self.logger.info('Plotting bias map between two datasets.')

            # Set 'sym' to True if either 'vmin' or 'vmax' is None, indicating a symmetric colorbar.
            sym = vmin is None or vmax is None

            title = (f"{self.var_name} global bias of {self.model} {self.exp} {self.startdate_data}/{self.enddate_data}\n"
                    f"relative to {self.model_obs} climatology {self.startdate_obs}/{self.enddate_obs}"
                    + (f" at {int(self.plev / 100)} hPa" if self.plev else ""))

            fig, ax = plot_single_map_diff(data=self.data[self.var_name].mean(dim='time'), 
                                           data_ref=self.data_ref[self.var_name].mean(dim='time'),
                                           return_fig=True,
                                           contour=True, 
                                           title=title,
                                           sym=sym,
                                           vmin_fill=vmin, vmax_fill=vmax)
            ax.set_xlabel("Longitude")
            ax.set_ylabel("Latitude")
            bias = self.data[self.var_name].mean(dim='time') - self.data_ref[self.var_name].mean(dim='time')

        return fig, ax, bias

    def plot_seasonal_bias(self, seasons_stat='mean', vmin=None, vmax=None):
        """
        Plots seasonal biases for each season (DJF, MAM, JJA, SON) and returns an xarray.Dataset
        containing the calculated seasonal biases.

        Args:
            seasons_stat (str): Statistic for seasonal analysis ('mean' by default).
            vmin (float, optional): Minimum colorbar value.
            vmax (float, optional): Maximum colorbar value.

        Returns:
            tuple: Matplotlib figure and an xarray.Dataset of the calculated seasonal biases.
        """
        self.logger.info('Plotting seasonal biases.')

        # Set 'sym' to True if either 'vmin' or 'vmax' is None, indicating a symmetric colorbar.
        sym = vmin is None or vmax is None

        # Check if pressure levels exist but are not specified
        if 'plev' in self.data[self.var_name].dims and self.plev is None:
            self.logger.warning(f"Variable {self.var_name} has multiple pressure levels, but no specific level was selected. Skipping 2D bias plotting.")
            return None  # Return None for both fig and ax  

        # Validate seasons_stat
        stat_funcs = {'mean': 'mean', 'max': 'max', 'min': 'min', 'std': 'std'}
        if seasons_stat not in stat_funcs:
            raise ValueError("Invalid statistic. Please choose one of 'mean', 'std', 'max', 'min'.")

        # Prepare seasonal data and compute biases
        season_list = ['DJF', 'MAM', 'JJA', 'SON']
        seasonal_biases = {}

        for season in season_list:
            # Select season for both data and reference
            data_season = select_season(self.data[self.var_name], season)
            data_ref_season = select_season(self.data_ref[self.var_name], season)

            # Compute seasonal statistics
            data_stat = getattr(data_season, stat_funcs[seasons_stat])(dim='time')
            data_ref_stat = getattr(data_ref_season, stat_funcs[seasons_stat])(dim='time')

            # Compute bias and store in dictionary
            bias = data_stat - data_ref_stat
            seasonal_biases[season] = bias

        # Combine seasonal biases into an xarray.Dataset
        bias_dataset = xr.Dataset(seasonal_biases)

        # Plot seasonal biases
        plot_kwargs = {
            'maps': [bias_dataset[season] for season in season_list],
            'return_fig': True,
            'titles': season_list,
            'contour': True,
            'sym': sym
        }

        if vmin is not None:
            plot_kwargs['vmin'] = vmin
        if vmax is not None:
            plot_kwargs['vmax'] = vmax

        fig = plot_maps(**plot_kwargs)

        return fig, bias_dataset

    def plot_vertical_bias(self, data=None, data_ref=None, var_name=None, plev_min=None, plev_max=None, vmin=None, vmax=None):
        """
        Calculates and plots the vertical bias between two datasets.

        Args:
            data (xr.Dataset, optional): Dataset for analysis.
            data_ref (xr.Dataset, optional): Reference dataset for comparison.
            var_name (str, optional): Variable name to analyze.
            plev_min (float, optional): Minimum pressure level.
            plev_max (float, optional): Maximum pressure level.
            vmin (float, optional): Minimum colorbar value.
            vmax (float, optional): Maximum colorbar value.

        Returns:
            tuple: Matplotlib figure, axis objects, and xarray Dataset of the calculated bias.
        """

        self.logger.info('Plotting vertical biases.')

        data, data_ref, var_name = data or self.data, data_ref or self.data_ref, var_name or self.var_name

        # Compute climatology for reference dataset
        ref_climatology = data_ref[var_name].mean(dim='time')

        # Calculate the bias between the two datasets
        bias = data[var_name] - ref_climatology

        # Filter pressure levels
        if plev_min is None:
            plev_min = bias['plev'].min().item()
        if plev_max is None:
            plev_max = bias['plev'].max().item()

        bias = bias.sel(plev=slice(plev_max, plev_min))

        # Calculate the mean bias along the time axis
        mean_bias = bias.mean(dim='time')
        nlevels = 18

        # Calculate the zonal mean bias
        zonal_bias = mean_bias.mean(dim='lon')

        # Determine colorbar limits if not provided
        if vmin is None or vmax is None:
            vmin, vmax = zonal_bias.min(), zonal_bias.max()
            if vmin * vmax < 0:  # if vmin and vmax have different signs
                vmax = max(abs(vmin), abs(vmax))
                vmin = -vmax

        levels = np.linspace(vmin, vmax, nlevels)

        title = (f"{var_name} vertical bias of {self.model} {self.exp} {self.startdate_data}/{self.enddate_data}\n" 
            f"relative to {self.model_obs} climatology {self.startdate_obs}/{self.enddate_obs}\n")

        # Plotting the zonal bias
        fig, ax = plt.subplots(figsize=(10, 8))
        cax = ax.contourf(zonal_bias['lat'], zonal_bias['plev'], zonal_bias, cmap='RdBu_r', levels=levels, extend='both')
        ax.set_title(title)
        ax.set_yscale('log')
        ax.set_ylabel('Pressure Level (Pa)')
        ax.set_xlabel('Latitude')
        ax.invert_yaxis()
        fig.colorbar(cax, ax=ax, label=f'{var_name} [{data[var_name].attrs.get("units", "")}]')
        ax.grid(True)

        return fig, ax, zonal_bias

