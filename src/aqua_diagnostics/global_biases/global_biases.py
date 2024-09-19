import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from aqua.exceptions import NoDataError
from aqua.graphics import plot_single_map, plot_single_map_diff, plot_maps_diff
from aqua.util import create_folder, add_cyclic_lon, select_season
from aqua.util import evaluate_colorbar_limits, ticks_round
from aqua.logger import log_configure

# Set default options for xarray
xr.set_options(keep_attrs=True)

class GlobalBiases:
    """
    A class to process and visualize atmospheric global mean data.
    
    Attributes:
        data (xr.Dataset): Input data for analysis.
        data_ref (xr.Dataset): Reference data for comparison.
        var_name (str): Name of the variable to analyze.
        plev (float): Pressure level to select.
        outputdir (str): Directory to save output plots.
        loglevel (str): Logging level.
    """

    def __init__(self, data=None, data_ref=None, var_name=None, plev=None, outputdir=None, loglevel='WARNING'):
        self.data = data
        self.data_ref = data_ref
        self.var_name = var_name
        self.plev = plev
        self.outputdir = outputdir
        self.logger = log_configure(log_level=loglevel, log_name='Atmospheric global')

        self._process_data()

    def _process_data(self):
        """
        Process the input data, fix precipitation units, select pressure level.
        """

        self.logger.info('Processing data.')

        # Fix precipitation units if necessary
        if self.var_name in ['tprate', 'mtpr']:
            self.logger.info(f'Fixing precipitation units for variable {self.var_name}.')
            self.data = self.fix_precipitation_units(self.data, self.var_name, 'mm/day')
            if self.data_ref is not None:
                self.data_ref = self.fix_precipitation_units(self.data_ref, self.var_name, 'mm/day')

        # Select pressure level if necessary
        if self.plev is not None:
            self.logger.info(f'Selecting pressure level {self.plev} for variable {self.var_name}.')
            self.data = self.select_pressure_level(self.data, self.plev, self.var_name)
            if self.data_ref is not None:
                self.data_ref = self.select_pressure_level(self.data_ref, self.plev, self.var_name)

    @staticmethod
    def select_pressure_level(data, plev, var_name):
        """
        Select a specific pressure level from the data.

        Args:
            data (xr.Dataset): Input dataset.
            plev (float): Desired pressure level.
            var_name (str): Name of the variable.
        
        Raises:
            NoDataError: If the pressure level is not present in the dataset.
        """

        if 'plev' in data[var_name].dims:
            try:
                return data.sel(plev=plev)
            except KeyError:
                raise NoDataError("The provided pressure level is absent in the dataset. Please try again.")
        else:
            raise NoDataError(f"The dataset for {var_name} variable does not have a 'plev' coordinate.")

    @staticmethod
    def fix_precipitation_units(data, var_name, target_units):
        """
        Fix the units of precipitation variables.
        
        Args:
            data (xr.Dataset): Input dataset.
            var_name (str): Name of the variable.
            target_units (str): Target units for precipitation.
        
        Returns:
            xr.Dataset: Dataset with corrected units.
        """
        if data[var_name].attrs['units'] != target_units:
            data[var_name] *= 86400
            data[var_name].attrs['units'] = target_units
        return data

    def plot_bias(self, seasons=False, seasons_stat='mean'):
        """
        Plot the global biases.

        Args:
            seasons (bool): Flag to indicate seasonal analysis.
            seasons_stat (str): Statistic to use for seasonal analysis.
        """

        self.logger.info('Plotting global biases.')
        self.seasons = seasons
        self.seasons_stat = seasons_stat

        # Plot a single map if only one dataset is provided
        if self.data_ref is None:
            self.logger.warning('Plotting single dataset map since no reference dataset is provided.')
            fig, ax = plot_single_map(self.data[self.var_name].mean(dim='time'), return_fig=True)
            return fig, ax
        else:
            # Plot the bias map if two datasets are provided
            self.logger.info('Plotting bias map between two datasets.')
            fig, ax = plot_single_map_diff(data=self.data[self.var_name].mean(dim='time'), 
                                           data_ref=self.data_ref[self.var_name].mean(dim='time'), return_fig=True)
            return fig, ax

        # Plot seasonal biases if seasons is True
        if self.seasons:
            self.logger.info('Plotting seasonal biases.')
            season_list = ['DJF', 'MAM', 'JJA', 'SON']
            stat_funcs = {'mean': 'mean', 'max': 'max', 'min': 'min', 'std': 'std'}

            if self.seasons_stat not in stat_funcs:
                raise ValueError("Invalid statistic. Please choose one of 'mean', 'std', 'max', 'min'.")

            seasonal_data = []
            seasonal_data_ref = []
            
            for season in season_list:
                data_season = select_season(self.data[self.var_name], season)
                data_ref_season = select_season(self.data_ref[self.var_name], season)
                data_stat =  getattr(data_season, stat_funcs[self.seasons_stat])(dim='time') 
                data_ref_stat = getattr(data_ref_season, stat_funcs[self.seasons_stat])(dim='time')

                seasonal_data.append(data_stat)  
                seasonal_data_ref.append(data_ref_stat)  
            
            plot_maps_diff(maps=seasonal_data, maps_ref=seasonal_data_ref, titles=season_list)


    def plot_vertical_bias(self, data=None, data_ref=None, var_name=None, plev_min=None, plev_max=None, vmin=None, vmax=None):
        """
        Calculate and plot the vertical bias between two datasets.

        Args:
            data (xr.Dataset, optional): Input dataset.
            data_ref (xr.Dataset, optional): Reference dataset.
            var_name (str, optional): Name of the variable.
            plev_min (float, optional): Minimum pressure level for bias calculation.
            plev_max (float, optional): Maximum pressure level for bias calculation.
            vmin (float, optional): Minimum value for colorbar.
            vmax (float, optional): Maximum value for colorbar.
        """

        self.logger.info('Plotting vertical biases.')

        if data:
            self.data = data
        if data_ref:
            self.data_ref = data_ref
        if var_name:
            self.var_name = var_name

        # Compute climatology for reference dataset
        self.logger.info('Computing climatology for reference dataset.')
        ref_climatology = self.data_ref[var_name].mean(dim='time')

        # Calculate the bias between the two datasets
        self.logger.info('Calculating bias between datasets.')
        bias = self.data[var_name] - ref_climatology

        # Filter pressure levels
        if plev_min is None:
            plev_min = bias['plev'].min().item()
        if plev_max is None:
            plev_max = bias['plev'].max().item()

        bias = bias.sel(plev=slice(plev_max, plev_min))

        # Calculate the mean bias along the time axis
        self.logger.info('Calculating mean bias along the time axis.')
        mean_bias = bias.mean(dim='time')
        nlevels = 18

        # Calculate the zonal mean bias
        self.logger.info('Calculating zonal mean bias.')
        zonal_bias = mean_bias.mean(dim='lon')

        # Determine colorbar limits if not provided
        if vmin is None or vmax is None:
            vmin, vmax = zonal_bias.min(), zonal_bias.max()
            if vmin * vmax < 0:  # if vmin and vmax have different signs
                vmax = max(abs(vmin), abs(vmax))
                vmin = -vmax

        levels = np.linspace(vmin, vmax, nlevels)

        # Plotting the zonal bias
        self.logger.info('Plotting the zonal bias.')
        plt.figure(figsize=(10, 8))
        cax = plt.contourf(zonal_bias['lat'], zonal_bias['plev'], zonal_bias, cmap='RdBu_r', levels=levels, extend='both')
        plt.title(f'Zonal Bias of {var_name}')
        plt.yscale('log')
        plt.ylabel('Pressure Level (Pa)')
        plt.xlabel('Latitude')
        plt.gca().invert_yaxis()
        plt.colorbar(cax, label=f'{var_name} [{self.data[self.var_name].attrs.get("units", "")}]')
        plt.grid(True)
        plt.show()

    @staticmethod
    def boxplot(datasets=None, model_names=None, variables=None):
        """
        Generate a boxplot showing the uncertainty of a global variable for different models.

        Args:
            datasets (list of xarray.Dataset): A list of xarray Datasets to be plotted.
            model_names (list of str): Names for the plotting, corresponding to the datasets.
            variables (list of str): List of variables to be plotted.
        """

        logger = log_configure(log_level='INFO', log_name='Boxplot')
        logger.info('Generating boxplot.')

        sns.set_palette("pastel")
        fontsize = 18

        # Initialize a dictionary to store data for the boxplot
        boxplot_data = {'Variables': [], 'Values': [], 'Datasets': []}

        # Default model names if not provided
        if model_names is None:
            model_names = [f"Model {i+1}" for i in range(len(datasets))]

        # Default variables if not provided
        if variables is None:
            variables = ['-mtnlwrf', 'mtnswrf']

        for dataset, model_name in zip(datasets, model_names):
            for variable in variables:
                var_name = variable[1:] if variable.startswith('-') else variable  # Adjusted variable name
                gm = dataset.aqua.fldmean()

                values = gm[var_name].values.flatten()
                if variable.startswith('-'):
                    values = -values
                boxplot_data['Variables'].extend([variable] * len(values))
                boxplot_data['Values'].extend(values)
                boxplot_data['Datasets'].extend([model_name] * len(values))

                units = dataset[var_name].attrs.get('units', 'Unknown')
                logger.info(f'Processed variable {var_name} for dataset {model_name} with units {units}.')

        # Plotting the boxplot
        logger.info('Plotting the boxplot.')
        df = pd.DataFrame(boxplot_data)
        plt.figure(figsize=(12, 8))
        sns.boxplot(data=df, x='Variables', y='Values', hue='Datasets')
        plt.title('Global Mean Variables Boxplot')
        plt.xlabel('Variables')
        plt.ylabel(f'Values [{units}]')
        plt.xticks(fontsize=fontsize)
        plt.yticks(fontsize=fontsize)
        plt.legend(loc='upper right', fontsize=fontsize)
        plt.grid(True)
        plt.show()
