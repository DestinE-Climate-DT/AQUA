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

class AtmosphericGlobalMean:
    """
    A class to process and visualize atmospheric global mean data.
    
    Attributes:
        data (xr.Dataset): Input data for analysis.
        data_ref (xr.Dataset): Reference data for comparison.
        var_name (str): Name of the variable to analyze.
        plev (float): Pressure level to select.
        vertical (bool): Flag to compute vertical biases.
        outputdir (str): Directory to save output plots.
        logger (logging.Logger): Logger for the class.
        seasons (bool): Flag to indicate seasonal analysis.
        seasons_stat (str): Statistic to use for seasonal analysis.
        plev_min (float): Minimum pressure level for analysis.
        plev_max (float): Maximum pressure level for analysis.
        vmin (float): Minimum value for colorbar.
        vmax (float): Maximum value for colorbar.
    """
    
    def __init__(self, data=None, data_ref=None, var_name=None,
                 plev=None, vertical=None, outputdir=None,
                 loglevel='WARNING', seasons=False, seasons_stat="mean",
                 plev_min=None, plev_max=None, vmin=None, vmax=None):
        self.data = data
        self.data_ref = data_ref
        self.var_name = var_name
        self.plev = plev
        self.vertical = vertical
        self.outputdir = outputdir
        self.logger = log_configure(log_level=loglevel, log_name='Atmospheric global')
        self.seasons = seasons
        self.seasons_stat = seasons_stat
        self.plev_min = plev_min
        self.plev_max = plev_max
        self.vmin = vmin
        self.vmax = vmax

        self._process_data()

    def _process_data(self):
        """
        Process the input data, fix precipitation units, select pressure level, 
        and generate appropriate plots based on the input parameters.
        """

        # Fix precipitation units if necessary
        if self.var_name == 'tprate' or self.var_name == 'mtpr':
            self.data = self.fix_precipitation_units(self.data, self.var_name, 'mm/day')
            if self.data_ref is not None:
                self.data_ref = self.fix_precipitation_units(self.data_ref, self.var_name, 'mm/day')

        # Select pressure level if necessary
        if self.plev is not None:
            self.select_pressure_level(self.data, self.plev, self.var_name)
            if self.data_ref is not None:
                self.select_pressure_level(self.data_ref, self.plev, self.var_name)
        
        # Plot a single map if only one dataset is provided
        if self.data_ref is None:
            plot_single_map(self.data[self.var_name].mean(dim='time'))
        else:
            # Plot the bias map if two datasets are provided
            plot_single_map_diff(data=self.data[self.var_name].mean(dim='time'), data_ref=self.data_ref[self.var_name].mean(dim='time'))
        
        # Plot seasonal biases if seasons is True
        if self.seasons:
            season_list = ['DJF', 'MAM', 'JJA', 'SON']
            stat_funcs = {'mean': 'mean', 'max': 'max', 'min': 'min', 'std': 'std'}
            
            if self.seasons_stat not in stat_funcs:
                raise ValueError("Invalid statistic. Please choose one between 'mean', 'std', 'max', 'min'.")

            seasonal_data = []
            for season in season_list:
                data_season = select_season(self.data[self.var_name], season)
                data_ref_season = select_season(self.data_ref[self.var_name], season)
                seasonal_diff = getattr(data_season, stat_funcs[self.seasons_stat])(dim='time') - \
                                getattr(data_ref_season, stat_funcs[self.seasons_stat])(dim='time')
                seasonal_data.append(seasonal_diff.compute())  # Force computation here

            plot_maps(seasonal_data, titles=season_list)
        
        if self.vertical:
            self.vertical_bias(data=self.data, data_ref=self.data_ref, var_name=self.var_name,
            plev_min=None, plev_max=None, vmin=None, vmax=None)

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
            if plev:
                try:
                    data[var_name] = data[var_name].sel(plev=plev)
                except KeyError:
                    raise NoDataError("The provided value of pressure level is absent in the dataset. Please try again.")
            else:
                raise NoDataError(f"The dataset for {var_name} variable has a 'plev' coordinate, but None is provided. Please try again.")

    def vertical_bias(self, data=None, data_ref=None, var_name=None, 
                      plev_min=None, plev_max=None, vmin=None, vmax=None):
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

        # Plotting the zonal bias
        plt.figure(figsize=(10, 8))
        cax = plt.contourf(zonal_bias['lat'], zonal_bias['plev'], zonal_bias, cmap='RdBu_r', levels=levels, extend='both')
        plt.title(f'Zonal Bias of {var_name}')
        plt.yscale('log')
        plt.ylabel('Pressure Level (Pa)')
        plt.xlabel('Latitude')
        plt.gca().invert_yaxis()
        plt.colorbar(cax, label=f'{var_name} [{data[var_name].units}]')
        plt.grid(True)

    @staticmethod
    def boxplot(datasets=None, model_names=None, variables=None):
        """
        Generate a boxplot showing the uncertainty of a global variable
        for different models.

        Args:
            datasets (list of xarray.Dataset): A list of xarray Datasets to be plotted 
            model_names (list of str): Your desired naming for the plotting, corresponding to the datasets.
            variables (list of str): List of variables to be plotted
        """

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

        # Create a DataFrame from the boxplot_data dictionary
        boxplot_df = pd.DataFrame(boxplot_data)
        ax = sns.boxplot(x='Variables', y='Values', hue='Datasets', data=boxplot_df)

        plt.xlabel('Variables', fontsize=fontsize)
        plt.ylabel(f'Global mean ({units})', fontsize=fontsize)  # Use units retrieved from the dataset
        plt.xticks(rotation=0, fontsize=fontsize - 2)
        plt.yticks(fontsize=fontsize - 2)
        plt.title("Global mean radiation for different models", fontsize=fontsize + 2)

        model_names_with_dates = [f"{name} ({pd.to_datetime(dataset[variable]['time'].values).min().strftime('%d-%m-%Y')} to {pd.to_datetime(dataset[variable]['time'].values).max().strftime('%d-%m-%Y')})" for name, dataset in zip(model_names, datasets)]
        handles, labels = ax.get_legend_handles_labels()
        ax.legend(handles, model_names_with_dates, loc='center left', bbox_to_anchor=(1, 0.5), title='Datasets', fontsize=fontsize - 2)
