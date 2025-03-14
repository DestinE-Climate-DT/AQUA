import pandas as pd
import xarray as xr
from aqua.logger import log_configure
from aqua.util import ConfigPath, OutputSaver
from aqua.util import create_folder, select_season
from aqua.util import evaluate_colorbar_limits, ticks_round
from aqua.graphics import plot_single_map, plot_single_map_diff, plot_maps
from .util import select_pressure_level


class PlotGlobalBiases: 
    def __init__(self, data=None, data_ref=None, 
                 var=None, plev=None,
                 model=None, exp=None, model_ref=None, 
                 save_pdf=True, 
                 save_png=True, dpi=300,
                 loglevel='WARNING'):
        """
        Initialize the PlotGlobalBiases class
        """
        self.data = data 
        self.data_ref = data_ref
        self.var = var
        self.plev = plev

        self.model = model
        self.exp = exp
        self.startdate = pd.to_datetime(self.data.time[0].values).strftime('%Y-%m-%d')
        self.enddate = pd.to_datetime(self.data.time[-1].values).strftime('%Y-%m-%d')
        self.model_ref = model_ref
        self.startdate_ref = pd.to_datetime(self.data_ref.time[0].values).strftime('%Y-%m-%d')
        self.enddate_ref = pd.to_datetime(self.data_ref.time[-1].values).strftime('%Y-%m-%d')

        self.save_pdf = save_pdf
        self.save_png = save_png
        self.dpi = dpi

        self.logger = log_configure(log_level=loglevel, log_name='Global Biases')

    def plot_bias(self, stat='mean', var=None, plev=None, vmin=None, vmax=None):
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

            self.var = var or self.var
            self.plev = plev or self.plev

            # Check if the variable has pressure levels but no specific level is selected
            if 'plev' in self.data.get(self.var, {}).dims:
                if self.plev is None:
                    self.logger.warning(
                        f"Variable {self.var} has multiple pressure levels, but no specific level was selected. "
                        "Skipping 2D bias plotting."
                    )
                    return None  # Return None for both fig and ax

                # If a pressure level is specified, select it
                self.logger.info(f"Selecting pressure level {self.plev} for variable {self.var}.")
                self.data = select_pressure_level(self.data, self.plev, self.var)
                self.data_ref = select_pressure_level(self.data_ref, self.plev, self.var)

            # If a pressure level is specified but the variable has no pressure levels
            elif self.plev is not None:
                self.logger.warning(f"Variable {self.var} does not have pressure levels!")

            # Plot a single map if only one dataset is provided 
            if self.data_ref is None:
                self.logger.warning('Plotting single dataset map since no reference dataset is provided.')

                title = (f"{self.var} map {self.model} {self.exp} {self.startdate}/{self.enddate}" 
                        + (f" at {int(self.plev / 100)} hPa" if self.plev else ""))

                fig, ax = plot_single_map(self.data[self.var].mean(dim='time'), 
                                        return_fig=True, 
                                        title=title,
                                        vmin=vmin, vmax=vmax)
                ax.set_xlabel("Longitude")
                ax.set_ylabel("Latitude")
                bias = self.data[self.var].mean(dim='time')
            else:
                # Plot the bias map if two datasets are provided
                self.logger.info('Plotting bias map between two datasets.')

                # Set 'sym' to True if either 'vmin' or 'vmax' is None, indicating a symmetric colorbar.
                sym = vmin is None or vmax is None

                title = (f"{self.var} global bias of {self.model} {self.exp} {self.startdate}/{self.enddate}\n"
                        f"relative to {self.model_ref} climatology {self.startdate_ref}/{self.enddate_ref}"
                        + (f" at {int(self.plev / 100)} hPa" if self.plev else ""))

                fig, ax = plot_single_map_diff(data=self.data[self.var].mean(dim='time'), 
                                            data_ref=self.data_ref[self.var].mean(dim='time'),
                                            return_fig=True,
                                            contour=True, 
                                            title=title,
                                            sym=sym,
                                            vmin_fill=vmin, vmax_fill=vmax)
                ax.set_xlabel("Longitude")
                ax.set_ylabel("Latitude")
                bias = self.data[self.var].mean(dim='time') - self.data_ref[self.var].mean(dim='time')

            return fig, ax, bias

    def plot_seasonal_bias(self, seasons_stat='mean', var=None, plev=None, vmin=None, vmax=None):
            """
            Plots seasonal biases for each season (DJF, MAM, JJA, SON) and returns an xarray.Dataset
            containing the calculated seasonal biases.

            Args:
                seasons_stat (str): Statistic for seasonal analysis ('mean' by default).
                vmin (float, optional): Minimum colorbar value.
                vmax (float, optional): Maximum colorbar value.

            Returns:
                tuple: Matplotlib figure, axis objects, and an xarray.Dataset of the calculated seasonal biases.
            """

            self.var = var or self.var
            self.plev = plev or self.plev

            self.logger.info('Plotting seasonal biases.')

            # Set 'sym' to True if either 'vmin' or 'vmax' is None, indicating a symmetric colorbar.
            sym = vmin is None or vmax is None
            
            # Check if the variable has pressure levels but no specific level is selected
            if 'plev' in self.data.get(self.var, {}).dims:
                if self.plev is None:
                    self.logger.warning(
                        f"Variable {self.var} has multiple pressure levels, but no specific level was selected. "
                        "Skipping 2D bias plotting."
                    )
                    return None  # Return None for both fig and ax

                # If a pressure level is specified, select it
                self.logger.info(f"Selecting pressure level {self.plev} for variable {self.var}.")
                self.data = select_pressure_level(self.data, self.plev, self.var)
                self.data_ref = select_pressure_level(self.data_ref, self.plev, self.var)

            # If a pressure level is specified but the variable has no pressure levels
            elif self.plev is not None:
                self.logger.warning(f"Variable {self.var} does not have pressure levels!")

            # Validate seasons_stat
            stat_funcs = {'mean': 'mean', 'max': 'max', 'min': 'min', 'std': 'std'}
            if seasons_stat not in stat_funcs:
                raise ValueError("Invalid statistic. Please choose one of 'mean', 'std', 'max', 'min'.")

            # Prepare seasonal data and compute biases
            season_list = ['DJF', 'MAM', 'JJA', 'SON']
            seasonal_biases = {}

            for season in season_list:
                # Select season for both data and reference
                data_season = select_season(self.data[self.var], season)
                data_ref_season = select_season(self.data_ref[self.var], season)

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

            fig, ax = plot_maps(**plot_kwargs)

            return fig, ax, bias_dataset


    def plot_vertical_bias(self, var=None, plev_min=None, plev_max=None, vmin=None, vmax=None):
            """
            Calculates and plots the vertical bias between two datasets.

            Args:
                var (str, optional): Variable name to analyze.
                plev_min (float, optional): Minimum pressure level.
                plev_max (float, optional): Maximum pressure level.
                vmin (float, optional): Minimum colorbar value.
                vmax (float, optional): Maximum colorbar value.

            Returns:
                tuple: Matplotlib figure, axis objects, and xarray Dataset of the calculated bias.
            """

            self.logger.info('Plotting vertical biases.')

            var = var or self.var

            # Compute climatology for reference dataset
            ref_climatology = data_ref[var].mean(dim='time')

            # Calculate the bias between the two datasets
            bias = data[var] - ref_climatology

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

            title = (f"{var} vertical bias of {self.model} {self.exp} {self.startdate_data}/{self.enddate_data}\n" 
                f"relative to {self.model_obs} climatology {self.startdate_obs}/{self.enddate_obs}\n")

            # Plotting the zonal bias
            fig, ax = plt.subplots(figsize=(10, 8))
            cax = ax.contourf(zonal_bias['lat'], zonal_bias['plev'], zonal_bias, cmap='RdBu_r', levels=levels, extend='both')
            ax.set_title(title)
            ax.set_yscale('log')
            ax.set_ylabel('Pressure Level (Pa)')
            ax.set_xlabel('Latitude')
            ax.invert_yaxis()
            fig.colorbar(cax, ax=ax, label=f'{var} [{data[var].attrs.get("units", "")}]')
            ax.grid(True)

            return fig, ax, zonal_bias