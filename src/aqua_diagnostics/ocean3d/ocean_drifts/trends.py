"""
Calculating Trends
"""

from .tools import *
from ocean3d import split_ocean3d_req
import pandas as pd

class TrendCalculator:
    @staticmethod
    def create_time_array(y_array, loglevel= "WARNING"):
        """
        Create an array representing time indices.

        Parameters:
            y_array (xarray.DataArray): Input array containing time coordinate.
            loglevel (str, optional): Log level for logging messages. Defaults to "WARNING".

        Returns:
            dask.array: Array representing time indices.
        """
        x_array = da.empty_like(y_array)
        for i in range(len(y_array["time"])):
            x_array[i, :, :, :] = i + 1
        x_array = da.where(da.isnan(y_array), np.nan, x_array)
        return x_array

    @staticmethod
    def compute_non_nan_count(x_array, loglevel= "WARNING"):
        """
        Compute the count of non-NaN values along the time dimension.

        Parameters:
            x_array (dask.array): Array containing time indices.
            loglevel (str, optional): Log level for logging messages. Defaults to "WARNING".

        Returns:
            dask.array: Count of non-NaN values along the time dimension.
        """
        return da.sum(~da.isnan(x_array), axis=0)

    @staticmethod
    def compute_trend(y_array, x_array, n, loglevel= "WARNING"):
        """
        Compute the trend values.

        Parameters:
            y_array (xarray.DataArray): Input array containing the variable of interest.
            x_array (dask.array): Array representing time indices.
            n (dask.array): Count of non-NaN values along the time dimension.
            loglevel (str, optional): Log level for logging messages. Defaults to "WARNING".

        Returns:
            dask.array: Trend values.
        """
        x_mean = da.nanmean(x_array, axis=0)
        y_mean = da.nanmean(y_array, axis=0)
        x_var = da.nanvar(x_array, axis=0)
        cov = da.nansum((x_array - x_mean) * (y_array - y_mean), axis=0) / n
        trend = cov / (x_var)
        return trend

    @staticmethod
    def filter_trend(trend, n, y_array, loglevel= "WARNING"):
        """
        Filter the trend values.

        Parameters:
            trend (dask.array): Trend values.
            n (dask.array): Count of non-NaN values along the time dimension.
            y_array (xarray.DataArray): Input array containing the variable of interest.
            loglevel (str, optional): Log level for logging messages. Defaults to "WARNING".

        Returns:
            xarray.DataArray: Filtered trend values.
        """
        n = n.astype(float)
        n = da.where(n < 3, np.nan, n)
        trend = da.where(da.isnan(n), np.nan, trend)
        trend = xr.DataArray(trend, coords={"lev": y_array.lev, "lat": y_array.lat,
                            "lon": y_array.lon}, name=f"{y_array.name} trends", dims=["lev", "lat", "lon"])
        return trend

    @staticmethod
    def adjust_trend_for_time_frequency(trend, y_array, loglevel= "WARNING"):
        """
        Adjust the trend values based on time frequency.

        Parameters:
            trend (dask.array): Trend values.
            y_array (xarray.DataArray): Input array containing the variable of interest.
            loglevel (str, optional): Log level for logging messages. Defaults to "WARNING".

        Returns:
            dask.array: Trend values adjusted for time frequency.
        """
        time_frequency = y_array["time"].to_index().inferred_freq
        
        if time_frequency == None:
            time_index = pd.to_datetime(y_array["time"].values)
            time_diffs = time_index[1:] - time_index[:-1]
            is_monthly = all(time_diff.days >= 28 for time_diff in time_diffs)
            if is_monthly == True:
                time_frequency= "MS"
            else:
                raise ValueError(f"The frequency of the data must be in Daily/Monthly/Yearly")
                
            
        if time_frequency == "MS":
            trend = trend * 12
        elif time_frequency == "H":
            trend = trend * 24 * 30 * 12
        elif time_frequency == "Y" or 'YE-DEC':
            trend = trend
        else:
            raise ValueError(f"The frequency: {time_frequency} of the data must be in Daily/Monthly/Yearly")
        return trend


    def lintrend_3D(y_array, loglevel="WARNING"):
        """
        Compute the trend values for a 3D variable.

        Parameters:
            y_array (xarray.DataArray): Input array containing the variable of interest.
            loglevel (str, optional): Log level for logging messages. Defaults to "WARNING".

        Returns:
            xarray.DataArray: Trend values for the 3D variable.
        """
        x_array = TrendCalculator.create_time_array(y_array)
        n = TrendCalculator.compute_non_nan_count(x_array)
        trend = TrendCalculator.compute_trend(y_array, x_array, n)
        trend = TrendCalculator.filter_trend(trend, n, y_array)
        trend = TrendCalculator.adjust_trend_for_time_frequency(trend, y_array)
        trend.attrs['units'] = f"{y_array.units}/year"
        return trend


    def TS_3dtrend(data, loglevel= "WARNING"):
        """
        Compute the trend values for temperature and salinity variables in a 3D dataset.

        Parameters:
            data (xarray.Dataset): Input dataset containing temperature (avg_thetao) and salinity (avg_so) variables.
            loglevel (str, optional): Log level for logging messages. Defaults to "WARNING".

        Returns:
            xarray.Dataset: Dataset with trend values for temperature and salinity variables.
        """
        logger = log_configure(loglevel, 'TS_3dtrend')
        TS_3dtrend_data = xr.Dataset()

        avg_so = TrendCalculator.lintrend_3D(data.avg_so)
        avg_thetao = TrendCalculator.lintrend_3D(data.avg_thetao)

        TS_3dtrend_data = TS_3dtrend_data.merge({"avg_thetao": avg_thetao, "avg_so": avg_so})

        logger.debug("Trend value calculated")
        return TS_3dtrend_data


class multilevel_trend:
    def __init__(self, o3d_request):
        split_ocean3d_req(self, o3d_request)

    def plot(self):
        data = self._select_data()
        TS_trend_data = self._calculate_trend_data(data)
        self._plot_multilevel_trend(TS_trend_data)
        return

    def _select_data(self):
        """
        Selects the relevant data for trend calculation.
        """
        data = area_selection(self.data, self.region, self.lat_s, self.lat_n, self.lon_w, self.lon_e)
        return data

    def _calculate_trend_data(self, data):
        """
        Calculates the trend data for temperature and salinity.
        """
        TS_trend_data = TrendCalculator.TS_3dtrend(data, loglevel=self.loglevel)
        TS_trend_data.attrs = data.attrs
        return TS_trend_data

    def _plot_multilevel_trend(self, data):
        """
        Plots the multilevel trend for temperature and salinity.
        """
        levels = self._define_levels()
        fig, axs = self._create_subplot_fig(len(self.levels))
        self._plot_contourf(data, axs)
        self._format_plot_axes(axs)
        self._add_plot_title()
        if self.output:
            self._save_plot_data(data)
        return

    def _define_levels(self):
        """
        Defines the levels for plotting.
        """
        if self.customise_level:
            if self.levels is None:
                raise ValueError("Custom levels are selected, but levels are not provided.")
        else:
            self.levels = [10, 100, 500, 1000, 3000, 5000]
        return

    def _create_subplot_fig(self, num_levels):
        """
        Creates subplot figure for the multilevel trend plot.
        """
        dim1 = 16
        dim2 = 5 * num_levels
        fig, axs = plt.subplots(nrows=num_levels, ncols=2, figsize=(dim1, dim2))
        fig.subplots_adjust(hspace=0.18, wspace=0.15, top=0.95)
        return fig, axs

    def _plot_contourf(self, data, axs):
        """
        Plots contourf for temperature and salinity at different levels.
        """
        for levs in range(len(self.levels)):
            data["avg_thetao"].interp(lev=self.levels[levs]).plot.contourf(cmap="coolwarm", ax=axs[levs, 0], levels=18)
            data["avg_so"].interp(lev=self.levels[levs]).plot.contourf(cmap="coolwarm", ax=axs[levs, 1], levels=18)
            axs[levs, 0].set_facecolor('grey')
            axs[levs, 1].set_facecolor('grey')
        return

    def _format_plot_axes(self, axs):
        """
        Formats plot axes.
        """
        for levs in range(len(self.levels)):
            axs[levs, 0].set_ylabel("Latitude (in deg North)", fontsize=9)
            axs[levs, 1].set_yticklabels([])
            if levs == (len(self.levels)-1):
                axs[levs, 0].set_xlabel("Longitude (in de East)", fontsize=12)
                axs[levs, 0].set_ylabel("Latitude (in deg North)", fontsize=12)
            if levs != (len(self.levels)-1):
                axs[levs, 0].set_xticklabels([])
                axs[levs, 1].set_xticklabels([])
        return

    def _add_plot_title(self):
        """
        Adds title to the plot.
        """
        region_title = custom_region(region=self.region, lat_s=self.lat_s, lat_n=self.lat_n, lon_w=self.lon_w, lon_e=self.lon_e)
        self.title = f'Linear Trends of T,S at different depths in the {region_title}'
        plt.suptitle(self.title, fontsize=24)
        return

    def _save_plot_data(self, data):
        """
        Saves plot data.
        """
        filename = file_naming(self.region, self.lat_s, self.lat_n, self.lon_w, self.lon_e, plot_name=f"{self.model}-{self.exp}-{self.source}_multilevel_t_s_trend")
        write_data(self.output_dir, filename, data.interp(lev=self.levels[-1]))
        export_fig(self.output_dir, filename, "pdf", metadata_value=self.title, loglevel=self.loglevel)
        return

class zonal_mean_trend:
    def __init__(self, o3d_request):
        """
        Initialize the ZonalMeanTrend object.

        Parameters:
            o3d_request: The request object containing the data.
        """
        split_ocean3d_req(self, o3d_request)

    def plot(self):
        """
        Plot the zonal mean trends for temperature and salinity.

        Returns:
            None
        """
        # Compute the trend data
        TS_trend_data = TrendCalculator.TS_3dtrend(self.data, loglevel=self.loglevel)
        TS_trend_data.attrs = self.data.attrs
        data = TS_trend_data

        # Compute the weighted zonal mean
        data = weighted_zonal_mean(data, self.region, self.lat_s, self.lat_n, self.lon_w, self.lon_e)

        # Create the plot
        fig, axs = plt.subplots(nrows=1, ncols=2, figsize=(14, 5))

        # Plot temperature
        data.avg_thetao.plot.contourf(levels=20, ax=axs[0])
        axs[0].set_ylim((5500, 0))
        axs[0].set_title("Temperature", fontsize=14)
        axs[0].set_ylabel("Depth (in m)", fontsize=9)
        axs[0].set_xlabel("Latitude (in deg North)", fontsize=9)
        axs[0].set_facecolor('grey')

        # Plot salinity
        data.avg_so.plot.contourf(levels=20, ax=axs[1])
        axs[1].set_ylim((5500, 0))
        axs[1].set_title("Salinity", fontsize=14)
        axs[1].set_ylabel("Depth (in m)", fontsize=12)
        axs[1].set_xlabel("Latitude (in deg North)", fontsize=12)
        axs[1].set_facecolor('grey')

        # Set the title
        region_title = custom_region(region=self.region, lat_s=self.lat_s, lat_n=self.lat_n,
                                     lon_w=self.lon_w, lon_e=self.lon_e)
        title = f"Zonally-averaged long-term trends in the {region_title}"
        fig.suptitle(title, fontsize=20)
        plt.subplots_adjust(top=0.85)

        # Save the plot if output is enabled
        if self.output:
            filename = file_naming(self.region, self.lat_s, self.lat_n, self.lon_w, self.lon_e,
                                   plot_name=f"{self.model}-{self.exp}-{self.source}_zonal_mean_trend")
            write_data(self.output_dir, filename, data)
            export_fig(self.output_dir, filename, "pdf", metadata_value=title, loglevel=self.loglevel)

        return