"""
Calculating Trends
"""

from .tools import *
from ocean3d import split_ocean3d_req
import pandas as pd

class TrendCalculator:
    @staticmethod
    def create_time_array(data, loglevel="WARNING"):
        """
        Create an array representing time indices.

        Parameters:
            y_array (xarray.DataArray): Input array containing time coordinate.
            loglevel (str, optional): Log level for logging messages. Defaults to "WARNING".

        Returns:
            dask.array: Array representing time indices.
        """
        logger = log_configure(loglevel, 'lintrend_3D')
        logger.debug("Creating Array representing time indices")
        
        # Create time indices as a Dask array
        time_indices = da.from_array(np.arange(1, len(data["time"]) + 1), chunks=len(data["time"]))
        
        time_indices = xr.DataArray(
            time_indices,
            dims=["time"],
            coords={"time": data["time"]},
            name="time_indices"
        )

        # Broadcast the time indices to match the shape of the input array lazily
        time_array = xr.broadcast(time_indices, data)[0]
        
        # Mask NaN values lazily
        time_array = time_array.where(~np.isnan(data), np.nan)
        logger.debug("Finished creating Array representing time indices")
        return time_array

    @staticmethod
    def filter_trend(trend, n, y_array, loglevel="WARNING"):
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
        logger = log_configure(loglevel, "filter_trend")
        logger.debug("Starting trend filtering")

        # Filter `n` and `trend` in a single step
        trend_filtered = xr.where(n >= 3, trend, np.nan)
        
        # Convert to xarray.DataArray with coordinates
        logger.debug("Converting filtered trends to xarray.DataArray")
        trend_da = xr.DataArray(
            trend_filtered,
            coords={
                "lev": y_array.lev,
                "lat": y_array.lat,
                "lon": y_array.lon
            },
            name=f"{y_array.name} trends",
            dims=["lev", "lat", "lon"]
        )
        logger.debug("Trend filtering completed")
        return trend_da


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
        
        trend.attrs['units'] = f"{trend.attrs['units']}/year"
            
        
        # trend.attrs['units'] = f"{y_array.units}/year"
        
        return trend

    @staticmethod
    def compute_covariances(x_array, data, dim="time", loglevel= "warning"):
        """
        Compute covariances for all matching variables in two xarray.Datasets.

        Parameters:
            x_array (xarray.Dataset): First dataset containing variables.
            data (xarray.Dataset): Second dataset containing variables.
            dim (str): Dimension along which to compute covariance.

        Returns:
            xarray.Dataset: A dataset containing covariance for each variable.
        """
        covariance_dict = {}

        # Iterate over variables in x_array
        for var in x_array.data_vars:
            covariance_dict[var] = xr.cov(x_array[var], data[var], dim=dim)
            covariance_dict[var].attrs = data[var].attrs
            covariance_dict[var] = TrendCalculator.adjust_trend_for_time_frequency(covariance_dict[var], data[var], loglevel= loglevel)
        # Merge covariances into a   single dataset
        covariance_ds = xr.Dataset(covariance_dict)
        return covariance_ds
    
    def lintrend_3D(y_array, loglevel="WARNING"):
        """
        Compute the trend values for a 3D variable.

        Parameters:
            y_array (xarray.DataArray): Input array containing the variable of interest.
            loglevel (str, optional): Log level for logging messages. Defaults to "WARNING".

        Returns:
            xarray.DataArray: Trend values for the 3D variable.
        """
        logger = log_configure(loglevel, 'lintrend_3D')
        x_array = TrendCalculator.create_time_array(y_array, loglevel=loglevel)
        n = x_array.count(dim="time")
        x_var = x_array.var(dim="time", skipna=True)
        covariance = TrendCalculator.compute_covariances(x_array, y_array, dim="time", loglevel=loglevel)
        trend = covariance/x_var
        trend = xr.where(n >= 3, trend, np.nan)
        return trend
    
    def chunking_trend(data, loglevel="WARNING"):
        lon_chunks = [
            slice(0, 60), slice(60, 120), slice(120, 180), slice(180, 240), slice(240, 300), slice(300, 360)
            ]

        # Define finer latitude chunks (every 45 degrees)
        lat_chunks = [
            slice(0, 45), slice(45, 90), slice(90, 135), slice(135, 180)
            ]

        for lon in lon_chunks:
            for lat in lat_chunks:
                subset = data.isel(lon=lon, lat=lat)
                print(f"exporing file: subset_{lon.start}_{lat.start}.nc")
                subset.to_netcdf(f"subset_{lon.start}_{lat.start}.nc")

        data = xr.open_mfdataset("subset_*.nc", combine="by_coords")
        return data

    def trend_from_polyfit(data, loglevel="WARNING"):
        """
        Calculate the linear trend (slope) from a dataset along the time dimension.

        Parameters:
            data (xarray.Dataset): Input dataset with variables to calculate trends.
            loglevel (str): Logging level for debugging. Default is "WARNING".

        Returns:
            xarray.Dataset: Dataset containing the linear trends (slopes) for each variable.
        """
        # Initialize the logger (assuming log_configure is defined elsewhere)
        logger = log_configure(loglevel, 'trend_from_polyfit')

        logger.debug("Starting trend calculation")

        trend_dict = {}

        # Iterate over variables in the input dataset
        for var in data.data_vars:
            logger.debug(f"Calculating trend for {var}")
            # Perform the polyfit to calculate the trend (slope)
            poly_coeffs = data.polyfit(dim="time", deg=1)
            
            # Extract the trend (degree=0) for the variable
            trend_dict[var] = poly_coeffs[f"{var}_polyfit_coefficients"].sel(degree=0)
            trend_dict[var].attrs = data[var].attrs
            # Apply necessary adjustments for time frequency if needed (optional)
            trend_dict[var] = TrendCalculator.adjust_trend_for_time_frequency(trend_dict[var], data[var], loglevel=loglevel)
            
            logger.debug(f"Trend for {var} calculated successfully")

        # Merge trends into a single dataset
        trend_ds = xr.Dataset(trend_dict)

        logger.info("Trend dataset created successfully")

        return trend_ds

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
        # logger.warning("decreasing the resolution of data to bypass the memory error issue for big data")

        logger.debug("Calculating linear trend")
        TS_3dtrend_data = TrendCalculator.lintrend_3D(data, loglevel= loglevel)
        # TS_3dtrend_data = TrendCalculator.trend_from_polyfit(data, loglevel= loglevel)
        TS_3dtrend_data.attrs = data.attrs
        # TS_3dtrend_data = TrendCalculator.chunking_trend(TS_3dtrend_data, loglevel= loglevel)
        logger.debug("Trend value calculated")
        # TS_3dtrend_data = TS_3dtrend_data.coarsen(lat=2, lon=2, boundary="trim").mean()
        return TS_3dtrend_data


class multilevel_trend:
    def __init__(self, o3d_request):
        split_ocean3d_req(self, o3d_request)

    def plot(self):
        
        self.data = area_selection(self.data, self.region, self.lat_s, self.lat_n, self.lon_w, self.lon_e)
        TS_trend_data = TrendCalculator.TS_3dtrend(self.data, loglevel=self.loglevel)
        self._plot_multilevel_trend(TS_trend_data)
        return

    def _plot_multilevel_trend(self, data):
        """
        Plots the multilevel trend for temperature and salinity.
        """
        self._define_levels()
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
        data = data.interp(lev=self.levels).persist()
        # print("export_trend")
        # data.to_zarr("test.zarr", consolidated=True)
        # print("exported_trend")
        for levs in range(len(self.levels)):
            subset_data = data.sel(lev=self.levels[levs])
            subset_data["avg_thetao"].plot.contourf(cmap="coolwarm", ax=axs[levs, 0], levels=18)
            subset_data["avg_so"].plot.contourf(cmap="coolwarm", ax=axs[levs, 1], levels=18)
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
        self.data = area_selection(self.data, self.region, self.lat_s, self.lat_n, self.lon_w, self.lon_e)
        # Compute the trend data
        TS_trend_data = TrendCalculator.TS_3dtrend(self.data, loglevel=self.loglevel)
        TS_trend_data.attrs = self.data.attrs
        data = TS_trend_data

        # Compute the weighted zonal mean
        data = weighted_zonal_mean(data, self.region, self.lat_s, self.lat_n, self.lon_w, self.lon_e)
        data = data.compute()
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