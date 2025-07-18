"""Module for computing ocean temperature and salinity trends using xarray and AQUA diagnostics."""

import xarray as xr
import pandas as pd
from aqua.logger import log_configure
from aqua.diagnostics.core import Diagnostic
from aqua.reader import Trender


xr.set_options(keep_attrs=True)


class Trends(Diagnostic):
    def __init__(
        self,
        catalog: str = None,
        model: str = None,
        exp: str = None,
        source: str = None,
        regrid: str = None,
        startdate: str = None,
        enddate: str = None,
        loglevel: str = "WARNING",
    ):
        """
        Initializes the class with configuration parameters for ocean trend diagnostics.

        Parameters:
            catalog (str, optional): Path or identifier for the data catalog. Defaults to None.
            model (str, optional): Name of the model to use. Defaults to None.
            exp (str, optional): Experiment identifier. Defaults to None.
            source (str, optional): Data source name. Defaults to None.
            regrid (str, optional): Regridding method or target grid. Defaults to None.
            startdate (str, optional): Start date for the analysis period (format: 'YYYY-MM-DD'). Defaults to None.
            enddate (str, optional): End date for the analysis period (format: 'YYYY-MM-DD'). Defaults to None.
            loglevel (str, optional): Logging level (e.g., 'WARNING', 'INFO'). Defaults to "WARNING".

        Sets up the logger and passes configuration to the parent class.
        """
        super().__init__(
            catalog=catalog,
            model=model,
            exp=exp,
            source=source,
            regrid=regrid,
            startdate=startdate,
            enddate=enddate,
            loglevel=loglevel,
        )
        self.logger = log_configure(log_name="Trends", log_level=loglevel)

    def run(
        self,
        outputdir: str = ".",
        rebuild: bool = True,
        region: str = None,
        var: list = ["thetao", "so"],
        dim_mean: type = None,
    ):
        """
        Executes the trend analysis workflow for ocean diagnostics.

        This method retrieves the specified variables, selects the appropriate region,
        optionally averages the data over a given dimension, computes the trend coefficients,
        and saves the results to a NetCDF file.

        Args:
            outputdir (str, optional): Directory where output files will be saved. Defaults to ".".
            rebuild (bool, optional): Whether to overwrite existing output files. Defaults to True.
            region (str, optional): Name of the region to analyze. If None, uses the default region.
            var (list, optional): List of variable names to retrieve. Defaults to ["thetao", "so"].
            dim_mean (type, optional): Dimension over which to average the data. If None, no averaging is performed.

        Returns:
            None
        """
        super().retrieve(var=var)
        region, lon_limits, lat_limits = super().select_region(
            diagnostic="ocean3d", region=region
        )

        if dim_mean:
            self.data = self.data.mean(dim=dim_mean, keep_attrs=True)
        self.trend_coef = self.compute_trend(data=self.data, loglevel=self.loglevel)
        self.save_netcdf(outputdir=outputdir, rebuild=rebuild, region=region)

    def adjust_trend_for_time_frequency(self, trend, y_array, loglevel="WARNING"):
        """
        Adjusts trend values to represent changes per year based on the time frequency of the input data.

        This method rescales the computed trend to an annual rate, depending on whether the input data is monthly, hourly, or yearly.
        If the time frequency cannot be inferred, it attempts to deduce if the data is monthly based on time differences.
        The method also updates the units attribute of the trend to reflect the annualized rate.

            trend (dask.array): The trend values to be adjusted. Should have an 'attrs' dictionary for units.
            y_array (xarray.DataArray): DataArray containing the variable of interest and a 'time' coordinate.
            loglevel (str, optional): Logging level for messages. Defaults to "WARNING".

            dask.array: The trend values adjusted to an annual rate, with updated units.

        Raises:
            ValueError: If the time frequency cannot be determined or is not supported (not daily, monthly, or yearly).
        """
        time_frequency = y_array["time"].to_index().inferred_freq

        if time_frequency == None:
            time_index = pd.to_datetime(y_array["time"].values)
            time_diffs = time_index[1:] - time_index[:-1]
            is_monthly = all(time_diff.days >= 28 for time_diff in time_diffs)
            if is_monthly:
                time_frequency = "MS"
            else:
                raise ValueError(
                    f"The frequency of the data must be in Daily/Monthly/Yearly"
                )

        if time_frequency == "MS":
            trend = trend * 12
        elif time_frequency == "H":
            trend = trend * 24 * 30 * 12
        elif time_frequency in ("Y", "YE-DEC"):
            trend = trend
        else:
            raise ValueError(
                f"The frequency: {time_frequency} of the data must be in Daily/Monthly/Yearly"
            )

        units = trend.attrs.get("units", "")
        trend.attrs["units"] = f"{units}/year" if units else "per year"
        return trend

    def compute_trend(self, data, loglevel="WARNING"):
        """
        Computes the linear trend of the input data along the time dimension.

        This method calculates the linear trend coefficients for each variable in the provided
        xarray Dataset, normalizes the trend, and adjusts it according to the time frequency.
        The resulting trends are returned as a new xarray Dataset with the same attributes as
        the input data.

        Parameters
        ----------
        data : xarray.Dataset
            The input dataset containing variables with a 'time' dimension over which the trend
            will be computed.
        loglevel : str, optional
            The logging level to use for diagnostic messages (default is "WARNING").

        Returns
        -------
        xarray.Dataset
            A dataset containing the linear trend for each variable in the input data, with
            attributes preserved.

        Notes
        -----
        The trend is computed using the `Trender` class and is normalized. The method also
        adjusts the trend values based on the time frequency of the input data.
        """

        logger = log_configure(loglevel, "TS_3dtrend")
        logger.debug("Calculating linear trend")

        trend_init = Trender()
        trend_data = trend_init.coeffs(data, dim="time", skipna=True, normalize=True)
        trend_data = trend_data.sel(degree=1)
        trend_data.attrs = data.attrs
        trend_dict = {}
        for var in data.data_vars:
            trend_dict[var] = self.adjust_trend_for_time_frequency(
                trend_data[var], data, loglevel=loglevel
            )
            trend_dict[var].attrs = data[var].attrs
        trend_data = xr.Dataset(trend_dict)
        logger.debug("Trend value calculated")
        return trend_data

    def save_netcdf(
        self,
        diagnostic: str = "trends",
        diagnostic_product: str = "spatial_trend",
        region: str = None,
        outputdir: str = ".",
        rebuild: bool = True,
    ):
        """
        Saves the trend coefficients to a NetCDF file.

        Parameters:
            diagnostic (str): The name of the diagnostic to save. Default is "trends".
            diagnostic_product (str): The type of diagnostic product. Default is "spatial_trend".
            region (str, optional): The name of the region to include in the output. If None, no region is specified.
            outputdir (str): The directory where the NetCDF file will be saved. Default is the current directory.
            rebuild (bool): Whether to rebuild the NetCDF file if it already exists. Default is True.

        Returns:
            None

        Notes:
            This method calls the parent class's `save_netcdf` method, passing the trend coefficients and additional metadata.
        """
        super().save_netcdf(
            diagnostic=diagnostic,
            diagnostic_product=diagnostic_product,
            outdir=outputdir,
            rebuild=rebuild,
            data=self.trend_coef,
            extra_keys={"region": region.replace(" ", "_") if region else None},
        )
