"""Module for computing trends using xarray."""

import xarray as xr
import pandas as pd
from aqua.logger import log_configure
from aqua.diagnostics.core import Diagnostic
from aqua.reader import Trender

xr.set_options(keep_attrs=True)


class Trends(Diagnostic):
    """Class to compute trends over time."""
    def __init__(
        self,
        model: str,
        exp: str,
        source: str,
        catalog: str = None,
        regrid: str = None,
        startdate: str = None,
        enddate: str = None,
        diagnostic_name: str = "trends",
        loglevel: str = "WARNING",
    ):
        """Initialize the Trends class.

        Args:
            model (str): Climate model name.
            exp (str): Experiment name.
            source (str): Data source name.
            catalog (str, optional): Path to the data catalog.
            regrid (str, optional): Regridding method.
            startdate (str, optional): Start date for data selection.
            enddate (str, optional): End date for data selection.
            diagnostic_name (str, optional): Name of the diagnostic for filenames. Defaults to "trends".
            loglevel (str, optional): Logging level. Default is "WARNING".
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
        self.diagnostic_name = diagnostic_name

    def run(
        self,
        outputdir: str = ".",
        rebuild: bool = True,
        region: str = None,
        var: list = ["thetao", "so"],
        dim_mean: type = None,
        reader_kwargs: dict = {}
    ):
        """Run the trend analysis workflow.

        Args:
            outputdir (str, optional): Directory to save output files. Default is current directory.
            rebuild (bool, optional): If True, rebuild existing files. Default is True.
            region (str, optional): Geographical region for analysis.
            var (list, optional): List of variable names to analyze. Default is ['thetao', 'so'].
            dim_mean (str or list, optional): Dimension(s) over which to compute the mean. Default is None.
            reader_kwargs (dict, optional): Additional keyword arguments for the data reader. Default is {}.
        """
        self.logger.info("Starting trend analysis workflow")
        super().retrieve(var=var, reader_kwargs=reader_kwargs)
        # self.data = self.data.chunk(chunks={"time": 12, "level": 1})  # this is needed to avoid a too large graph

        # If a region is specified, apply area selection to self.data
        if region:
            self.logger.info(f"Selecting region: {region} for diagnostic '{self.diagnostic_name}'.")
            res_dict = super()._select_region(
                data=self.data, region=region, diagnostic="ocean3d", drop=True
            )
            self.lat_limits = res_dict["lat_limits"]
            self.lon_limits = res_dict["lon_limits"]
            self.region = res_dict["region"]
        else:
            self.logger.debug("No region specified, using global data")
            self.region = 'global'
            self.lat_limits = None
            self.lon_limits = None

        # If a dimension mean is specified, compute the mean over that dimension
        # otherwise use the data as is, with a region selection if applied
        if dim_mean:
            self.logger.debug("Averaging data over dimension: %s", dim_mean)
            self.data = self.reader.fldmean(self.data, dim=dim_mean,
                                            lat=self.lat_limits, lon=self.lon_limits)
        else:
            self.data = res_dict.get("data", self.data) if 'res_dict' in locals() else self.data

        self.logger.info("Computing trend coefficients")
        self.trend_coef = self.compute_trend(data=self.data)
        self.logger.info("Saving results to NetCDF")
        self.save_netcdf(outputdir=outputdir, rebuild=rebuild)
        self.logger.info("Trend analysis workflow completed")

    def adjust_trend_for_time_frequency(self, trend, y_array):
        """Adjust trend values based on the time frequency of the data.

        Args:
            trend (xr.DataArray): Trend values to adjust.
            y_array (xr.DataArray): Original data array with time coordinate.
        
        Returns:
            xr.DataArray: Adjusted trend values.
        """
        self.logger.debug("Adjusting trend for time frequency")
        time_frequency = y_array["time"].to_index().inferred_freq

        if time_frequency == None:
            self.logger.debug("Time frequency not inferred, checking for monthly data")
            time_index = pd.to_datetime(y_array["time"].values)
            time_diffs = time_index[1:] - time_index[:-1]
            is_monthly = all(time_diff.days >= 28 for time_diff in time_diffs)
            if is_monthly:
                time_frequency = "MS"
                self.logger.debug("Data inferred as monthly")
            else:
                self.logger.error("Unable to determine time frequency")
                raise ValueError(
                    f"The frequency of the data must be in Daily/Monthly/Yearly"
                )

        if time_frequency == "MS":
            self.logger.debug("Monthly data detected, scaling trend by 12")
            trend = trend * 12
        elif time_frequency == "H":
            self.logger.debug("Hourly data detected, scaling trend by 24*30*12")
            trend = trend * 24 * 30 * 12
        elif time_frequency in ("Y", "YE-DEC"):
            self.logger.debug("Yearly data detected, no scaling applied")
            trend = trend
        else:
            self.logger.error("Unsupported time frequency: %s", time_frequency)
            raise ValueError(
                f"The frequency: {time_frequency} of the data must be in Daily/Monthly/Yearly"
            )

        units = trend.attrs.get("units", "")
        trend.attrs["units"] = f"{units}/year" if units else "per year"
        self.logger.debug("Trend units updated to: %s", trend.attrs["units"])
        return trend

    def compute_trend(self, data: xr.DataArray | xr.Dataset):
        """Compute linear trend coefficients over time.

        Args:
            data (xr.DataArray or xr.Dataset): Input data with a time dimension.

        Returns:
            xr.DataArray or xr.Dataset: Trend coefficients adjusted for time frequency.
        """
        self.logger.info("Calculating linear trend")
        trend_init = Trender()
        trend_data = trend_init.coeffs(data, dim="time", skipna=True, normalize=True)
        trend_data = trend_data.sel(degree=1)
        trend_data.attrs = data.attrs
        trend_dict = {}
        for var in data.data_vars:
            self.logger.debug("Adjusting trend for variable: %s", var)
            trend_data[var].attrs = data[var].attrs
            trend_dict[var] = self.adjust_trend_for_time_frequency(
                trend_data[var], data
            )
        trend_data = xr.Dataset(trend_dict)
        trend_data.attrs["AQUA_region"] = self.region
        self.logger.info("Trend value calculated")

        self.logger.debug("Loading trend data in memory")
        trend_data.load()
        self.logger.debug("Loaded trend data in memory")
        return trend_data

    def save_netcdf(
        self,
        diagnostic_product: str = "trend",
        outputdir: str = ".",
        rebuild: bool = True,
    ):
        """Save trend coefficients to a NetCDF file.

        Args:
            diagnostic (str, optional): Diagnostic name for filenames. Default is "trends".
            diagnostic_product (str, optional): Product type for filenames. Default is "spatial_trend".
            region (str, optional): Geographical region for analysis.
            outputdir (str, optional): Directory to save output files. Default is current directory.
            rebuild (bool, optional): If True, rebuild existing files. Default is True.
        """
        self.logger.info("Saving trend coefficients to NetCDF file")
        super().save_netcdf(
            diagnostic=self.diagnostic_name,
            diagnostic_product=diagnostic_product,
            outputdir=outputdir,
            rebuild=rebuild,
            data=self.trend_coef,
            extra_keys={"region": self.region},
        )
        self.logger.info("Trend coefficients saved to NetCDF file")
