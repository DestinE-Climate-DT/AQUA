"""Module for computing trends using xarray."""

import xarray as xr
import pandas as pd
from aqua.logger import log_configure
from aqua.diagnostics.core import Diagnostic
from aqua.reader import Trender

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
        self.logger.debug("Initialized Trends class with loglevel '%s'", loglevel)

    def run(
        self,
        outputdir: str = ".",
        rebuild: bool = True,
        region: str = None,
        var: list = ["thetao", "so"],
        dim_mean: type = None,
    ):
        self.logger.info("Starting trend analysis workflow")
        super().retrieve(var=var)
        self.logger.debug("Retrieved variables: %s", var)
        res_dict = super()._select_region(data=self.data,
                region=region, diagnostic="ocean3d",
                drop=True
            )
        self.data = res_dict['data']
        self.region = res_dict['region']
        # self.logger.debug("Selected region: %s, lon_limits: %s, lat_limits: %s", region, lon_limits, lat_limits)

        if dim_mean:
            self.logger.debug("Averaging data over dimension: %s", dim_mean)
            self.data = self.data.mean(dim=dim_mean, keep_attrs=True)
        self.logger.info("Computing trend coefficients")
        self.trend_coef = self.compute_trend(data=self.data, loglevel=self.loglevel)
        self.logger.info("Saving results to NetCDF")
        self.save_netcdf(outputdir=outputdir, rebuild=rebuild, region=region)
        self.logger.info("Trend analysis workflow completed")

    def adjust_trend_for_time_frequency(self, trend, y_array, loglevel="WARNING"):
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

    def compute_trend(self, data, loglevel="WARNING"):
        self.logger.info("Calculating linear trend")
        trend_init = Trender()
        trend_data = trend_init.coeffs(data, dim="time", skipna=True, normalize=True)
        trend_data = trend_data.sel(degree=1)
        trend_data.attrs = data.attrs
        trend_dict = {}
        for var in data.data_vars:
            self.logger.debug("Adjusting trend for variable: %s", var)
            trend_dict[var] = self.adjust_trend_for_time_frequency(
                trend_data[var], data, loglevel=loglevel
            )
            trend_dict[var].attrs = data[var].attrs
        trend_data = xr.Dataset(trend_dict)
        self.logger.info("Trend value calculated")
        return trend_data

    def save_netcdf(
        self,
        diagnostic: str = "trends",
        diagnostic_product: str = "spatial_trend",
        region: str = None,
        outputdir: str = ".",
        rebuild: bool = True,
    ):
        self.logger.info("Saving trend coefficients to NetCDF file")
        super().save_netcdf(
            diagnostic=diagnostic,
            diagnostic_product=diagnostic_product,
            outdir=outputdir,
            rebuild=rebuild,
            data=self.trend_coef,
            extra_keys={"region": region.replace(" ", "_") if region else None},
        )
        self.logger.info("Trend coefficients saved to NetCDF file")
