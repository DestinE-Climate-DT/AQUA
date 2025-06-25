import xarray as xr
from aqua.logger import log_configure
from aqua.diagnostics.core import Diagnostic
from aqua.util import area_selection
from itertools import product
from aqua import reader

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
        super().__init__(catalog=catalog, model=model,
                         exp=exp, source=source, regrid=regrid,
                         startdate=startdate, enddate=enddate,
                         loglevel=loglevel)
        self.logger = log_configure(log_name="Trends", log_level=loglevel)
    
    def run(self, outputdir: str = ".", rebuild: bool = True,
            region: str = None, var: list = ["thetao", "so"], dim_mean: type = None):
        super().retrieve(var=var)
        region, lon_limits, lat_limits = super().select_region(diagnostic="ocean3d", region=region)
        
        if dim_mean:
            self.data = self.data.mean(dim=dim_mean, keep_attrs=True)
        self.trend_coef = self.compute_trend(data = self.data, loglevel=self.loglevel)
        self.save_netcdf(outputdir= outputdir, rebuild=rebuild, region=region)
    
    @staticmethod
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
        
        time_frequency = data["time"].to_index().inferred_freq

        if time_frequency is not None:
            # Define the conversion factor
            if time_frequency.startswith("D"):
                factor = 1e9 * 60 * 60 * 24  # Convert ns⁻¹ to days⁻¹
            elif time_frequency.startswith("M"):
                factor = 1e9 * 60 * 60 * 24 * 30.4375  # Convert ns⁻¹ to months⁻¹
            elif time_frequency.startswith("A") or time_frequency.startswith("Y"):  
                factor = 1e9 * 60 * 60 * 24 * 365.25  # Convert ns⁻¹ to years⁻¹
            else:
                factor = 1
                logger.warning (f"Unsupported time frequency: {time_frequency}. It will result wrong values in trend. To fix it please report of look at the unit of time in the dataset")

        # Iterate over variables in the input dataset
        for var in data.data_vars:
            logger.debug(f"Calculating trend for {var}")
            # Perform the polyfit to calculate the trend (slope)
            poly_coeffs = data.polyfit(dim="time", deg=1)
            
            trend_dict[var] = poly_coeffs[f"{var}_polyfit_coefficients"].sel(degree=1)* factor
            trend_dict[var].attrs = data[var].attrs
            # Apply necessary adjustments for time frequency if needed (optional)
            trend_dict[var] = Trends.adjust_trend_for_time_frequency(trend_dict[var], data[var], loglevel=loglevel)
            
            logger.debug(f"Trend for {var} calculated successfully")

        # Merge trends into a single dataset
        trend_ds = xr.Dataset(trend_dict)

        logger.info("Trend dataset created successfully")

        return trend_ds

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
    def compute_trend(data, loglevel= "WARNING"):
        """
        Compute the trend values for temperature and salinity variables in a 3D dataset.

        Parameters:
            data (xarray.Dataset): Input dataset containing temperature (thetao) and salinity (so) variables.
            loglevel (str, optional): Log level for logging messages. Defaults to "WARNING".

        Returns:
            xarray.Dataset: Dataset with trend values for temperature and salinity variables.
        """
        logger = log_configure(loglevel, 'TS_3dtrend')
        logger.debug("Calculating linear trend")
        TS_3dtrend_data = Trends.trend_from_polyfit(data, loglevel= loglevel)
        TS_3dtrend_data.attrs = data.attrs
        logger.debug("Trend value calculated")
        return TS_3dtrend_data
    
    
    def save_netcdf(self, diagnostic: str = "trends",
                    diagnostic_product: str = "spatial_trend",
                    region: str = None,
                    outputdir: str = '.', rebuild: bool = True):
    
        save_kwargs = {}

        if region is not None:
            save_kwargs["region"] = region
        
        super().save_netcdf(
            diagnostic=diagnostic,
            diagnostic_product=diagnostic_product,
            outputdir=outputdir,
            rebuild=rebuild,
            data=self.trend_coef,
            **save_kwargs
        )