"""SeasonalCycles class for retrieve and netcdf saving of a single experiment"""
import xarray as xr
from .base import BaseMixin

xr.set_options(keep_attrs=True)


class SeasonalCycles(BaseMixin):
    """SeasonalCycles class for retrieve and netcdf saving of a single experiment"""

    def __init__(self, diagnostic_name: str = 'seasonalcycles',
                 catalog: str = None, model: str = None,
                 exp: str = None, source: str = None,
                 regrid: str = None,
                 startdate: str = None, enddate: str = None,
                 std_startdate: str = None, std_enddate: str = None,
                 region: str = None, lon_limits: list = None, lat_limits: list = None,
                 loglevel: str = 'WARNING'):
        """
        Initialize the Timeseries class.

        Args:
            diagnostic_name (str): The name of the diagnostic. Used for logger and filenames. Default is 'seasonalcycles'.
            catalog (str): The catalog to be used. If None, the catalog will be determined by the Reader.
            model (str): The model to be used.
            exp (str): The experiment to be used.
            source (str): The source to be used.
            regrid (str): The target grid to be used for regridding. If None, no regridding will be done.
            startdate (str): The start date of the data to be retrieved.
                             If None, all available data will be retrieved.
            enddate (str): The end date of the data to be retrieved.
                           If None, all available data will be retrieved.
            std_startdate (str): The start date of the standard deviation evaluation period.
            std_enddate (str): The end date of the standard deviation evaluation period.
            region (str): The region to select. This will define the lon and lat limits.
            lon_limits (list): The longitude limits to be used. Overriden by region.
            lat_limits (list): The latitude limits to be used. Overriden by region.
            loglevel (str): The log level to be used. Default is 'WARNING'.
        """
        super().__init__(diagnostic_name=diagnostic_name,
                         catalog=catalog, model=model, exp=exp, source=source, regrid=regrid,
                         startdate=startdate, enddate=enddate, std_startdate=std_startdate, std_enddate=std_enddate,
                         region=region, lon_limits=lon_limits, lat_limits=lat_limits, loglevel=loglevel)

    def run(self, var: str, formula: bool = False, long_name: str = None,
            units: str = None, short_name: str = None, std: bool = False,
            exclude_incomplete: bool = True, center_time: bool = True,
            box_brd: bool = True, outputdir: str = './', rebuild: bool = True,
            reader_kwargs: dict = {}, create_catalog_entry: bool = False):
        """
        Run all the steps necessary for the computation of the SeasonalCyles.
        Save the results to netcdf files.

        Args:
            var (str): The variable to be used.
            formula (bool): If True, the variable is a formula.
            long_name (str): The long name of the variable, if different from the variable name.
            units (str): The units of the variable, if different from the original units.
            short_name (str): The short name of the variable, if different from the variable name.
            std (bool): If True, compute the standard deviation. Default is False.
            exclude_incomplete (bool): If True, exclude incomplete periods.
            center_time (bool): If True, the time will be centered.
            box_brd (bool): choose if coordinates are comprised or not in area selection.
            outputdir (str): The directory to save the data.
            rebuild (bool): If True, rebuild the data.
            reader_kwargs (dict): Additional keyword arguments for the Reader. Default is an empty dictionary.
            create_catalog_entry (bool): If True, create a catalog entry for the data. Default is False.
        """
        self.logger.info("Running SeasonalCycles for %s", var)
        self.retrieve(var=var, formula=formula, long_name=long_name, units=units,
                      short_name=short_name, reader_kwargs=reader_kwargs)

        # Notice that if you compute after, self.monthly will be the seasonal cycle
        # and the compute_std routine will fail
        if std:
            self.compute_std(freq='monthly', exclude_incomplete=exclude_incomplete, center_time=center_time,
                             box_brd=box_brd)

        self.logger.info("Computing the seasonal cycles")
        self.compute(exclude_incomplete=exclude_incomplete, center_time=center_time, box_brd=box_brd)

        self.save_netcdf(diagnostic_product='seasonalcycles', freq='monthly', outputdir=outputdir,
                         rebuild=rebuild, create_catalog_entry=create_catalog_entry)

    def compute(self, exclude_incomplete: bool = True, center_time: bool = True,
                box_brd: bool = True):
        """
        Compute the seasonal cycles.

        Args:
            exclude_incomplete (bool): If True, exclude incomplete periods.
            center_time (bool): If True, the time will be centered.
            box_brd (bool): choose if coordinates are comprised or not in area selection.
        """
        data = self.data

        # Field and time average
        data = self.reader.fldmean(data, box_brd=box_brd,
                                   lon_limits=self.lon_limits, lat_limits=self.lat_limits)
        data = self.reader.timmean(data, freq='MS', exclude_incomplete=exclude_incomplete,
                                   center_time=center_time)

        if self.region is not None:
            data.attrs['AQUA_region'] = self.region

        data = data.groupby('time.month').mean('time')

        # Load data in memory for faster plot
        self.logger.debug(f"Loading seasonal cycle data in memory")
        data.load()
        self.logger.debug(f"Loaded seasonal cycle data in memory")

        self.monthly = data
