from aqua.exceptions import NotEnoughDataError
from aqua.logger import log_configure
from aqua.util.sci_util import lon_to_360
from .base import BaseMixin


class ENSO(BaseMixin):
    """
    Class for calculating the El Niño Southern Oscillation (ENSO) index.
    This class is used to calculate the ENSO index from a given dataset.
    It inherits from the BaseMixin class and implements the necessary methods
    to calculate the ENSO index.
    """
    def __init__(self, catalog: str = None, model: str = None,
                 exp: str = None, source: str = None,
                 regrid: str = None,
                 startdate: str = None, enddate: str = None,
                 configdir: str = None,
                 definition: str = 'teleconnections-destine',
                 loglevel: str = 'WARNING'):
        """
        Initialize the ENSO class.
        
        Args:
            catalog (str): Catalog name.
            model (str): Model name.
            exp (str): Experiment name.
            source (str): Source name.
            regrid (str): Regrid target. Default is None.
            startdate (str): Start date for data retrieval. Default is None.
            enddate (str): End date for data retrieval. Default is None.
            configdir (str): Configuration directory. Default is None.
            definition (str): definition filename. Default is 'teleconnections-destine'.
                             This is used to deduce the variable name and the lat/lon for the index.
            loglevel (str): Logging level. Default is 'WARNING'.
        """
        super().__init__(telecname='ENSO', catalog=catalog, model=model, exp=exp, source=source,
                         regrid=regrid, startdate=startdate, enddate=enddate,
                         configdir=configdir, definition=definition,
                         loglevel=loglevel)
        self.logger = log_configure(log_name='ENSO', log_level=loglevel)

        self.var = self.definition.get('field')

    def retrieve(self, reader_kwargs: dict = {}) -> None:
        """Retrieve the data for the ENSO index.
        
        Args:
            reader_kwargs (dict): Additional keyword arguments for the Reader.
                                  Default is an empty dictionary.
        """
        # Assign self.data, self.reader, self.catalog
        super().retrieve(var=self.var, reader_kwargs=reader_kwargs, months_required=24)

        self.reader.timmean(self.data, freq='MS')
    
    def compute_index(self, months_window: int = 3, box_brd: bool = True,
                       rebuild: bool = False):
        """"
        Evaluate station based index for a teleconnection.
        Field data must be monthly gridded data.

        Args:
            months_window (int, opt): months for rolling average, default is 3
            box_brd (bool, opt): choose if coordinates are comprised or not.
                                 Default is True
            rebuild (bool, opt): if True, the index is recalculated, default is False
        """
        
        if self.index is not None and not rebuild:
            self.logger.info('ENSO index already calculated, skipping.')
            return
        if self.data is None:
            raise NotEnoughDataError('Data not retrieved')
        if len(self.data[self.var].time) < 24:
            raise NotEnoughDataError('Data have less than 24 months')
        
        latN = self.definition.get('latN')
        latS = self.definition.get('latS')
        lonW = self.definition.get('lonW')
        lonE = self.definition.get('lonE')

        if self.data[self.var].lon.min() >= 0:
            lonW = lon_to_360(lonW)
            lonE = lon_to_360(lonE)

        self.logger.debug(f'lonW: {lonW}, lonE: {lonE}')
        self.logger.debug(f'latN: {latN}, latS: {latS}')

        data = self.reader.fldmean(self.data[self.var], lon_limits=[lonW, lonE],
                                   lat_limits=[latS, latN], box_brd=box_brd)

        # For the groupby operation it is better to load the data in memory
        data.load()

        data_an = data.groupby('time.month') - data.groupby('time.month').mean(dim='time')
        field_mean_an = data_an.rolling(time=months_window, center=True).mean(skipna=True)
        field_mean_an = field_mean_an.rename('index')

        self.logger.debug('Index evaluated')

        self.index = field_mean_an
