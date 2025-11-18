from aqua.exceptions import NotEnoughDataError
from aqua.logger import log_configure
from aqua.util.sci_util import lon_to_360
from .base import BaseMixin


class NAO(BaseMixin):
    """
    North Atlantic Oscillation (NAO) index calculation class.
    This class is used to calculate the NAO index from a given dataset.
    It inherits from the BaseMixin class and implements the necessary methods
    to calculate the NAO index.
    """
    def __init__(self, catalog: str = None, model: str = None,
                 exp: str = None, source: str = None,
                 regrid: str = None,
                 startdate: str = None, enddate: str = None,
                 configdir: str = None,
                 definition: str = 'teleconnections-destine',
                 loglevel: str = 'WARNING'):
        """
        Initialize the NAO class.

        Args:
            catalog (str): Catalog name.
            model (str): Model name.
            exp (str): Experiment name.
            source (str): Source name.
            regrid (str): Regrid method.
            startdate (str): Start date for data retrieval.
            enddate (str): End date for data retrieval.
            configdir (str): Configuration directory. Default is the installation directory.
            definition (str): definition filename. Default is 'teleconnections-destine'.
            loglevel (str): Logging level. Default is 'WARNING'.
        """
        super().__init__(telecname='NAO', catalog=catalog, model=model, exp=exp, source=source,
                         regrid=regrid, startdate=startdate, enddate=enddate,
                         configdir=configdir, definition=definition,
                         loglevel=loglevel)
        self.logger = log_configure(log_name='NAO', log_level=loglevel)

        self.var = self.definition.get('field')

    def retrieve(self, reader_kwargs: dict = {}) -> None:
        """
        Retrieve the data for the NAO index.
        
        Args:
            reader_kwargs (dict): Additional keyword arguments for the Reader.
                                  Default is an empty dictionary.
        """
        # Assign self.data, self.reader, self.catalog
        super().retrieve(var=self.var, reader_kwargs=reader_kwargs, months_required=24)

        self.reader.timmean(self.data, freq='MS')
    
    def compute_index(self, months_window: int = 3,
                       rebuild: bool = False):
        """"
        Evaluate station based index for a teleconnection.
        Field data must be monthly gridded data.

        Args:
            months_window (int, opt): months for rolling average, default is 3
            rebuild (bool, opt): if True, the index is recalculated, default is False
        """

        if self.index is not None and not rebuild:
            self.logger.info('NAO index already calculated, skipping.')
            return
        if self.data is None:
            raise NotEnoughDataError('Data not retrieved')
        if len(self.data[self.var].time) < 24:
            raise NotEnoughDataError('Data have less than 24 months')
        
        lat1 = self.definition.get('lat1')
        lat2 = self.definition.get('lat2')
        lon1 = self.definition.get('lon1')
        lon2 = self.definition.get('lon2')

        if self.data[self.var].lon.min() >= 0:
            lon1 = lon_to_360(lon1)
            lon2 = lon_to_360(lon2)

        self.logger.debug(f'Station 1: lon={lon1}, lat={lat1}')
        self.logger.debug(f'Station 2: lon={lon2}, lat={lat2}')

        # The index is evaluated with data at the two stations
        field1 = self.data[self.var].sel(lat=lat1, lon=lon1, method='nearest')
        field2 = self.data[self.var].sel(lat=lat2, lon=lon2, method='nearest')

        # For the groupby operation it is better to load the data in memory
        field1.load()
        field2.load()

        # Monthly field average and anomalies
        field1_av = field1.groupby("time.month").mean(dim="time")
        field1_an = field1.groupby("time.month") - field1_av

        field2_av = field2.groupby("time.month").mean(dim="time")
        field2_an = field2.groupby("time.month") - field2_av

        # Rolling average over months = months_window
        field1_an_ma = field1_an.rolling(time=months_window, center=True).mean()
        field2_an_ma = field2_an.rolling(time=months_window, center=True).mean()

        # Evaluate average and std for the station based difference
        diff_ma = field1_an_ma-field2_an_ma
        mean_ma = diff_ma.mean()
        std_ma = diff_ma.std()

        # Evaluate the index and rename the variable in the DataArray
        indx = (diff_ma-mean_ma)/std_ma
        indx = indx.rename('index')

        # Drop NaNs
        indx = indx.dropna(dim='time')

        self.logger.debug('Index evaluated')
        
        # Save the index in the class
        self.index = indx
