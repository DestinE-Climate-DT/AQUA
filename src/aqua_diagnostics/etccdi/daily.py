import os
import xarray as xr

from aqua import Reader
from aqua.logger import log_configure
from aqua.util import OutputSaver

xr.set_options(keep_attrs=True)


class DailyETCCDI():
    def __init__(self, catalog: str = None, model: str = None,
                 exp: str = None, source: str = None,
                 year: int = None, loglevel: str = 'WARNING'):
        """
        Initialize the DailyETCCDI class.

        Args:
            catalog (str): The catalog to be used. If None, the catalog will be determined by the Reader.
            model (str): The model to be used.
            exp (str): The experiment to be used.
            source (str): The source to be used.
            year (int): The year to be used.
            loglevel (str): The log level to be used. Default is 'WARNING'.
        """
        self.logger = log_configure(log_name='DailyETCCDI', log_level=loglevel)
        self.model = model
        self.exp = exp
        self.source = source

        if self.model is None or self.exp is None or self.source is None:
            raise ValueError('Model, experiment and source must be provided')

        self.year = year
        self.startdate = f'{year}0101'
        self.enddate = f'{year}1231'
        self.reader = Reader(catalog=catalog, model=self.model, exp=self.exp, source=self.source,
                             startdate=self.startdate, enddate=self.enddate,
                             streaming=True, aggregation='D', loglevel=loglevel)
        
        self.catalog = catalog if catalog is not None else self.reader.catalog

        # Data to be retrieved
        self.data = None

        # Index to be calculated
        self.index = None

    def retrieve(self, var: str = None):
        """
        Retrieve the data from the Reader.

        Args:
            var (str): The variable to be retrieved. Default all variables.
        """
        self.data = self.reader.retrieve(var=var)
    
    def save_monthly_index(self, data, diagnostic_product : str, month : int,
                           default_path: str = '.', rebuild : bool = True, **kwargs):
        """
        Save the monthly index as netcdf for the given diagnostic.

        Args:
            data (xr.Dataset or xr.DataArray): The data to be saved as netcdf.
            diagnostic_product (str): The name of the ETCCDI diagnostic to be saved.
            default_path (str): The default path to save the data. Default is '.'.
            rebuild (bool): If True, the data will be overwritten if it already exists. Default is True.
        """
        if isinstance(data, xr.Dataset) is False and isinstance(data, xr.DataArray) is False:
            self.logger.error('Data to save as netcdf must be an xarray Dataset or DataArray')

        outputsaver = OutputSaver(diagnostic='ETCCDI', diagnostic_product=diagnostic_product,
                                  default_path=default_path, rebuild=rebuild, catalog=self.catalog,
                                  model=self.model, exp=self.exp, loglevel=self.logger.level)
        
        time_start = f'{self.year}{month:02d}'

        #HACK: If I use time_start I need to define time_end as well, which I don't want
        outputsaver.save_netcdf(dataset=data, model_2=time_start, **kwargs)

    def save_annual_index(self, data, diagnostic_product : str, 
                          default_path: str = '.', rebuild : bool = True, **kwargs):
        """
        Save the annual index as netcdf for the given diagnostic.

        Args:
            data (xr.Dataset or xr.DataArray): The data to be saved as netcdf.
            diagnostic_product (str): The name of the ETCCDI diagnostic to be saved.
            default_path (str): The default path to save the data. Default is '.'.
            rebuild (bool): If True, the data will be overwritten if it already exists. Default is True.
        """
        if isinstance(data, xr.Dataset) is False and isinstance(data, xr.DataArray) is False:
            self.logger.error('Data to save as netcdf must be an xarray Dataset or DataArray')
        
        outputsaver = OutputSaver(diagnostic='ETCCDI', diagnostic_product=diagnostic_product,
                                  default_path=default_path, rebuild=rebuild, catalog=self.catalog,
                                  model=self.model, exp=self.exp, loglevel=self.logger.level)
        
        #HACK: If I use time_start I need to define time_end as well, which I don't want
        outputsaver.save_netcdf(dataset=data, model_2=f"{self.year}", **kwargs)

    def combine_monthly_index(self, diagnostic_product : str,
                              default_path: str = '.', rebuild : bool = True, **kwargs):
        """
        Combine the monthly index into a single dataset for the given diagnostic.

        Args:
            diagnostic_product (str): The name of the ETCCDI diagnostic to be combined.
            default_path (str): The default path to save the data. Default is '.'.
            rebuild (bool): If True, the data will be overwritten if it already exists. Default is True.
        """
        outputsaver = OutputSaver(diagnostic='ETCCDI', diagnostic_product=diagnostic_product,
                                  default_path=default_path, rebuild=rebuild, catalog=self.catalog,
                                  model=self.model, exp=self.exp, loglevel=self.logger.level)

        index = None

        for i in range(1, 13):
            time_start = f'{self.year}{i:02d}'
            wildcard = outputsaver.generate_name(model_2=time_start, **kwargs)

            # Open all the files that match the wildcard and sum them
            path = os.path.join(default_path, 'netcdf', wildcard)
            self.logger.debug(f"Combining {diagnostic_product} from {path}")
            index_loop = xr.open_dataarray(path)

            if i == 1:
                index = index_loop
            else:
                index += index_loop

        return index
