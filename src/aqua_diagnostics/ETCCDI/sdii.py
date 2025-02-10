import xarray as xr

from aqua.util import convert_units
from .daily import DailyETCCDI

xr.set_options(keep_attrs=True)


class SDII(DailyETCCDI):
    def __init__(self, model: str, exp: str, source: str, year: int,
                 catalog: str = None, loglevel: str = 'WARNING'):
        """
        Initialize the class to compute the Simple pricipitation intensity index (SDII).

        Args:
            catalog (str, opt): The catalog to use. Default is None.
            model (str): The model to use. Default is None.
            exp (str): The experiment to use. Default is None.
            source (str): The source to use. Default is None.
            year (int): The year to compute the index. Default is None.
            loglevel (str, opt): The log level to use. Default is 'WARNING'.
        """
        super().__init__(catalog=catalog, model=model, exp=exp,
                         source=source, year=year, loglevel=loglevel)
        
        self.index_cumulated = None
    
    def compute_index(self, var: str = 'tprate', output_dir: str = '.',
                      rebuild: bool = True, threshold: float = 1.0, **kwargs):
        """
        Compute the Simple pricipitation intensity index (SDII) for the given month.

        Args:
            var (str): The variable to use for the computation. Default is 'tprate'.
            output_dir (str): The output directory to save the index. Default is '.'.
            rebuild (bool): Whether to rebuild the index. Default is True.
            threshold (float): The threshold to use for the computation. Default is 1.0 mm/day.
            **kwargs: Arbitrary keyword arguments.
        """
        self.logger.info('Computing SDII for year %d using variable %s', self.year, var)
        self.logger.info('Threshold: %f mm/day', threshold)
        month = 1

        while(self.index is None or self.data is not None):
            super().retrieve(var=var)

            if self.data is not None:
                self.data = self.data[var]
                self.logger.info('Computing SDII from %s to %s', self.data.time.values[0], self.data.time.values[-1])

                new_month = self.data.time.values[0].astype('datetime64[M]').astype(int) % 12 + 1
                if new_month != month:
                    
                    # Save the index on disk
                    super().save_monthly_index(data=self.index, diagnostic_product='SDII_days', month=month,
                                               default_path=output_dir, rebuild=rebuild, **kwargs)
                    super().save_monthly_index(data=self.index_cumulated, diagnostic_product='SDII_cumulated', month=month,
                                               default_path=output_dir, rebuild=rebuild, **kwargs)

                    self.logger.debug('New month: %d', new_month)
                    month = new_month
                    self.index = None
                    self.index_cumulated = None
                
                self._check_data(var=var)
                self._index_evaluation(var=var, threshold=threshold)
            else:
                self.logger.info('No more data to compute the index')
                super().save_monthly_index(data=self.index, diagnostic_product='SDII_days', month=month,
                                           default_path=output_dir, rebuild=rebuild, **kwargs)
                super().save_monthly_index(data=self.index_cumulated, diagnostic_product='SDII_cumulated', month=month,
                                           default_path=output_dir, rebuild=rebuild, **kwargs)
    
    def _check_data(self, var: str):
        """
        Make sure that the data is in mm/day.
        """
        final_units = 'mm/day'
        initial_units = self.data.units
        data = self.data

        conversion = convert_units(initial_units, final_units)

        factor = conversion.get('factor', 1)
        offset = conversion.get('offset', 0)

        if factor != 1 or offset != 0:
            self.logger.debug('Converting %s from %s to %s', var, initial_units, final_units)
            data = data * factor + offset
            self.data = data

    def _index_evaluation(self, var: str, threshold: float):
        """
        Evaluate the index for the given day.
        
        Args:
            var (str): The variable to use for the computation.
            threshold (float): The threshold to use for the computation.
        """

        if self.data is None:
            self.logger.error('No data to compute the index')
            return
        else:
            daily = self.reader.timmean(self.data, freq='D')
        
        wet_day = xr.where(daily >= threshold, 1, 0)
        wet_value = xr.where(daily >= threshold, daily, 0)

        if self.index is None:
            self.index = wet_day
        else:
            self.index = self.index + wet_day
        if self.index_cumulated is None:
            self.index_cumulated = wet_value
        else:
            self.index_cumulated = self.index_cumulated + wet_value

    def combine_monthly_index(self, output_dir: str = '.', rebuild: bool = True, **kwargs):
        """
        Combine the monthly indices to get the annual index.

        Args:
            output_dir (str): The output directory to save the index. Default is '.'.
            rebuild (bool): Whether to rebuild the index. Default is True.
            **kwargs: Arbitrary keyword arguments.
        """
        days = super().combine_monthly_index(diagnostic_product='SDII_days', default_path=output_dir,
                                             rebuild=rebuild, **kwargs)
        cumulated = super().combine_monthly_index(diagnostic_product='SDII_cumulated', default_path=output_dir,
                                                  rebuild=rebuild, **kwargs)

        sdii = cumulated / days

        super().save_annual_index(data=cumulated, diagnostic_product='SDII_cumulated', default_path=output_dir, rebuild=rebuild, **kwargs)
        super().save_annual_index(data=days, diagnostic_product='SDII_days', default_path=output_dir, rebuild=rebuild, **kwargs)
        super().save_annual_index(data=sdii, diagnostic_product='SDII', default_path=output_dir, rebuild=rebuild, **kwargs)
