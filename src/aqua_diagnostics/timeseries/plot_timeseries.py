from aqua.graphics import plot_timeseries
from aqua.logger import log_configure
from aqua.util import to_list


class PlotTimeseries:
    def __init__(self,hourly_data=None, daily_data=None,
                 monthly_data=None, annual_data=None,
                 ref_hourly_data=None, ref_daily_data=None,
                 ref_monthly_data=None, ref_annual_data=None,
                 std_hourly_data=None, std_daily_data=None,
                 std_monthly_data=None, std_annual_data=None,
                 loglevel: str = 'WARNING'):
        """
        Initialize the PlotTimeseries class.
        This class is used to plot time series data previously processed
        by the Timeseries class.

        Any subset of frequency can be provided, however the order and length
        of the list of data arrays must be the same for each frequency.

        Args:
            hourly_data (list): List of hourly data arrays.
            daily_data (list): List of daily data arrays.
            monthly_data (list): List of monthly data arrays.
            annual_data (list): List of annual data arrays.
            ref_hourly_data (list): List of reference hourly data arrays.
            ref_daily_data (list): List of reference daily data arrays.
            ref_monthly_data (list): List of reference monthly data arrays.
            ref_annual_data (list): List of reference annual data arrays.
            std_hourly_data (list): List of standard deviation hourly data arrays.
            std_daily_data (list): List of standard deviation daily data arrays.
            std_monthly_data (list): List of standard deviation monthly data arrays.
            std_annual_data (list): List of standard deviation annual data arrays.
            loglevel (str): Logging level. Default is 'WARNING'.
        """
        self.loglevel = loglevel
        self.logger = log_configure(self.loglevel, 'PlotTimeseries')

        for data in [hourly_data, daily_data, ref_hourly_data, ref_daily_data,
                     std_hourly_data, std_daily_data]:
            if data is not None:
                self.logger.warning('Hourly and daily data are not yet supported, they will be ignored')
        
        # self.hourly_data = to_list(hourly_data)
        # self.daily_data = to_list(daily_data)
        self.monthly_data = to_list(monthly_data)
        self.annual_data = to_list(annual_data)

        # self.ref_hourly_data = to_list(ref_hourly_data)
        # self.ref_daily_data = to_list(ref_daily_data)
        self.ref_monthly_data = to_list(ref_monthly_data)
        self.ref_annual_data = to_list(ref_annual_data)

        # self.std_hourly_data = to_list(std_hourly_data)
        # self.std_daily_data = to_list(std_daily_data)
        self.std_monthly_data = to_list(std_monthly_data)
        self.std_annual_data = to_list(std_annual_data)

        self.len_data, self.len_ref = self._check_data_length()

        # self.data_dict = {'monthly': self.monthly_data, 'annual': self.annual_data}
        # self.ref_dict = {'monthly': self.ref_monthly_data, 'annual': self.ref_annual_data}
        # self.std_dict = {'monthly': self.std_monthly_data, 'annual': self.std_annual_data}

        # Data info initialized as empty
        self.catalogs = None
        self.models = None
        self.exps = None
        self.ref_catalogs = None
        self.ref_models = None
        self.ref_exps = None
        self.std_startdate = None
        self.std_enddate = None
        # Filling them
        self.get_data_info()

    def run(self, region: str = None, outputdir: str = './'):

        self.logger.info('Running PlotTimeseries')
        data_label = self.set_data_labels()
        ref_label = self.set_ref_label()
        description = self.set_description(region=region)
        fig, _ = self.plot_timeseries(data_labels=data_label,
                                      ref_label=ref_label)
        self.save_plot(fig, description=description,
                      region=region, outputdir=outputdir)
        self.logger.info('PlotTimeseries completed successfully')

    def get_data_info(self):
        """
        We extract the data needed for labels, description etc
        from the data arrays attributes.

        The attributes are:
        - AQUA_catalog
        - AQUA_model
        - AQUA_exp
        """
        for data in [self.monthly_data, self.annual_data]:
            if data is not None:
                # Make a list from the data array attributes
                self.catalogs = [d.AQUA_catalog for d in data]
                self.models = [d.AQUA_model for d in data]
                self.exps = [d.AQUA_exp for d in data]
                break
        
        for ref in [self.ref_monthly_data, self.ref_annual_data]:
            if ref is not None:
                # Make a list from the data array attributes
                self.ref_catalogs = [d.AQUA_catalog for d in ref]
                self.ref_models = [d.AQUA_model for d in ref]
                self.ref_exps = [d.AQUA_exp for d in ref]
                break
        
        for std in [self.std_monthly_data, self.std_annual_data]:
            if std is not None:
                # Make a list from the data array attributes
                self.std_startdate = std[0].time[0].strftime('%Y-%m-%d')
                self.std_enddate = std[0].time[-1].strftime('%Y-%m-%d')
                break

    def set_data_labels(self):
        """
        Set the data labels for the plot.
        The labels are extracted from the data arrays attributes.
        """
        data_labels = []
        for i in range(self.len_data):
            label = f'{self.models[i]} {self.exps[i]}'
            data_labels.append(label)
        
        return data_labels

    def set_ref_label(self):
        """
        Set the reference label for the plot.
        The label is extracted from the reference data arrays attributes.
        """
        ref_label = []
        for i in range(self.len_ref):
            label = f'{self.ref_models[i]} {self.ref_exps[i]}'
            ref_label.append(label)

        # Convert to string if only one reference data
        if len(ref_label) == 1:
            ref_label = ref_label[0]
        
        return ref_label

    def set_description(self, region: str = None):

        description = 'Time series '
        if region is not None:
            description += f'for region {region} '

        for i in range(self.len_data):
            description += f'for {self.catalogs[i]} {self.models[i]} {self.exps[i]} '
        
        return description

    def plot_timeseries(self, data_labels=None, ref_label=None):
        
        fig, ax = plot_timeseries(monthly_data=self.monthly_data,
                            ref_monthly_data=self.ref_monthly_data,
                            std_monthly_data=self.std_monthly_data,
                            annual_data=self.annual_data,
                            ref_annual_data=self.ref_annual_data,
                            std_annual_data=self.std_annual_data,
                            data_labels=data_labels,
                            ref_label=ref_label,
                            return_fig=True,
                            ref_label=self.ref_label, loglevel=self.loglevel)
        
        return fig, ax
    
    def save_plot(self, fig, description: str = None,
                  region: str = None, outputdir: str = './'):
        return
    
    def _check_data_length(self):
        """
        Check if all data arrays have the same length.
        Does the same for the reference data.
        If not, raise a ValueError.
        
        Return:
            data_length (int): Length of the data arrays.
            ref_length (int): Length of the reference data arrays.
        """
        data_length = 0
        ref_length = 0

        if self.monthly_data and self.annual_data:
            if len(self.monthly_data) != len(self.annual_data):
                raise ValueError('Monthly and annual data list must have the same length')
            else:
                data_length = len(self.monthly_data)
        
        if self.ref_monthly_data and self.ref_annual_data:
            if len(self.ref_monthly_data) != len(self.ref_annual_data):
                raise ValueError('Reference monthly and annual data list must have the same length')
            else:
                ref_length = len(self.ref_monthly_data)
            
        if self.std_monthly_data and self.std_annual_data:
            if len(self.std_monthly_data) != len(self.std_annual_data):
                raise ValueError('Standard deviation monthly and annual data list must have the same length')
            else:
                if len(self.std_monthly_data) != ref_length:
                    raise ValueError('Standard deviation monthly and annual data list must have the same length as reference data')

        return data_length, ref_length