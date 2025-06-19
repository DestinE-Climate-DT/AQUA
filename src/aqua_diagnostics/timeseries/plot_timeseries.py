import xarray as xr
from aqua.graphics import plot_timeseries
from aqua.util import to_list
from .base import PlotBaseMixin


class PlotTimeseries(PlotBaseMixin):
    """Class to plot time series data."""
    def __init__(self, diagnostic_name: str = 'timeseries',
                 hourly_data=None, daily_data=None,
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

        Note: Currently, only monthly and annual data are supported.
        Additionally, only one reference data array is supported for each frequency.

        Args:
            diagnostic_name (str): The name of the diagnostic. Used for logger and filenames. Default is 'timeseries'.
            hourly_data (list): List of hourly data arrays.
            daily_data (list): List of daily data arrays.
            monthly_data (list): List of monthly data arrays.
            annual_data (list): List of annual data arrays.
            ref_hourly_data (xr.DataArray): Reference hourly data array.
            ref_daily_data (xr.DataArray): Reference daily data array.
            ref_monthly_data (xr.DataArray): Reference monthly data array.
            ref_annual_data (xr.DataArray): Reference annual data array.
            std_hourly_data (xr.DataArray): Standard deviation hourly data array.
            std_daily_data (xr.DataArray): Standard deviation daily data array.
            std_monthly_data (xr.DataArray): Standard deviation monthly data array.
            std_annual_data (xr.DataArray): Standard deviation annual data array.
            loglevel (str): Logging level. Default is 'WARNING'.
        """
        super().__init__(loglevel=loglevel, diagnostic_name=diagnostic_name)

        # TODO: support hourly and daily data
        for data in [hourly_data, daily_data, ref_hourly_data, ref_daily_data,
                     std_hourly_data, std_daily_data]:
            if data is not None:
                self.logger.warning('Hourly and daily data are not yet supported, they will be ignored')

        # self.hourly_data = to_list(hourly_data)
        # self.daily_data = to_list(daily_data)
        self.monthly_data = to_list(monthly_data) if monthly_data is not None else None
        self.annual_data = to_list(annual_data) if annual_data is not None else None

        # TODO: support ref list
        # self.ref_hourly_data = to_list(ref_hourly_data)
        # self.ref_daily_data = to_list(ref_daily_data)
        self.ref_monthly_data = to_list(ref_monthly_data)[0] if ref_monthly_data is not None else None
        self.ref_annual_data = to_list(ref_annual_data)[0] if ref_annual_data is not None else None

        # self.std_hourly_data = to_list(std_hourly_data)
        # self.std_daily_data = to_list(std_daily_data)
        self.std_monthly_data = to_list(std_monthly_data)[0] if std_monthly_data is not None else None
        self.std_annual_data = to_list(std_annual_data)[0] if std_annual_data is not None else None

        self.len_data, self.len_ref = self._check_data_length()

        # Filling them
        self.get_data_info()

    def run(self, var: str, units: str = None, region: str = None, outputdir: str = './',
            rebuild: bool = True, dpi: int = 300, format: str = 'png'):
        """
        Run the PlotTimeseries class.

        Args:
            var (str): Variable name to be used in the title and description.
            units (str): Units of the variable to be used in the title.
            region (str): Region to be used in the title and description.
            outputdir (str): Output directory to save the plot.
            rebuild (bool): If True, rebuild the plot even if it already exists.
            dpi (int): Dots per inch for the plot.
            format (str): Format of the plot ('png' or 'pdf'). Default is 'png'.
        """

        self.logger.info('Running PlotTimeseries')
        data_label = self.set_data_labels()
        ref_label = self.set_ref_label()
        description = self.set_description(region=region)
        title = self.set_title(region=region, var=var, units=units)
        fig, _ = self.plot_timeseries(data_labels=data_label, ref_label=ref_label, title=title)
        region_short = region.replace(' ', '').lower() if region is not None else None
        self.save_plot(fig, var=var, description=description, region=region_short, rebuild=rebuild,
                       outputdir=outputdir, dpi=dpi, format=format)
        self.logger.info('PlotTimeseries completed successfully')

    def get_data_info(self):
        """
        We extract the data needed for labels, description etc
        from the data arrays attributes.

        The attributes are:
        - AQUA_catalog
        - AQUA_model
        - AQUA_exp
        - std_startdate
        - std_enddate
        """
        for data in [self.monthly_data, self.annual_data]:
            if data is not None:
                # Make a list from the data array attributes
                self.catalogs = [d.AQUA_catalog for d in data]
                self.models = [d.AQUA_model for d in data]
                self.exps = [d.AQUA_exp for d in data]
                break
        self.logger.debug(f'Catalogs: {self.catalogs}')
        self.logger.debug(f'Models: {self.models}')
        self.logger.debug(f'Experiments: {self.exps}')

        # TODO: support ref list
        for ref in [self.ref_monthly_data, self.ref_annual_data]:
            if ref is not None:
                self.ref_catalogs = ref.AQUA_catalog
                self.ref_models = ref.AQUA_model
                self.ref_exps = ref.AQUA_exp
                break
        self.logger.debug(f'Reference: {self.ref_catalogs} {self.ref_models} {self.ref_exps}')

        for std in [self.std_monthly_data, self.std_annual_data]:
            if std is not None:
                self.std_startdate = std.std_startdate if std.std_startdate is not None else None
                self.std_enddate = std.std_enddate if std.std_enddate is not None else None
                break
        self.logger.debug(f'Standard deviation dates: {self.std_startdate} - {self.std_enddate}')

    def set_title(self, region: str = None, var: str = None, units: str = None):
        """
        Set the title for the plot.

        Args:
            region (str): Region to be used in the title.
            var (str): Variable name to be used in the title.
            units (str): Units of the variable to be used in the title.

        Returns:
            title (str): Title for the plot.
        """
        title = super().set_title(region=region, var=var, units=units, diagnostic='Time series')
        return title

    def set_description(self, region: str = None):
        """
        Set the caption for the plot.
        The caption is extracted from the data arrays attributes and the
        reference data arrays attributes.
        The caption is stored as 'Description' in the metadata dictionary.

        Args:
            region (str): Region to be used in the caption.

        Returns:
            description (str): Caption for the plot.
        """
        description = super().set_description(region=region, diagnostic='Time series')
        return description

    def plot_timeseries(self, data_labels=None, ref_label=None, title=None):
        """
        Plot the time series data.

        Args:
            data_labels (list): List of data labels.
            ref_label (str): Reference label.
            title (str): Title of the plot.

        Returns:
            fig (matplotlib.figure.Figure): Figure object.
            ax (matplotlib.axes.Axes): Axes object.
        """
        fig, ax = plot_timeseries(monthly_data=self.monthly_data,
                                  ref_monthly_data=self.ref_monthly_data,
                                  std_monthly_data=self.std_monthly_data,
                                  annual_data=self.annual_data,
                                  ref_annual_data=self.ref_annual_data,
                                  std_annual_data=self.std_annual_data,
                                  data_labels=data_labels, ref_label=ref_label,
                                  title=title, return_fig=True, loglevel=self.loglevel)

        return fig, ax

    def save_plot(self, fig, var: str, description: str = None, region: str = None, rebuild: bool = True,
                  outputdir: str = './', dpi: int = 300, format: str = 'png'):
        """
        Save the plot to a file.

        Args:
            fig (matplotlib.figure.Figure): Figure object.
            var (str): Variable name to be used in the title and description.
            description (str): Description of the plot.
            region (str): Region to be used in the title and description.
            rebuild (bool): If True, rebuild the plot even if it already exists.
            outputdir (str): Output directory to save the plot.
            dpi (int): Dots per inch for the plot.
            format (str): Format of the plot ('png' or 'pdf'). Default is 'png'.
        """
        super().save_plot(fig=fig, var=var, description=description,
                          region=region, rebuild=rebuild,
                          outputdir=outputdir, dpi=dpi, format=format, diagnostic_product='timeseries')

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

        if self.ref_monthly_data is not None or self.ref_annual_data is not None:
            ref_length = 1
        # # TODO: uncomment when support to list is implemented
        # if self.ref_monthly_data and self.ref_annual_data:
        #     if len(self.ref_monthly_data) != len(self.ref_annual_data):
        #         raise ValueError('Reference monthly and annual data list must have the same length')
        #     else:
        #         ref_length = len(self.ref_monthly_data)

        # if self.std_monthly_data and self.std_annual_data:
        #     if len(self.std_monthly_data) != len(self.std_annual_data):
        #         raise ValueError('Standard deviation monthly and annual data list must have the same length')
        #     else:
        #         if len(self.std_monthly_data) != ref_length:
        #             raise ValueError('Standard deviation monthly and annual data list must have the same length as reference data')

        return data_length, ref_length
