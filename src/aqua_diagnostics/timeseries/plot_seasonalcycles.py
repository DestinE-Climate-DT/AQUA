import xarray as xr
from aqua.graphics import plot_seasonalcycle
from aqua.util import to_list, get_realizations
from .base import PlotBaseMixin


class PlotSeasonalCycles(PlotBaseMixin):
    def __init__(self, diagnostic_name: str = 'seasonalcycles',
                 monthly_data=None, ref_monthly_data=None,
                 std_monthly_data=None, loglevel: str = 'WARNING'):
        """
        Initialize the PlotSeasonalCycles class.
        This class is used to plot seasonal cycles data previously processed
        by the SeasonalCycles class.

        Args:
            diagnostic_name (str): The name of the diagnostic. Used for logger and filenames. Default is 'seasonalcycles'.
            monthly_data (list): List of monthly data arrays.
            ref_monthly_data (xr.DataArray): Reference monthly data array.
            std_monthly_data (xr.DataArray): Standard deviation monthly data array.
            loglevel (str): Logging level. Default is 'WARNING'.
        """
        super().__init__(loglevel=loglevel, diagnostic_name=diagnostic_name)

        # TODO: support ref list
        self.monthly_data = to_list(monthly_data)
        self.ref_monthly_data = ref_monthly_data  if isinstance(ref_monthly_data, xr.DataArray) else ref_monthly_data[0] if isinstance(ref_monthly_data, list) else None
        self.std_monthly_data = std_monthly_data if isinstance(std_monthly_data, xr.DataArray) else std_monthly_data[0] if isinstance(std_monthly_data, list) else None

        self.len_data, self.len_ref = len(self.monthly_data), 1 if self.ref_monthly_data is not None else 0

        # Filling them
        self.get_data_info()

    def run(self, outputdir: str = './',
            rebuild: bool = True, dpi: int = 300, format: str = 'png'):
        """
        Run the PlotTimeseries class.

        Args:
            outputdir (str): Output directory to save the plot.
            rebuild (bool): If True, rebuild the plot even if it already exists.
            dpi (int): Dots per inch for the plot.
            format (str): Format of the plot ('png' or 'pdf'). Default is 'png'.
        """

        self.logger.info('Running PlotSeasonalCycles')
        data_label = self.set_data_labels()
        ref_label = self.set_ref_label()
        description = self.set_description()
        title = self.set_title()
        fig, _ = self.plot_seasonalcycles(data_labels=data_label, ref_label=ref_label, title=title)
        self.save_plot(fig, description=description, rebuild=rebuild,
                       outputdir=outputdir, dpi=dpi, format=format)
        self.logger.info('PlotSeasonalCycles completed successfully')

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
        - short_name
        - long_name
        - units
        """
        if self.monthly_data is not None:
            # Make a list from the data array attributes
            self.catalogs = [d.AQUA_catalog for d in self.monthly_data]
            self.models = [d.AQUA_model for d in self.monthly_data]
            self.exps = [d.AQUA_exp for d in self.monthly_data]
            self.region = self.monthly_data[0].AQUA_region if hasattr(self.monthly_data[0], 'AQUA_region') else None
            self.short_name = self.monthly_data[0].short_name if hasattr(self.monthly_data[0], 'short_name') else None
            self.long_name = self.monthly_data[0].long_name if hasattr(self.monthly_data[0], 'long_name') else None
            self.units = self.monthly_data[0].units if hasattr(self.monthly_data[0], 'units') else None
            self.realizations = get_realizations(self.monthly_data)
        self.logger.debug(f'Catalogs: {self.catalogs}')
        self.logger.debug(f'Models: {self.models}')
        self.logger.debug(f'Experiments: {self.exps}')
        self.logger.debug(f'Region: {self.region}')

        if self.ref_monthly_data is not None:
            # Make a list from the data array attributes
            self.ref_catalogs = self.ref_monthly_data.AQUA_catalog
            self.ref_models = self.ref_monthly_data.AQUA_model
            self.ref_exps = self.ref_monthly_data.AQUA_exp
            self.logger.debug(f'Reference: {self.ref_catalogs} {self.ref_models} {self.ref_exps}')

        if self.std_monthly_data is not None:
            for std in self.std_monthly_data:
                self.std_startdate = std.std_startdate if std.std_startdate is not None else None
                self.std_enddate = std.std_enddate if std.std_enddate is not None else None
                self.logger.debug(f'Standard deviation dates: {self.std_startdate} - {self.std_enddate}')
                break

    def set_title(self):
        """
        Set the title for the plot.

        Returns:
            title (str): Title for the plot.
        """
        return super().set_title(diagnostic='Seasonal cycle')

    def set_description(self):
        """
        Set the caption for the plot.
        The caption is extracted from the data arrays attributes and the
        reference data arrays attributes.
        The caption is stored as 'Description' in the metadata dictionary.

        Returns:
            description (str): Caption for the plot.
        """
        return super().set_description(diagnostic='Seasonal cycle')

    def plot_seasonalcycles(self, data_labels=None, ref_label=None, title=None):
        """
        Plot the seasonal cycle using the plot_seasonalcycle function.

        Args:
            data_labels (list): List of data labels.
            ref_label (str): Reference label.
            title (str): Title of the plot.

        Returns:
            fig (matplotlib.figure.Figure): Figure object.
            ax (matplotlib.axes.Axes): Axes object.
        """
        fig, ax = plot_seasonalcycle(data=self.monthly_data,
                                     ref_data=self.ref_monthly_data,
                                     std_data=self.std_monthly_data,
                                     data_labels=data_labels,
                                     ref_label=ref_label,
                                     title=title,
                                     loglevel=self.loglevel)

        return fig, ax

    def save_plot(self, fig, description: str = None, rebuild: bool = True,
                  outputdir: str = './', dpi: int = 300, format: str = 'png'):
        """
        Save the plot to a file.

        Args:
            fig (matplotlib.figure.Figure): Figure object.
            description (str): Description of the plot.
            rebuild (bool): If True, rebuild the plot even if it already exists.
            outputdir (str): Output directory to save the plot.
            dpi (int): Dots per inch for the plot.
            format (str): Format of the plot ('png' or 'pdf'). Default is 'png'.
        """
        super().save_plot(fig, description=description, rebuild=rebuild,
                          outputdir=outputdir, dpi=dpi, format=format, diagnostic_product='seasonalcycles')
