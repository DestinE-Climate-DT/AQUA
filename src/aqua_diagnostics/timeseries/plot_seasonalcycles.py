import xarray as xr
from aqua.graphics import plot_seasonalcycle
from aqua.logger import log_configure
from aqua.util import to_list
from .base import PlotBaseMixin


class PlotSeasonalCycles(PlotBaseMixin):
    def __init__(self, monthly_data=None, ref_monthly_data=None,
                 std_monthly_data=None, loglevel: str = 'WARNING'):
        """
        Initialize the PlotSeasonalCycles class.
        This class is used to plot seasonal cycles data previously processed
        by the SeasonalCycles class.

        Args:
            monthly_data (list): List of monthly data arrays.
            ref_monthly_data (xr.DataArray): Reference monthly data array.
            std_monthly_data (xr.DataArray): Standard deviation monthly data array.
            loglevel (str): Logging level. Default is 'WARNING'.
        """
        super().__init__(loglevel=loglevel)
        self.logger = log_configure(self.loglevel, 'PlotSeasonalCycles')

        # TODO: support ref list
        self.monthly_data = to_list(monthly_data)
        self.ref_monthly_data = ref_monthly_data  if isinstance(ref_monthly_data, xr.DataArray) else ref_monthly_data[0]
        self.std_monthly_data = std_monthly_data if isinstance(std_monthly_data, xr.DataArray) else std_monthly_data[0]

        self.len_data, self.len_ref = len(self.monthly_data), 1

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

        self.logger.info('Running PlotSeasonalCycles')
        data_label = self.set_data_labels()
        ref_label = self.set_ref_label()
        description = self.set_description(region=region)
        title = self.set_title(region=region, var=var, units=units)
        fig, _ = self.plot_seasonalcycles(data_labels=data_label, ref_label=ref_label, title=title)
        self.save_plot(fig, var=var, description=description, region=region, rebuild=rebuild,
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
        """
        if self.monthly_data is not None:
            # Make a list from the data array attributes
            self.catalogs = [d.AQUA_catalog for d in self.monthly_data]
            self.models = [d.AQUA_model for d in self.monthly_data]
            self.exps = [d.AQUA_exp for d in self.monthly_data]

        if self.ref_monthly_data is not None:
            # Make a list from the data array attributes
            self.ref_catalogs = [d.AQUA_catalog for d in self.ref_monthly_data]
            self.ref_models = [d.AQUA_model for d in self.ref_monthly_data]
            self.ref_exps = [d.AQUA_exp for d in self.ref_monthly_data]

        if self.std_monthly_data is not None:
            for std in self.std_monthly_data:
                self.std_startdate = std.std_startdate if std.std_startdate is not None else None
                self.std_enddate = std.std_enddate if std.std_enddate is not None else None
                break

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
        title = super().set_title(region=region, var=var, units=units, diagnostic='Seasonal cycle')
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
        description = super().set_description(region=region, diagnostic='Seasonal cycle')
        return description

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
        super().save_plot(fig, var=var, description=description,
                          region=region, rebuild=rebuild,
                          outputdir=outputdir, dpi=dpi, format=format, diagnostic='seasonalcycle')
