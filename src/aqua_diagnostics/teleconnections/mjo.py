import xarray as xr
from aqua.graphics import plot_hovmoller
from aqua.logger import log_configure
from aqua.diagnostics.core import OutputSaver
from .base import BaseMixin

# set default options for xarray
xr.set_options(keep_attrs=True)


class MJO(BaseMixin):
    """
    MJO (Madden-Julian Oscillation) class.
    """
    def __init__(self, catalog: str = None, model: str = None,
                 exp: str = None, source: str = None,
                 regrid: str = None,
                 startdate: str = None, enddate: str = None,
                 configdir: str = None,
                 definition: str = 'teleconnections-destine',
                 loglevel: str = 'WARNING'):
        """
        Initialize the MJO class.

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
        super().__init__(telecname='MJO', catalog=catalog, model=model, exp=exp, source=source,
                         regrid=regrid, startdate=startdate, enddate=enddate,
                         configdir=configdir, definition=definition,
                         loglevel=loglevel)
        self.logger = log_configure(log_name='MJO', log_level=loglevel)

        self.var = self.definition.get('field')
        self.data_hovmoller = None

        # Delete the self.index attribute if it exists
        if hasattr(self, 'index'):
            del self.index

    def retrieve(self, reader_kwargs: dict = {}) -> None:
        """
        Retrieve the data for the MJO Hovmoller plot.

        Args:
            reader_kwargs (dict): Additional keyword arguments for the Reader.
                                  Default is an empty dictionary.
        """
        # Assign self.data, self.reader, self.catalog
        super().retrieve(var=self.var, reader_kwargs=reader_kwargs)
        self.data = self.data[self.var]

        self.reader.timmean(self.data, freq='D')

    def compute_hovmoller(self, day_window: int = None):
        """
        Compute the Hovmoller plot for the MJO index.
        This method prepares the data for a Hovmoller plot by selecting the MJO box,
        evaluating anomalies, and smoothing the data if required.

        Args:
            day_window (int, optional): Number of days to be used in the smoothing window.
                                        If None, no smoothing is performed. Default is None.
        """
        if self.definition.get('flip_sign', True):
            self.logger.info("Flipping the sign of the variable.")
            self.data = -self.data
        
        # Acquiring MJO box
        lat = [self.definition['latS'], self.definition['latN']]
        lon = [self.definition['lonW'], self.definition['lonE']]

        # Selecting the MJO box
        data_sel = self.reader.select_area(self.data, lat=lat, lon=lon, drop=True)

        # Evaluating anomalies
        data_mean = data_sel.mean(dim='time')
        data_anom = data_sel - data_mean

        # Smoothing the data
        if day_window:
            self.logger.info("Smoothing the data with a window of " + str(day_window) + " days.")
            self.data_hovmoller = data_anom.rolling(time=day_window, center=True).mean()
        else:
            self.data_hovmoller = data_anom


class PlotMJO():
    """
    PlotMJO class for plotting the MJO Hovmoller data.
    This class is a placeholder for future plotting methods.
    """
    def __init__(self, data, outputdir: str = './', loglevel: str = 'WARNING'):
        """
        Initialize the PlotMJO class.

        Args:
            data (xarray.DataArray): Data to be plot.
            outputdir (str): Directory where the plots will be saved. Default is './'.
            loglevel (str): Logging level. Default is 'WARNING'.
        """
        # Data info initalized as empty
        self.loglevel = loglevel
        self.logger = log_configure(self.loglevel, 'PlotMJO')
        self.catalogs = data.AQUA_catalog if hasattr(data, 'AQUA_catalog') else None
        self.models = data.AQUA_model if hasattr(data, 'AQUA_model') else None
        self.exps = data.AQUA_exp if hasattr(data, 'AQUA_exp') else None
        self.data = data

        self.outputsaver = OutputSaver(diagnostic='mjo',  catalog=self.catalogs, model=self.models,
                                       exp=self.exps, outputdir=outputdir, loglevel=self.loglevel)

    def plot_hovmoller(self, invert_axis: bool = True,
                       invert_time: bool = True,
                       nlevels: int = 21,
                       cmap: str = 'PuOr',
                       vmin: float = -90, vmax: float = 90):
        """
        Plot the Hovmoller diagram for the MJO data.

        Args:
            invert_axis (bool): If True, invert the axis. Default is True.
            invert_time (bool): If True, invert the time axis. Default is True.
            nlevels (int): Number of contour levels. Default is 21.
            cmap (str): Colormap to use for the plot. Default is 'PuOr'.
            vmin (float): Minimum value for the colorbar. Default is -90.
            vmax (float): Maximum value for the colorbar. Default is 90.

        Returns:
            fig (matplotlib.figure.Figure): The Hovmoller plot figure.
        """
        fig, _ = plot_hovmoller(self.data, dim='lat', 
                                 invert_axis=invert_axis,
                                 invert_time=invert_time,
                                 nlevels=nlevels,
                                 cmap=cmap, return_fig=True,
                                 vmin=vmin, vmax=vmax,
                                 loglevel=self.loglevel)
        
        return fig
    
    def save_plot(self, fig, diagnostic_product: str = 'hovmoller', extra_keys: dict = None,
                  rebuild: bool = True,
                  dpi: int = 300, format: str = 'png', metadata: dict = None):
        """
        Save the plot to a file.

        Args:
            fig (matplotlib.figure.Figure): The figure to be saved.
            diagnostic_product (str): The name of the diagnostic product. Default is None.
            extra_keys (dict): Extra keys to be used for the filename (e.g. season). Default is None.
            rebuild (bool): If True, the output files will be rebuilt. Default is True.
            dpi (int): The dpi of the figure. Default is 300.
            format (str): The format of the figure. Default is 'png'.
            metadata (dict): The metadata to be used for the figure. Default is None.
                             They will be complemented with the metadata from the outputsaver.
                             We usually want to add here the description of the figure.
        """
        if format == 'png':
            _ = self.outputsaver.save_png(fig, diagnostic_product=diagnostic_product, rebuild=rebuild,
                                          extra_keys=extra_keys, metadata=metadata, dpi=dpi)
        elif format == 'pdf':
            _ = self.outputsaver.save_pdf(fig, diagnostic_product=diagnostic_product, rebuild=rebuild,
                                          extra_keys=extra_keys, metadata=metadata)
