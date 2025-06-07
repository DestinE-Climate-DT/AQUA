import xarray as xr
from aqua.graphics import plot_hovmoller
from aqua.logger import log_configure
from aqua.util import area_selection
from aqua.diagnostics.core import OutputSaver
from .base import BaseMixin

# set default options for xarray
xr.set_options(keep_attrs=True)


class QBO(BaseMixin):
    """
    QBO (Quasi-Biennial Oscillation) class.
    """
    def __init__(self, catalog: str = None, model: str = None,
                 exp: str = None, source: str = None,
                 regrid: str = None,
                 startdate: str = None, enddate: str = None,
                 configdir: str = None,
                 definition: str = 'teleconnections-destine',
                 loglevel: str = 'WARNING'):
        """
        Initialize the QBO class.

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
        super().__init__(telecname='QBO', catalog=catalog, model=model, exp=exp, source=source,
                         regrid=regrid, startdate=startdate, enddate=enddate,
                         configdir=configdir, definition=definition,
                         loglevel=loglevel)
        self.logger = log_configure(log_name='QBO', log_level=loglevel)

        self.var = self.definition.get('field')
        self.data_hovmoller = None

        # Delete the self.index attribute if it exists
        if hasattr(self, 'index'):
            del self.index

    def retrieve(self):
        # Assign self.data, self.reader, self.catalog
        super().retrieve(var=self.var)
        self.data = self.data[self.var]

    def compute_hovmoller(self):
        """
        Compute the Hovmoller plot for QBO.
        """
        # Acquiring QBO limits
        lat = [self.definition['latS'], self.definition['latN']]

        # Selecting the QBO box
        data_sel = area_selection(self.data, lat=lat, lon=None, drop=True)

        data_sel = data_sel.mean(dim='lon', keep_attrs=True)

        # Select the levels, they are in hPa
        hpa_min = self.definition['Pamin']
        hpa_max = self.definition['Pamax']

        # Convert to hPa the data if needed
        if 'plev' in data_sel.dims:
            if data_sel.plev.units == 'Pa':
                self.logger.info("The pressure levels are in Pa, converting to hPa.")
                data_sel['plev'] = data_sel['plev'] / 100.0
                data_sel['plev'].attrs['units'] = 'hPa'
            elif data_sel.plev.units != 'hPa':
                raise ValueError(f"Unsupported pressure unit: {data_sel.plev.units}")
        else:
            raise ValueError("The data does not contain pressure levels (plev).")

        # Flip because they are in hPa
        data_sel = data_sel.sel(plev=slice(hpa_max, hpa_min))

        self.data_hovmoller = data_sel


class PlotQBO():
    """
    PlotQBO class for plotting the QBO Hovmoller plot.
    """
    def __init__(self, data, outputdir: str = './', rebuild: bool = True, loglevel: str = 'WARNING'):
        """
        Initialize the PlotQBO class.

        Args:
            data (xarray.DataArray): DataArray containing the QBO data.
            outputdir (str): Directory where the plots will be saved. Default is './'.
            rebuild (bool): Whether to rebuild the plot if it already exists. Default is True.
            loglevel (str): Logging level. Default is 'WARNING'.
        """
        # Data info initialized as empty
        self.loglevel = loglevel
        self.logger = log_configure(log_name='PlotQBO', log_level=self.loglevel)
        self.catalogs = data.AQUA_catalog if hasattr(data, 'AQUA_catalog') else None
        self.models = data.AQUA_model if hasattr(data, 'AQUA_model') else None
        self.exps = data.AQUA_exp if hasattr(data, 'AQUA_exp') else None
        self.data = data

        self.outputsaver = OutputSaver(diagnostic='qbo',  catalog=self.catalogs, model=self.models,
                                       exp=self.exps, outdir=outputdir, rebuild=rebuild, loglevel=self.loglevel)
        
    def plot_hovmoller(self, vmin=-40, vmax=40):
        """
        Plot the Hovmoller diagram for the QBO data.
        
        Returns:
            fig (matplotlib.figure.Figure): The figure object containing the plot.
        """
        title = self._set_title()
        fig, _ = plot_hovmoller(self.data, dim='lat',
                                title=title,
                                vim=vmin, vmax=vmax,
                                sym=True if vmin is None and vmax is None else False,
                                return_fig=True,
                                loglevel=self.loglevel)

        return fig
    
    def _set_title(self):
        """
        Set the title for the plot.
        """
        title = f"QBO Hovmoller for {self.models}, {self.exps}"
        return title
    
    def set_description(self):
        """
        Set the description for the plot.
        """
        description = "Hovmoller diagram of the Quasi-Biennial Oscillation (QBO) "
        description += f"for {self.catalogs}, {self.models}, {self.exps}."
        description += f" Data from {self.data.time.min().dt.strftime('%Y-%m-%d').item()} "
        description += f"to {self.data.time.max().dt.strftime('%Y-%m-%d').item()}."

        return description
    
    def save_plot(self, fig, diagnostic_product: str = None, extra_keys: dict = None,
                  dpi: int = 300, format: str = 'png', metadata: dict = None):
        """
        Save the plot to a file.

        Args:
            fig (matplotlib.figure.Figure): The figure to be saved.
            diagnostic_product (str): The name of the diagnostic product. Default is None.
            extra_keys (dict): Extra keys to be used for the filename (e.g. season). Default is None.
            dpi (int): The dpi of the figure. Default is 300.
            format (str): The format of the figure. Default is 'png'.
            metadata (dict): The metadata to be used for the figure. Default is None.
                             They will be complemented with the metadata from the outputsaver.
                             We usually want to add here the description of the figure.
        """
        if format == 'png':
            _ = self.outputsaver.save_png(fig, diagnostic_product=diagnostic_product,
                                          extra_keys=extra_keys, metadata=metadata, dpi=dpi)
        elif format == 'pdf':
            _ = self.outputsaver.save_pdf(fig, diagnostic_product=diagnostic_product,
                                          extra_keys=extra_keys, metadata=metadata)
