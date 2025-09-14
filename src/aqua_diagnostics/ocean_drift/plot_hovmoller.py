import xarray as xr
from aqua.logger import log_configure
from aqua.diagnostics.core import OutputSaver
from .multiple_hovmoller import plot_multi_hovmoller

xr.set_options(keep_attrs=True)

class PlotHovmoller:
    def __init__(self,
                 data: list [xr.Dataset],
                 diagnostic_name: str = "oceandrift",
                 outputdir: str = ".",
                 loglevel: str = "WARNING"):
        """
        Initialize the PlotHovmoller class.

        Args:
            data (list[xr.Dataset]): List of xarray datasets containing the data to be plotted
            diagnostic_name (str): Name of the diagnostic, default is "oceandrift"
            outputdir (str): Directory where the output will be saved, default is current directory
            loglevel (str): Logging level, default is "WARNING"
        """
        self.data = data

        self.loglevel = loglevel
        self.logger = log_configure(self.loglevel, "PlotHovmoller")

        self.diagnostic = diagnostic_name
        self.vars = list(self.data[0].data_vars)
        self.logger.debug("Variables in data: %s", self.vars)

        # Getting metadata from the first dataset
        self.catalog = self.data[0][self.vars[0]].AQUA_catalog
        self.model = self.data[0][self.vars[0]].AQUA_model
        self.exp = self.data[0][self.vars[0]].AQUA_exp
        self.region = self.data[0].AQUA_region

        self.outputsaver = OutputSaver(
            diagnostic=self.diagnostic,
            catalog=self.catalog,
            model=self.model,
            exp=self.exp, 
            outputdir=outputdir, 
            loglevel=self.loglevel)

    def plot_hovmoller(self, rebuild: bool = True, save_pdf: bool = True,
                       save_png: bool = True, dpi: int = 300):
        """
        Plot the Hovmoller diagram for the given data.
        This method sets the title, description, vmax, vmin, and texts for the plot.
        It then calls the `plot_multi_hovmoller` function to create the plot and
        saves it using the `OutputSaver`.

        Args:
            rebuild (bool): Whether to rebuild the output, default is True
            save_pdf (bool): Whether to save the plot as a PDF, default is True
            save_png (bool): Whether to save the plot as a PNG, default is True
        """
        self.set_suptitle()
        self.set_title()
        self.set_description()
        self.set_data_type()
        self.set_texts()
        self.set_vmax_vmin()
        self.logger.debug("Plotting Hovmoller for variables: %s", self.vars)
        fig = plot_multi_hovmoller(
            maps=self.data,
            variables=self.vars,
            loglevel=self.loglevel,
            title=self.suptitle,
            titles=self.title_list,
            vmax=self.vmax,
            vmin=self.vmin,
            cmap=self.cmap,
            text=self.texts
        )
        extra_keys = {'region': self.region.replace(" ", "_").lower()}
        if save_pdf:
            self.outputsaver.save_pdf(fig, diagnostic_product="hovmoller", metadata=self.description,
                                      rebuild=rebuild, extra_keys=extra_keys)
        if save_png:
            self.outputsaver.save_png(fig, diagnostic_product="hovmoller", metadata=self.description,
                                      rebuild=rebuild, dpi=dpi, extra_keys=extra_keys)

    def set_suptitle(self):
        """Set the suptitle for the Hovmoller plot."""
        self.suptitle = f"{self.catalog} {self.model} {self.exp} {self.region}"
        self.logger.debug(f"Suptitle set to: {self.suptitle}")

    def set_title(self):
        """
        Set the title for the Hovmoller plot.
        This method can be extended to set specific titles based on the data.
        """
        self.title_list = []
        for j in range(len(self.data)):
            for _, var in enumerate(self.vars):
                title = f"{var} ({self.data[j][var].attrs.get('units')})"
                self.title_list.append(title)
        self.logger.debug("Title list set to: %s", self.title_list)


    def set_description(self):
        """Set the description for the Hovmoller plot."""
        self.description = {}
        self.description['description'] = {f'Spatially averaged {self.region} region {self.diagnostic} of {self.catalog} {self.model} {self.exp}'}

    def set_vmax_vmin(self):
        """
        Set the vmax and vmin for the Hovmoller plot.
        This method can be extended to set specific vmax and vmin values.
        """
        self.logger.debug("Setting vmax and vmin")
        hovmoller_plot_dic = {
            'thetao' :
                {
                    'full': {'vmax': 40, 'vmin': 10 },
                    'anom_t0': {'vmax': 6, 'vmin': -6, 'cbar': 'coolwarm'},
                    'std_anom_t0': {'vmax': 5, 'vmin': -5, 'cbar': 'coolwarm'},
                    
                    'anom_tmean': {'vmax': 6, 'vmin': -6, 'cbar': 'coolwarm'},
                    'std_anom_tmean': {'vmax': 5, 'vmin': -5, 'cbar': 'coolwarm'},
                },
            'so' :
                {
                    'full': {'vmax': 38, 'vmin': 33, 'cbar': 'coolwarm'},
                    'anom_t0': {'vmax': 0.9, 'vmin': -0.3, 'cbar': 'coolwarm'},
                    'std_anom_t0': {'vmax': 5, 'vmin': -6, 'cbar': 'coolwarm'},
                    
                    'anom_tmean': {'vmax': 5, 'vmin': -5, 'cbar': 'coolwarm'},
                    'std_anom_tmean': {'vmax': 1, 'vmin': -1, 'cbar': 'coolwarm'},
                }
        }
        self.vmax = []
        self.vmin = []
        self.cmap = []
        for type in self.data_type:
            for var in self.vars:
                self.vmax.append(hovmoller_plot_dic[var][type].get('vmax'))
                self.vmin.append(hovmoller_plot_dic[var][type].get('vmin'))
                self.cmap.append(hovmoller_plot_dic[var][type].get('cbar', 'jet'))
                
        
    def set_data_type(self):
        """
        Set the texts for the Hovmoller plot.
        This method can be extended to set specific texts.
        """
        self.logger.debug("Setting texts")
        self.data_type = []
        for data in self.data:
            type = data.attrs.get('AQUA_ocean_drift_type', 'NA')
            self.data_type.append(type)

    def set_texts(self):
        """
        Set the texts for the Hovmoller plot.
        This method can be extended to set specific texts.
        """
        self.texts = []
        for _, data in enumerate(self.data):
            for j, _ in enumerate(self.vars):
                if j == 0:
                    type = data.attrs.get('AQUA_ocean_drift_type', 'NA')
                    self.texts.append(type)
                else:
                    self.texts.append(None)
        self.logger.debug("Texts set to: %s", self.texts)