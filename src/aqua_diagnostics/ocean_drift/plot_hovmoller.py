import xarray as xr
from aqua.logger import log_configure
from .multiple_hovmoller import plot_multi_hovmoller
from aqua.diagnostics.core import OutputSaver

xr.set_options(keep_attrs=True)

class PlotHovmoller:
    def __init__(self,
                 data: list [xr.Dataset],
                 diagnostic: str = "ocean_drift",
                 outputdir: str = ".",
                 rebuild: bool = True,
                 loglevel: str = "WARNING"):
        self.data = data
        self.loglevel = loglevel
        self.logger = log_configure(self.loglevel, "PlotHovmoller")
        self.diagnostic = diagnostic
        self.vars = list(self.data[0].data_vars)
        self.logger.debug("Variables in data: %s", self.vars)
        self.catalog = self.data[0][self.vars[0]].AQUA_catalog
        self.model = self.data[0][self.vars[0]].AQUA_model
        self.exp = self.data[0][self.vars[0]].AQUA_exp
        self.region = self.data[0].AQUA_region
        self.outputsaver = OutputSaver(
            diagnostic=self.diagnostic,
            catalog=self.catalog,
            model=self.model,
            exp=self.exp, 
            outdir=outputdir, 
            rebuild=rebuild, 
            loglevel=self.loglevel)
            
    
    def plot_hovmoller(self):
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
            vars=self.vars,
            vmax=self.vmax,
            vmin=self.vmin,
            cmap=self.cmap,
            text=self.texts
        )
        self.outputsaver.save_pdf(fig, diagnostic_product="Hovmoller", metadata= self.description)

    def set_suptitle(self):
        """
        Set the title for the Hovmoller plot.
        """
        self.logger.debug("Setting title")
        self.suptitle = f"{self.catalog} {self.model} {self.exp} {self.region}"

    def set_title(self):
        """
        Set the title for the Hovmoller plot.
        This method can be extended to set specific titles based on the data.
        """
        self.logger.debug("Setting suptitle")
        self.title_list = []
        for j in range(len(self.data)):
            for i, var in enumerate(self.vars):
                if j == 0:
                    title = f"{var} ({self.data[j][var].attrs.get('units')})"
                    self.title_list.append(title)
                else:
                    self.title_list.append(None)


    def set_description(self):
        self.description = {}
        self.description['description'] = {f'{self.region} {self.diagnostic} {self.catalog} {self.model} {self.exp}'}
    
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
                    'anom_t0': {'vmax': 5, 'vmin': -5, 'cbar': 'coolwarm'},
                    'std_anom_t0': {'vmax': 1, 'vmin': -1, 'cbar': 'coolwarm'},
                    
                    'anom_tmean': {'vmax': 5, 'vmin': -5, 'cbar': 'coolwarm'},
                    'std_anom_tmean': {'vmax': 1, 'vmin': -1, 'cbar': 'coolwarm'},
                },
            'so' :
                {
                    'full': {'vmax': 400, 'vmin': 200, 'cbar': 'coolwarm'},
                    'anom_t0': {'vmax': 100, 'vmin': -100, 'cbar': 'coolwarm'},
                    'std_anom_t0': {'vmax': 1, 'vmin': -1, 'cbar': 'coolwarm'},
                    
                    'anom_tmean': {'vmax': 5, 'vmin': -5, 'cbar': 'coolwarm'},
                    'std_anom_tmean': {'vmax': 1, 'vmin': -1, 'cbar': 'coolwarm'},
                }
        }
        self.vmax = []
        self.vmin = []
        self.cmap = []
        for var in self.vars:
            for type in self.data_type:
                self.vmax.append(hovmoller_plot_dic[var][type]['vmax'])
                self.vmin.append(hovmoller_plot_dic[var][type]['vmin'])
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
        self.texts = []
        for i, data in enumerate(self.data):
            for j, var in enumerate(self.vars):
                if j == 0:
                    type = data.attrs.get('AQUA_ocean_drift_type', 'NA')
                    self.texts.append(type)
                else:
                    self.texts.append(None)