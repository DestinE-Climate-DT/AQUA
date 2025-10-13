import xarray as xr
from aqua.logger import log_configure
from aqua.diagnostics.core import OutputSaver
import cartopy.crs as ccrs
from aqua.util import cbar_get_label
import math

from .mld_profiles import plot_maps
from .multiple_vertical_line import plot_multi_vertical_lines

xr.set_options(keep_attrs=True)


class PlotStratification:
    def __init__(
        self,
        data: xr.Dataset,
        obs: xr.Dataset = None,
        clim_time: str = "January",
        diagnostic_name: str = "ocean_stratification",
        outputdir: str = ".",
        loglevel: str = "WARNING",
    ):
        self.data = data
        self.obs = obs
        self.clim_time = clim_time

        self.loglevel = loglevel
        self.logger = log_configure(self.loglevel, "PlotStratification")

        self.diagnostic = diagnostic_name
        self.vars = list(self.data.data_vars)
        self.logger.debug("Variables in data: %s", self.vars)

        self.catalog = self.data[self.vars[0]].AQUA_catalog
        self.model = self.data[self.vars[0]].AQUA_model
        self.exp = self.data[self.vars[0]].AQUA_exp
        self.region = self.data.attrs.get("AQUA_region", "global")

        self.outputsaver = OutputSaver(
            diagnostic=self.diagnostic,
            catalog=self.catalog,
            model=self.model,
            exp=self.exp,
            outputdir=outputdir,
            loglevel=self.loglevel,
        )

    def plot_stratification(self, rebuild: bool = True, save_pdf: bool = True,
                       save_png: bool = True, dpi: int = 300):
        self.data_list = [self.data, self.obs] if self.obs else [self.data]
        self.set_data_list()
        self.set_suptitle()
        self.set_title()
        self.set_description()
        self.set_ytext()
        self.set_nrowcol()
        self.set_cbar_labels(var= 'rho')
        self.set_label_line_plot()
        fig = plot_multi_vertical_lines(
            maps=self.data_list,
            nrows=self.nrows,
            ncols=self.ncols,
            variables=self.vars,
            data_label=self.data_label,
            obs_label=self.obs_label if self.obs else None,
            title=self.suptitle,
            figsize=(4 * self.ncols, 10 * self.nrows),
            return_fig=True,
            loglevel=self.loglevel,
        )


        self.save_plot(
            fig,
            rebuild=rebuild,
            dpi=dpi,
            format='pdf',
            diagnostic_product=self.diagnostic,
            metadata=self.description,
            extra_keys={"region": self.region.replace(' ','_')},
        )

    def set_nrowcol(self):
        if hasattr(self, "levels") and self.levels:
            self.nrows = len(self.levels)
        else:
            self.nrows = 1
        self.ncols = len(self.vars)
        if self.obs:
            self.ncols = self.ncols * 2

    def set_ytext(self):
        self.ytext = []
        if hasattr(self, "levels") and self.levels:
            for level in self.levels:
                for i in range(len(self.vars)):
                    if i == 0:
                        self.ytext.append(f"{level}m")
                    else:
                        self.ytext.append(None)

    def set_label_line_plot(self):
        self.data_label = 'Model'
        if self.obs:
            self.obs_label = 'Obs'
    def set_data_list(self):
        self.data_list = [self.data]
        if self.obs:
            self.obs_data_list = [self.obs]
        # for data in self.data:
        #     for var in self.vars:
        #         data_var = data[[var]]
        #         self.data_list.append(data_var)

    def set_cbar_labels(self, var: str = None):
        self.cbar_label = cbar_get_label(data=self.data[var], cbar_label=None, loglevel=self.loglevel)

    def _round_up(self, value):
        if value % 100 == 0:
            return value  # Already a multiple of 100
        elif value % 100 <= 50:
            return math.ceil(value / 50) * 50  # Round up to next 50
        else:
            return math.ceil(value / 100) * 100  # Round up to next 100

    def set_cbar_limits(self):
        self.vmin = 0.0
        if self.obs:
            self.vmax = max(self.obs["mld"].max(), self.obs["mld"].max())
        else: 
            self.vmax = self.data["mld"].max()
        self.vmax = self._round_up(self.vmax)
        if self.vmax < 200:
            nlevels = 10
        elif self.vmax > 1500:
            nlevels = 100
        else:
            nlevels = 50
        self.nlevels = nlevels
        self.logger.debug(f"Colorbar limits set to vmin: {self.vmin}, vmax: {self.vmax}, nlevels: {self.nlevels}")


    def set_suptitle(self, plot_type = None):
        """Set the title for the MLD plot."""
        if plot_type is None:
            plot_type = ""
        clim_time = self.data.attrs.get("AQUA_stratification_climatology", "Total")
        # self.suptitle = f"{clim_time} climatology {self.catalog} {self.model} {self.exp} {self.region}"
        self.suptitle = f"MLD {clim_time} climatology {self.catalog} {self.model} {self.exp} {self.region}"
        self.logger.debug(f"Suptitle set to: {self.suptitle}")

    def set_title(self):
        """
        Set the title for the Hovmoller plot.
        This method can be extended to set specific titles based on the data.
        """
        self.title_list = []
        for j in range(len(self.data_list)):
            attrs = self.data_list[j].attrs
            for i, var in enumerate(self.vars):
                # if j == 0:
                    # title = f"{var} ({self.data[var].attrs.get('units')})"
                title = f"{attrs.get('AQUA_catalog')} {attrs.get('AQUA_model')} {attrs.get('AQUA_exp')}"
                self.title_list.append(title)
                # else:
                #     self.title_list.append(" ")
        self.logger.debug("Title list set to: %s", self.title_list)

    def set_description(self):
        self.description = {}
        self.description["description"] = {
            f"Spatially averaged {self.region} region {self.diagnostic} of {self.catalog} {self.model} {self.exp}"
        }

    def save_plot(self, fig, diagnostic_product: str = None, extra_keys: dict = None,
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
            result = self.outputsaver.save_png(fig, diagnostic_product=diagnostic_product, rebuild=rebuild,
                                          extra_keys=extra_keys, metadata=metadata, dpi=dpi)
        elif format == 'pdf':
            result = self.outputsaver.save_pdf(fig, diagnostic_product=diagnostic_product, rebuild=rebuild,
                                          extra_keys=extra_keys, metadata=metadata)
        self.logger.info(f"Figure saved as {result}")