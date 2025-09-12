import xarray as xr
from aqua.logger import log_configure
from aqua.diagnostics.core import OutputSaver

from .multiple_map import plot_maps
from .multivar_vertical_profiles import plot_multivars_vertical_profile

xr.set_options(keep_attrs=True)


class PlotTrends:
    def __init__(
        self,
        data: xr.Dataset,
        diagnostic: str = "ocean_trends",
        outputdir: str = ".",
        rebuild: bool = True,
        loglevel: str = "WARNING",
    ):
        self.data = data

        self.loglevel = loglevel
        self.logger = log_configure(self.loglevel, "PlotHovmoller")

        self.diagnostic = diagnostic
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

    def plot_multilevel(self):
        self.set_levels()
        self.set_data_list()
        self.set_suptitle()
        self.set_title()
        self.set_description()
        self.set_ytext()
        self.set_nrowcol()
        fig = plot_maps(
            maps=self.data_list,
            nrows=self.nrows,
            ncols=self.ncols,
            title=self.suptitle,
            titles=self.title_list,
            cbar_number='separate',
            ytext=self.ytext,
            return_fig=True
        )
        self.save_plot(
            fig,
            diagnostic_product=self.diagnostic,
            format='pdf',
            metadata=self.description
        )

    def plot_zonal(self):
        # self.set_levels()
        self.set_data_list()
        self.set_suptitle(plot_type='Zonal mean ')
        self.set_title()
        self.set_description()
        self.set_ytext()
        self.set_nrowcol()
        fig = plot_multivars_vertical_profile(
            maps=self.data_list,
            nrows=self.nrows,
            ncols=self.ncols,
            title=self.suptitle,
            titles=self.title_list,
            cbar_number='separate',
            ytext=self.ytext,
            return_fig=True
        )
        self.save_plot(
            fig,
            diagnostic_product=self.diagnostic,
            format='pdf',
            metadata=self.description
        )

    def set_nrowcol(self):
        if hasattr(self, "levels") and self.levels:
            self.nrows = len(self.levels)
        else:
            self.nrows = 1
        self.ncols = len(self.vars)

    def set_ytext(self):
        self.ytext = []
        if hasattr(self, "levels") and self.levels:
            for level in self.levels:
                for i in range(len(self.vars)):
                    if i == 0:
                        self.ytext.append(f"{level}m")
                    else:
                        self.ytext.append(None)

    def set_levels(self):
        self.levels = [50, 100]
        self.logger.debug(f"Levels set to: {self.levels}")

    def set_data_list(self):
        self.data_list = []
        if hasattr(self, "levels") and self.levels:
            self.data = self.data.interp(level=self.levels)
            for level in self.levels:
                for var in self.vars:
                    if level == 0:
                        data_level_var = self.data[var].isel(level=-1)
                    else:
                        data_level_var = self.data[var].sel(level=level)

                    data_level_var.attrs["long_name"] = (
                        f"{data_level_var.attrs.get('long_name', var)} at {level}m"
                    )
                    self.data_list.append(data_level_var)
        else:
            for var in self.vars:
                data_var = self.data[var]
                self.data_list.append(data_var)

    def set_suptitle(self, plot_type = None):
        """Set the title for the Hovmoller plot."""
        if plot_type is None:
            plot_type = ""
        self.suptitle = f"{self.catalog} {self.model} {self.exp} {self.region} {plot_type}Trends"
        self.logger.debug(f"Suptitle set to: {self.suptitle}")

    def set_title(self):
        """
        Set the title for the Hovmoller plot.
        This method can be extended to set specific titles based on the data.
        """
        self.title_list = []
        for j in range(len(self.data_list)):
            for i, var in enumerate(self.vars):
                if j == 0:
                    title = f"{var} ({self.data[var].attrs.get('units')})"
                    self.title_list.append(title)
                else:
                    self.title_list.append(" ")
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