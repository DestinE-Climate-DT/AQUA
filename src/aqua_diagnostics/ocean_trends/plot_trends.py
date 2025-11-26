import xarray as xr
from aqua.logger import log_configure
from aqua.diagnostics.core import OutputSaver
from aqua.util import get_realizations
import cartopy.crs as ccrs

from .multiple_maps import plot_maps
from .multivar_vertical_profiles import plot_multivars_vertical_profile

xr.set_options(keep_attrs=True)


class PlotTrends:
    def __init__(
        self,
        data: xr.Dataset,
        diagnostic_name: str = "trends",
        outputdir: str = ".",
        rebuild: bool = True,
        loglevel: str = "WARNING",
    ):
        """Class to plot trends from xarray Dataset.
    
        Args:
            data (xr.Dataset): Input xarray Dataset containing trend data.
            diagnostic_name (str, optional): Name of the diagnostic for filenames. Defaults to "trends".
            outputdir (str, optional): Directory to save output plots. Defaults to ".".
            rebuild (bool, optional): Whether to rebuild output files. Defaults to True.
            loglevel (str, optional): Logging level. Default is "WARNING".
        """
        self.loglevel = loglevel
        self.logger = log_configure(self.loglevel, "PlotTrends")

        self.data = data
        self.diagnostic_name = diagnostic_name
        self.outputdir = outputdir
        self.rebuild = rebuild

        self.vars = list(self.data.data_vars)
        self.logger.debug("Variables in data: %s", self.vars)

        # Initialize metadata attributes
        self._get_info()

        self.outputsaver = OutputSaver(
            diagnostic=self.diagnostic_name,
            catalog=self.catalog,
            model=self.model,
            exp=self.exp,
            outputdir=outputdir,
            realization = self.realizations,
            loglevel=self.loglevel
        )

    def plot_multilevel(self,
                        levels = None,
                        rebuild: bool = True,
                        save_pdf: bool = True,
                        save_png: bool = True, dpi: int = 300):
        """Plot multi-level maps of trends.
        
        Args:
            levels (list, optional): List of depth levels to plot. Defaults to None.
            formats (list, optional): List of output formats. Defaults to ['pdf'].
        """
        self.diagnostic_product = 'multilevel_trend'
        if levels is None:
            self.levels = [10, 100, 500, 1000, 3000, 5000]
        self.logger.debug(f"Levels set to: {self.levels}")
        self.set_central_longitude()
        self.set_data_list()
        self.set_suptitle(plot_type='Multi-level Trends')
        self.set_title()
        self.set_description(content="Multi-level Trends")
        self.set_ytext()
        self.set_cbar_labels()
        self.set_nrowcol()
        fig = plot_maps(
            maps=self.data_list,
            nrows=self.nrows,
            ncols=self.ncols,
            proj=ccrs.PlateCarree(central_longitude=self.central_longitude),
            title=self.suptitle,
            titles=self.title_list,
            cbar_labels=self.cbar_labels,
            ytext=self.ytext,
            return_fig=True,
            loglevel=self.loglevel
        )
        formats = []
        if save_pdf:
            formats.append('pdf')
        if save_png:
            formats.append('png')

        for format in formats:
            self.save_plot(fig, diagnostic_product=self.diagnostic_product, metadata={"description": self.description},
                           rebuild=rebuild, dpi=dpi, format=format, extra_keys={'region': self.region})


    def plot_zonal(self,
                    rebuild: bool = True,
                    save_pdf: bool = True,
                    save_png: bool = True, dpi: int = 300):
        """
        Plot zonal mean vertical profiles of trends.
        
        Args:
            formats (list, optional): List of output formats. Defaults to ['pdf'].
            dpi (int, optional): Dots per inch for the output figure. Defaults to 300.
        """
        self.diagnostic_product = 'zonal_mean'
        self.set_data_list()
        self.set_suptitle(plot_type='Zonal mean Trends')
        self.set_title()
        self.set_description(content="Zonal mean Trends")
        self.set_ytext()
        self.set_cbar_labels()
        self.set_nrowcol()
        fig = plot_multivars_vertical_profile(
            maps=self.data_list,
            nrows=self.nrows,
            ncols=self.ncols,
            title=self.suptitle,
            titles=self.title_list,
            cbar_labels=self.cbar_labels,
            ytext=self.ytext,
            return_fig=True,
            sym=True,
            loglevel=self.loglevel
        )
        formats = []
        if save_pdf:
            formats.append('pdf')
        if save_png:
            formats.append('png')

        for format in formats:
            self.save_plot(fig, diagnostic_product=self.diagnostic_product, metadata={"description": self.description},
                           rebuild=rebuild, dpi=dpi, format=format, extra_keys={'region': self.region})

    def set_nrowcol(self):
        if hasattr(self, "levels") and self.levels:
            self.nrows = len(self.levels)
        else:
            self.nrows = 1
        self.ncols = len(self.vars)

    def set_ytext(self):
        """Set the y-axis text for the multi-level plots."""
        self.ytext = []
        if hasattr(self, "levels") and self.levels:
            for level in self.levels:
                for i in range(len(self.vars)):
                    if i == 0:
                        self.ytext.append(f"{level}m")
                    else:
                        self.ytext.append(None)

    def set_central_longitude(self):
        self.central_longitude = self.data.lon.mean().values
        self.logger.debug(f"Central longitude set to: {self.central_longitude}")
    def set_data_list(self):
        """Prepare the list of data arrays to plot."""
        self.data_list = []
        if hasattr(self, "levels") and self.levels:
            self.data = self.data.interp(level=self.levels)
            for level in self.levels:
                for var in self.vars:
                    if level == 0:
                        data_level_var = self.data[var].isel(level=-1)
                    else:
                        data_level_var = self.data[var].sel(level=level)

                    if data_level_var.isnull().all():
                        self.logger.warning(f"All values are NaN for {var} at {level}m")
                        self.levels.pop(self.levels.index(level))
                        break

                    data_level_var.attrs["long_name"] = (
                        f"{data_level_var.attrs.get('long_name', var)} at {level}m"
                    )
                    self.data_list.append(data_level_var)
        else:
            for var in self.vars:
                data_var = self.data[var]
                self.data_list.append(data_var)

    def set_suptitle(self, plot_type = None):
        """Set the title for the plot."""
        if plot_type is None:
            plot_type = ""
        self.suptitle = f"{plot_type} in {self.region} - {self.catalog} {self.model} {self.exp}"
        self.logger.debug(f"Suptitle set to: {self.suptitle}")

    def set_title(self):
        """
        Set the title for the plot.
        This method can be extended to set specific titles based on the data.
        """
        self.title_list = []
        for j in range(len(self.data_list)):
            for var in self.vars:
                if j == 0:
                    title = f"{self.data[var].attrs.get('long_name', var)} ({self.data[var].attrs.get('units')})"
                    self.title_list.append(title)
                else:
                    self.title_list.append(" ")
        self.logger.debug("Title list set to: %s", self.title_list)
    
    def set_cbar_labels(self):
        """
        Set the colorbar labels for the plot.
        This method can be extended to set specific colorbar labels based on the data.
        """
        self.cbar_labels = []
        for _ in range(len(self.data_list)):
            for var in self.vars:
                cbar_label = f"{self.data[var].attrs.get('short_name', var)} ({self.data[var].attrs.get('units')})"
                self.cbar_labels.append(cbar_label)
        self.logger.debug("Colorbar labels set to: %s", self.cbar_labels)

    def set_description(self, content=None):
        """
        Set the description metadata for the plot.
        """

        self.description = f"{content} in the {self.region} region of {self.catalog} {self.model} {self.exp}."

    def save_plot(self, fig, diagnostic_product: str, extra_keys: dict = {},
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
        extra_keys.update({"region": self.region})

        if format == 'png':
            result = self.outputsaver.save_png(fig, diagnostic_product=diagnostic_product, rebuild=rebuild,
                                               extra_keys=extra_keys, metadata=metadata, dpi=dpi)
        elif format == 'pdf':
            result = self.outputsaver.save_pdf(fig, diagnostic_product=diagnostic_product, rebuild=rebuild,
                                               extra_keys=extra_keys, metadata=metadata)
        self.logger.info(f"Figure saved as {result}")

    def _get_info(self):
        """Extract model, catalog, exp, region from data attributes."""
        self.catalog = self.data[self.vars[0]].AQUA_catalog
        self.model = self.data[self.vars[0]].AQUA_model
        self.exp = self.data[self.vars[0]].AQUA_exp
        self.realizations = get_realizations(self.data[self.vars[0]])
        self.region = self.data.attrs.get("AQUA_region", "global")
