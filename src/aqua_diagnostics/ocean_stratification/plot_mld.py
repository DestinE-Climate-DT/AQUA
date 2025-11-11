import xarray as xr
from aqua.logger import log_configure
from aqua.diagnostics.core import OutputSaver
import cartopy.crs as ccrs
from aqua.util import cbar_get_label
import math

from .mld_profiles import plot_maps

# from .multivar_vertical_profiles import plot_multivars_vertical_profile

xr.set_options(keep_attrs=True)


class PlotMLD:
    def __init__(
        self,
        data: xr.Dataset,
        obs: xr.Dataset = None,
        diagnostic_name: str = "ocean_stratification",
        outputdir: str = ".",
        loglevel: str = "WARNING",
    ):
        """
        Class to plot Mixed Layer Depth (MLD) maps.

        Args:
            data (xr.Dataset): Dataset containing the MLD data to be plotted.
            obs (xr.Dataset, optional): Dataset containing observational MLD data for comparison. Default is None.
            clim_time (str, optional): Climatological time period for the data. Default is "January".
            diagnostic_name (str, optional): Name of the diagnostic. Default is "ocean_stratification".
            outputdir (str, optional): Directory to save the output plots. Default is the current directory.
            loglevel (str, optional): Logging level. Default is "WARNING".
        """
        self.data = data
        self.obs = obs

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

    def plot_mld(
        self,
        rebuild: bool = True,
        save_pdf: bool = True,
        save_png: bool = True,
        dpi: int = 300,
    ):
        self.diagnostic_product = "mld"
        self.clim_time = self.data.attrs.get("AQUA_stratification_climatology", "Total")
        self.data_list = [self.data, self.obs] if self.obs else [self.data]
        self.set_central_longitude()
        self.set_data_map_list()
        self.set_suptitle()
        self.set_title()
        self.set_description()
        self.set_ytext()
        self.set_nrowcol()
        self.set_figsize()
        self.set_cbar_labels(var="mld")
        self.set_cbar_limits()
        fig = plot_maps(
            maps=self.data_map_list,
            nrows=self.nrows,
            ncols=self.ncols,
            proj=ccrs.PlateCarree(central_longitude=self.central_longitude),
            title=self.suptitle,
            titles=self.title_list,
            cbar_number="single",
            cbar_label=self.cbar_label,
            figsize=self.figsize,
            cmap="jet",
            ytext=self.ytext,
            return_fig=True,
            vmax=self.vmax,
            vmin=self.vmin,
            nlevels=self.nlevels,
            sym=False,
        )

        formats = []
        if save_pdf:
            formats.append('pdf')
        if save_png:
            formats.append('png')

        for format in formats:
            self.save_plot(fig, diagnostic_product=self.diagnostic_product, metadata={"description": self.description},
                           rebuild=rebuild, dpi=dpi, format=format, extra_keys={'region': self.region})

    def set_figsize(self):
        self.figsize = (9 * self.ncols, 8 * self.nrows)

        # lon_span = abs(self.data.lon.max() - self.data.lon.min())
        # lat_span = abs(self.data.lat.max() - self.data.lat.min())

        # # Avoid division by zero
        # if lat_span == 0:
        #     lat_span = 1e-6

        # # Set figure size proportional to lon:lat ratio
        # base_width = 9 * self.ncols
        # base_height = 8 * self.nrows

        # aspect_ratio = lon_span / lat_span * 0.6
        # self.figsize = (base_width * aspect_ratio, base_height)

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

    def set_central_longitude(self):
        self.central_longitude = self.data.lon.mean().values
        self.logger.debug(f"Central longitude set to: {self.central_longitude}")

    def set_data_map_list(self):
        self.data_map_list = []
        for data in self.data_list:
            if hasattr(self, "levels") and self.levels:
                data = data.interp(level=self.levels)
                for level in self.levels:
                    for var in self.vars:
                        if level == 0:
                            data_level_var = data[var].isel(level=-1)
                        else:
                            data_level_var = data[var].sel(level=level)

                        data_level_var.attrs["long_name"] = (
                            f"{data_level_var.attrs.get('long_name', var)} at {level}m"
                        )
                        self.data_map_list.append(data_level_var)
            else:
                for var in self.vars:
                    data_var = data[var]
                    self.data_map_list.append(data_var)

    def set_cbar_labels(self, var: str = None):
        self.cbar_label = cbar_get_label(
            data=self.data[var], cbar_label=None, loglevel=self.loglevel
        )
    def set_convert_lon(self, data=None):
        '''Convert longitude from 0-360 to -180 to 180 and sort accordingly.'''
        data = data.assign_coords(lon=((data.lon + 180) % 360) - 180)
        data = data.sortby('lon')

        lat_limits = data.attrs['AQUA_lat_limits']
        lon_limits = data.attrs['AQUA_lon_limits']


        if lon_limits != None:
            lon_min, lon_max = lon_limits
            lon_min = ((lon_min + 180) % 360) - 180
            lon_max = ((lon_max + 180) % 360) - 180
            ds_reg = self.data
            if lon_min < lon_max:
                ds_reg = ds_reg.sel(lon=slice(lon_min, lon_max))
            else:
                ds_reg = xr.concat(
                    [
                        ds_reg.sel(lon=slice(lon_min, 180)),
                        ds_reg.sel(lon=slice(-180, lon_max)),
                    ],
                    dim="lon"
                )
            data = ds_reg
        return data
    
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
        self.logger.debug(
            f"Colorbar limits set to vmin: {self.vmin}, vmax: {self.vmax}, nlevels: {self.nlevels}"
        )

    def set_suptitle(self, plot_type=None):
        """Set the title for the MLD plot."""
        if plot_type is None:
            plot_type = ""
        self.suptitle = f"MLD in {self.region} - {self.clim_time} climatology - {self.catalog} {self.model} {self.exp}"
        self.logger.debug(f"Suptitle set to: {self.suptitle}")

    def set_title(self):
        """
        Set the title for the Hovmoller plot.
        This method can be extended to set specific titles based on the data.
        """
        self.title_list = []
        for j in range(len(self.data_map_list)):
            attrs = self.data_map_list[j].attrs
            for i, var in enumerate(self.vars):
                # if j == 0:
                # title = f"{var} ({self.data[var].attrs.get('units')})"
                title = f"{attrs.get('AQUA_catalog')} {attrs.get('AQUA_model')} {attrs.get('AQUA_exp')}"
                self.title_list.append(title)
                # else:
                #     self.title_list.append(" ")
        self.logger.debug("Title list set to: %s", self.title_list)

    def set_description(self):
        self.description = f"Mixed layer depth plot of spatially averaged {self.region} region, {self.clim_time} climatology for the {self.catalog} {self.model} {self.exp} experiment"
        if self.obs:
            self.description = self.description + (f" with the reference data from {self.obs.attrs['catalog']} {self.obs.attrs['model']} {self.obs.attrs['exp']}")

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
