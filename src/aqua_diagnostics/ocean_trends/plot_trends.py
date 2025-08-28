import xarray as xr
from aqua.logger import log_configure
from aqua.diagnostics.core import OutputSaver

# from .multiple_hovmoller import plot_multi_hovmoller
from .multiple_map import plot_maps

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
        self.region = self.data.region

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
        plot_maps(
            maps=self.data_list,
            nrows=4,
            ncols=2,
            title=self.suptitle,
            titles=self.title_list,
            cbar_number='separate',
            ytext=self.ytext,
        )
    def set_ytext(self):
        self.ytext = []
        for level in self.levels:
            for i in range(len(self.vars)):
                if i == 0:
                    self.ytext.append(f"{level}m")
                else:
                    self.ytext.append(None)

    def set_levels(self):
        self.levels = [100, 200, 300, 400]
        self.logger.debug(f"Levels set to: {self.levels}")

    def set_data_list(self):
        self.data_list = []
        self.data = self.data.interp(level=self.levels)
        for level in self.levels:
            for var in self.vars:
                data_level_var = self.data[var].sel(level=level)
                data_level_var.attrs["long_name"] = (
                    f"{data_level_var.attrs.get('long_name', var)} at {level}m"
                )
                self.data_list.append(data_level_var)

    def set_suptitle(self):
        """Set the title for the Hovmoller plot."""
        self.suptitle = f"{self.catalog} {self.model} {self.exp} {self.region}"
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
