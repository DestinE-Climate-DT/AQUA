"""
Module to evaluate the QBO teleconnection.
"""
import os
import xarray as xr

from aqua.util import OutputSaver
from aqua.graphics import plot_hovmoller
from .tc_class import Teleconnection

# set default options for xarray
xr.set_options(keep_attrs=True)


class QBO(Teleconnection):
    """
    Class to evaluate the QBO teleconnection.
    """

    def __init__(self, model: str, exp: str, source: str,
                 catalog=None,
                 configdir=None, aquaconfigdir=None,
                 interface='teleconnections-destine',
                 regrid=None, freq=None,
                 save_pdf=False, save_png=False,
                 save_netcdf=False, outputdir='./',
                 filename=None,
                 startdate=None, enddate=None,
                 loglevel: str = 'WARNING',
                 filename_keys: list = None,
                 rebuild: bool = True, dpi=None):
        """
        Initialize the QBO class.

        Args:
            config: Configuration object.
        """
        super().__init__(model=model, exp=exp, source=source,
                         telecname='QBO', catalog=catalog,
                         configdir=configdir, aquaconfigdir=aquaconfigdir,
                         interface=interface,
                         regrid=regrid, freq=freq,
                         save_pdf=save_pdf, save_png=save_png,
                         save_netcdf=save_netcdf, outputdir=outputdir,
                         filename=filename,
                         startdate=startdate, enddate=enddate,
                         loglevel=loglevel,
                         filename_keys=filename_keys,
                         rebuild=rebuild, dpi=dpi)
    
    def run(self, **kwargs):
        """
        Run the QBO teleconnection.
        """
        self.retrieve(**kwargs)
        self.plot_hovmoller(**kwargs)
    
    def retrieve(self, **kwargs):
        """
        Retrieve teleconnection data with the AQUA reader.
        The data is saved as teleconnection attribute and can be accessed
        with self.data.

        Args:
            **kwargs: Keyword arguments to be passed to the reader.

        Raises:
            NoDataError: If the data is not available.
        """
        super().retrieve(**kwargs)

        # Select the variable
        self.data = self.data[self.var]

        # Process the data
        self._process_hovmoller()
    
    def plot_hovmoller(self, **kwargs):
        """
        Plot the Hovmoller diagram.
        """
        title = f'QBO {self.model} {self.exp} {self.source}'

        if self.save_pdf or self.save_png:
            outputsaver = self._get_output_saver()
        else:
            filename = None

        formats = []
        if self.save_pdf:
            formats.append('pdf')
        if self.save_png:
            formats.append('png')

        plot_kwargs = {'title': title, 'dim': ['lat'], 'vmin': -40, 'vmax': 40,
                       'cmap': 'RdBu_r', 'nlevels': 21, 'figsize': (13,5),
                       'ylog': True, 'yinvert': True, 'loglevel': self.loglevel,
                       **kwargs}
        
        
        fig, ax = plot_hovmoller(self.data, **plot_kwargs, return_fig=True)
        fig.tight_layout()
        ax.set_ylabel('Pressure levels (hPa)')
    
        for format in formats:
            filename = outputsaver.generate_name(diagnostic_product=self.telecname,
                                                suffix=format)
            self.logger.debug(f"Filename: {filename}")
            filepath = os.path.join(self.outputdir, filename)
            fig.savefig(filepath, dpi=self.dpi)

    def _process_hovmoller(self):
        """
        Prepare the variable to be used by the Hovmoller plot.
        """
        # Select the time range
        self.data = self.data.sel(time=slice(self.startdate, self.enddate))

        # Select the latitude range
        latN = self.namelist[self.telecname]['latN']
        latS = self.namelist[self.telecname]['latS']

        self.data = self.data.sel(lat=slice(latS, latN))

        # Select the longitude range -> No range in QBO
        # lonW = self.namelist[self.telecname]['lonW']
        # lonE = self.namelist[self.telecname]['lonE']
        try:
            self.data = self.data.mean(dim='lon')
        except ValueError:
            self.logger.debug("Longitude mean already taken.")

        # self.data = self.data.sel(lon=slice(lonW, lonE))

        # Select the level range. This is flipped because they are in Pa
        Pamin = self.namelist[self.telecname]['Pamin']
        Pamax = self.namelist[self.telecname]['Pamax']

        self.data = self.data.sel(plev=slice(Pamax, Pamin))

        # Change plev units to hPa
        if self.data['plev'].attrs['units'] == 'Pa':
            self.logger.info("Changing plev units to hPa.")
            self.data['plev'] = self.data['plev'] / 100
            self.data['plev'].attrs['units'] = 'hPa'

    def _get_output_saver(self):
        """
        Create and return an OutputSaver instance.
        """
        return OutputSaver(diagnostic='teleconnections', catalog=self.catalog,
                           model=self.model, exp=self.exp,
                           loglevel=self.loglevel, default_path=self.outputdir)
