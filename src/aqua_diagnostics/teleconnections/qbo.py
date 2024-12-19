"""
Module to evaluate the QBO teleconnection.
"""
import xarray as xr

from aqua.graphics import plot_hovmoller
from .tc_class import Teleconnection

# set default options for xarray
xr.set_options(keep_attrs=True)


class QBO(Teleconnection):
    """
    Class to evaluate the QBO teleconnection.
    """

    def __init__(self, model: str, exp: str, source: str,
                 telecname: str,
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
        super().__init__(model, exp, source, telecname,
                         catalog=catalog,
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
    
    def _process_hovmoller(self):
        """
        Prepare the variable to be used by the Hovmoller plot.
        """
