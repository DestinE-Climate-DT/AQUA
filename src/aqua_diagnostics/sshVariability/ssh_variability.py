import gc
import sys
import xarray as xr
from .base import BaseMixin

# import pandas as pd
# from aqua.fldstat import AreaSelection
# from aqua.exceptions import NoDataError, NoObservationError, NotEnoughDataError

xr.set_options(keep_attrs=True)


class sshVariabilityCompute(BaseMixin):
    """
    SSH Computation
    """

    def __init__(
        self,
        diagnostic_name: str = "sshVariability",
        catalog: str = None,
        model: str = None,
        exp: str = None,
        source: str = None,
        startdate: str = None,
        enddate: str = None,
        freq: str = None,
        region: str = None,
        regrid: str = None,
        lon_limits: list[float] = None,
        lat_limits: list[float] = None,
        var: str = "zos",
        long_name: str = None,
        short_name: str = None,
        units: str = None,
        save_netcdf: bool = True,
        rebuild: bool = True,
        outputdir: str = "./",
        reader_kwargs: dict = {},
        loglevel: str = "WARNING",
    ):
        """
        Initialize the 'sshVariabilityCompute' class.

        This class is designed to load an xarray.Dataset and computes STD.
        Args:
            diagnostic_name (str): Default is 'sshVariability'.
            catalog (str): catalog. It is Mandatory, if 'save_netcdf=True'.
            model (str): Name of the data
            exp (str): Name of the experiment
            source (str): the source.
            It is important to give these dates and input. Otherwise the whole dataset is retrieved.
            startdate (str): Start date.
            enddate  (str): End date.
            freq (str): Frequency of the data. In the TODO list. This becomes important when implementing the 'variance of the variances formula'.
            region (str): For subregion selection. Default is 'None'. In case of sub-region STD computation, this variable is mandatory.
            regrid (str): Regrid option for the data. NOTE: the regridding will be applied before computing the STD.
            If 'lon_limits' and 'lat_limits' are None, they are taken from region file in AQUA.
            lon_limits (list[float]): list of lon limits. Default is 'None'.
            lat_limits (list[float]): list of lat limits. Default is 'None'.
            var (str): Variable name for ssh data. Default is 'zos'.
            long_name (str): If not given extracted from the data.
            short_name (str): If not given extracted from the data.
            units (str): If not given extracted from the data.

            save_netcdf (bool): Default is 'True'.
            rebuild (bool): Recomputes and saves the netcdf. Default is "True".
            outputdir (str): output directory. Default is './'
            loglevel (str): Default WARNING.

        Keyword Args:
            zoom (int, optional): HEALPix grid zoom level (e.g. zoom=10 is h1024). Allows for multiple gridname definitions.
            realization (int, optional): The ensemble realization number, included in the output filename.
            **kwargs: Additional arbitrary keyword arguments to be passed as additional parameters to the intake catalog entry.

        """
        # TODO:
        #   If the catalog entry of the output exists retrieve that data and check the regridding option for the data, i.e., Retrieve the data if the STD file already exits.
        #   Implement the technique: "Variance of Variances fomula" for computing STD.
        #   Include information about freq of the data.
        #   The STD is computed using xarray.std(dim="time"). Test if this works for the native grids.

        super().__init__(
            catalog=catalog,
            model=model,
            exp=exp,
            source=source,
            startdate=startdate,
            enddate=enddate,
            region=region,
            regrid=regrid,
            lon_limits=lon_limits,
            lat_limits=lat_limits,
            reader_kwargs=reader_kwargs,
            var=var,
            long_name=long_name,
            short_name=short_name,
            units=units,
            outputdir=outputdir,
            rebuild=rebuild,
            loglevel=loglevel,
        )

        self.save_netcdf = save_netcdf
        self.freq = freq
        # To be assigned inside after STD computation run()
        self.data_std = None
        self.startdate = startdate
        self.enddate = enddate

    def run(self):
        """
        Args:
            create_catalog_entry (bool): Option for creating catalog entry. Default is 'False'.

        This function performs following three functions:
        a) Retrieve data and regrid if given then
        b) Compute STD
        c) Save netcdf
        """

        super().retrieve()
        if self.data is None:
            raise ValueError(f"Variable {self.var} not found in the data. " "Check the variable name and the data source.")
        try:
            # b)
            # Compute STD
            self.data_std = self.data.std(dim="time", skipna=True).compute()
            # Removing the reference and releasing the memory from the Object reference, which is no longer needed
            del self.data
            gc.collect()

            # c)
            # Save STD as netcdf
            if self.save_netcdf:
                self.logger.info(f"Output std netcdf file is saved at {self.outputdir}.")
                self.netcdf_save(data=self.data_std, create_catalog_entry=True)
            else:
                self.logger.info("Output in netcdf is not saved.")
        except Exception as e:
            raise RuntimeError(f"No model data found: {e}")
            sys.exit("SSH diagnostic terminated.")
