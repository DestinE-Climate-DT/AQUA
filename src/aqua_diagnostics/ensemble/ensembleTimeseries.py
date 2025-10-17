import xarray as xr
from aqua.logger import log_configure

# from aqua.exceptions import NoDataError
from .base import BaseMixin
from .util import compute_statistics

xr.set_options(keep_attrs=True)


class EnsembleTimeseries(BaseMixin):
    """
    This class computes mean and standard deviation of the timeseries ensemble.

    NOTE: The STD is computed Point-wise along the mean.
    """

    def __init__(
        self,
        var=None,
        hourly_data=None,
        daily_data=None,
        monthly_data=None,
        annual_data=None,
        catalog_list=None,
        model_list=None,
        exp_list=None,
        source_list=None,
        ensemble_dimension_name="ensemble",
        description=None,
        outputdir="./",
        loglevel="WARNING",
    ):
        """
        Args:
            var (str): Variable name.
            hourly_data: xarray Dataset of ensemble members of hourly timeseries.
                     The ensemble memebers are concatenated along a new dimension "ensemble".
            daily_data: xarray Dataset of ensemble members of daily timeseries.
                     The ensemble memebers are concatenated along a new dimension "ensemble".
            monthly_data: xarray Dataset of ensemble members of monthly timeseries.
                     The ensemble memebers are concatenated along a new dimension "ensemble".
            annual_data: xarray Dataset of ensemble members of annual timeseries.
                     The ensemble members are concatenated along the dimension "ensemble"
            ensemble_dimension_name="ensemble" (str): a default name given to the
                     dimensions along with the individual Datasets were concatenated.
            catalog_list (list): list of catalog names.
            model_list (list): list of model names. This is mandotory.
            exp_list (list): list of experiment names.
            source_list (list): list of source list.
            description (str): Description of the netcdf.
            outputdir (str): String input for output path.
            loglevel (str): Log level. Default is "WARNING".
        """
        self.loglevel = loglevel
        self.logger = log_configure(log_level=self.loglevel, log_name="Ensemble Timeseries")
        self.var = var
        self.dim = ensemble_dimension_name
        self.diagnostic_product = "EnsembleTimeseries"

        self.hourly_data = hourly_data
        self.daily_data = daily_data
        self.monthly_data = monthly_data
        self.annual_data = annual_data

        self.catalog_list = catalog_list
        self.model_list = model_list
        self.exp_list = exp_list
        self.source_list = source_list

        self.hourly_data_mean = None
        self.hourly_data_std = None

        self.daily_data_mean = None
        self.daily_data_std = None

        self.monthly_data_mean = None
        self.monthly_data_std = None

        self.annual_data_mean = None
        self.annual_data_std = None

        self.description = description
        self.outputdir = outputdir

        super().__init__(
            diagnostic_product="EnsembleTimeseries",
            catalog_list=catalog_list,
            model_list=model_list,
            exp_list=exp_list,
            source_list=source_list,
            loglevel=loglevel,
            outputdir=self.outputdir,
        )

    def run(self):
        """
        A function to compute the mean and standard devivation of the input dataset
        It is import to make sure that the dim along which the mean is compute is correct.
        The default dim="ensemble". TODO: Test DASK's .compute() function here.
        """
        self.logger.info("Compute function in EnsembleTimeseries")

        # For Hourly data
        if self.hourly_data is not None:
            self.hourly_data_mean, self.hourly_data_std = compute_statistics(
                variable=self.var, ds=self.hourly_data, ens_dim=self.dim, loglevel=self.loglevel
            )
            self.save_netcdf(
                var=self.var,
                freq="hourly",
                data_name="mean",
                data=self.hourly_data_mean,
                description=self.description,
                startdate=self.hourly_data_mean.time.values[0],
                enddate=self.hourly_data_mean.time.values[-1],
            )
            self.save_netcdf(
                var=self.var,
                freq="hourly",
                data_name="std",
                data=self.hourly_data_std,
                description=self.description,
                startdate=self.hourly_data_std.time.values[0],
                enddate=self.hourly_data_std.time.values[-1],
            )
        else:
            self.logger.info("No hourly ensemble data is provided")

        # For Daily data
        if self.daily_data is not None:
            self.daily_data_mean, self.daily_data_std = compute_statistics(
                variable=self.var, ds=self.daily_data, ens_dim=self.dim, loglevel=self.loglevel
            )
            self.save_netcdf(
                var=self.var,
                freq="daily",
                data_name="mean",
                data=self.daily_data_mean,
                description=self.description,
                startdate=self.daily_data_mean.time.values[0],
                enddate=self.daily_data_mean.time.values[-1],
            )
            self.save_netcdf(
                var=self.var,
                freq="daily",
                data_name="std",
                data=self.daily_data_std,
                description=self.description,
                startdate=self.daily_data_std.time.values[0],
                enddate=self.daily_data_std.time.values[-1],
            )
        else:
            self.logger.info("No daily ensemble data is provided")

        # For Monthly data
        if self.monthly_data is not None:
            self.monthly_data_mean, self.monthly_data_std = compute_statistics(
                variable=self.var, ds=self.monthly_data, ens_dim=self.dim, loglevel=self.loglevel
            )
            self.save_netcdf(
                var=self.var,
                freq="monthly",
                data_name="mean",
                data=self.monthly_data_mean,
                description=self.description,
                startdate=self.monthly_data_mean.time.values[0],
                enddate=self.monthly_data_mean.time.values[-1],
            )
            self.save_netcdf(
                var=self.var,
                freq="monthly",
                data_name="std",
                data=self.monthly_data_std,
                description=self.description,
                startdate=self.monthly_data_std.time.values[0],
                enddate=self.monthly_data_std.time.values[-1],
            )
        else:
            self.logger.info("No monthly ensemble data is provided")

        # For Annual data
        if self.annual_data is not None:
            self.annual_data_mean, self.annual_data_std = compute_statistics(
                variable=self.var, ds=self.annual_data, ens_dim=self.dim, loglevel=self.loglevel
            )
            self.save_netcdf(
                var=self.var,
                freq="annual",
                data_name="mean",
                data=self.annual_data_mean,
                description=self.description,
                startdate=self.annual_data_mean.time.values[0],
                enddate=self.annual_data_mean.time.values[-1],
            )
            self.save_netcdf(
                var=self.var,
                freq="annual",
                data_name="std",
                data=self.annual_data_std,
                description=self.description,
                startdate=self.annual_data_std.time.values[0],
                enddate=self.annual_data_std.time.values[-1],
            )
        else:
            self.logger.info("No annual ensemble data is provided")
