import matplotlib.pyplot as plt
import xarray as xr
from aqua.exceptions import NoDataError
from aqua.logger import log_configure

from .base import BaseMixin
from .util import compute_statistics

xr.set_options(keep_attrs=True)


class EnsembleZonal(BaseMixin):
    """
    A class to compute ensemble mean and standard deviation of the Zonal averages
    Make sure that the dataset has correct lev-lat dimensions.
    """

    def __init__(
        self,
        var=None,
        dataset=None,
        catalog_list=None,
        model_list=None,
        exp_list=None,
        source_list=None,
        ensemble_dimension_name="ensemble",
        outputdir="./",
        loglevel="WARNING",
    ):
        """
        Args:
            var (str): Variable name.
            dataset: xarray Dataset composed of ensembles 2D Zonal data, i.e.,
                     the individual Dataset (lev-lat) are concatenated along.
                     a new dimension "ensemble". This ensemble name can be changed.
            catalog_list (str): This variable defines the catalog list. The default is 'None'.
                                    If None, the variable is assigned to 'None_catalog'. In case of Multi-catalogs,
                                    the variable is assigned to 'multi-catalog'.
            model_list (str): This variable defines the model list. The default is 'None'.
                                    If None, the variable is assigned to 'None_model'. In case of Multi-Model,
                                    the variable is assigned to 'multi-model'.
            exp_list (str): This variable defines the exp list. The default is 'None'.
                                    If None, the variable is assigned to 'None_exp'. In case of Multi-Exp,
                                    the variable is assigned to 'multi-exp'.
            source_list (str): This variable defines the source list. The default is 'None'.
                                    If None, the variable is assigned to 'None_source'. In case of Multi-Source,
                                    the variable is assigned to 'multi-source'.
            ensemble_dimension_name="ensemble" (str): a default name given to the
                     dimensions along with the individual Datasets were concatenated.
            outputdir (str): String input for output path.
            loglevel (str): Log level. Default is "WARNING".
        """
        self.loglevel = loglevel
        self.logger = log_configure(log_level=self.loglevel, log_name="Ensemble Zonal Averages")

        self.var = var
        self.dataset = dataset
        self.dim = ensemble_dimension_name
        self.dataset_mean = None
        self.dataset_std = None
        self.outputdir = outputdir

        super().__init__(
            diagnostic_product="EnsembleZonal",
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
        The default dim="ensemble".
        """

        self.logger.info("Compute function in EnsembleZonal")

        if self.dataset is not None:
            self.dataset_mean, self.dataset_std = compute_statistics(
                variable=self.var, ds=self.dataset, ens_dim=self.dim, loglevel=self.loglevel
            )
            self.save_netcdf(
                var=self.var,
                data_name="mean",
                data=self.dataset_mean,
            )
            self.save_netcdf(
                var=self.var,
                data_name="std",
                data=self.dataset_std,
            )
        else:
            self.logger.info("No ensemble data is provided to the compute method")
            raise NoDataError("No data is given to the compute method")
