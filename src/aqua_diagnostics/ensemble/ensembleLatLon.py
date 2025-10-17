import xarray as xr
from aqua.exceptions import NoDataError
from aqua.logger import log_configure

from .base import BaseMixin
from .util import compute_statistics

xr.set_options(keep_attrs=True)


class EnsembleLatLon(BaseMixin):
    """
    A class to compute ensemble mean and standard deviation of a 2D (lon-lat) Dataset.
    Make sure that the dataset has correct lon-lat dimensions.
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
        description=None,
        outputdir="./",
        loglevel="WARNING",
    ):
        """
        Args:
            var (str): Variable name.
            dataset: xarray Dataset composed of ensembles 2D lon-lat data, i.e.,
                     the individual Dataset (lon-lat) are concatenated along.
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
            description (str): Description of the netcdf.
            loglevel (str): Log level. Default is "WARNING".
        """
        self.loglevel = loglevel
        self.logger = log_configure(log_level=self.loglevel, log_name="EnsembleLatLon")

        self.var = var
        self.dataset = dataset
        self.dataset_mean = None
        self.dataset_std = None
        self.dim = ensemble_dimension_name
        self.outputdir = outputdir
        self.description = description
        super().__init__(
            diagnostic_product="EnsembleLatLon",
            catalog_list=catalog_list,
            model_list=model_list,
            exp_list=exp_list,
            source_list=source_list,
            outputdir=self.outputdir,
        )

    def run(self):
        """
        A function to compute the mean and standard devivation of the input dataset
        It is import to make sure that the dim along which the mean is compute is correct.
        The default dim="ensemble".
        """
        self.logger.info("Compute function in EnsembleLatLon")

        if self.dataset is not None:
            self.dataset_mean, self.dataset_std = compute_statistics(
                variable=self.var, ds=self.dataset, ens_dim=self.dim, loglevel=self.loglevel
            )
            self.save_netcdf(var=self.var, data_name="mean", data=self.dataset_mean, description=self.description)
            self.save_netcdf(var=self.var, data_name="std", data=self.dataset_std, description=self.description)
        else:
            self.logger.info("No ensemble data is provided to the compute method")
            raise NoDataError("No data is given to the compute method")
