import os
import xarray as xr
# from dask import compute
from aqua.graphics import plot_timeseries
from aqua.logger import log_configure
from aqua.util import create_folder
from aqua.exceptions import NoDataError
from .util import retrieve_merge_ensemble_data, compute_statistics

xr.set_options(keep_attrs=True)


class EnsembleTimeseries:
    """
    This class computes mean and standard deviation of the ensemble.mulit-model ensemble.
    This also plots ensemble mean and standard deviation.
    """

    def __init__(
        self,
        var=None,
        mon_model_dataset=None,
        ann_model_dataset=None,
        mon_ref_data=None,
        ann_ref_data=None,
        ensemble_dimension_name="ensemble",
        outputdir="./",
        loglevel="WARNING",
        **kwargs,
    ):
        """
        Args:
            var (str): Variable name.
            mon_model_dataset: xarray Dataset of ensemble members of monthly timeseries.
                     The ensemble memebers are concatenated along a new dimension "ensemble".
                     This can be editted with the function edit_attributes.
            ann_model_dataset: xarray Dataset of ensemble members of annual timeseries.
                     Similarly, the members are concatenated along the dimension "ensemble"
            ensemble_dimension_name="ensemble" (str): a default name given to the
                     dimensions along with the individual Datasets were concatenated.
            mon_ref_data: xarray Dataset of monthly reference data.
            ann_ref_data: xarray Dataset of annual reference data.
            outputdir (str): String input for output path.
            loglevel (str): Log level. Default is "WARNING".
            **kwargs (dict): dictonary contains options for plots. Defaults values have been set already.
        """
        self.loglevel = loglevel
        self.logger = log_configure(log_level=self.loglevel, log_name="Ensemble Timeseries")
        self.var = var
        self.dim = ensemble_dimension_name
        self.mon_model_dataset = mon_model_dataset
        self.ann_model_dataset = ann_model_dataset
        self.mon_ref_data = mon_ref_data
        self.ann_ref_data = ann_ref_data
        self.mon_dataset_mean = None
        self.mon_dataset_std = None
        self.ann_dataset_mean = None
        self.ann_dataset_std = None
        self.outputdir = outputdir + "Ensemble_Timeseries"
        self.outfile = f"ensemble_time_series_timeseries_{self.var}"

        self.figure = None
        plot_options = kwargs.get("plot_options", {})
        self.plot_label = plot_options.get("plot_label", True)
        self.plot_std = plot_options.get("plot_std", True)
        self.plot_ensemble_members = plot_options.get("plot_ensemble_members", True)
        self.plot_ref = plot_options.get("plot_ref", True)
        self.figure_size = plot_options.get("figure_size", [10, 5])
        self.ensemble_label = plot_options.get("ensemble_label", "Ensemble")
        self.ref_label = plot_options.get("ref_label", "ERA5")
        self.plot_title = plot_options.get("plot_title", "Ensemble statistics for " + self.var)

    def compute(self):
        """
        A function to compute the mean and standard devivation of the input dataset
        It is import to make sure that the dim along which the mean is compute is correct.
        The default dim="ensemble". TODO: Test DASK's .compute() function here.
        """
        self.logger.info("Compute function in EnsembleTimeseries")
        
        # For Monthly dataset
        if self.mon_model_dataset is not None:
            self.mon_dataset_mean, self.mon_dataset_std = compute_statistics(
                variable=self.var, 
                ds=self.mon_model_dataset, 
                ens_dim=self.dim,
                log_level=self.loglevel
            )
        else:
            self.logger.info("No monthly ensemble data is provided")

        # For Annual dataset
        if self.ann_model_dataset is not None:
            self.ann_dataset_mean, self.ann_dataset_std = compute_statistics(
                variable=self.var, 
                ds=self.ann_model_dataset, 
                ens_dim=self.dim,
                log_level=self.loglevel
            )
        else:
            self.logger.info("No annual ensemble data is provided")

    def plot(self):
        """
        This plots the ensemble mean and +/- 2 x standard deviation of the ensemble statistics
        around the ensemble mean.
        In this method, it is also possible to plot the individual ensemble members.
        It does not plots +/- 2 x STD for the referene. 
        
        Returns:
            fig, ax
        """
        self.logger.info("Plotting the ensemble timeseries")

        fig, ax = plot_timeseries( 
            ref_monthly_data=self.mon_ref_data, 
            ref_annual_data=self.ann_ref_data, 
            ens_monthly_data=self.mon_dataset_mean, 
            ens_annual_data=self.ann_dataset_mean, 
            std_ens_monthly_data=self.mon_dataset_std, 
            std_ens_annual_data=self.ann_dataset_std,
            ref_label=self.ref_label,
            ens_label=self.ensemble_label,
            figsize=self.figure_size,
            title=self.plot_title,
            loglevel=self.loglevel,
        )
        # Loop over if need to plot the ensemble members
        if self.plot_ensemble_members:
            for i in range(0, len(self.mon_model_dataset[self.var][:, 0])):
                fig1, ax1 = plot_timeseries(
                    fig=fig, ax=ax,
                    ens_monthly_data=self.mon_dataset_mean,
                    ens_annual_data=self.ann_dataset_mean,
                    monthly_data=self.mon_model_dataset[self.var][i, :], 
                    annual_data=self.ann_model_dataset[self.var][i, :],
                    figsize=self.figure_size,
                    title=self.plot_title,
                    loglevel=self.loglevel,
                )

        # Saving plots
        self.logger.info("Saving plots as png and pdf the output folder {self.outputdir}/plots")
        plots_path = os.path.join(self.outputdir, "plots")
        create_folder(plots_path, self.loglevel)
        # pdf plots
        fig.savefig(
            os.path.join(plots_path, self.outfile) + ".pdf", bbox_inches="tight", pad_inches=0.1
        )
        # pdf plots
        fig.savefig(
            os.path.join(plots_path, self.outfile) + ".png", bbox_inches="tight", pad_inches=0.1
        )
        return fig, ax


