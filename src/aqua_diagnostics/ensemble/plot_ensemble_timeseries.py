import pandas as pd
import xarray as xr
from aqua.graphics import plot_timeseries

# from aqua.logger import log_configure
# from aqua.exceptions import NoDataError
from .base import BaseMixin

xr.set_options(keep_attrs=True)


class PlotEnsembleTimeseries(BaseMixin):
    """Class to plot the ensmeble timeseries"""

    # TODO: support hourly and daily data

    def __init__(
        self,
        diagnostic_product: str = "EnsembleTimeseries",
        catalog_list: list[str] = None,
        model_list: list[str] = None,
        exp_list: list[str] = None,
        source_list: list[str] = None,
        ref_catalog: str = None,
        ref_model: str = None,
        ref_exp: str = None,
        region: str = None,
        outputdir="./",
        loglevel: str = "WARNING",
    ):
        """
        Args:
            diagnostic_name (str): The name of the diagnostic. Default is 'ensemble'.
                                   This will be used to configure the logger and the output files.
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
            ref_catalog (str): This is specific to timeseries reference data catalog. Default is None.
            ref_model (str): This is specific to timeseries reference data model. Default is None.
            ref_exp (str): This is specific to timeseries reference data exp. Default is None.
            ensemble_dimension_name="ensemble" (str): a default name given to the
                     dimensions along with the individual Datasets were concatenated.
            outputdir (str): String input for output path. Default is './'
            loglevel (str): Log level. Default is "WARNING".
        """

        self.diagnostic_product = diagnostic_product

        self.catalog_list = catalog_list
        self.model_list = model_list
        self.exp_list = exp_list
        self.source_list = source_list
        self.ref_catalog = ref_catalog
        self.ref_model = ref_model
        self.ref_exp = ref_exp
        # TODO: Include region information
        # self.region = region

        self.outputdir = outputdir
        self.loglevel = loglevel

        super().__init__(
            loglevel=self.loglevel,
            diagnostic_product=self.diagnostic_product,
            catalog_list=self.catalog_list,
            model_list=self.model_list,
            exp_list=self.exp_list,
            source_list=self.source_list,
            ref_catalog=self.ref_catalog,
            ref_model=self.ref_model,
            ref_exp=self.ref_exp,
            outputdir=self.outputdir,
        )

    def plot(
        self,
        var=None,
        title=None,
        startdate=None,
        enddate=None,
        hourly_data=None,
        hourly_data_mean=None,
        hourly_data_std=None,
        daily_data=None,
        daily_data_mean=None,
        daily_data_std=None,
        monthly_data=None,
        monthly_data_mean=None,
        monthly_data_std=None,
        annual_data=None,
        annual_data_mean=None,
        annual_data_std=None,
        ref_hourly_data=None,
        ref_daily_data=None,
        ref_monthly_data=None,
        ref_annual_data=None,
        description=None,
        save_pdf=True,
        save_png=True,
        dpi=300,
        figure_size=[10, 5],
        plot_ensemble_members=True,
    ):
        """
        This plots the ensemble mean and +/- 2 x standard deviation of the ensemble statistics
        around the ensemble mean.
        In this method, it is also possible to plot the individual ensemble members.
        It does not plots +/- 2 x STD for the referene.

        Args:
            title (str): Title for plot.
            startdate (str): startdate to be included in title if 'None'. Default is 'None'.
            enddate (str): enddate to be included in title if 'None'. Default is 'None'.
            description (str): specific for saving the plot.
            figure_size: figure_size can be changed. Default is [10, 5],
            save_pdf (bool): Default is True.
            save_png (bool): Default is True.
            dpi (int): Resolution for saved figures. Default is 300.
            plot_ensemble_members=True.
            ref_hourly_data: reference hourly timesereis xarray.Dataset. Default is None.
            ref_daily_data: reference daily timeseries xarray.Dataset. Default is None.
            ref_monthly_data: reference monthly timeseries xarray.Dataset. Default is None.
            ref_annual_data: reference annual timeseries xarray.Dataset. Default is None.
            hourly_data: xarray Dataset of ensemble members of hourly timeseries.
                     The ensemble memebers are concatenated along a new dimension "ensemble".
            hourly_data_mean: None
            hourly_data_std: None
            daily_data: xarray Dataset of ensemble members of daily timeseries.
                     The ensemble memebers are concatenated along a new dimension "ensemble".
            daily_data_mean: None
            daily_data_std: None
            monthly_data: xarray Dataset of ensemble members of monthly timeseries.
                     The ensemble memebers are concatenated along a new dimension "ensemble".
            annual_data: xarray Dataset of ensemble members of annual timeseries.
                     The ensemble members are concatenated along the dimension "ensemble"
            ensemble_dimension_name="ensemble" (str): a default name given to the
                     dimensions along with the individual Datasets were concatenated.
            monthly_data_mean: xarray.Dataset timeseries monthly mean.
            monthly_data_std: xarray.Dataset timeseries monthly std.
            annual_data_mean: xarray.Dataset timeseries annual mean.
            annual_data_std: xarray.Dataset timeseries annual std.

        Returns:
            fig, ax

        NOTE: The STD is computed and plotted Point-wise along the mean.
        """
        if hourly_data is not None or daily_data is not None:
            self.logger.warning("Hourly and daily data are not yet supported, they will be ignored")

        self.logger.info("Plotting the ensemble timeseries")
        self.logger.info("Assigning label to the given model name")

        if isinstance(self.model, list):
            model_str = " ".join(str(x) for x in self.model)
        else:
            model_str = str(self.model)

        if title is None:
            if startdate is None and enddate is None:
                title = "Ensemble analysis of " + model_str
            else:
                startdate = pd.Timestamp(startdate)
                startdate = startdate.strftime("%Y-%m-%d")
                enddate = pd.Timestamp(enddate)
                enddate = enddate.strftime("%Y-%m-%d")
                title = f"Ensemble analysis of {model_str} ({startdate} - {enddate})"

        fig, ax = plot_timeseries(
            ref_monthly_data=ref_monthly_data,
            ref_annual_data=ref_annual_data,
            ens_monthly_data=monthly_data_mean,
            ens_annual_data=annual_data_mean,
            std_ens_monthly_data=monthly_data_std,
            std_ens_annual_data=annual_data_std,
            ref_label=self.ref_model,
            ens_label=model_str,
            figsize=figure_size,
            title=title,
            loglevel=self.loglevel,
        )
        # Loop over if need to plot the ensemble members
        if plot_ensemble_members:
            for i in range(0, len(monthly_data[var][:, 0])):
                fig1, ax1 = plot_timeseries(
                    fig=fig,
                    ax=ax,
                    ens_monthly_data=monthly_data_mean,
                    ens_annual_data=annual_data_mean,
                    monthly_data=monthly_data[var][i, :] if monthly_data is not None else None,
                    annual_data=annual_data[var][i, :] if annual_data is not None else None,
                    figsize=figure_size,
                    title=title,
                    loglevel=self.loglevel,
                )

        # Saving plots
        if save_png:
            self.save_figure(var=var, fig=fig, startdate=startdate, enddate=enddate, description=description, format="png", dpi=dpi)
        if save_pdf:
            self.save_figure(var=var, fig=fig, startdate=startdate, enddate=enddate, description=description, format="pdf")
        return fig, ax
