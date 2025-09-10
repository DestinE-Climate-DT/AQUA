import xarray as xr
#from aqua.logger import log_configure
#from aqua.exceptions import NoDataError
from .base import BaseMixin
from aqua.graphics import plot_timeseries

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
        figure_size = [10, 5],
        save_pdf=True,
        save_png=True,
        var: str = None,
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
        plot_ensemble_members=True,
        description=None,
        title=None,
        outputdir="./",
        log_level: str = "WARNING",
    ):
        """
        Args:
            var (str): Variable name.
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
            monthly_data_mean: xarray.Dataset timeseries monthly mean 
            monthly_data_std: xarray.Dataset timeseries monthly std
            annual_data_mean: xarray.Dataset timeseries annual mean
            annual_data_std: xarray.Dataset timeseries annual std
            outputdir (str): String input for output path.
            figure_size: figure_size can be changed. Default is None,
            save_pdf (bool): Default is True.
            save_png (bool): Default is True.
            title (str): Title for plot.
            description (str): specific for saving the plot.
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
        self.region = region
        self.figure_size = figure_size
        self.var = var
        self.save_pdf = save_pdf
        self.save_png = save_png

        self.hourly_data = hourly_data
        self.hourly_data_mean = hourly_data_mean
        self.hourly_data_std = hourly_data_std

        self.daily_data = daily_data
        self.daily_data_mean = daily_data_mean
        self.daily_data_std = daily_data_std

        self.monthly_data = monthly_data
        self.monthly_data_mean = monthly_data_mean
        self.monthly_data_std = monthly_data_std

        self.annual_data = annual_data
        self.annual_data_mean = annual_data_mean
        self.annual_data_std = annual_data_std

        self.ref_hourly_data = ref_hourly_data
        self.ref_daily_data = ref_daily_data
        self.ref_monthly_data = ref_monthly_data
        self.ref_annual_data = ref_annual_data
        self.plot_ensemble_members = plot_ensemble_members

        self.outputdir = outputdir
        self.log_level = log_level

        super().__init__(
            log_level=self.log_level,
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

        self.title = "Ensemble analysis of " + self.model if title is None else title
        self.description = self.catalog + "_" + self.model if description is None else description

        if hourly_data is not None or daily_data is not None:
            self.logger.warning("Hourly and daily data are not yet supported, they will be ignored")

    def plot(self):
        """
        This plots the ensemble mean and +/- 2 x standard deviation of the ensemble statistics
        around the ensemble mean.
        In this method, it is also possible to plot the individual ensemble members.
        It does not plots +/- 2 x STD for the referene.

        Returns:
            fig, ax

        NOTE: The STD is computed and plotted Point-wise along the mean.
        """
        self.logger.info("Plotting the ensemble timeseries")
        self.logger.info("Assigning label to the given model name")

        fig, ax = plot_timeseries(
            ref_monthly_data=self.ref_monthly_data,
            ref_annual_data=self.ref_annual_data,
            ens_monthly_data=self.monthly_data_mean,
            ens_annual_data=self.annual_data_mean,
            std_ens_monthly_data=self.monthly_data_std,
            std_ens_annual_data=self.annual_data_std,
            ref_label=self.ref_model,
            ens_label=self.model,
            figsize=self.figure_size,
            title=self.title,
            loglevel=self.log_level,
        )
        # Loop over if need to plot the ensemble members
        if self.plot_ensemble_members:
            for i in range(0, len(self.monthly_data[self.var][:, 0])):
                fig1, ax1 = plot_timeseries(
                    fig=fig,
                    ax=ax,
                    ens_monthly_data=self.monthly_data_mean,
                    ens_annual_data=self.annual_data_mean,
                    monthly_data=self.monthly_data[self.var][i, :],
                    annual_data=self.annual_data[self.var][i, :],
                    figsize=self.figure_size,
                    title=self.title,
                    loglevel=self.loglevel,
                )

        # Saving plots
        if self.save_png:
            self.save_figure(var=self.var, fig=fig, description=self.description, format='png')
        if self.save_pdf:
            self.save_figure(var=self.var, fig=fig, description=self.description, format='pdf')
        return fig, ax
