from collections import Counter

import pandas as pd
import xarray as xr
from aqua.diagnostics.core import Diagnostic, OutputSaver
from aqua.logger import log_configure
from aqua.util import pandas_freq_to_string

xr.set_options(keep_attrs=True)


class BaseMixin(Diagnostic):
    """The BaseMixin class is used to save the outputs from the ensemble module."""

    def __init__(
        self,
        diagnostic_name: str = "ensemble",
        diagnostic_product: str = None,
        catalog_list: list[str] = None,
        model_list: list[str] = None,
        exp_list: list[str] = None,
        source_list: list[str] = None,
        ref_catalog: str = None,
        ref_model: str = None,
        ref_exp: str = None,
        region: str = None,
        outputdir: str = "./",
        loglevel: str = "WARNING",
    ):
        """
        BaseMixin class for managing and saving outputs from the ensemble module.

        This class provides functionality to assign catalog, model, exp, and source
        names, handle None or multi-value cases, and save outputs as PDF, PNG, or
        NetCDF files. It also configures logging for diagnostics.

        Attributes:
            catalog_list (list[str]): List of catalogs; None defaults to 'None_catalog'.
            model_list (list[str]): List of models; None defaults to 'None_model'.
            exp_list (list[str]): List of experiments; None defaults to 'None_exp'.
            source_list (list[str]): List of sources; None defaults to 'None_source'.
            catalog (str): Assigned catalog name or 'multi-catalog' for multiple catalogs.
            model (str): Assigned model name or 'multi-model' for multiple models.
            exp (str): Assigned experiment name or 'multi-exp' for multiple experiments.
            source (str): Assigned source name or 'multi-source' for multiple sources.
            ref_catalog (str): Reference catalog (used for timeseries).
            ref_model (str): Reference model (used for timeseries).
            ref_exp (str): Reference experiment (used for timeseries).
            region (str): Region name for the outputs.
            diagnostic_name (str): Name of the diagnostic (default 'ensemble').
            diagnostic_product (str): Class of the ensemble module used.
            outputdir (str): Directory to save outputs (default './').
            logger (logging.Logger): Configured logger for the class.

        Args:
            diagnostic_name (str, optional): Name of the diagnostic. Default is 'ensemble'.
            diagnostic_product (str, optional): Class of the ensemble module (e.g.,
                'EnsembleTimeseries', 'EnsembleLatLon', 'EnsembleZonal'). Default is None.
            catalog_list (list[str], optional): List of catalog names. Default is None.
            model_list (list[str], optional): List of model names. Default is None.
            exp_list (list[str], optional): List of experiment names. Default is None.
            source_list (list[str], optional): List of source names. Default is None.
            ref_catalog (str, optional): Reference catalog for timeseries. Default is None.
            ref_model (str, optional): Reference model for timeseries. Default is None.
            ref_exp (str, optional): Reference experiment for timeseries. Default is None.
            region (str, optional): Region name. Default is None.
            outputdir (str, optional): Output directory path. Default is './'.
            loglevel (str, optional): Logging level. Default is 'WARNING'.
        """
        self.loglevel = loglevel
        self.logger = log_configure(log_name="BaseMixin", log_level=loglevel)
        self.logger.info("Initializing the BaseMixin class")

        self.region = region
        self.diagnostic_name = diagnostic_name
        self.diagnostic_product = diagnostic_product

        # Reference in case of timeseries
        self.ref_catalog = ref_catalog
        self.ref_model = ref_model
        self.ref_exp = ref_exp

        # To handle None case
        self.None_catalog = "ensemble_catalog"
        self.None_model = "ensemble_model"
        self.None_exp = "ensemble_exp"
        self.None_source = "ensemble_source"

        # Multi catalog/model/exp/source
        self.multi_catalog = "multi_catalog"
        self.multi_model = "multi_model"
        self.multi_exp = "multi_exp"
        self.multi_source = "multi_source"

        # Handling catalog name
        self.catalog_list = catalog_list
        if self.catalog_list is None:
            self.logger.info(f"No catalog names given. Assigning it to {self.None_catalog}.")
            self.catalog = self.None_catalog
            self.catalog_list = self.None_catalog
        else:
            if isinstance(self.catalog_list, str):
                self.catalog_list = [self.catalog_list]
            catalog_counts = dict(Counter(self.catalog_list))
            if len(catalog_counts.keys()) <= 1:
                self.logger.info("Catalog name is given. Single model ensemble is given.")
                catalog_str_list = [str(item) for item in self.catalog_list]
                if catalog_str_list[0] is None:
                    catalog_str_list[0] = self.None_catalog
                # if catalog_str_list[0] == "None": catalog_str_list[0] = self.None_catalog
                self.catalog = catalog_str_list[0]
            else:
                self.logger.info(f"Multi model ensemble is given. Assigning catalog name to {self.multi_catalog}")
                self.catalog = self.multi_catalog

        # Handling model name:
        self.model_list = model_list
        if model_list is None:
            self.logger.info(f"No model name is given. Assigning it to {self.None_model}")
            self.model = self.None_model
            self.model_list = self.None_model
        else:
            if isinstance(self.model_list, str):
                self.model_list = [self.model_list]
            model_counts = dict(Counter(self.model_list))
            if len(model_counts.keys()) <= 1:
                self.logger.info("Model name is given. Single model ensemble is given.")
                model_str_list = [str(item) for item in self.model_list]
                if model_str_list[0] == "None":
                    model_str_list[0] = self.None_model
                self.model = model_str_list[0]
            else:
                self.logger.info(f"Multi model ensemble is given. Assigning model name to {self.multi_model}")
                self.model = self.multi_model

        # Handling exp name:
        self.exp_list = exp_list
        if self.exp_list is None:
            self.logger.info(f"No exp name is given. Assigning it to {self.None_exp}")
            self.exp = self.None_exp
            self.exp_list = self.None_exp
        else:
            if isinstance(self.exp_list, str):
                self.exp_list = [self.exp_list]
            exp_counts = dict(Counter(self.exp_list))
            if len(exp_counts.keys()) <= 1:
                self.logger.info("Model name is given. Single-exp ensemble is given.")
                exp_str_list = [str(item) for item in self.exp_list]
                if exp_str_list[0] == "None":
                    exp_str_list[0] = self.None_exp
                self.exp = exp_str_list[0]
            else:
                self.logger.info(f"Multi exp ensemble is given. Assigning exp name to {self.multi_exp}")
                self.exp = self.multi_exp

        # Handling source name:
        self.source_list = source_list
        if source_list is None:
            self.logger.info(f"No source name is given. Assigning it to {self.None_source}")
            self.source = self.None_source
            self.source_list = self.None_source
        else:
            if isinstance(self.source_list, str):
                self.source_list = [self.source_list]
            source_counts = dict(Counter(self.source_list))
            if len(source_counts.keys()) <= 1:
                self.logger.info("Model name is given. Single-source ensemble is given.")
                source_str_list = [str(item) for item in self.source_list]
                if source_str_list[0] == "None":
                    source_str_list[0] = self.None_source
                self.source = source_str_list[0]
            else:
                self.logger.info(f"Multi source ensemble is given. Assigning source name to {self.multi_source}")
                self.source = self.multi_source

        super().__init__(
            catalog=self.catalog,
            model=self.model,
            exp=self.exp,
            source=self.source,
            loglevel=loglevel,
        )
        self.logger.info(f"Outputs will be saved with {self.catalog}, {self.model} and {self.exp}.")
        self.outputdir = outputdir

    def save_netcdf(
        self,
        var: str = None,
        freq: str = None,
        diagnostic_product=None,
        description=None,
        data_name=None,
        data=None,
        startdate=None,
        enddate=None,
    ):
        """
        Commented-out: Save data as a NetCDF file using OutputSaver or directly if catalog/model/exp are None or multi-values.
        
        Save data as a NetCDF file using OutputSaver.
        This method handles Timeseries, Lat-Lon, and Zonal data. It automatically generates
        metadata including model, experiment, source, region, and optional start/end dates.
        The filename and description are dynamically generated based on the diagnostic,
        catalog, model, exp, and region.

        Args:
            var (str, optional): Variable name. Defaults to None. If None, uses data.standard_name.
            freq (str, optional): Data frequency (e.g., 'monthly'). Defaults to None.
                                  For Lat-Lon or Zonal data, this is typically None.
            diagnostic_product (str, optional): Product name for the filename
                (e.g., 'EnsembleTimeseries', 'EnsembleLatLon', 'EnsembleZonal').
            description (str, optional): Description to include in metadata. Defaults to auto-generated.
            data_name (str, optional): Label for output file (e.g., 'mean' or 'std'). Defaults to 'data'.
            data (xarray.Dataset or xarray.DataArray, optional): Data to save.
            startdate (str, optional): Start date to include in metadata. Defaults to None.
            enddate (str, optional): End date to include in metadata. Defaults to None.

        Notes:
            - If catalog/model/exp are None or multi-values, data is saved without OutputSaver.
            - Metadata includes diagnostic name, catalog, model, experiment, source, region, and description.
            - Filenames are automatically generated based on catalog, model, exp, data_name, and variable.
        """
        # In case of Timeseries data
        if data_name is None:
            data_name = "data"
        if var is None:
            var = getattr(data, "standard_name", None)
        extra_keys = {"var": var, "data_name": data_name}
        if freq is not None:
            str_freq = pandas_freq_to_string(freq)
            self.logger.info("%s frequency is given", str_freq)
            if data is None:
                self.logger.error("No %s %s available", str_freq, data_name)
            self.logger.info(
                "Saving %s data for %s to netcdf in %s",
                str_freq,
                self.diagnostic_product,
                self.outputdir,
            )
            extra_keys.update({"freq": str_freq})

        if data.name is None and var is not None:
            data.name = var

        region = self.region if self.region is not None else None
        extra_keys.update({"region": region})

        self.logger.info("Saving %s for %s to netcdf in %s", data_name, self.diagnostic_product, self.outputdir)

        if description is None:
            description = " ".join(
                filter(
                    None,
                    [
                        self.diagnostic_name,
                        self.diagnostic_product,
                        "for",
                        str(self.catalog),
                        "and",
                        str(self.model),
                        "with",
                        str(self.exp),
                        self.region,
                    ],
                )
            )

        metadata = {"Description": description}
        metadata.update({"model": self.model_list})
        metadata.update({"experiment": self.exp_list})
        metadata.update({"source": self.source_list})

        if startdate is not None:
            startdate = pd.Timestamp(startdate)
            startdate = startdate.strftime("%Y-%m-%d")
            metadata.update({"startdate": startdate})
        if enddate is not None:
            enddate = pd.Timestamp(enddate)
            enddate = enddate.strftime("%Y-%m-%d")
            metadata.update({"enddate": enddate})

        if (
            self.catalog is not None
            and self.model is not None
            and self.exp is not None
            #and str(self.catalog) != str(self.None_catalog)
            #and str(self.catalog) != str(self.multi_catalog)
        ):
            outputsaver = OutputSaver(
                diagnostic=self.diagnostic_name,
                # diagnostic_product=self.diagnostic_product,
                catalog=self.catalog,
                model=self.model,
                exp=self.exp,
                model_ref=self.ref_model,
                exp_ref=self.ref_exp,
                outputdir=self.outputdir,
                loglevel=self.loglevel,
            )
            outputsaver.save_netcdf(
                dataset=data,
                # diagnostic=self.diagnostic_name,
                diagnostic_product=self.diagnostic_product,
                metadata=metadata,
                extra_keys=extra_keys,
            )
        else:
            self.logger.info(f"Output is not saved, please check {self.catalog}, {self.model} and {self.exp}")

            #data.attrs = {
            #    "AQUA diagnostic": self.diagnostic_product,
            #    "AQUA catalog": self.catalog_list,
            #    "model": self.model_list,
            #    "experiment": self.exp_list,
            #    "description": description,
            #}

            #catalog_str = "_".join(self.catalog_list)
            #model_str = "_".join(self.model_list)
            #exp_str = "_".join(self.exp_list)

            #filename = f"{self.outputdir}/{catalog_str}_{model_str}_{exp_str}_{data_name}_{var}.nc"
            #
            #data.to_netcdf(filename)
            #self.logger.info(
            #    f"Saving the output without the OutputSaver to {self.outputdir}/{self.catalog_list}_{self.model_list}_{self.exp_list}_{data_name}_{var}.nc"
            #)

    # Save figure
    def save_figure(self, var, fig=None, fig_std=None, startdate=None, enddate=None, description=None, format="png", dpi=300):    
        """
        Save figure(s) to file using OutputSaver or directly to disk if catalog/model/exp are None or multi-values.

        This method supports saving mean and standard deviation figures for a given variable.
        Metadata is automatically generated, including model, experiment, source, region, startdate, enddate,
        and a description. Figures can be saved in PNG or PDF formats.

        Args:
            var (str): Name of the variable in the dataset.
            fig (matplotlib.figure.Figure, optional): Figure object for the main data. Defaults to None.
            fig_std (matplotlib.figure.Figure, optional): Figure object for standard deviation. Defaults to None.
            startdate (str, optional): Start date to include in metadata. Defaults to None.
            enddate (str, optional): End date to include in metadata. Defaults to None.
            description (str, optional): Description to include in metadata. Defaults to auto-generated.
            format (str, optional): File format to save the figure ('png' or 'pdf'). Defaults to 'png'.
            dpi (int, optional): Resolution for saved figures in PNG format. Default is 300.

        Notes:
            - If catalog/model/exp are None or multi-values, figures are saved directly without OutputSaver.
            - Metadata includes diagnostic name, catalog, model, experiment, source, region, and description.
            - Filenames are automatically generated based on catalog, model, exp, variable, and whether it's mean or STD.
            - Raises ValueError if an unsupported format is specified.
        """
        if description is None:
            description = " ".join(
                filter(
                    None,
                    [
                        self.diagnostic_name,
                        self.diagnostic_product,
                        "for",
                        str(self.catalog),
                        "and",
                        str(self.model),
                        "with",
                        str(self.exp),
                        self.region,
                    ],
                )
            )

        metadata = {"Description": description}
        metadata.update({"model": self.model_list})
        metadata.update({"experiment": self.exp_list})
        metadata.update({"source": self.source_list})

        if startdate is not None:
            startdate = pd.Timestamp(startdate)
            startdate = startdate.strftime("%Y-%m-%d")
            metadata.update({"startdate": startdate})
        if enddate is not None:
            enddate = pd.Timestamp(enddate)
            enddate = enddate.strftime("%Y-%m-%d")
            metadata.update({"enddate": enddate})

        if (
            self.catalog is not None
            and self.model is not None
            and self.exp is not None
            #and str(self.catalog) != str(self.None_catalog)
            #and str(self.catalog) != str(self.multi_catalog)
        ):
            if fig is not None:
                outputsaver = OutputSaver(
                    diagnostic=self.diagnostic_name,
                    # diagnostic_product=self.diagnostic_product,
                    catalog=self.catalog,
                    model=self.model,
                    exp=self.exp,
                    model_ref=self.ref_model,
                    exp_ref=self.ref_exp,
                    outputdir=self.outputdir,
                    loglevel=self.loglevel,
                )
                extra_keys = {}
                # if fig_std is not None:
                #    data = "std"
                # else:
                #    data = "mean"
                data = "mean"
                if var is not None:
                    extra_keys.update({"var": var, "data": data})
                if self.region is not None:
                    extra_keys.update({"region": self.region})
                if format == "pdf":
                    outputsaver.save_pdf(
                        fig,
                        # diagnostic=self.diagnostic_name,
                        diagnostic_product=self.diagnostic_product,
                        extra_keys=extra_keys,
                        metadata=metadata,
                    )
                elif format == "png":
                    outputsaver.save_png(
                        fig,
                        # diagnostic=self.diagnostic_name,
                        diagnostic_product=self.diagnostic_product,
                        extra_keys=extra_keys,
                        metadata=metadata,
                        dpi=dpi,
                    )
                else:
                    raise ValueError(f"Format {format} not supported. Use png or pdf.")

            if fig_std is not None:
                metadata = {"Description": description}
                extra_keys = {}

                if fig_std is not None:
                    outputsaver = OutputSaver(
                        diagnostic=self.diagnostic_name,
                        # diagnostic_product=self.diagnostic_product,
                        catalog=self.catalog,
                        model=self.model,
                        exp=self.exp,
                        model_ref=self.ref_model,
                        exp_ref=self.ref_exp,
                        outputdir=self.outputdir,
                        loglevel=self.loglevel,
                    )
                    extra_keys = {}
                    # if fig_std is not None:
                    #    data = "std"
                    data = "std"
                    if var is not None:
                        extra_keys.update({"var": var, "data": data})
                    if self.region is not None:
                        extra_keys.update({"region": self.region})
                    if format == "pdf":
                        outputsaver.save_pdf(
                            fig_std,
                            diagnostic_product=self.diagnostic_product,
                            extra_keys=extra_keys,
                            metadata=metadata,
                        )
                    elif format == "png":
                        outputsaver.save_png(
                            fig_std,
                            # diagnostic=self.diagnostic_name,
                            diagnostic_product=self.diagnostic_product,
                            extra_keys=extra_keys,
                            metadata=metadata,
                            dpi=dpi,
                        )
                    else:
                        raise ValueError(f"Format {format} not supported. Use png or pdf.")
        else:
            self.logger.info(f"Output plot is not saved, please check {self.catalog}, {self.model} and {self.exp}")
            #if fig:
            #    extra_keys = {"statistics": "mean"}
            #    extra_keys.update(metadata)
            #    extra_keys = {k: str(v) for k, v in extra_keys.items()}
            #    fig.savefig(
            #        f"{self.outputdir}/{self.catalog}_{self.model}_{self.exp}_{var}_mean.png",
            #        bbox_inches="tight",
            #        metadata=extra_keys,
            #    )
            #    self.logger.info(
            #        f"Saving the figure without the OutputSaver to {self.outputdir}/{self.catalog}_{self.model}_{self.exp}_{var}_mean.png"
            #    )
            #if fig_std:
            #    extra_keys = {"statistics": "standard deviation"}
            #    extra_keys.update(metadata)
            #    extra_keys = {k: str(v) for k, v in extra_keys.items()}
            #    fig_std.savefig(
            #        f"{self.outputdir}/{self.catalog}_{self.model}_{self.exp}_{var}_STD.png",
            #        bbox_inches="tight",
            #        metadata=extra_keys,
            #    )
            #    self.logger.info(
            #        f"Saving the STD figure without the OutputSaver to {self.outputdir}/{self.catalog}_{self.model}_{self.exp}_{var}_STD.png"
            #    )
