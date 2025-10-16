import pandas as pd
import xarray as xr
from aqua.logger import log_configure
from aqua.diagnostics.core import Diagnostic, OutputSaver
from collections import Counter

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
        Initialize the Base class. This class provides functions to assign 
        names and save the outputs as pdf, png and netcdf.

        Args:
            diagnostic_name (str): The name of the diagnostic. Default is 'ensemble'.
                                   This will be used to configure the logger and the output files.
            diagnostic_product (str): This is to define which class of the ensemble module is used.
                                    The default is 'None'. Options are: 'EnsembleTimeseries', 
                                    'EnsembleLatLon', 'EnsembleZonal'.
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
            region (str): This is variable assigns region name. Default is None. 
            outputdir (str): String input for output path. Default is './'
            loglevel (str): Default is set to "WARNING"
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
        self.None_catalog = ["ensemble_catalog"]
        self.None_model = ["ensemble_model"]
        self.None_exp = ["ensemble_exp"]
        self.None_source = ["ensemble_source"]

        # Multi catalog/model/exp/source
        self.multi_catalog = ["multi-catalog"]
        self.multi_model = ["multi-model"]
        self.multi_exp = ["multi-exp"]
        self.multi_source = ["multi-source"]

        # Handling catalog name
        self.catalog_list = catalog_list
        if self.catalog_list is None:
            self.logger.info("No catalog names given. Assigning it to catalog_name.")
            self.catalog = self.None_catalog
            self.catalog_list = self.None_catalog
        else:
            if isinstance(self.catalog_list, str): self.catalog_list = [self.catalog_list]
            catalog_counts = dict(Counter(self.catalog_list))
            if len(catalog_counts.keys()) <= 1:
                self.logger.info("Catalog name is given. Single-model ensemble is given.")
                catalog_str_list = [str(item) for item in self.catalog_list]
                if catalog_str_list[0] is None: catalog_str_list[0] = self.None_catalog
                #if catalog_str_list[0] == "None": catalog_str_list[0] = self.None_catalog 
                self.catalog = catalog_str_list[0]
            else:
                self.logger.info(
                    "Multi-model ensemble is given. Assigning catalog name to multi-catalog"
                )
                self.catalog = self.multi_catalog

        # Handling model name:
        self.model_list = model_list
        if model_list is None:
            self.logger.info("No model name is given. Assigning it to model_name")
            self.model = self.None_model
            self.model_list = self.None_model
        else:
            if isinstance(self.model_list, str): self.model_list = [self.model_list]
            model_counts = dict(Counter(self.model_list))
            if len(model_counts.keys()) <= 1:
                self.logger.info("Model name is given. Single-model ensemble is given.")
                model_str_list = [str(item) for item in self.model_list]
                if model_str_list[0] == "None": model_str_list[0] = self.None_model
                self.model = model_str_list[0]
            else:
                self.logger.info("Multi-model ensmeble is given. Assigning model name to multi-model")
                self.model = self.multi_model

        # Handling exp name:
        self.exp_list = exp_list
        if self.exp_list is None:
            self.logger.info("No exp name is given. Assigning it to exp_name")
            self.exp = self.None_exp
            self.exp_list = self.None_exp
        else:
            if isinstance(self.exp_list, str): self.exp_list = [self.exp_list]
            exp_counts = dict(Counter(self.exp_list))
            if len(exp_counts.keys()) <= 1:
                self.logger.info("Model name is given. Single-exp ensemble is given.")
                exp_str_list = [str(item) for item in self.exp_list]
                if exp_str_list[0] == "None": exp_str_list[0] = self.None_exp
                self.exp = exp_str_list[0] 
            else:
                self.logger.info("Multi-exp ensmeble is given. Assigning exp name to multi-exp")
                self.exp = self.multi_exp

        # Handling source name:
        self.source_list = source_list
        if source_list is None:
            self.logger.info("No source name is given. Assigning it to source_name")
            self.source = self.None_source
            self.source_list = self.None_source
        else:
            if isinstance(self.source_list, str): self.source_list = [self.source_list]
            source_counts = dict(Counter(self.source_list))
            if len(source_counts.keys()) <= 1:
                self.logger.info("Model name is given. Single-source ensemble is given.")
                source_str_list = [str(item) for item in self.source_list]
                if source_str_list[0] == "None": source_str_list[0] = self.None_source
                self.source = source_str_list[0]
            else:
                self.logger.info("Multi-source ensmeble is given. Assigning source name to multi-source")
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

    def _str_freq(self, freq: str):
        """
        Args:
            freq (str): The frequency to be used.

        Returns:
            str_freq (str): The frequency as a string.
        """
        if freq in ["h", "hourly"]:
            str_freq = "hourly"
        elif freq in ["D", "daily"]:
            str_freq = "daily"
        elif freq in ["MS", "ME", "M", "mon", "monthly"]:
            str_freq = "monthly"
        elif freq in ["YS", "YE", "Y", "annual"]:
            str_freq = "annual"
        else:
            self.logger.error("Frequency %s not recognized", freq)

        return str_freq

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
        Handles the saving of the input data as 
        netcdf file using OutputSaver.
        
        NOTE: The output can also be saved without the OutputSaver class if 'self.catalog' in 'None' or if Multi-Model catalog is given.
        Args:
            var (str): Variable name. Default is None.
            freq (str): The frequency of the data. Default is None
                        In case of Lat-Lon or Zonal data it is None.
            diagnostic_product (str): The product name to be used in the filename 
            (e.g., 'EnsembleTimeseries' or 'EnsembleLatLon' or 'EnsembleZonal').
            description (str): Description of the figure.
            data_name (str): The variable is used to label the output file for mean or std. 
                             The default is set to None.
            data (xarray.Dataset) or (xarray.Dataarray).
            startdate and enddate (str) to include them in the metadata. Default is 'None'.    
        """
        # In case of Timeseries data
        if data_name is None:
            data_name = "data"
        if var is None:
            var = getattr(data, "standard_name", None)
        extra_keys = {"var": var, "data_name": data_name}
        if freq is not None:
            str_freq = self._str_freq(freq)
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

        region = self.region.replace(" ", "").lower() if self.region is not None else None
        extra_keys.update({"region": region})

        self.logger.info(
            "Saving %s for %s to netcdf in %s", data_name, self.diagnostic_product, self.outputdir
        )

        if description is None:
            description = " ".join(filter(None, [self.diagnostic_name, self.diagnostic_product, "for",str(self.catalog),"and",str(self.model),"with",str(self.exp),self.region])) 
        metadata = {"Description": description}
        metadata.update({"model" : self.model_list})
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

        if self.catalog is not None and self.model is not None and self.exp is not None and str(self.catalog) != str(self.None_catalog) and str(self.catalog) != str(self.multi_catalog):
            outputsaver = OutputSaver(
                diagnostic=self.diagnostic_name,
                #diagnostic_product=self.diagnostic_product,
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
                #diagnostic=self.diagnostic_name,
                diagnostic_product=self.diagnostic_product,
                metadata=metadata,
                extra_keys=extra_keys,
            )
        else:
            data.attrs = {"AQUA diagnostic": self.diagnostic_product, "AQUA catalog": self.catalog_list, "model": self.model_list, "experiment": self.exp_list, "description": description}  
            data.to_netcdf(f"{self.outputdir}/{self.catalog_list}_{self.model_list}_{self.exp_list}_{data_name}_{var}.nc")
            self.logger.info(f"Saving the output without the OutputSaver to {self.outputdir}/{self.catalog_list}_{self.model_list}_{self.exp_list}_{data_name}_{var}.nc")

    # Save figure
    def save_figure(self, var, fig=None, fig_std=None, startdate=None, enddate=None, description=None, format="png"):
        """
        Handles the saving of a figure using OutputSaver.
        
        Args:
            var: The variable in the dataset
            fig (matplotlib.figure.Figure): Figure object.
            fig_std (matplotlib.figure.Figure): Figure object.
            description (str): Description of the figure.
            format (str): Format to save the figure ('png' or 'pdf'). Default is 'png'.
            startdate and enddate (str) to include them in the metadata. Default is 'None'. 
        """
        if description is None:
            description = " ".join(filter(None, [self.diagnostic_name, self.diagnostic_product, "for",str(self.catalog),"and",str(self.model),"with",str(self.exp),self.region]))  
        
        metadata = {"Description": description}
        metadata.update({"model" : self.model_list})
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

        if self.catalog is not None and self.model is not None and self.exp is not None and str(self.catalog) is not str(self.None_catalog) and str(self.catalog) is not str(self.multi_catalog):
            if fig is not None:
                outputsaver = OutputSaver(
                    diagnostic=self.diagnostic_name,
                    #diagnostic_product=self.diagnostic_product,
                    catalog=self.catalog,
                    model=self.model,
                    exp=self.exp,
                    model_ref=self.ref_model,
                    exp_ref=self.ref_exp,
                    outputdir=self.outputdir,
                    loglevel=self.loglevel,
                )
                extra_keys = {}
                #if fig_std is not None:
                #    data = "std"
                #else:
                #    data = "mean"
                data = "mean"
                if var is not None:
                    extra_keys.update({"var": var, "data": data})
                if self.region is not None:
                    extra_keys.update({"region": self.region})
                if format == "pdf":
                    outputsaver.save_pdf(
                        fig,
                        #diagnostic=self.diagnostic_name,
                        diagnostic_product=self.diagnostic_product,
                        extra_keys=extra_keys,
                        metadata=metadata,
                    )
                elif format == "png":
                    outputsaver.save_png(
                        fig,
                        #diagnostic=self.diagnostic_name,
                        diagnostic_product=self.diagnostic_product,
                        extra_keys=extra_keys,
                        metadata=metadata,
                    )
                else:
                    raise ValueError(f"Format {format} not supported. Use png or pdf.")

            if fig_std is not None:
                outputsaver = OutputSaver(
                    diagnostic=self.diagnostic_name,
                    #diagnostic_product=self.diagnostic_product,
                    catalog=self.catalog,
                    model=self.model,
                    exp=self.exp,
                    model_ref=self.ref_model,
                    exp_ref=self.ref_exp,
                    outputdir=self.outputdir,
                    loglevel=self.loglevel,
                )
                extra_keys = {}
                #if fig_std is not None:
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
                        #diagnostic=self.diagnostic_name,
                        diagnostic_product=self.diagnostic_product,
                        extra_keys=extra_keys,
                        metadata=metadata,
                    )
                else:
                    raise ValueError(f"Format {format} not supported. Use png or pdf.")
        else:
            if fig:
                extra_keys = {"statistics": "mean"}
                extra_keys.update(metadata)
                fig.savefig(f"{self.outputdir}/{self.catalog}_{self.model}_{self.exp}_{data}_{var}.png",bbox_inches="tight", metadata=extra_keys)
                self.logger.info(f"Saving the figure without the OutputSaver to {self.outputdir}/{self.catalog}_{self.model}_{self.exp}_{data}_{var}.png")
            if fig_std:
                extra_keys = {"statistics": "standard deviation"}
                extra_keys.update(metadata)
                fig_std.savefig(f"{self.outputdir}/{self.catalog}_{self.model}_{self.exp}_{data}_{var}_STD.png",bbox_inches="tight", metadata=extra_keys)
                self.logger.info(f"Saving the STD figure without the OutputSaver to {self.outputdir}/{self.catalog}_{self.model}_{self.exp}_{data}_{var}_STD.png")

