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
        None_catalog = "ensemble_catalog"
        None_model = "ensemble_model"
        None_exp = "ensemble_exp"
        None_source = "ensemble_source"

        # Multi catalog/model/exp/source
        multi_catalog = "multi-catalog"
        multi_model = "multi-model"
        multi_exp = "multi-exp"
        multi_source = "multi-source"
        

        # Handling catalog name
        if catalog_list is None:
            self.logger.info("No catalog names given. Assigning it to catalog_name.")
            self.catalog = None_catalog
        else:
            catalog_counts = dict(Counter(catalog_list))
            if len(catalog_counts.keys()) <= 1:
                self.logger.info("Catalog name is given. Single-model ensemble is given.")
                catalog_str_list = [str(item) for item in catalog_list]
                if catalog_str_list[0] == "None": catalog_str_list[0] = None_catalog 
                self.catalog = catalog_str_list[0]
            else:
                self.logger.info(
                    "Multi-model ensemble is given. Assigning catalog name to multi-catalog"
                )
                self.catalog = multi_catalog

        # Handling model name:
        self.model_list = model_list
        if model_list is None:
            self.logger.info("No model name is given. Assigning it to model_name")
            self.model = None_model
        else:
            model_counts = dict(Counter(model_list))
            if len(model_counts.keys()) <= 1:
                self.logger.info("Model name is given. Single-model ensemble is given.")
                model_str_list = [str(item) for item in model_list]
                if model_str_list[0] == "None": model_str_list[0] = None_model
                self.model = model_str_list[0]
            else:
                self.logger.info("Multi-model ensmeble is given. Assigning model name to multi-model")
                self.model = multi_model

        # Handling exp name:
        if exp_list is None:
            self.logger.info("No exp name is given. Assigning it to exp_name")
            self.exp = None_exp
        else:
            exp_counts = dict(Counter(exp_list))
            if len(exp_counts.keys()) <= 1:
                self.logger.info("Model name is given. Single-exp ensemble is given.")
                exp_str_list = [str(item) for item in exp_list]
                if exp_str_list[0] == "None": exp_str_list[0] = None_exp
                self.exp = exp_str_list[0] 
            else:
                self.logger.info("Multi-exp ensmeble is given. Assigning exp name to multi-exp")
                self.exp = multi_exp

        # Handling source name:
        if source_list is None:
            self.logger.info("No source name is given. Assigning it to source_name")
            self.source = None_source
        else:
            source_counts = dict(Counter(source_list))
            if len(source_counts.keys()) <= 1:
                self.logger.info("Model name is given. Single-source ensemble is given.")
                source_str_list = [str(item) for item in source_list]
                if source_str_list[0] == "None": source_str_list[0] = None_source
                self.source = source_str_list[0]
            else:
                self.logger.info("Multi-source ensmeble is given. Assigning source name to multi-source")
                self.source = multi_source

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
    ):
        """
        Handles the saving of the input data as 
        netcdf file using OutputSaver.

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
            description = self.diagnostic_name + " " + self.diagnostic_product + " for " + self.catalog + " and " + self.model + " with " + self.model_list + " " + self.exp + " " + self.region  
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

        metadata = {"Description": description}

        outputsaver.save_netcdf(
            dataset=data,
            #diagnostic=self.diagnostic_name,
            diagnostic_product=self.diagnostic_product,
            metadata=metadata,
            extra_keys=extra_keys,
        )

    # Save figure
    def save_figure(self, var, fig, fig_std=None, description=None, format="png"):
        """
        Handles the saving of a figure using OutputSaver.
        
        Args:
            var: The variable in the dataset
            fig (matplotlib.figure.Figure): Figure object.
            fig_std (matplotlib.figure.Figure): Figure object.
            description (str): Description of the figure.
            format (str): Format to save the figure ('png' or 'pdf'). Default is 'png'.
        """
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
        if description is None:
            description = self.diagnostic_name + " " + self.diagnostic_product + " for " + self.catalog + " and " + self.model + " with " + self.model_list + " " + self.exp + " " + self.region
        metadata = {"Description": description}
        extra_keys = {}
        if fig_std is not None:
            data = "mean"
        else:
            data = None
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
            if description is None:
                description = self.diagnostic_name + " " + self.diagnostic_product + " for " + self.catalog + " and " + self.model + " with " + self.model_list + " " + self.exp + " " + self.region
            metadata = {"Description": description}
            extra_keys = {}
            if fig_std is not None:
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
