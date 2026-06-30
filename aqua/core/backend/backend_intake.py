from abc import ABC, abstractmethod

import xarray as xr

from aqua.core.configurer import ConfigPath
from aqua.core.data_model import DataModel
from aqua.core.fixer import Fixer

from .backend import Backend


class BackendIntake(Backend, ABC):
    """
    Abstract class for backends that use intake catalogs to retrieve data.
    This class defines the interface and common functionality for all intake-based backends.
    """

    def __init__(
        self,
        model: str,
        exp: str,
        source: str,
        configurer: ConfigPath,
        catalog: str = None,
        chunks: str | dict = "auto",
        fixer: Fixer = None,
        datamodel: DataModel = None,
        loglevel: str = "WARNING",
        **kwargs,
    ):
        """
        Initialize the BackendIntake instance.

        Args:
            exp (str): Experiment name.
            model (str): Model name.
            source (str): Data source.
            configurer (ConfigPath): An instance of ConfigPath to manage configuration paths.
            catalog (str, optional): Catalog name. Defaults to None.
            format (str, optional): Format of the data file. If None, it will be detected automatically.
            chunks (str | dict, optional): Chunking strategy for xarray. Defaults to "auto".
            fixer (Fixer, optional): An instance of Fixer to apply data fixes. Defaults to None.
            datamodel (DataModel, optional): An instance of DataModel to define the data structure. Defaults to None.
            loglevel (str, optional): Logging level. Defaults to 'WARNING'.
            kwargs: Additional keyword arguments to pass to xarray's open_dataset or open_zarr functions.
        """

        super().__init__(fixer=fixer, datamodel=datamodel, loglevel=loglevel)
        self.model = model
        self.exp = exp
        self.source = source
        self.chunks = chunks
        self.xr_kwargs = kwargs

        # Determine the catalog and machine file paths using the configurer
        self.cat, self.catalog_file, self.machine_file = configurer.deliver_intake_catalog(
            catalog=catalog, model=model, exp=exp, source=source
        )
        self.catalog = self.cat.name
        self.machine = configurer.get_machine()
        machine_paths, intake_vars = configurer.get_machine_info()
        self.expcat = self.cat(**intake_vars)[self.model][self.exp]  # Top-level experiment entry
        self.esmcat = self.expcat[self.source]()

        self.kwargs = self._filter_kwargs(kwargs, intake_vars=intake_vars)
        self.kwargs = self._format_realization_reader_kwargs(self.kwargs)
        self.logger.debug("Using filtered kwargs: %s", self.kwargs)

        # HACK for intake2 following https://github.com/intake/intake-xarray/issues/150
        self.esmcat = self.expcat._entries[self.source](**self.kwargs)

        # NOTE: exposing the underlying intake2 data object (url, xarray_kwargs, ...) is
        # backend-specific and therefore delegated to the concrete subclasses:
        #   - netcdf/zarr expose esmcat.reader.kwargs["args"][0], esmcat.xarray_kwargs (see BackendIntakeXarray)
        #   - GSV/FDB expose esmcat._request (see BackendIntakeFDB)
        # Keeping it out of this base __init__ avoids assuming an xarray-like source for every
        # intake backend (the legacy Reader guarded this block with an isinstance NetCDF/Zarr check).

    @abstractmethod
    def retrieve(
        self,
        var: str | list = None,
        level: str | list = None,
        level_coord: str = None,
        startdate: str = None,
        enddate: str = None,
    ):
        """Open data, apply filters, return xr.Dataset."""

    @abstractmethod
    def _seldate(self, data: xr.Dataset, startdate: str = None, enddate: str = None):
        return super()._seldate(data=data, startdate=startdate, enddate=enddate)

    @abstractmethod
    def _sellevel(self, data: xr.Dataset, level: str | list = None, level_coord: str = None):
        return super()._sellevel(data=data, level=level, level_coord=level_coord)

    @abstractmethod
    def _selvar(self, data: xr.Dataset, var: str | list = None):
        return super()._selvar(data=data, var=var)

    @property
    def intake_user_parameters(self):
        """Lazy loader for intake user parameters to avoid expensive describe() calls."""
        if not hasattr(self, "_intake_user_parameters"):
            self._intake_user_parameters = [v.describe() for v in self.esmcat._entry._user_parameters]  # intake2 change
            self.logger.debug("Intake user parameters: %s", self._intake_user_parameters)
        return self._intake_user_parameters

    def _filter_kwargs(self, kwargs: dict = {}, intake_vars: dict = {}) -> dict:
        """
        Uses the esmcat.describe() to remove the intake_vars, then check in the parameters if the kwargs are present.
        Kwargs which are not present in the intake_vars will be removed.

        Args:
            kwargs (dict): The keyword arguments passed to the reader, which are intake parameters in the source.
            intake_vars (dict): Machine-specific intake variables to exclude from checks.

        Returns:
            A dictionary of kwargs filtered to only include parameters that are present in the intake_vars.
        """
        if intake_vars is None:
            intake_vars = {}

        filtered_kwargs = {}

        # Create a dictionary lookup of parameter definitions
        # This avoids repeated iteration and index lookups in the loop
        param_defs = {p["name"]: p for p in self.intake_user_parameters}
        valid_params = set(param_defs.keys())

        if kwargs:
            # Filter kwargs that are valid parameters
            filtered_kwargs = {k: v for k, v in kwargs.items() if k in valid_params}

            # Find and log dropped keys efficiently using set difference
            dropped_keys = kwargs.keys() - valid_params
            for key in dropped_keys:
                self.logger.warning("kwarg %s is not in the intake parameters of the source, removing it", key)

        # HACK: Keep chunking info if present as reader kwarg
        if self.chunks is not None:
            self.logger.warning("Keeping chunks=%s in the filtered kwargs", self.chunks)
            filtered_kwargs["chunks"] = self.chunks

        # Check for missing required parameters and apply defaults, with logging
        # We identify parameters that are valid but not present in either filtered_kwargs or intake_vars

        # params that are already covered by user kwargs or machine-specific intake_vars
        covered_params = set(filtered_kwargs) | set(intake_vars)

        # Identify missing parameters using set difference
        missing_params = valid_params - covered_params

        for param in missing_params:
            element = param_defs[param]
            default_val = element.get("default")

            # Log the default application
            self.logger.info("%s parameter is required but is missing, setting to default %s", param, default_val)

            allowed = element.get("allowed", None)
            if allowed is not None:
                self.logger.info("Available values for %s are: %s", param, allowed)

            filtered_kwargs[param] = default_val

        return filtered_kwargs

    def _format_realization_reader_kwargs(self, kwargs: dict):
        """
        Reformats the realization string for the access to the reader
        If realization is in the format rXX and the intake type is int, it converts to int XX.
        """
        realization = kwargs.get("realization")
        if realization is None:
            return kwargs

        param_types = {p["name"]: p["type"] for p in self.intake_user_parameters}
        realization_type = param_types.get("realization")

        if realization_type is None:
            self.logger.info("'realization' not in intake parameters %s — removing it.", list(param_types))
            kwargs.pop("realization", None)
            return kwargs

        # if type is string, return as is
        if realization_type == "str":
            self.logger.debug("realization parameter is of type string, will use it is as is: %s", str(realization))
            kwargs["realization"] = str(realization)
            return kwargs

        # if it is in the rXX format and the type is int, convert to int
        if realization_type == "int":
            if isinstance(realization, str) and realization.startswith("r") and realization[1:].isdigit():
                kwargs["realization"] = int(realization[1:])
                self.logger.info("realization parameter converted from rXXX format to int: %d", kwargs["realization"])
                return kwargs
            if isinstance(realization, int):
                return kwargs  # already an int

        raise ValueError(f"Realization {kwargs['realization']} format not recognized for type {realization_type}")
