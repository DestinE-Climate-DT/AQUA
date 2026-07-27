"""Factory class to create backend instances based on the provided parameters."""

# Register custom intake drivers (gsv, icechunk, etc.)
import aqua.core.intake_drivers  # noqa: F401
from aqua.core.configurer import ConfigPath
from aqua.core.data_model import DataModel
from aqua.core.default import DEFAULT_CONVENTION, DEFAULT_DATAMODEL
from aqua.core.fixer import Fixer
from aqua.core.logger import log_configure

from .backend_intake_fdb import BackendIntakeFDB
from .backend_intake_icechunk import BackendIntakeIcechunk
from .backend_intake_xarray import BackendIntakeXarray
from .backend_xarray import BackendXarray


class BackendFactory:
    """
    Factory class to create backend instances based on the provided parameters.
    """

    BACKEND_TYPES = {
        "gsv": BackendIntakeFDB,
        "icechunk": BackendIntakeIcechunk,
        "netcdf": BackendIntakeXarray,
        "zarr": BackendIntakeXarray,
        "xarray": BackendXarray,
    }

    # Parameters accepted by each driver's backend constructor.
    # netcdf and zarr share the same intake-xarray signature.
    _BACKEND_PARAMS = {
        "gsv": {
            "model",
            "exp",
            "source",
            "configurer",
            "catalog",
            "chunks",
            "fixer",
            "datamodel",
            "engine",
            "databridge",
            "loglevel",
        },
        "netcdf": {
            "model",
            "exp",
            "source",
            "configurer",
            "catalog",
            "chunks",
            "fixer",
            "datamodel",
            "loglevel",
        },
        "zarr": {
            "model",
            "exp",
            "source",
            "configurer",
            "catalog",
            "chunks",
            "fixer",
            "datamodel",
            "loglevel",
        },
        "icechunk": {
            "model",
            "exp",
            "source",
            "configurer",
            "catalog",
            "chunks",
            "fixer",
            "datamodel",
            "loglevel",
        },
        "xarray": {
            "path",
            "xarray_engine",
            "chunks",
            "fixer",
            "datamodel",
            "loglevel",
        },
    }

    def __init__(
        self,
        configurer: ConfigPath,
        model: str = None,
        exp: str = None,
        source: str = None,
        path: str = None,
        catalog: str = None,
        loglevel: str = "WARNING",
    ):
        # Set the provided parameters as instance attributes
        self.model = model
        self.exp = exp
        self.source = source
        self.catalog = catalog
        self.path = path

        self.configurer = configurer
        self.loglevel = loglevel

        self.logger = log_configure(log_level=loglevel, log_name="BackendFactory")

        self._check_required_params()

        # Attributes to be populated:
        self.cat = None
        self.catalog_file = None
        self.machine_file = None
        self.esmcat = None
        self.driver = None
        self.metadata = None
        self.machine_paths = None

    def select_backend(self):
        """
        Select the appropriate backend based on the provided parameters.
        If a path is provided, the xarray backend is selected.
        Otherwise, the intake backend family is selected.

        Please notiche the _check_required_params method ensures that either a path or model/exp/source are provided.
        """
        if self.path:
            self._select_backend_xarray()
        else:
            self._select_backend_intake()

    def _select_backend_intake(self):
        """
        Activate the driver for intake backends (gsv, netcdf, zarr, icechunk).
        Configure catalogs, machine paths, and metadata based on the provided parameters.
        """
        # Explore the intake catalog
        self.cat, self.catalog_file, self.machine_file = self.configurer.deliver_intake_catalog(
            catalog=self.catalog, model=self.model, exp=self.exp, source=self.source
        )
        # Replace the catalog name if it was not provided
        self.catalog = self.cat.name
        # Extract the machine paths and intake variables
        self.machine_paths, intake_vars = self.configurer.get_machine_info()

        # Extract the source catalog and explore it
        self.esmcat = self.cat(**intake_vars)[self.model][self.exp]._entries[self.source]()
        self.driver = self.esmcat._entry._driver
        if self.driver not in self.BACKEND_TYPES:
            raise ValueError(f"Unsupported driver: {self.driver}. Supported drivers are: {list(self.BACKEND_TYPES.keys())}")

        self.metadata = self.esmcat.reader.metadata

    def _select_backend_xarray(self):
        """
        Activate the driver for xarray
        """
        self.driver = "xarray"

    def get_metadata(
        self,
        fixer_name: str = None,
        src_grid_name: str = None,
        convention: str = None,
        datamodel_name: str = None,
    ):
        """
        Return populated fixer_name, src_grid_name, convention,
        and datamodel_name based on the provided parameters and metadata.
        """
        fixer_name = fixer_name or self.metadata.get("fixer_name") if self.metadata else None
        # Catalog template writes 'source_grid_name'
        src_grid_name = src_grid_name or self.metadata.get("source_grid_name") if self.metadata else None

        convention = convention or self.metadata.get("convention", DEFAULT_CONVENTION) if self.metadata else DEFAULT_CONVENTION
        # An explicit datamodel=False must be preserved to disable the data model, so we
        # cannot use `or` here (False or "aqua" would wrongly re-enable it).
        if datamodel_name is None:
            datamodel_name = self.metadata.get("data_model", DEFAULT_DATAMODEL) if self.metadata else DEFAULT_DATAMODEL

        if convention is not None and convention != DEFAULT_CONVENTION:
            raise ValueError(f"Convention {convention} not supported, only 'eccodes' is supported so far.")

        return fixer_name, src_grid_name, convention, datamodel_name

    def create_backend(
        self,
        fixer: Fixer = None,
        datamodel: DataModel = None,
        chunks: str | dict = None,
        engine: str = None,
        databridge: str = None,
        loglevel: str = None,
        **kwargs,
    ):
        """
        Create and return a backend instance based on the provided parameters.

        Only kwargs accepted by the target backend's constructor are forwarded;
        driver-specific parameters (e.g. engine/databridge for GSV, path for xarray)
        are included or excluded automatically via _BACKEND_PARAMS.
        """
        all_kwargs = dict(
            model=self.model,
            exp=self.exp,
            source=self.source,
            path=self.path,
            configurer=self.configurer,
            catalog=self.catalog,
            chunks=chunks,
            fixer=fixer,
            datamodel=datamodel,
            engine=engine,
            xarray_engine=None,
            databridge=databridge,
            loglevel=loglevel,
            **kwargs,
        )
        # Store all kwargs for debugging/logging purposes
        # Filter kwargs to only include those accepted by the selected backend plus any additional kwargs provided by the user
        accepted = self._BACKEND_PARAMS[self.driver] | set(kwargs.keys())
        filtered = {k: v for k, v in all_kwargs.items() if k in accepted}
        self.logger.debug("Creating backend %s with kwargs: %s", self.driver, list(filtered))
        return self.BACKEND_TYPES[self.driver](**filtered)

    def _check_required_params(self):
        """Check if the required parameters are provided."""
        if self.path is None and not all(v is not None for v in [self.model, self.exp, self.source]):
            raise ValueError(
                "Nor path nor model/exp/source are provided. Please provide either a path or model, exp, and source."
            )
        if self.path is not None and any(v is not None for v in [self.model, self.exp, self.source]):
            self.logger.error(
                "Both path and model/exp/source are provided.\n"
                "The model/exp/source parameters will be ignored in favor of the path."
            )
