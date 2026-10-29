"""Full-field Polytope access through Earthkit, using the shared FDB planner.

Example:
    Use an existing Intake FDB catalog through the public Reader API::

        reader = Reader(model="IFS-NEMO", exp="ssp370", source="hourly-hpz7-atm2d",
                        engine="polytope-earthkit", chunks="D")
        data = reader.retrieve(var="2t")

    Catalog metadata may override the service with ``polytope.address`` and
    ``polytope.collection``. Credentials are managed by the Polytope client.
"""

from copy import deepcopy

import xarray as xr

from aqua.core.exceptions import NoDataError
from aqua.core.logger import _check_loglevel, log_history
from aqua.core.util import to_list

from .dates import FDBDatesMixin
from .fdb_source import FDBSource

try:
    import earthkit.data as earthkit_data
except ImportError:
    earthkit_data = None

xr.set_options(keep_attrs=True)


class EarthkitPolytopeSource(FDBSource, FDBDatesMixin):
    """Retrieve partitioned full fields without the GSV interface."""

    def __init__(self, request, *args, databridge=None, engine="polytope-earthkit", **kwargs):
        """Initialize Earthkit access with the usual FDB source arguments.

        Args:
            request (dict): Base MARS request for full fields.
            *args: Positional arguments accepted by FDBSource.
            databridge (str, optional): DestinE bridge name. Defaults to "lumi".
            engine (str, optional): Engine identifier. Defaults to "polytope-earthkit".
            **kwargs: Arguments accepted by FDBSource, including catalog metadata.

        Raises:
            ImportError: When Earthkit is unavailable.
            ValueError: When service configuration or the request is invalid.
        """
        self.databridge = databridge or "lumi"
        if "feature" in request:
            raise ValueError("The Earthkit FDB driver supports full fields only; remove the feature request.")
        super().__init__(deepcopy(request), *args, engine=engine, **kwargs)

    def _check_availability(self):
        """Check the optional Earthkit dependency."""
        if earthkit_data is None:
            raise ImportError("Earthkit Polytope access requires earthkit-data; install aqua-core[polytope].")

    def _read_metadata(self, metadata):
        """Read physical levels and declarative Polytope service configuration."""
        metadata = metadata or {}
        self.levels = metadata.get("levels")
        self.fdb_info_file = None
        options = metadata.get("polytope", {})
        if not isinstance(options, dict):
            raise ValueError("metadata.polytope must be a mapping of service options.")
        self.collection = options.get("collection", "destination-earth")
        self.address = options.get("address")
        if self.address is None:
            if not isinstance(self.databridge, str) or self.databridge.lower() not in ("lumi", "mn5", "leonardo"):
                raise ValueError("Unknown databridge; specify metadata.polytope.address for a custom service.")
            self.address = f"polytope.{self.databridge.lower()}.apps.dte.destination-earth.eu"
        if not all(isinstance(value, str) and value.strip() for value in (self.address, self.collection)):
            raise ValueError("Polytope address and collection must be non-empty strings.")

    def _post_init(self):
        """Remote access needs no local FDB configuration."""

    def _retrieve_partition(self, request, chunk_type=None, first=False):
        """Retrieve and decode one partition using Earthkit's GRIB backend."""
        fields = earthkit_data.from_source(
            "polytope", self.collection, request, stream=False, address=self.address,
            log_level=_check_loglevel(self.logger.getEffectiveLevel()),
        ).to_fieldlist()
        if not len(fields):
            raise NoDataError("Polytope returned no GRIB fields for the requested partition.")

        has_levels = "levelist" in request
        dataset = fields.to_xarray(
            time_dims=["valid_time"],
            rename_dims={"valid_time": "time"},
            ensure_dims=["valid_time", "level"] if has_levels else ["valid_time"],
            drop_dims=[] if has_levels else ["level", "level_type"],
            # Normalized parameter units can differ from the unconverted GRIB values.
            variable_attrs=[
                {
                    "standard_name": "key=metadata.cfName",
                    "long_name": "key=metadata.name",
                    "units": "key=metadata.units",
                    "GRIB_paramId": "key=metadata.paramId",
                },
            ],
            add_earthkit_attrs=False,
        )
        if has_levels:
            levels = to_list(request["levelist"])
            dataset = dataset.sel(level=[dataset.level.dtype.type(level) for level in levels])
            if self.levels is not None:
                physical_levels = dict(zip(to_list(self._request["levelist"]), self.levels, strict=True))
                dataset = dataset.assign_coords(
                    level=("level", [physical_levels[level] for level in levels], dataset.level.attrs)
                )

        log_history(dataset, "Full fields retrieved through Earthkit Polytope")
        return dataset
