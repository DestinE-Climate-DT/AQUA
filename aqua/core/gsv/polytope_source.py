"""Concrete GSV/FDB intake source built on top of :class:`FDBPartitionedSource`.

Everything in this module is specific to the GSV retrieval engine (``gsv.retriever``):
the FDB environment setup, the HPC/bridge switching, the pyfdb double-initialisation
workaround and the ecCodes paramId handling. The backend-agnostic machinery (partition
planning, request building, schema and dask assembly) lives in
:mod:`aqua.core.gsv.partitioned`, and the date-resolution strategies in
:mod:`aqua.core.gsv.dates`.

To add a different engine (e.g. Polytope or z3fdb) subclass
:class:`FDBPartitionedSource` and implement ``_retrieve_partition`` only.
"""

import eccodes

from aqua.core.util.eccodes import get_eccodes_attr

from .dates import FDBDatesMixin
from .fdb_source import FDBSource

# Test if FDB5 binary library is available
try:
    from gsv.retriever import GSVRetriever

    gsv_available = True
except RuntimeError:
    gsv_available = False
    gsv_error_cause = "FDB5 binary library not present on system or outdated"
except KeyError:
    gsv_available = False
    gsv_error_cause = "Environment variables for gsv, such as GRID_DEFINITION_PATH, not set."
except ImportError:
    gsv_available = False
    gsv_error_cause = (
        "GSVRetriever cannot be imported. "
        "Check if the gsv package is installed and if the FDB5 binary library is available and properly set up."
    )


class PolytopeSource(FDBSource, FDBDatesMixin):
    """Open a GSV/FDB source through the ``gsv`` retrieval engine."""

    #: ``gsv`` holds the (bridge) GSVRetriever handle, which is not picklable and must
    #: never be shipped to dask workers on top of the generic exclusions.
    # _PICKLE_EXCLUDE = FDBSource._PICKLE_EXCLUDE | {"gsv"}

    def __init__(
        self,
        request,
        data_start_date=None,
        data_end_date=None,
        bridge_start_date=None,
        bridge_end_date=None,
        hpc_expver=None,
        timestyle="date",
        chunks="S",
        savefreq="h",
        timestep="h",
        timeshift=None,
        startdate=None,
        enddate=None,
        var=None,
        metadata=None,
        level=None,
        loglevel="WARNING",
        engine=None,
        databridge=None,
        **kwargs,
    ):
        print("Running Polytope reader!")

        engine = engine or "polytope"

        if engine == "polytope":
            self.databridge = "lumi" if databridge is None else databridge
        else:
            self.databridge = None

        super().__init__(
            request,
            data_start_date=data_start_date,
            data_end_date=data_end_date,
            bridge_start_date=bridge_start_date,
            bridge_end_date=bridge_end_date,
            hpc_expver=hpc_expver,
            timestyle=timestyle,
            chunks=chunks,
            savefreq=savefreq,
            timestep=timestep,
            timeshift=timeshift,
            startdate=startdate,
            enddate=enddate,
            var=var,
            metadata=metadata,
            level=level,
            loglevel=loglevel,
            engine=engine,
            databridge=self.databridge,
            **kwargs,
        )

    # ------------------------------------------------------------ init helpers
    def _check_availability(self):
        if not gsv_available:
            raise ImportError(gsv_error_cause)

    def _read_metadata(self, metadata):
        """Extract the FDB/eccodes/level paths from the catalog metadata."""
        if metadata:
            self.fdb_info_file = metadata.get("fdb_info_file", None)
            self.levels = metadata.get("levels", None)
        else:
            self.fdb_info_file = None
            self.levels = None

    def _map_output_variable(self, ds_var):
        """Translate the raw GRIB variable to (current-ecCodes short name, paramId).

        We consider the paramId stable between ecCodes versions, not the short name.
        So we read the ``GRIB_paramId`` attribute and derive the short name from the
        current ecCodes definitions; if it differs from the retrieved short name a
        warning is issued (this only affects the final name when ``fix=False``). Set
        ``switch_eccodes=True`` in the catalog to read short names from a pinned
        ecCodes version instead.
        """
        original_paramid = self._ds[ds_var].attrs.get("GRIB_paramId", ds_var)
        updated_var = get_eccodes_attr(original_paramid)["shortName"]
        if updated_var != ds_var:
            self.logger.warning(
                "Variable shortname %s has been interpreted with another eccodes. "
                "Current eccodes %s will read paramid %s as %s",
                ds_var,
                eccodes.__version__,
                original_paramid,
                updated_var,
            )
        return updated_var, original_paramid

    # -------------------------------------------------------------- retrieval
    def _retrieve_partition(self, request, chunk_type=None, first=False):
        """Retrieve a single partition through Polytope."""

        fstream_iterator = False  # We set it False, but it works also with True

        # The following is a hack around a pyfdb/fdb5 bug which requires a double initialization when reading from bridge
        # See https://github.com/DestinE-Climate-DT/AQUA/issues/1715
        # Notice also that for some mysterious reason this works only if the
        # result is stored in self (even if then it is not used)
        # if chunk_type:
        #    self.gsv = GSVRetriever(engine=self.engine, source=self.databridge, logging_level=self.gsv_log_level)

        gsv = GSVRetriever(engine=self.engine, source=self.databridge, logging_level=self.gsv_log_level)

        self.logger.debug("Request %s", request)
        dataset = gsv.request_data(
            request, use_stream_iterator=fstream_iterator, process_derived_variables=False
        )  # following 2.9.2 we avoid derived variables

        return dataset
