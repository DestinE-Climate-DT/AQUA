"""Concrete GSV/FDB intake source built on top of :class:`FDBSource`.

Everything in this module is specific to the GSV retrieval engine (``gsv.retriever``):
the FDB environment setup, the HPC/bridge switching, the pyfdb double-initialisation
workaround and the ecCodes paramId handling. The backend-agnostic machinery (partition
planning, request building, schema and dask assembly) lives in
:mod:`aqua.core.intake_drivers.fdb.openers.fdb_source`, and the date-resolution strategies in
:mod:`aqua.core.intake_drivers.fdb.openers.dates`.

To add a different engine (e.g. Polytope or z3fdb) subclass
:class:`FDBSource` and implement ``_retrieve_partition`` only.
"""

import os

import eccodes
import numpy as np

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


class GSVSource(FDBSource, FDBDatesMixin):
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
        switch_eccodes=False,
        loglevel="WARNING",
        engine=None,
        databridge=None,
        **kwargs,
    ):
        self.switch_eccodes = switch_eccodes
        engine = engine or "fdb"

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
            self.fdbhome = metadata.get("fdb_home", None)
            self.fdbpath = metadata.get("fdb_path", None)
            self.fdbhome_bridge = metadata.get("fdb_home_bridge", None)
            self.fdbpath_bridge = metadata.get("fdb_path_bridge", None)
            if self.switch_eccodes:
                self.eccodes_path = metadata.get("eccodes_path", None)
                self.logger.info("ECCODES switching to %s", self.eccodes_path)
            else:
                self.logger.debug("ECCODES switching is off")
                self.eccodes_path = None
            self.levels = metadata.get("levels", None)
            self.fdb_info_file = metadata.get("fdb_info_file", None)
        else:
            self.fdbpath = None
            self.fdbhome = None
            self.fdbhome_bridge = None
            self.fdbpath_bridge = None
            self.fdb_info_file = None
            self.eccodes_path = None
            self.levels = None

    def _post_init(self):
        # GSV/FDB-specific validation of the configured FDB paths (needs chk_type from the plan)
        self._check_fdb_paths()
        self._switch_eccodes()

    def _check_fdb_paths(self):
        """Validate the configured HPC/bridge FDB paths against the partition types."""
        if self.engine == "fdb" and not self.dummy_run:
            # We run the checks only on the real init, to avoid issues with the probe call of intake
            if np.any(self.chk_type == 0):  # We have HPC chunks
                if not self.fdbpath and not self.fdbhome:
                    raise ValueError("Some data is on HPC but no local FDB path or FDB home is specified in catalog.")
                # Check that the specified paths actually exist
                if self.fdbhome and not os.path.exists(self.fdbhome):
                    raise FileNotFoundError(f"fdbhome path {self.fdbhome} does not exist!")
                if self.fdbpath and not os.path.exists(self.fdbpath):
                    raise FileNotFoundError(f"fdbpath path {self.fdbpath} does not exist!")

            if np.any(self.chk_type == 1):  # We have bridge chunks
                if not self.fdbpath_bridge and not self.fdbhome_bridge:
                    raise ValueError("Some data is on bridge but no bridge FDB path or FDB home specified in catalog.")
                # Check that the specified paths actually exist
                if self.fdbhome_bridge and not os.path.exists(self.fdbhome_bridge):
                    raise FileNotFoundError(f"fdbhome_bridge path {self.fdbhome_bridge} does not exist!")
                if self.fdbpath_bridge and not os.path.exists(self.fdbpath_bridge):
                    raise FileNotFoundError(f"fdbpath_bridge path {self.fdbpath_bridge} does not exist!")

    # --------------------------------------------------------------- ecCodes
    def _switch_eccodes(self):
        """
        Internal method to switch ECCODES version if needed.
        """
        if self.eccodes_path:  # if needed switch eccodes path
            # unless we have already switched
            if self.eccodes_path and (self.eccodes_path != eccodes.codes_definition_path()):
                eccodes.codes_context_delete()  # flush old definitions in cache
                eccodes.codes_set_definitions_path(self.eccodes_path)

    # -------------------------------------------------------------- retrieval
    def _retrieve_partition(self, request, chunk_type, first=False):
        """Retrieve a single partition through the GSV engine (FDB or Polytope).

        Sets the FDB environment for the relevant store (HPC vs bridge), applies the
        pyfdb/fdb5 double-initialisation workaround for the bridge and calls
        ``GSVRetriever.request_data``.
        """
        # Select based on type of FDB
        fstream_iterator = False  # We set it False, but it works also with True

        if chunk_type:
            # Bridge FDB type
            if self.fdbhome_bridge:
                os.environ["FDB_HOME"] = self.fdbhome_bridge
                self.logger.debug("Access is BRIDGE and FDB_HOME is set to %s", self.fdbhome_bridge)
            if self.fdbpath_bridge:
                os.environ["FDB5_CONFIG_FILE"] = self.fdbpath_bridge
                self.logger.debug("Access is BRIDGE and FDB5_CONFIG_FILE is set to %s", self.fdbpath_bridge)
            fstream_iterator = True
        else:
            # HPC FDB type
            if self.fdbhome:  # if fdbhome is provided, use it, since we are creating a new gsv
                os.environ["FDB_HOME"] = self.fdbhome
                self.logger.debug("Access is HPC and FDB_HOME is set to %s", self.fdbhome)
            if self.fdbpath:  # if fdbpath provided, use it, since we are creating a new gsv
                os.environ["FDB5_CONFIG_FILE"] = self.fdbpath
                self.logger.debug("Access is HPC and FDB5_CONFIG_FILE is set to %s", self.fdbpath)
            if self.hpc_expver:
                request["expver"] = self.hpc_expver

        self._switch_eccodes()

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
