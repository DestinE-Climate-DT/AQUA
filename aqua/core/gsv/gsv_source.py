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

import os

import eccodes
import numpy as np

from aqua.core.logger import _check_loglevel, log_configure
from aqua.core.util import to_list
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


class GSVSource(FDBSource, FDBDatesMixin):
    """Open a GSV/FDB source through the ``gsv`` retrieval engine."""

    #: ``gsv`` holds the (bridge) GSVRetriever handle, which is not picklable and must
    #: never be shipped to dask workers on top of the generic exclusions.
    _PICKLE_EXCLUDE = FDBSource._PICKLE_EXCLUDE | {"gsv"}

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
        """
        Initializes the GSVSource class. These are typically specified in the catalog entry,
        but can also be specified upon accessing the catalog.

        Args:
            request (dict): Request dictionary
            data_start_date (str): Start date of the available data.
            data_end_date (str): End date of the available data.
            bridge_end_date (str, optional): End date of the bridge data. Defaults to None.
            bridge_start_date (str, optional): Start date of the bridge data. Defaults to None.
            hpc_expver (str, optional): Alternative expver to be used if the data are on hpc
            timestyle (str, optional): Time style. Defaults to "date".
            chunks (str or dict, optional): Time and vertical chunking.
                                        If a string is provided, it is assumed to be time chunking.
                                        If it is a dictionary the keys 'time' and 'vertical' are looked for.
                                        Time chunking can be one of S (step), 10M, 15M, 30M, h, 1h, 3h, 6h, D, 5D, W, M, Y.
                                        Defaults to "S".
                                        Vertical chunking is expressed as the number of vertical levels to be used.
                                        Defaults to None (no vertical chunking).
            timestep (str, optional): Time step. Can be one of 10M, 15M, 30M, 1h, h, 3h, 6h, D, 5D, W, M, Y.
                                      Defaults to "h".
            startdate (str, optional): Start date for request. Defaults to None.
            enddate (str, optional): End date for request. Defaults to None.
            var (str, optional): Variable ID. Defaults to those in the catalog.
            metadata (dict, optional): Metadata read from catalog. Contains path to FDB.
            level (int, float, list): optional level(s) to be read. Must use the same units as the original source.
            switch_eccodes (bool, optional): Flag to activate switching of eccodes path. Defaults to False.
            engine (str, optional): Engine to be used for GSV retrieval: 'polytope' or 'fdb'.
                Defaults to 'fdb' if not specified.
            databridge (str, optional): Only for the Polytope engine. Sets wether the data must be retrieved from the
            Lumi databridge or from the MN5 databridge. Defaults to None.
            loglevel (string) : The loglevel for the GSVSource
            kwargs: other keyword arguments.
        """
        # If engine is not specified, we set it to 'fdb' and we activate the dummy_run flag.
        # This means that we are running a dummy run, where the GSVRetriever is not actually used.
        # This is useful for testing and for the probe call of intake, which is used to get
        # the schema without actually reading the data.
        self.engine = engine or "fdb"
        self.dummy_run = engine is None

        self.logger = log_configure(log_level=loglevel, log_name="GSVSource")

        if self.engine == "polytope":
            self.databridge = "lumi" if databridge is None else databridge
        else:
            self.databridge = None
        self.gsv_log_level = _check_loglevel(self.logger.getEffectiveLevel())
        self.logger.debug("Init of the GSV source class")

        if not gsv_available:
            raise ImportError(gsv_error_cause)

        self._request = request.copy()

        self._read_metadata(metadata, switch_eccodes)

        # set the timestyle
        self.timestyle = timestyle
        self.timeshift = timeshift

        self._resolve_paramids(request, var)

        self._kwargs = kwargs
        self.hpc_expver = hpc_expver

        # set all the start/end dates for data and bridge
        self.data_start_date = None
        self.data_end_date = None
        self.bridge_start_date = None
        self.bridge_end_date = None

        self._define_start_end_dates(data_start_date, data_end_date, bridge_start_date, bridge_end_date)
        # set all the start/end dates for the retrieval
        self._define_retrieve_dates(startdate, enddate)

        # compute the (engine-agnostic) time/level partition plan
        self._compute_partition_plan(data_start_date, savefreq, timestep, chunks, level)

        self.logger.debug("Data frequency (i.e. savefreq): %s", savefreq)
        self.logger.debug(
            "Data_start_date: %s, Data_end_date: %s, Bridge_start_date: %s, Bridge_end_date: %s",
            self.data_start_date,
            self.data_end_date,
            self.bridge_start_date,
            self.bridge_end_date,
        )
        self.logger.debug("Request startdate: %s, Request enddate: %s", self.startdate, self.enddate)

        # GSV/FDB-specific validation of the configured FDB paths (needs chk_type from the plan)
        self._check_fdb_paths()

        self._switch_eccodes()
        super().__init__(metadata=metadata)

    # ------------------------------------------------------------ init helpers
    def _read_metadata(self, metadata, switch_eccodes):
        """Extract the FDB/eccodes/level paths from the catalog metadata."""
        if metadata:
            self.fdbhome = metadata.get("fdb_home", None)
            self.fdbpath = metadata.get("fdb_path", None)
            self.fdbhome_bridge = metadata.get("fdb_home_bridge", None)
            self.fdbpath_bridge = metadata.get("fdb_path_bridge", None)
            if switch_eccodes:
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

    def _resolve_paramids(self, request, var):
        """Resolve the requested variables into a list of ecCodes paramIds."""
        if not var:  # if no var provided keep the default in the catalog
            self._var = request["param"]
        else:
            self._var = var

        self._var = to_list(self._var)  # Make sure self._var is a list

        # Convert var names to paramId. The usage of strings is discouraged, so a warning is issued
        for i, v in enumerate(self._var):
            if isinstance(v, str):
                self.logger.warning("Variable %s is a string, conversion to paramid may lead to errors", v)
                self._var[i] = int(get_eccodes_attr(v)["paramId"])

        self.logger.debug("List of paramid to retrieve %s", self._var)

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
        if chunk_type:
            self.gsv = GSVRetriever(engine=self.engine, source=self.databridge, logging_level=self.gsv_log_level)
        gsv = GSVRetriever(engine=self.engine, source=self.databridge, logging_level=self.gsv_log_level)

        self.logger.debug("Request %s", request)
        dataset = gsv.request_data(
            request, use_stream_iterator=fstream_iterator, process_derived_variables=False
        )  # following 2.9.2 we avoid derived variables

        return dataset
