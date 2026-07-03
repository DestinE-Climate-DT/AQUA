"""Backend-agnostic partitioned source for FDB/MARS-like data access.

This module isolates everything that is **not** specific to the GSV/FDB retrieval
engine, so that alternative drivers (e.g. Polytope, z3fdb) can be implemented by
subclassing :class:`FDBSource` and overriding a single method,
:meth:`FDBSource._retrieve_partition`.

Responsibilities kept here (all engine-agnostic):

* partition planning: time axis, chunk start/end indices and vertical chunking
  (delegated to :mod:`aqua.core.intake_drivers.fdb.openers.timeutil`);
* MARS/FDB request construction per ``timestyle`` (date / step / yearmonth);
* the intake ``Schema`` and the dask assembly (``to_dask``, ``read``,
  ``read_chunked``, ``get_part_delayed``);
* robust pickling for dask workers (snapshot of ``__dict__`` minus transient
  state), so ``__init__`` is *not* re-executed on the workers.

Engine-specific behaviour is delegated to overridable hooks:

* :meth:`_retrieve_partition` (mandatory) — pull one partition from the backend;
* :meth:`_check_availability` (optional) — check library availability at init;
* :meth:`_read_metadata` (optional) — configure engine path parameters;
* :meth:`_post_init` (optional) — final engine-specific init steps;
* :meth:`_postprocess_partition` — per-partition fixups (default: time shift);
* :meth:`_map_output_variable` — map a raw variable to its output name and the
  identifier used to re-request it (default: identity).

Concrete subclasses must implement :meth:`_retrieve_partition` and populate the
request/date/level attributes before calling :meth:`_compute_partition_plan` (see
:class:`aqua.core.intake_drivers.fdb.openers.gsv_source.GSVSource` for a reference implementation).
"""

import datetime

import dask
import eccodes
import xarray as xr

from aqua.core.logger import log_configure
from aqua.core.util import to_list
from aqua.core.util.eccodes import get_eccodes_attr

from .timeutil import FDBTimeMixin


class FDBSource(FDBTimeMixin):
    """Generic intake source that reads FDB/MARS-like data in time/level partitions.

    Concrete subclasses must implement :meth:`_retrieve_partition` and populate the
    request/date/level attributes before calling :meth:`_compute_partition_plan` (see
    :class:`aqua.core.intake_drivers.fdb.openers.GSVSource` for a reference implementation).
    """

    _ds = None  # _ds and _da will contain samples of the data for dask access
    _da = None
    dask_access = False  # Flag if dask has been requested

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
        self.engine = engine
        self.dummy_run = engine is None

        self.logger = log_configure(log_level=loglevel, log_name=self.__class__.__name__)

        from aqua.core.logger import _check_loglevel

        self.gsv_log_level = _check_loglevel(self.logger.getEffectiveLevel())
        self.logger.debug("Init of the %s class", self.__class__.__name__)

        self._check_availability()

        self._request = request.copy()

        self._read_metadata(metadata)

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

        self._post_init()

    def _check_availability(self):
        """Hook for checking external library availability."""
        pass

    def _read_metadata(self, metadata):
        """Hook for reading metadata."""
        pass

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

    def _post_init(self):
        """Hook for any subclass-specific post-init logic."""
        pass

    #: Instance attributes that must never be pickled to dask workers. They are
    #: either heavy (data samples) or hold non-serialisable backend handles. Anything
    #: not listed here is snapshotted as-is, so no manual bookkeeping is required when
    #: new attributes are added (contrast with the old explicit ``__getstate__`` list).
    # _PICKLE_EXCLUDE = frozenset({"_ds", "_da", "_schema"})

    # # ------------------------------------------------------------------ pickling
    # def __getstate__(self):
    #     """Snapshot the state needed by :meth:`_get_partition` on dask workers.

    #     Unlike the intake default (which re-runs ``__init__`` from the captured
    #     constructor arguments), this returns a copy of ``__dict__`` minus the
    #     transient/non-serialisable keys in :attr:`_PICKLE_EXCLUDE`. This avoids
    #     repeating expensive init-time work (date resolution, catalog probing) on
    #     every worker while remaining robust to new attributes.
    #     """
    #     return {k: v for k, v in self.__dict__.items() if k not in self._PICKLE_EXCLUDE}

    # def __setstate__(self, state):
    #     """Restore the snapshot produced by :meth:`__getstate__` without re-init."""
    #     self.__dict__.update(state)

    # ------------------------------------------------------------- planning
    def _compute_partition_plan(self, data_start_date, savefreq, timestep, chunks, level):
        """Compute the time/level partition plan and store it on ``self``.

        Requires the following attributes to be already set by the caller:
        ``self._request``, ``self.timestyle``, ``self.timeshift``, ``self.levels``,
        and the resolved ``self.data_start_date``/``self.data_end_date``/
        ``self.startdate``/``self.enddate``/``self.bridge_start_date``/
        ``self.bridge_end_date``.

        Args:
            data_start_date (str): The *raw* data start date argument (used as the
                base for the optional step offset; kept separate from the floored
                ``self.data_start_date`` to preserve historical behaviour).
            savefreq (str): Frequency at which the data are saved.
            timestep (str): Native time step.
            chunks (str | dict): Time (and optional vertical) chunking specification.
            level (int | float | list | None): Level(s) requested, overriding the
                catalog default.
        """
        # flooring to the frequency the time to ensure that hourly, daily and monthly data
        # are read at the right time frequency
        # setting hpc and bridge availability dates
        for attr in ["data_start_date", "data_end_date", "bridge_end_date", "bridge_start_date", "startdate", "enddate"]:
            setattr(self, attr, self._floor_datetime(getattr(self, attr), savefreq))

        if self.timestyle != "yearmonth":
            offset = int(self._request.get("step", 0))  # optional initial offset for steps (in timesteps)
            # special for 6h: set offset startdate if needed
            self._add_offset(data_start_date, offset, timestep)

        if isinstance(chunks, dict):
            chunking_time = chunks.get("time", "S")
            chunking_vertical = chunks.get("vertical", None)
        else:
            chunking_time = chunks
            chunking_vertical = None

        if chunking_time.upper() == "S":  # special case: time chunking is single saved frame
            chunking_time = savefreq

        self.data_startdate, self.data_starttime = self._split_date(self.data_start_date)

        self._resolve_levels(level)

        self._compute_timeaxis(
            timestep=timestep,
            savefreq=savefreq,
            chunkfreq=chunking_time,
        )

        self._compute_vertical_chunks(chunking_vertical)

        self.itime = 0  # position of time dim (recomputed in to_dask)
        self.ilevel = None

    def _resolve_levels(self, level):
        """Resolve ``levelist``/``idx_3d``/``onelevel`` from the request and metadata."""
        if "levelist" in self._request:
            levelist = to_list(self._request["levelist"])
            if level:
                level = to_list(level)
                idx = list(map(levelist.index, level))
                self.idx_3d = idx
                self._request["levelist"] = level  # override default levels
                if self.levels:  # if levels in metadata select them too
                    self.levels = to_list(self.levels)
                    self.levels = [self.levels[i] for i in idx]
            else:
                self.idx_3d = list(range(0, len(levelist)))
        else:
            self.idx_3d = None

        self.onelevel = False
        if "levelist" in self._request:
            if self.levels:  # Do we have physical levels specified in metadata?
                lev = self._request["levelist"]
                if isinstance(lev, list) and len(lev) > 1:
                    self.onelevel = True  # If yes we can afford to read only one level
            else:
                self.logger.warning(
                    "A speedup of data retrieval could be achieved by specifying the levels keyword in metadata."
                )

    def _compute_vertical_chunks(self, chunking_vertical):
        """Split the level list into vertical chunks and update the partition count."""
        self.chk_vert = None
        self.ntimechunks = self._npartitions
        self.nlevelchunks = None
        self.chunking_vertical = None  # default: no vertical chunking

        if "levelist" in self._request:
            self.chunking_vertical = chunking_vertical
            if self.chunking_vertical:
                levelist = to_list(self._request["levelist"])
                if len(levelist) <= self.chunking_vertical:
                    self.chunking_vertical = None
                else:
                    self.chk_vert = [
                        levelist[i : i + self.chunking_vertical] for i in range(0, len(levelist), self.chunking_vertical)
                    ]
                    self.ntimechunks = self._npartitions
                    self.nlevelchunks = len(self.chk_vert)
                    self._npartitions = self._npartitions * len(self.chk_vert)

    # ------------------------------------------------------------------ schema

    def _get_schema(self):
        """
        Standard method providing data schema.
        For dask access it is assumed that all DataArrays read share the same shape and data type.
        """

        # check if dates are within acceptable range
        self._check_dates()

        if self.dask_access:  # We need a better schema for dask access
            if not self._ds or not self._da:  # we still have to retrieve a sample dataset
                self._ds = self._get_partition(0, var=self._var, first=True, onelevel=self.onelevel)

                var = list(self._ds.data_vars)[0]
                da = self._ds[var]  # get first variable dataarray

                # If we have multiple levels, then this array needs to be expanded
                if self.onelevel:
                    lev = self.levels
                    apos = da.dims.index("level")  # expand the size of the "level" axis
                    attrs = da["level"].attrs
                    da = da.squeeze("level").drop_vars("level").expand_dims(level=lev, axis=apos)
                    da["level"].attrs.update(attrs)

                self._da = da

            metadata = {"dims": self._da.dims, "attrs": self._ds.attrs}
            schema = Schema(
                datashape=None,
                dtype=str(self._da.dtype),
                shape=self._da.shape,
                name=None,
                npartitions=self._npartitions,
                extra_metadata=metadata,
            )
        else:
            schema = Schema(
                datashape=None,
                dtype=str(xr.Dataset),
                shape=None,
                name=None,
                npartitions=self._npartitions,
                extra_metadata={},
            )

        return schema

    def _index_to_timelevel(self, ii):
        """
        Internal method to convert partition index to time and level indices
        """
        if self.chunking_vertical:
            i = ii // len(self.chk_vert)
            j = ii % len(self.chk_vert)
        else:
            i = ii
            j = 0
        return i, j

    # -------------------------------------------------------- request building
    def _build_partition_request(self, i, j, var=None, first=False, onelevel=False):
        """Build the MARS/FDB request dict for the (time ``i``, level ``j``) partition.

        This is engine-agnostic: it only manipulates the request dictionary according to
        the configured ``timestyle`` and the requested variable/levels. The actual data
        pull is delegated to :meth:`_retrieve_partition`.
        """
        request = self._request.copy()  # We are going to change it, threads do need this

        if self.chunking_vertical:
            request["levelist"] = self.chk_vert[j]

        if self.timestyle == "date":
            dds, tts = self._date2str(self.chk_start_date[i])
            dde, tte = self._date2str(self.chk_end_date[i])
            if ((dds == dde) and (tts == tte)) or first:
                request["date"] = f"{dds}"
                request["time"] = f"{tts}"
            else:
                request["date"] = f"{dds}/to/{dde}"
                request["time"] = f"{tts}/to/{tte}"

        elif self.timestyle == "step":  # style is 'step'
            request["date"] = self.data_startdate
            request["time"] = self.data_starttime
            s0 = self.chk_start_idx[i]
            s1 = self.chk_end_idx[i]
            if s0 == s1 or first:
                request["step"] = f"{s0}"
            else:
                request["step"] = f"{s0}/to/{s1}"

        elif self.timestyle == "yearmonth":  # style is 'yearmonth'
            yys, mms = self._date2yyyymm(self.chk_start_date[i])
            yye, mme = self._date2yyyymm(self.chk_end_date[i])
            if (yys == yye) or first:
                request["year"] = f"{yys}"
            else:
                request["year"] = f"{yys}/to/{yye}"
            if (mms == mme) or first:
                request["month"] = f"{mms}"
            else:
                request["month"] = f"{mms}/to/{mme}"
        else:
            raise ValueError(f"Timestyle {self.timestyle} not supported")

        if onelevel:  # limit to one single level
            request["levelist"] = request["levelist"][0]

        # If a var is used and it is a string, it means that previous parts of the code have failed
        # to convert it to paramId. The conversion is then delegated to the retrieval engine, which
        # relies on a dictionary of paramId to shortName. This is not guaranteed to work, so a warning
        # is issued.
        if var:
            if isinstance(var, str):
                self.logger.warning("Asking for variable %s as string, this may lead to errors", var)
            request["param"] = var
        else:
            request["param"] = self._var

        return request

    def _get_partition(self, ii, var=None, first=False, onelevel=False):
        """
        Standard internal method reading the i-th data partition.

        Builds the request (engine-agnostic) and delegates the actual retrieval to
        :meth:`_retrieve_partition`, then applies per-partition post-processing.

        Args:
            ii (int): partition number
            var (string, optional): single variable to retrieve. Defaults to using those set at init
            first (bool, optional): read only the first step (used for schema retrieval)
            onelevel (bool, optional): read only one level. Defaults to False.

        Returns:
            An xarray.Dataset
        """
        i, j = self._index_to_timelevel(ii)
        request = self._build_partition_request(i, j, var=var, first=first, onelevel=onelevel)

        dataset = self._retrieve_partition(request, chunk_type=self.chk_type[i], first=first)
        dataset = self._postprocess_partition(dataset)

        return dataset

    # --------------------------------------------------------------- hooks
    def _retrieve_partition(self, request, chunk_type, first=False):
        """Pull a single partition from the backend for ``request``.

        This is the **only** method a new engine (Polytope, z3fdb, ...) must implement.

        Args:
            request (dict): the fully-built MARS/FDB request for this partition.
            chunk_type (int): 0 if the partition lives on HPC FDB, 1 if on the bridge.
            first (bool): whether this is the schema-probing read of the first step.

        Returns:
            xr.Dataset: the retrieved (lazy or in-memory) dataset for this partition.
        """
        raise NotImplementedError("Concrete FDB sources must implement _retrieve_partition()")

    def _postprocess_partition(self, dataset):
        """Per-partition fixups applied right after retrieval. Default: time shift."""
        if self.timeshift:  # shift time by one month (special case)
            dataset = self._shift_time_dataset(dataset)
        return dataset

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

    # ---------------------------------------------------------------- readers
    def read(self):
        """Return a in-memory dask dataset"""
        ds = [self._get_partition(i) for i in range(self._npartitions)]
        ds = xr.concat(ds, dim="time", coords="different")
        return ds

    def _get_partition_data(self, ii, var):
        """Helper to retrieve the raw numpy array of the first variable in a partition.

        This avoids the heavy overhead of xarray's `.to_array()` call and keeps the
        Dask task graph simple by running the extraction entirely inside the delayed task.
        """
        ds = self._get_partition(ii, var=var)
        var_name = list(ds.data_vars)[0]
        return ds[var_name].data

    def get_part_delayed(self, ii, var, shape, dtype):
        """
        Function to read a delayed partition.
        Returns a dask.array

        Args:
            ii (int): partition number
            var (string): variable name
            shape: shape of the schema
            dtype: data type of the schema
        """

        i, j = self._index_to_timelevel(ii)

        ds = dask.delayed(self._get_partition_data)(ii, var=var)

        newshape = list(shape)
        newshape[self.itime] = self.chk_size[i]
        if self.chunking_vertical:  # if we have vertical chunking
            newshape[self.ilevel] = len(self.chk_vert[j])

        return dask.array.from_delayed(ds, newshape, dtype)

    def to_dask(self):
        """Return a dask xarray dataset for this data source"""

        self.dask_access = True  # This is used to tell _get_schema() to load dask info
        schema = self._get_schema()

        shape = schema.shape
        dtype = schema.dtype

        self.itime = self._da.dims.index("time")
        if self.chunking_vertical:
            self.ilevel = self._da.dims.index("level")

        if "valid_time" in self._da.coords:  # temporary hack because valid_time is inconsistent anyway
            self._da = self._da.drop_vars("valid_time")

        coords = self._da.coords.copy()
        coords["time"] = self.timeaxis

        ds = xr.Dataset()

        # Now works only with the variables which have been read (the fixer may change names later).
        # Variable-name mapping (e.g. paramId -> current-ecCodes short name) is delegated to the
        # engine-specific _map_output_variable() hook.
        for var in self._ds.data_vars:
            output_var, retrieval_var = self._map_output_variable(var)

            # Create a dask array from a list of delayed get_partition calls
            if not self.chunking_vertical:
                dalist = [self.get_part_delayed(i, retrieval_var, shape, dtype) for i in range(self._npartitions)]
                darr = dask.array.concatenate(dalist, axis=self.itime)  # This is a lazy dask array
            else:
                dalist = []
                for j in range(self.nlevelchunks):
                    dalistlev = [
                        self.get_part_delayed(i * self.nlevelchunks + j, retrieval_var, shape, dtype)
                        for i in range(self.ntimechunks)
                    ]  # noqa: E501
                    dalist.append(dask.array.concatenate(dalistlev, axis=self.itime))
                darr = dask.array.concatenate(dalist, axis=self.ilevel)  # This is a lazy dask array

            da = xr.DataArray(
                darr,
                name=output_var,
                attrs=self._ds[var].attrs,  # We need the original var to retrieve the attributes
                dims=self._da.dims,
                coords=coords,
            )

            log_history(da, "Dataset retrieved by GSV interface")

            ds[output_var] = da

        ds.attrs.update(self._ds.attrs)
        if self.idx_3d:
            ds = ds.assign_coords(idx_level=("level", self.idx_3d))

        return ds

    # Overload read_chunked() from base.DataSource
    def read_chunked(self):
        """Return iterator over container fragments of data source"""
        # self._get_schema()
        for i in range(self._npartitions):
            ds = self._get_partition(i)
            if self.idx_3d:
                ds = ds.assign_coords(idx_level=("level", self.idx_3d))
            yield ds


# This function is repeated here in order not to create a cross dependency between the gsv
# subpackage and the rest of AQUA.
def log_history(data, msg):
    """Elementary provenance logger in the history attribute"""

    if isinstance(data, (xr.DataArray, xr.Dataset)):
        now = datetime.datetime.now()
        date_now = now.strftime("%Y-%m-%d %H:%M:%S")
        hist = data.attrs.get("history", "") + f"{date_now} {msg};\n"
        data.attrs.update({"history": hist})


class Schema(dict):
    def __getattr__(self, item):
        return self[item]
