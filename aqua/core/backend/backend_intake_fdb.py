"""Backend realization using the intake 'gsv' driver (FDB/GSV) for data handling."""

import xarray as xr

from aqua.core.configurer import ConfigPath
from aqua.core.data_model import DataModel
from aqua.core.fixer import Fixer
from aqua.core.util import to_list

from .backend_intake import BackendIntake


class BackendIntakeFDB(BackendIntake):
    """
    Concrete backend retrieving data from FDB/GSV through the intake ``gsv`` driver.

    The underlying source is an :class:`aqua.core.gsv.GSVSource`. Unlike the xarray-based
    backends, date and level selection are pushed *down into the GSV request* (handled by
    ``GSVSource`` at read time), so they must NOT be re-applied with ``.sel()`` afterwards.
    Only variable fixes and the data model are applied on top of the retrieved dataset.

    This mirrors the legacy ``Reader.reader_fdb`` flow, see ``aqua/core/reader/reader.py``.
    """

    def __init__(
        self,
        model: str,
        exp: str,
        source: str,
        configurer: ConfigPath,
        catalog: str = None,
        chunks: str | dict = None,
        fixer: Fixer = None,
        datamodel: DataModel = None,
        engine: str = "fdb",
        databridge: str = None,
        loglevel: str = "WARNING",
        **kwargs,
    ):
        """
        Initialize the BackendIntakeFDB instance.

        Args:
            model (str): Model name.
            exp (str): Experiment name.
            source (str): Data source.
            configurer (ConfigPath): An instance of ConfigPath to manage configuration paths.
            catalog (str, optional): Catalog name. Defaults to None.
            chunks (str | dict, optional): Time/vertical chunking forwarded to the GSV request.
                                           Defaults to None (use the catalog default).
            fixer (Fixer, optional): An instance of Fixer to apply data fixes. Defaults to None.
            datamodel (DataModel, optional): An instance of DataModel to standardize coordinates. Defaults to None.
            engine (str, optional): Engine used for GSV retrieval, 'fdb' or 'polytope'. Defaults to 'fdb'.
            databridge (str, optional): Only for the polytope engine: 'lumi' or 'mn5'. Defaults to None.
            loglevel (str, optional): Logging level. Defaults to 'WARNING'.
            kwargs: Additional intake parameters forwarded to the catalog source entry.
        """
        # TODO: the legacy Reader._filter_kwargs injected 'engine' (and 'databridge' for polytope)
        # into the intake kwargs *before* the GSVSource was instantiated, because GSVSource runs in
        # 'dummy_run' mode when engine is None (it will not actually read). The refactored
        # BackendIntake._filter_kwargs no longer does this. Decide where to thread engine/databridge:
        #   (a) re-add GSV-awareness to BackendIntake._filter_kwargs, or
        #   (b) pass engine via **kwargs here so it flows into self.kwargs and the source constructor.
        # For now we keep them as attributes and forward them at retrieve() time.
        super().__init__(
            model,
            exp,
            source,
            configurer=configurer,
            catalog=catalog,
            chunks=chunks,
            fixer=fixer,
            datamodel=datamodel,
            loglevel=loglevel,
            **kwargs,
        )
        self.loglevel = loglevel
        self.engine = engine
        self.databridge = databridge

        # GSV/FDB specific handle: the request dict carried by the GSVSource
        # (the xarray-style esmcat.data/.xarray_kwargs handles do not exist for GSV).
        self.request = self.esmcat._request

        # default list of variables (paramids) available in this source, read from catalog metadata
        self.fdb_var = self.esmcat.metadata.get("variables")

    def retrieve(
        self,
        var: str | list = None,
        level: str | list = None,
        level_coord: str = None,
        startdate: str = None,
        enddate: str = None,
    ):
        """
        Retrieve data from FDB/GSV as a lazy (dask-backed) xarray.Dataset.

        Date and level selection are delegated to the GSV request itself, so they are passed
        to the source here and intentionally NOT re-applied in ``_postprocess_data``.
        """
        # Resolve which variables (paramids) to request from FDB.
        loadvar = self._resolve_loadvar(var)

        level = to_list(level) if level else None

        # Build/configure the GSV source for this request and return a lazy dask dataset.
        # Mirrors the legacy Reader.reader_fdb(...).to_dask() call.
        # TODO: thread engine/databridge through (see __init__ note); also honour aggregation/streaming
        #       and the chunks dict semantics handled in the legacy reader_fdb if those are needed here.
        call_kwargs = {
            "request": self.request,
            "startdate": startdate,
            "enddate": enddate,
            "var": loadvar,
            "level": level,
            "logging": True,
            "loglevel": self.loglevel,
        }
        # Only override the catalog chunking when the user actually specified it: passing
        # chunks=None would clobber the source default (e.g. 'chunks: h') and break GSVSource.
        if self.chunks is not None:
            call_kwargs["chunks"] = self.chunks

        source = self.esmcat(**call_kwargs)
        data = source.to_dask()

        # Only fixer + datamodel (+ optional variable selection) are applied on top.
        # We deliberately pass startdate/enddate/level=None so the base post-selection (.sel)
        # is skipped: GSV already selected dates and levels inside the request above.
        data = super()._postprocess_data(
            data=data,
            var=var,
            level=None,
            level_coord=level_coord,
            startdate=None,
            enddate=None,
        )

        return data

    def _resolve_loadvar(self, var: str | list = None):
        """
        Resolve the requested variable(s) into the paramid list to send to FDB.

        If ``var`` is None the catalog default (metadata 'variables', falling back to the request
        'param') is used.

        TODO: port the full var<->paramid matching from the legacy Reader.reader_fdb. That logic
        handles: integer paramids, short-name -> fixer 'source' lookup, derived variables, and the
        eccodes fallback. It is intentionally left as a stub here to keep the skeleton focused.
        """
        if self.fdb_var is not None:
            fdb_var = to_list(self.fdb_var)
        else:
            self.logger.warning("No 'variables' metadata defined in the catalog, this is deprecated!")
            fdb_var = to_list(self.request.get("param"))

        if not var:
            return fdb_var

        # Skeleton fallback: pass the requested variables straight through.
        # Replace with the matching logic from reader_fdb (var_match construction).
        return to_list(var)

    # The three selection helpers below mirror BackendIntakeXarray (delegate to the base
    # implementation) so the interface stays uniform across intake backends. In the FDB flow
    # date/level selection is pushed into the GSV request, so retrieve() does not invoke
    # _seldate/_sellevel; they remain available and correct if called directly on FDB data.
    def _seldate(self, data: xr.Dataset, startdate: str = None, enddate: str = None):
        return super()._seldate(data=data, startdate=startdate, enddate=enddate)

    def _sellevel(self, data: xr.Dataset, level: str | list = None, level_coord: str = None):
        return super()._sellevel(data=data, level=level, level_coord=level_coord)

    def _selvar(self, data: xr.Dataset, var: str | list = None):
        return super()._selvar(data=data, var=var)
