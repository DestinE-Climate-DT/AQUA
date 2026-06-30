"""Backend realization using the intake 'gsv' driver (FDB/GSV) for data handling."""

from aqua.core.configurer import ConfigPath
from aqua.core.data_model import DataModel
from aqua.core.fixer import Fixer
from aqua.core.util import to_list

from .backend import Backend
from .catalog_mixin import CatalogMixin


class BackendIntakeFDB(Backend, CatalogMixin):
    """
    Concrete backend retrieving data from FDB/GSV through the intake ``gsv`` driver.

    The underlying source is an :class:`aqua.core.gsv.GSVSource`. Unlike the xarray-based
    backends, date and level selection are pushed *down into the GSV request* (handled by
    ``GSVSource`` at read time), so they must NOT be re-applied with ``.sel()`` afterwards.
    Only variable fixes and the data model are applied on top of the retrieved dataset.

    This mirrors the legacy ``Reader.reader_fdb`` flow, see ``aqua/core/reader/reader.py``.

    Example usage::

        backend = BackendIntakeFDB(
            model="IFS",
            exp="test",
            source="long",
            configurer=configurer,
            engine="fdb",
        )
        data = backend.retrieve(var="2t", startdate="2020-01-01", enddate="2020-01-31")
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
        Backend.__init__(self, fixer=fixer, datamodel=datamodel, loglevel=loglevel)
        self.loglevel = loglevel
        self.engine = engine
        self.databridge = databridge

        self.setup_catalog(model, exp, source, configurer, catalog, chunks, **kwargs)

        # Check machine compatibility
        self.machine_from_catalog = self.expcat.metadata.get("machine")
        if engine != "polytope":
            if self.machine_from_catalog and self.machine_from_catalog.lower() != self.machine.lower():
                self.logger.warning(
                    "The machine configured (%s) is different from the machine in the catalog (%s). "
                    "Please check that the data you are looking for are on the machine you are working on.",
                    self.machine.lower(),
                    self.machine_from_catalog.lower(),
                )

        # Inject engine and databridge into kwargs for GSV/FDB sources.
        # CatalogMixin._filter_kwargs is source-agnostic and does not add these;
        # we mirror the legacy Reader._filter_kwargs GSV logic here.
        # Use the catalog 'machine' metadata as the polytope databridge target when
        # the caller has not supplied one explicitly (mirrors Reader.machine_from_catalog).
        needs_rebuild = False
        if "engine" not in self.kwargs:
            self.kwargs["engine"] = engine
            self.logger.debug("Adding engine=%s to filtered kwargs", engine)
            needs_rebuild = True
        effective_databridge = databridge if databridge is not None else self.expcat.metadata.get("machine")
        if engine == "polytope" and effective_databridge is not None and "databridge" not in self.kwargs:
            self.kwargs["databridge"] = effective_databridge
            self.logger.debug("Adding databridge=%s to filtered kwargs", effective_databridge)
            needs_rebuild = True
        if needs_rebuild:
            self.esmcat = self.expcat._entries[self.source](**self.kwargs)

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

        Args:
            var (str | list, optional): Variable(s) to retrieve. Defaults to None (all catalog variables).
            level (str | list, optional): Level(s) to select. Defaults to None.
            level_coord (str, optional): Name of the vertical coordinate. Defaults to None.
            startdate (str, optional): Start date (YYYY-MM-DD). Defaults to None.
            enddate (str, optional): End date (YYYY-MM-DD). Defaults to None.

        Returns:
            xr.Dataset: Lazy dask-backed dataset with fixes and data model applied.
        """
        loadvar = self._resolve_loadvar(var)
        level = to_list(level) if level else None

        # Re-instantiate the source from the catalog entry with the retrieve-specific
        # parameters merged on top of the base kwargs (engine, databridge, …).
        # self.esmcat is an already-instantiated source whose __call__ returns self unchanged,
        # so we must go back through the entry to apply startdate/enddate/var/level.
        retrieve_kwargs = {
            "startdate": startdate,
            "enddate": enddate,
            "var": loadvar,
            "level": level,
            "loglevel": self.loglevel,
        }
        # Only override the catalog chunking when the user actually specified it: passing
        # chunks=None would clobber the source default (e.g. 'chunks: h') and break GSVSource.
        if self.chunks is not None:
            retrieve_kwargs["chunks"] = self.chunks

        source = self.expcat._entries[self.source](**{**self.kwargs, **retrieve_kwargs})
        data = source.to_dask()

        # Date/level selection was already baked into the GSV source at construction time;
        # pass None so _postprocess_data does not re-apply .sel() on top.
        data = self._postprocess_data(
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

        When ``var`` is provided, each element is matched against the catalog variable list
        (``fdb_var``) using the Fixer ``vars`` block to translate user-facing names (e.g. ``tos``)
        to the names/paramids actually stored in FDB (e.g. ``263101`` / ``avg_tos``).  This mirrors
        the matching logic in :meth:`aqua.core.reader.Reader.reader_fdb`.

        Args:
            var (str | list, optional): Requested variable name(s) or paramid(s). Defaults to None.

        Returns:
            list: Paramid/variable list to forward to the GSV source.
        """
        if self.fdb_var is not None:
            fdb_var = to_list(self.fdb_var)
        else:
            self.logger.warning("No 'variables' metadata defined in the catalog, this is deprecated!")
            fdb_var = to_list(self.esmcat._entry._open_args["request"]["param"])

        if not var:
            return fdb_var

        var = to_list(var)

        # Short-circuit: nothing to resolve when the request already matches the catalog list.
        if var == fdb_var:
            return var

        # Build the fixer vars dict once.  An absent fixer or missing 'vars' block means we fall
        # back to passing variable names directly to ecCodes (same behaviour as fix=False in
        # the legacy reader_fdb).
        fixer_dict = {}
        if self.fixer is not None and self.fixer.fixes is not None:
            fixer_dict = self.fixer.fixes.get("vars", {})
            if not fixer_dict:
                self.logger.debug("No 'vars' block in the fixer, guessing variable names based on ecCodes")

        var_match = []
        for element in var:
            # Integer paramid or a numeric string — intersect directly with fdb_var.
            if isinstance(element, int) or (isinstance(element, str) and element.isdigit()):
                element = int(element)
                match = list(set(fdb_var) & {element})
                if len(match) == 1:
                    var_match.append(match[0])
                elif len(match) > 1:
                    self.logger.warning("Multiple matches found for %s, using the first one", element)
                    var_match.append(match[0])
                else:
                    self.logger.warning("No match found for %s, skipping it", element)
            elif isinstance(element, str):
                if self.fixer is not None and element in fixer_dict:
                    src_element = fixer_dict[element].get("source", None)
                    derived_element = fixer_dict[element].get("derived", None)
                    if derived_element is not None or src_element is None:
                        # Derived variable — let ecCodes / the fixer handle it at post-process time.
                        var_match.append(derived_element)
                    else:
                        match = list(set(fdb_var) & set(src_element))
                        if len(match) == 1:
                            var_match.append(match[0])
                        elif len(match) > 1:
                            self.logger.warning("Multiple paramids found for %s: %s, using: %s", element, match, match[0])
                            var_match.append(match[0])
                        else:
                            self.logger.warning("No match found for %s, using eccodes to find the paramid", element)
                            var_match.append(element)
                else:
                    # No fixer or variable not in the fixer dict — pass through and let ecCodes resolve.
                    var_match.append(element)
            elif isinstance(element, list):
                if self.fixer is None:
                    self.logger.error("Var %s is a list and fixer is None, skipping it", element)
                    continue
                match = list(set(fdb_var) & set(element))
                if len(match) == 1:
                    var_match.append(match[0])
                elif len(match) > 1:
                    self.logger.warning("Multiple matches found for %s, using the first one", element)
                    var_match.append(match[0])
                else:
                    self.logger.error("No match found for %s, skipping it", element)
            else:
                self.logger.error("Element %s is not a valid type, skipping it", element)

        if not var_match:
            self.logger.error("No match found for the variables you are asking for!")
            self.logger.error("Please be sure the metadata 'variables' is defined in the catalog")
            return var

        self.logger.debug("Found variables: %s", var_match)
        return var_match
