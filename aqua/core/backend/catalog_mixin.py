"""Mixin providing intake catalog setup for concrete backend classes."""

from aqua.core.configurer import ConfigCatalog


class CatalogMixin:
    """
    Mixin that handles all intake catalog setup: catalog discovery, esmcat
    construction, kwargs filtering, and realization formatting.

    Must be combined with :class:`Backend` (which provides ``self.logger``).
    Call :meth:`setup_catalog` explicitly in the concrete ``__init__`` *after*
    ``Backend.__init__`` so that ``self.logger`` is available.

    Example usage::

        class MyBackend(Backend, CatalogMixin):
            def __init__(self, model, exp, source, configurer, ...):
                Backend.__init__(self, fixer=fixer, datamodel=datamodel, loglevel=loglevel)
                self.setup_catalog(model, exp, source, configurer, catalog, chunks, **kwargs)
                # backend-specific setup ...
    """

    def setup_catalog(
        self,
        model: str,
        exp: str,
        source: str,
        configurer_catalog: ConfigCatalog,
        catalog: str = None,
        chunks=None,
        **kwargs,
    ):
        """
        Open the intake catalog, build the esmcat source entry, and filter kwargs.

        Populates ``self.model``, ``self.exp``, ``self.source``, ``self.chunks``,
        ``self.cat``, ``self.catalog_file``, ``self.machine_file``, ``self.catalog``,
        ``self.machine``, ``self.expcat``, ``self.esmcat``, and ``self.kwargs``.

        Args:
            model (str): Model name.
            exp (str): Experiment name.
            source (str): Data source identifier.
            configurer_catalog (ConfigCatalog): Manages catalog and machine configuration paths.
            catalog (str, optional): Catalog name. Defaults to None (auto-detect).
            chunks (str | dict, optional): Chunking strategy forwarded to the source. Defaults to None.
            **kwargs: Additional intake parameters forwarded to the catalog source entry.
        """
        self.model = model
        self.exp = exp
        self.source = source
        self.chunks = chunks

        self.cat, self.catalog_file, self.machine_file = configurer_catalog.deliver_intake_catalog(
            catalog=catalog, model=model, exp=exp, source=source
        )
        self.catalog = self.cat.name
        self.machine = configurer_catalog.paths.get_machine()
        machine_paths, intake_vars = configurer_catalog.get_machine_info()
        self.expcat = self.cat(**intake_vars)[self.model][self.exp]

        # First plain instantiation — needed to read _entry._user_parameters for _filter_kwargs.
        self.esmcat = self.expcat[self.source]()

        self.kwargs = self._filter_kwargs(kwargs, intake_vars=intake_vars)
        self.kwargs = self._format_realization_reader_kwargs(self.kwargs)
        self.logger.debug("Using filtered kwargs: %s", self.kwargs)

        # HACK for intake2 following https://github.com/intake/intake-xarray/issues/150
        # Rebuild esmcat with the filtered (and possibly realization-formatted) kwargs.
        self.esmcat = self.expcat._entries[self.source](**self.kwargs)

    @property
    def intake_user_parameters(self):
        """Lazy loader for intake user parameters to avoid expensive describe() calls."""
        if not hasattr(self, "_intake_user_parameters"):
            self._intake_user_parameters = [v.describe() for v in self.esmcat._entry._user_parameters]  # intake2
            self.logger.debug("Intake user parameters: %s", self._intake_user_parameters)
        return self._intake_user_parameters

    @property
    def metadata(self):
        """Uniform metadata accessor - returns metadata dict safely.

        Falls back to reader.metadata if esmcat.metadata is not available.
        """
        metadata = getattr(self.esmcat, "metadata", None)
        if metadata is not None:
            return metadata
        # Fallback to reader.metadata if available
        try:
            return self.esmcat.reader.metadata
        except (AttributeError, KeyError):
            return {}

    def _get_xarray_kwargs_from_catalog(self):
        """Safely extract xarray_kwargs from catalog definition.

        Encapsulates intake2 hack: self.esmcat._entry._captured_init_kwargs
        Only used by BackendIntakeXarray.
        """
        try:
            return self.esmcat._entry._captured_init_kwargs.get("args", {}).get("xarray_kwargs", {})
        except (AttributeError, KeyError, TypeError):
            return {}

    def _get_source_urls(self):
        """Safely extract URL list from netcdf/zarr source.

        Encapsulates intake2 hack: self.esmcat.reader.kwargs["args"][0]
        Only used by BackendIntakeXarray for glob expansion.
        """
        try:
            return self.esmcat.reader.kwargs["args"][0]
        except (AttributeError, KeyError, IndexError, TypeError):
            return []

    def _filter_kwargs(self, kwargs: dict = {}, intake_vars: dict = {}) -> dict:
        """
        Filter kwargs to only include parameters defined in the intake source.

        Uses the esmcat user parameters to determine valid keys. Kwargs absent
        from the source definition are dropped with a warning. Missing required
        parameters receive their catalog default value.

        Args:
            kwargs (dict): Keyword arguments passed by the caller (intake source parameters).
            intake_vars (dict): Machine-specific intake variables excluded from validation.

        Returns:
            dict: Filtered kwargs safe to pass to the intake source entry.
        """
        if intake_vars is None:
            intake_vars = {}

        filtered_kwargs = {}

        param_defs = {p["name"]: p for p in self.intake_user_parameters}
        valid_params = set(param_defs.keys())

        if kwargs:
            filtered_kwargs = {k: v for k, v in kwargs.items() if k in valid_params}
            dropped_keys = kwargs.keys() - valid_params
            for key in dropped_keys:
                self.logger.warning("kwarg %s is not in the intake parameters of the source, removing it", key)

        # HACK: Keep chunking info if present as reader kwarg
        if self.chunks is not None:
            self.logger.warning("Keeping chunks=%s in the filtered kwargs", self.chunks)
            filtered_kwargs["chunks"] = self.chunks

        covered_params = set(filtered_kwargs) | set(intake_vars)
        missing_params = valid_params - covered_params

        for param in missing_params:
            element = param_defs[param]
            default_val = element.get("default")
            self.logger.info("%s parameter is required but is missing, setting to default %s", param, default_val)
            allowed = element.get("allowed", None)
            if allowed is not None:
                self.logger.info("Available values for %s are: %s", param, allowed)
            filtered_kwargs[param] = default_val

        return filtered_kwargs

    def _format_realization_reader_kwargs(self, kwargs: dict):
        """
        Reformat the realization string for intake access.

        Converts ``rXX`` format to int when the intake source expects an integer,
        and removes the key entirely when it is not a recognised parameter.

        Args:
            kwargs (dict): Filtered kwargs, potentially containing a ``realization`` key.

        Returns:
            dict: kwargs with realization cast to the correct type.
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

        if realization_type == "str":
            self.logger.debug("realization parameter is of type string, will use it as is: %s", str(realization))
            kwargs["realization"] = str(realization)
            return kwargs

        if realization_type == "int":
            if isinstance(realization, str) and realization.startswith("r") and realization[1:].isdigit():
                kwargs["realization"] = int(realization[1:])
                self.logger.info("realization parameter converted from rXXX format to int: %d", kwargs["realization"])
                return kwargs
            if isinstance(realization, int):
                return kwargs

        raise ValueError(f"Realization {kwargs['realization']} format not recognized for type {realization_type}")
