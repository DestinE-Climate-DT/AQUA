"""Backend for opening xarray datasets from STAC catalogs with pystac.

Example:
    Open a STAC asset through the public :class:`aqua.Reader` interface::

        # walk catalog -> collection -> item, default asset ('data' or the first one)
        reader = Reader(url="https://example.org/catalog.json", collection="ifs", item="item-id", areas=False)
        # or select the asset with a nested mapping
        reader = Reader(
            url="https://example.org/catalog.json",
            stac_kwargs={"collection": {"item": "asset"}},
            areas=False,
        )
        data = reader.retrieve()
"""

import inspect

import pystac
import xarray as xr

from aqua.core.data_model import DataModel
from aqua.core.fixer import Fixer
from aqua.core.intake_drivers.stac import select_asset_key
from aqua.core.logger import log_history
from aqua.core.version import __version__ as aqua_version

from .backend import Backend


class BackendSTAC(Backend):
    """Retrieve an xarray dataset from an asset of a STAC catalog, collection or item."""

    def __init__(
        self,
        url: str,
        stac_kwargs: dict | str = None,
        model: str = None,
        exp: str = None,
        source: str = None,
        catalog: str = None,
        chunks: str | dict = "auto",
        fixer: Fixer = None,
        datamodel: DataModel = None,
        loglevel: str = "WARNING",
        **kwargs,
    ):
        """Initialize the STAC backend.

        Args:
            url (str): URL or path of the STAC JSON document (catalog, collection or item).
            stac_kwargs (dict | str, optional): Single-branch nested mapping of child/item ids ending with the
                asset key, for example ``{"collection": {"item": "asset"}}``. A plain string selects
                the asset of an item (or a collection-level asset). If None, the asset is selected
                with model/exp/source (collection/item/asset). Defaults to None.
            model (str, optional): Collection id. Selects the collection when ``stac_kwargs`` is None,
                otherwise it is only a label. Defaults to the collection of the selected asset.
            exp (str, optional): Item id. Selects the item when ``stac_kwargs`` is None,
                otherwise it is only a label. Defaults to the item of the selected asset.
            source (str, optional): Asset key or role. Selects the asset when ``stac_kwargs`` is None,
                otherwise it is only a label. If no asset is given, 'data' or the first asset is used.
            catalog (str, optional): Label for the catalog. Defaults to the id of the catalog at ``url``.
            chunks (str | dict, optional): Chunking passed to ``xarray.open_dataset``.
                ``None`` falls back to ``"auto"`` so STAC assets remain lazy. Defaults
                to ``"auto"``.
            fixer (Fixer, optional): Fixer applied after reading. Defaults to None.
            datamodel (DataModel, optional): Data model applied after the fixer.
                Defaults to None.
            loglevel (str, optional): Logging level. Defaults to "WARNING".
            **kwargs: Additional keyword arguments passed to ``xarray.open_dataset``.
        """
        super().__init__(fixer=fixer, datamodel=datamodel, loglevel=loglevel)
        self.url = url
        self.stac_kwargs = stac_kwargs
        self.labels = {"catalog": catalog, "model": model, "exp": exp, "source": source}
        self.read_kwargs = kwargs
        self.read_kwargs["chunks"] = "auto" if chunks is None else chunks

    def retrieve_plain(self, startdate: str = None) -> xr.Dataset:
        """Retrieve a minimal sample from the selected STAC asset."""
        _, _, asset = self._get_asset()
        data = self._open_asset(asset)
        return self._select_minimum_sample(data, startdate=startdate)

    def retrieve(
        self,
        var: str | list = None,
        level: str | list = None,
        level_coord: str = None,
        startdate: str = None,
        enddate: str = None,
    ) -> xr.Dataset:
        """Retrieve and post-process the selected STAC asset.

        Args:
            var (str | list, optional): Variable or variables to retrieve. Defaults to None.
            level (str | list, optional): Vertical levels to select. Defaults to None.
            level_coord (str, optional): Vertical coordinate name. Defaults to None.
            startdate (str, optional): Start date for selection. Defaults to None.
            enddate (str, optional): End date for selection. Defaults to None.

        Returns:
            xr.Dataset: Lazily opened and post-processed dataset, with ``AQUA_catalog``,
            ``AQUA_model``, ``AQUA_exp`` and ``AQUA_source`` set from the STAC hierarchy.
        """
        start, key, asset = self._get_asset()
        data = self._open_asset(asset)
        data = self._postprocess_data(
            data=data,
            var=var,
            level=level,
            level_coord=level_coord,
            startdate=startdate,
            enddate=enddate,
        )
        labels = self._get_labels(start, key, asset)
        data = self.log_history(data, selection=" ".join(labels[k] for k in ("model", "exp", "source") if labels[k]))
        metadata = {**labels, "url": self.url, "stac_kwargs": self.stac_kwargs, "version": aqua_version}
        return self._set_metadata(data, {k: v for k, v in metadata.items() if v is not None})

    def log_history(self, data: xr.Dataset, selection: str = None) -> xr.Dataset:
        """Add STAC retrieval provenance (the selected model, exp and source) to the dataset history."""
        selection = selection or self.stac_kwargs
        return log_history(data, f"Retrieved {selection} from {self.url} using AQUA v{aqua_version} with STAC Backend")

    def _get_labels(self, start, key: str, asset: pystac.Asset) -> dict:
        """Map the STAC catalog/collection/item/asset of the selected asset to catalog/model/exp/source.
        Labels given by the user take priority."""
        owner = asset.owner
        is_item = isinstance(owner, pystac.Item)
        derived = {
            # only the catalog read from url, to avoid fetching the root of a collection or item
            "catalog": start.id if type(start) is pystac.Catalog else None,
            "model": owner.collection_id if is_item else owner.id,
            "exp": owner.id if is_item else None,
            "source": key,
        }
        return {k: self.labels[k] if self.labels[k] is not None else v for k, v in derived.items()}

    def _split_selection(self) -> tuple[list, str | None]:
        """Return the child/item ids to walk and the asset to select, from stac_kwargs or model/exp/source."""
        if self.stac_kwargs is None:
            return [k for k in (self.labels["model"], self.labels["exp"]) if k is not None], self.labels["source"]
        ids, spec = [], self.stac_kwargs
        while isinstance(spec, dict):
            if len(spec) != 1:
                raise ValueError(f"stac_kwargs must be a single-branch mapping, got {self.stac_kwargs}")
            ((key, spec),) = spec.items()
            ids.append(key)
        return ids, spec

    def _get_asset(self) -> tuple:
        """Walk the STAC tree from url and select the asset.

        Returns:
            tuple: The STAC object read from url, the asset key and the pystac.Asset.

        Raises:
            KeyError: If a child, item or asset is not found.
            ValueError: If the object reached has no assets (e.g. a catalog).
        """
        start = obj = pystac.read_file(self.url)
        ids, asset = self._split_selection()

        for key in ids:
            # the url can already point to the requested collection or item
            if key in (obj.id, getattr(obj, "collection_id", None)):
                continue
            if isinstance(obj, pystac.Item):
                raise KeyError(f"cannot descend into '{key}': '{obj.id}' is an Item")
            nxt = obj.get_child(key) or obj.get_item(key)
            if nxt is None:
                raise KeyError(f"'{key}' not found in '{obj.id}'")
            obj = nxt

        # Items and collections with their own assets
        assets = getattr(obj, "assets", None)
        if not assets:
            raise ValueError(f"'{obj.id}' has no assets: please select a collection and an item")
        key = select_asset_key({k: v.roles or [] for k, v in assets.items()}, asset, owner=obj.id)
        self.logger.info("Selected STAC asset '%s' of '%s'", key, obj.id)
        return start, key, assets[key]

    def _open_asset(self, asset: pystac.Asset) -> xr.Dataset:
        """Open the selected STAC asset as an xarray Dataset."""
        # backend kwargs (chunks, user kwargs) are defaults, overridden by the asset's own xarray:open_kwargs
        open_kwargs = {**self.read_kwargs, **self._asset_open_kwargs(asset)}
        storage_options = asset.extra_fields.get("xarray:storage_options")
        if storage_options is not None:
            open_kwargs["storage_options"] = storage_options

        # relative hrefs (e.g. local catalogs) are resolved against the STAC object they belong to
        return xr.open_dataset(asset.get_absolute_href() or asset.href, **open_kwargs)

    def _asset_open_kwargs(self, asset: pystac.Asset) -> dict:
        """Return the asset ``xarray:open_kwargs`` accepted by ``xarray.open_dataset`` and its engine.

        Some catalogs (e.g. DKRZ EERIE) put there options for opening another target than the href
        (``url``, ``root``, ``metadata`` and their ``storage_options``): these are dropped, and the
        storage options are taken only from ``xarray:storage_options``.
        """
        open_kwargs = asset.extra_fields.get("xarray:open_kwargs") or {}
        allowed = set(inspect.signature(xr.open_dataset).parameters)
        engine = open_kwargs.get("engine", self.read_kwargs.get("engine"))
        if isinstance(engine, str) and engine in xr.backends.list_engines():
            allowed |= set(inspect.signature(xr.backends.list_engines()[engine].open_dataset).parameters)
        allowed -= {"storage_options", "filename_or_obj", "kwargs"}
        dropped = [k for k in open_kwargs if k not in allowed]
        if dropped:
            self.logger.warning("Ignoring STAC asset xarray:open_kwargs %s, not accepted by xarray.open_dataset", dropped)
        return {k: v for k, v in open_kwargs.items() if k in allowed}
