"""Backend for opening xarray datasets from STAC catalogs with Intake.

Example:
    Open a STAC asset through the public :class:`aqua.Reader` interface::

        reader = Reader(
            url="https://example.org/catalog.json",
            stac_kwargs={"collection": {"item": "asset"}},
            areas=False,
        )
        data = reader.retrieve()
"""

import pystac
import xarray as xr

from aqua.core.data_model import DataModel
from aqua.core.fixer import Fixer
from aqua.core.logger import log_history
from aqua.core.version import __version__ as aqua_version

from .backend import Backend

xr.set_options(keep_attrs=True)


class BackendSTAC(Backend):
    """Retrieve an xarray dataset from an asset in an Intake STAC catalog."""

    def __init__(
        self,
        url: str,
        stac_kwargs: dict | str,
        chunks: str | dict = "auto",
        fixer: Fixer = None,
        datamodel: DataModel = None,
        loglevel: str = "WARNING",
        **kwargs,
    ):
        """Initialize the Intake STAC backend.

        Args:
            url (str): URL of the STAC catalog JSON document.
            stac_kwargs (dict | str): Single-branch nested mapping describing successive catalog
                lookups, for example ``{"collection": {"item": "asset"}}``.
            chunks (str | dict, optional): Chunking passed to the selected asset reader.
                ``None`` falls back to ``"auto"`` so STAC assets remain lazy. Defaults
                to ``"auto"``.
            fixer (Fixer, optional): Fixer applied after reading. Defaults to None.
            datamodel (DataModel, optional): Data model applied after the fixer.
                Defaults to None.
            loglevel (str, optional): Logging level. Defaults to "WARNING".
            stac_kwargs (dict, optional): Additional keyword arguments passed to the selected asset reader.
        """
        super().__init__(fixer=fixer, datamodel=datamodel, loglevel=loglevel)
        self.url = url
        self.stac_kwargs = stac_kwargs
        self.read_kwargs = kwargs
        self.read_kwargs["chunks"] = "auto" if chunks is None else chunks

    def retrieve_plain(self, startdate: str = None) -> xr.Dataset:
        """Retrieve a minimal sample from the selected STAC asset."""
        data = self._read_asset()
        data = self._select_minimum_sample(data, startdate=startdate)
        return data

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
            xr.Dataset: Lazily opened and post-processed dataset.
        """
        data = self._read_asset()
        data = self._postprocess_data(
            data=data,
            var=var,
            level=level,
            level_coord=level_coord,
            startdate=startdate,
            enddate=enddate,
        )
        data = self.log_history(data)
        return self._set_metadata(
            data,
            {
                "url": self.url,
                "stac_kwargs": self.stac_kwargs,
                "version": aqua_version,
            },
        )

    def log_history(self, data: xr.Dataset) -> xr.Dataset:
        """Add STAC retrieval provenance to the dataset history."""
        return log_history(
            data,
            f"Retrieved {self.stac_kwargs} from {self.url} using AQUA v{aqua_version} with STAC Backend",
        )

    def _get_asset(self):
        """Follow the dict path (if any) and resolve the asset key on the object reached.
        Raises KeyError if not found, ValueError if ambiguous."""
        obj = pystac.read_file(self.url)
        spec = self.stac_kwargs  # don't mutate self

        # Dict: each key is a child or item id
        while isinstance(spec, dict):
            ((key, spec),) = spec.items()
            if isinstance(obj, pystac.Item):
                raise KeyError(f"cannot descend into '{key}': '{obj.id}' is an Item")
            nxt = obj.get_child(key) or obj.get_item(key)
            if nxt is None:
                raise KeyError(f"'{key}' not found in '{obj.id}'")
            obj = nxt

        # Leaf: string asset key
        if isinstance(obj, pystac.Item):
            if spec not in obj.assets:
                raise KeyError(f"asset '{spec}' not found in item '{obj.id}'. Available: {list(obj.assets)}")
            return obj.assets[spec]

        # Collection: its own assets take priority
        if isinstance(obj, pystac.Collection) and spec in obj.assets:
            return obj.assets[spec]

        raise ValueError(f"'{spec}' is not a valid asset key for '{obj.id}'")

    def _read_asset(self) -> xr.Dataset:
        """Open the selected STAC asset as an xarray Dataset."""

        asset = self._get_asset()
        if not isinstance(asset, pystac.Asset):
            raise TypeError(f"STAC asset {self.stac_kwargs} from {self.url} is not a pystac.Asset")

        data = xr.open_dataset(
            asset.href,
            **asset.extra_fields.get("xarray:open_kwargs", {}),
            storage_options=asset.extra_fields.get("xarray:storage_options"),
        )

        if not isinstance(data, xr.Dataset):
            raise TypeError(f"STAC asset {self.stac_kwargs} from {self.url} did not produce an xarray.Dataset")
        return data

    # def _read_asset(self) -> xr.Dataset:
    #     """Open the selected STAC asset as an xarray Dataset."""
    #     asset_reader = self._get_asset_reader()
    #     data = asset_reader.read(**self.read_kwargs)
    #     if not isinstance(data, xr.Dataset):
    #         raise TypeError(f"STAC asset {'/'.join(self.catalog_path)!r} did not produce an xarray.Dataset")
    #     return data

    # def _get_asset_reader(self):
    #     """Build and cache the Intake reader for the selected STAC asset."""
    #     if not hasattr(self, "_asset_reader"):
    #         stac_data = intake.datatypes.STACJSON(self.url)
    #         catalog = intake.catalogs.StacCatalogReader(stac_data).read()
    #         selected = catalog
    #         for entry in self.catalog_path:
    #             selected = selected[entry]
    #         self._asset_reader = selected
    #     return self._asset_reader

    # def _drop_nonserializable_sample_attrs(self, data: xr.Dataset):
    #     """Remove backend-only mapping attributes before grid sample serialization."""
    #     for variable in data.variables.values():
    #         mapping_attrs = [key for key, value in variable.attrs.items() if isinstance(value, dict)]
    #         for key in mapping_attrs:
    #             self.logger.debug("Removing non-serializable sample attribute %s", key)
    #             variable.attrs.pop(key)

    # @staticmethod
    # def _parse_stac_path(stac: dict) -> tuple[str, ...]:
    #     """Convert a single-branch nested STAC selection mapping to a tuple."""
    #     if not isinstance(stac, dict) or not stac:
    #         raise ValueError("stac must be a non-empty nested mapping")

    #     path = []
    #     node = stac
    #     while isinstance(node, dict):
    #         if len(node) != 1:
    #             raise ValueError("each level of stac must contain exactly one catalog entry")
    #         entry, node = next(iter(node.items()))
    #         if not isinstance(entry, str) or not entry:
    #             raise ValueError("stac catalog entry names must be non-empty strings")
    #         path.append(entry)

    #     if not isinstance(node, str) or not node:
    #         raise ValueError("the final stac catalog entry must be a non-empty string")
    #     path.append(node)
    #     return tuple(path)
