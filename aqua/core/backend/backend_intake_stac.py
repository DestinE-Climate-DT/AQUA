"""Backend for opening xarray datasets from STAC catalogs with Intake.

Example:
    Open a STAC asset through the public :class:`aqua.Reader` interface::

        reader = Reader(
            path="https://example.org/catalog.json",
            stac={"collection": {"item": "asset"}},
            areas=False,
        )
        data = reader.retrieve()
"""

import intake
import xarray as xr

from aqua.core.data_model import DataModel
from aqua.core.fixer import Fixer
from aqua.core.logger import log_history
from aqua.core.version import __version__ as aqua_version

from .backend import Backend

xr.set_options(keep_attrs=True)


class BackendIntakeSTAC(Backend):
    """Retrieve an xarray dataset from an asset in an Intake STAC catalog."""

    def __init__(
        self,
        path: str,
        stac: dict,
        chunks: str | dict = "auto",
        fixer: Fixer = None,
        datamodel: DataModel = None,
        loglevel: str = "WARNING",
        **kwargs,
    ):
        """Initialize the Intake STAC backend.

        Args:
            path (str): URL or local path of the STAC catalog JSON document.
            stac (dict): Single-branch nested mapping describing successive catalog
                lookups, for example ``{"collection": {"item": "asset"}}``.
            chunks (str | dict, optional): Chunking passed to the selected asset reader.
                ``None`` falls back to ``"auto"`` so STAC assets remain lazy. Defaults
                to ``"auto"``.
            fixer (Fixer, optional): Fixer applied after reading. Defaults to None.
            datamodel (DataModel, optional): Data model applied after the fixer.
                Defaults to None.
            loglevel (str, optional): Logging level. Defaults to "WARNING".
            **kwargs: Additional keyword arguments passed to the selected asset reader.
        """
        super().__init__(fixer=fixer, datamodel=datamodel, loglevel=loglevel)
        self.path = path
        self.stac = stac
        self.catalog_path = self._parse_stac_path(stac)
        self.read_kwargs = kwargs
        self.read_kwargs["chunks"] = "auto" if chunks is None else chunks

    def retrieve_plain(self, startdate: str = None) -> xr.Dataset:
        """Retrieve a minimal sample from the selected STAC asset."""
        data = self._read_asset()
        data = self._select_minimum_sample(data, startdate=startdate)
        self._drop_nonserializable_sample_attrs(data)
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
                "path": self.path,
                "stac": "/".join(self.catalog_path),
                "version": aqua_version,
            },
        )

    def log_history(self, data: xr.Dataset) -> xr.Dataset:
        """Add STAC retrieval provenance to the dataset history."""
        selection = "/".join(self.catalog_path)
        return log_history(
            data,
            f"Retrieved {selection} from {self.path} using AQUA v{aqua_version} with Intake STAC",
        )

    def _read_asset(self) -> xr.Dataset:
        """Open the selected STAC asset as an xarray Dataset."""
        asset_reader = self._get_asset_reader()
        data = asset_reader.read(**self.read_kwargs)
        if not isinstance(data, xr.Dataset):
            raise TypeError(f"STAC asset {'/'.join(self.catalog_path)!r} did not produce an xarray.Dataset")
        return data

    def _get_asset_reader(self):
        """Build and cache the Intake reader for the selected STAC asset."""
        if not hasattr(self, "_asset_reader"):
            stac_data = intake.datatypes.STACJSON(self.path)
            catalog = intake.catalogs.StacCatalogReader(stac_data).read()
            selected = catalog
            for entry in self.catalog_path:
                selected = selected[entry]
            self._asset_reader = selected
        return self._asset_reader

    def _drop_nonserializable_sample_attrs(self, data: xr.Dataset):
        """Remove backend-only mapping attributes before grid sample serialization."""
        for variable in data.variables.values():
            mapping_attrs = [key for key, value in variable.attrs.items() if isinstance(value, dict)]
            for key in mapping_attrs:
                self.logger.debug("Removing non-serializable sample attribute %s", key)
                variable.attrs.pop(key)

    @staticmethod
    def _parse_stac_path(stac: dict) -> tuple[str, ...]:
        """Convert a single-branch nested STAC selection mapping to a tuple."""
        if not isinstance(stac, dict) or not stac:
            raise ValueError("stac must be a non-empty nested mapping")

        path = []
        node = stac
        while isinstance(node, dict):
            if len(node) != 1:
                raise ValueError("each level of stac must contain exactly one catalog entry")
            entry, node = next(iter(node.items()))
            if not isinstance(entry, str) or not entry:
                raise ValueError("stac catalog entry names must be non-empty strings")
            path.append(entry)

        if not isinstance(node, str) or not node:
            raise ValueError("the final stac catalog entry must be a non-empty string")
        path.append(node)
        return tuple(path)
