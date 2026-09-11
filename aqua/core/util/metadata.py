"""Metadata classes for AQUA components."""

import hashlib
from pathlib import Path

from aqua.core.default import DEFAULT_WEIGHTS_AREAS_PARAMETERS


class RegridderMetadata:
    """Immutable metadata for regridder filename templating.

    Supports both catalog-based (model/exp/source) and path-based workflows.
    Extra parameters are determined by DEFAULT_WEIGHTS_AREAS_PARAMETERS.

    Attributes:
        model: Model name (e.g., "IFS", "FESOM")
        exp: Experiment name (e.g., "test-tco79", "historical-1990")
        source: Source name (e.g., "short", "monthly")
        path: File path for path-based backend (e.g., "/data/output.nc")
        Additional attributes from DEFAULT_WEIGHTS_AREAS_PARAMETERS (e.g., zoom)

    Examples:
        >>> # Catalog-based
        >>> meta = RegridderMetadata(model="IFS", exp="test", source="short")
        >>> meta.is_catalog_based()
        True
        >>> meta.get_filename_base()
        'IFS_test_short'

        >>> # Path-based
        >>> meta = RegridderMetadata(path="/data/file.nc")
        >>> meta.is_path_based()
        True
        >>> meta.get_path_identifier()
        'a3f4c8b2'
    """

    def __init__(self, model=None, exp=None, source=None, path=None, **extra_params):
        """Initialize RegridderMetadata.

        Args:
            model: Model name (or None for path-based)
            exp: Experiment name (or None for path-based)
            source: Source name (or None for path-based)
            path: File path (or None for catalog-based)
            **extra_params: Additional parameters from DEFAULT_WEIGHTS_AREAS_PARAMETERS
        """
        self.model = model
        self.exp = exp
        self.source = source
        self.path = path

        # Store extra parameters directly as attributes
        for param in DEFAULT_WEIGHTS_AREAS_PARAMETERS:
            setattr(self, param, extra_params.get(param))

    def is_catalog_based(self):
        """Check if this is catalog-based (has complete model/exp/source triplet).

        Returns:
            True if model, exp, and source are all provided.
        """
        return all([self.model, self.exp, self.source]) and self.path is None

    def is_path_based(self):
        """Check if this is path-based (has path but no catalog triplet).

        Returns:
            True if path is provided and catalog triplet is incomplete.
        """
        return self.path is not None and not self.is_catalog_based()

    def get_path_identifier(self, length=8):
        """Generate stable identifier from path for filename templating.

        Uses SHA256 hash of normalized path to create a reproducible identifier.
        The same path will always generate the same identifier across runs.

        Args:
            length: Length of hash to use (default 8 characters)

        Returns:
            Stable identifier string (e.g., "a3f4c8b2")

        Raises:
            ValueError: If path is None

        Examples:
            >>> meta = RegridderMetadata(path="/data/output.nc")
            >>> meta.get_path_identifier()
            'a3f4c8b2'
            >>> meta.get_path_identifier(length=16)
            'a3f4c8b2e1d7f9a0'
        """
        if not self.path:
            raise ValueError("Cannot generate path identifier: path is None")

        # Normalize path for consistent hashing across different representations
        normalized = Path(self.path).resolve().as_posix()

        # Use SHA256 for cryptographic stability
        hash_obj = hashlib.sha256(normalized.encode("utf-8"))
        return hash_obj.hexdigest()[:length]

    def to_template_dict(self):
        """Return dict for template.format(), excluding None values.

        Args:
            use_path_fallback: If True and catalog info missing,
                              use path identifier for model/exp/source

        Returns:
            Dict with template variables (non-None values only)
        """
        # Loop through all attributes, exclude path and private ones
        if self.is_catalog_based():
            return {k: v for k, v in self.__dict__.items() if not k.startswith("_") and k != "path" and v is not None}
        raise ValueError("Cannot generate template dict: catalog triplet is missing. Use path-based metadata instead.")

    @classmethod
    def from_reader(cls, model, exp, source, path, **kwargs):
        """Factory method to create RegridderMetadata from Reader parameters.

        Dynamically extracts parameters from kwargs based on DEFAULT_WEIGHTS_AREAS_PARAMETERS.

        Args:
            model: Model name (or None for path-based)
            exp: Experiment name (or None for path-based)
            source: Source name (or None for path-based)
            path: File path (or None for catalog-based)
            **kwargs: Additional params (e.g., zoom)

        Returns:
            RegridderMetadata instance
        """
        # Extract only parameters defined in DEFAULT_WEIGHTS_AREAS_PARAMETERS
        extra_params = {param: kwargs[param] for param in DEFAULT_WEIGHTS_AREAS_PARAMETERS if param in kwargs}

        return cls(model=model, exp=exp, source=source, path=path, **extra_params)
