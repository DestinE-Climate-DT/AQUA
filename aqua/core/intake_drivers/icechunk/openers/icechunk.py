import xarray as xr
from icechunk import Repository, local_filesystem_storage


def open_icechunk(
    url: str,
    branch: str = "main",
    chunks: "str | dict" = "auto",
    xarray_kwargs: dict = None,
    **kwargs,
) -> xr.Dataset:
    """
    Open an IceChunk Zarr repository as a lazy dask-enabled xarray dataset.

    Args:
        url (str): Path to the IceChunk Zarr repository.
        branch (str, optional): Branch of the IceChunk repository to open.
            Defaults to "main".
        chunks (str | dict, optional): Chunking strategy forwarded to
            ``xr.open_zarr``. Defaults to "auto".
        xarray_kwargs (dict, optional): Additional keyword arguments
            forwarded verbatim to ``xr.open_zarr``. Defaults to None.
    """
    storage = local_filesystem_storage(url)
    repo = Repository.open(storage)
    session = repo.readonly_session(branch)

    return xr.open_zarr(
        session.store,
        consolidated=False,
        chunks=chunks,
        **(xarray_kwargs or {}),
    )
