"""Functional entry point for FDB/GSV access through intake.

``open_gsv`` is the stable, intake2-friendly way to open GSV data as a lazy,
dask-enabled xarray dataset. It is used both by the intake ``gsv`` driver
(:mod:`aqua.core.gsv.readers`) and directly (e.g. from tests or notebooks without a
catalog). The heavy lifting lives in :class:`aqua.core.gsv.gsv_source.GSVSource`,
which builds on the backend-agnostic :class:`aqua.core.gsv.partitioned.FDBPartitionedSource`.

``GSVSource``, ``gsv_available`` and ``log_history`` are re-exported here for backward
compatibility with the previous single-module layout.
"""

from .gsv_source import GSVSource, gsv_available  # noqa: F401  (re-exported for back-compat)
from .partitioned import log_history  # noqa: F401  (re-exported for back-compat)


def open_gsv(
    request,
    *args,
    **kwargs,
):
    """
    Open GSV data as a dask-enabled xarray dataset directly.

    This function wraps the GSVSource intake driver, allowing it to be used
    as a standalone function. This is useful for direct testing, calling from
    other code without a catalog, and for adaptation to Intake 2.

    Args:
        request (dict): Request dictionary
        data_start_date (str): Start date of the available data.
        data_end_date (str): End date of the available data.
        bridge_start_date (str, optional): Start date of the bridge data. Defaults to None.
        bridge_end_date (str, optional): End date of the bridge data. Defaults to None.
        hpc_expver (str, optional): Alternative expver to be used if the data are on hpc
        timestyle (str, optional): Time style. Defaults to "date".
        chunks (str or dict, optional): Time and vertical chunking. Defaults to "S".
        savefreq (str, optional): Data saving frequency. Defaults to "h".
        timestep (str, optional): Time step. Defaults to "h".
        timeshift (str, optional): Time shift. Defaults to None.
        startdate (str, optional): Start date for request. Defaults to None.
        enddate (str, optional): End date for request. Defaults to None.
        var (str, optional): Variable ID. Defaults to those in the catalog.
        metadata (dict, optional): Metadata containing paths to FDB.
        level (int, float, list, optional): level(s) to be read.
        switch_eccodes (bool, optional): Flag to activate switching of eccodes path. Defaults to False.
        loglevel (str, optional): The loglevel. Defaults to "WARNING".
        engine (str, optional): Engine to be used for GSV retrieval: 'polytope' or 'fdb'. Defaults to None.
        databridge (str, optional): Databridge to be used. Defaults to None.
        kwargs: other keyword arguments.

    Returns:
        xr.Dataset: A lazy dask-enabled xarray dataset.
    """

    source = GSVSource(
        request=request,
        *args,
        **kwargs,
    )
    return source.to_dask()
