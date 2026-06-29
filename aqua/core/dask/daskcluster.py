"""Centralised manager for dask cluster in AQUA"""

import logging
import os

from dask.distributed import LocalCluster

from aqua.core.logger import log_configure


# TODO: expand and implement in DROP
class DaskCluster:
    """Manages the lifecycle of a Dask LocalCluster for parallel diagnostic execution."""

    def __init__(self, loglevel="WARNING"):
        self.loglevel = loglevel
        self.logger = log_configure(log_level=loglevel, log_name="ClusterManager")
        self._cluster = None
        self._address = None

    @property
    def address(self):
        """Return the scheduler address, or None if the cluster is not running."""
        return self._address

    @property
    def active(self):
        """Return True if the cluster is active."""
        return self._cluster is not None

    def setup(self, nworkers: int, nthreads: int, mem_limit: str, connect_timeout: float = None, tcp_timeout: float = None):
        """
        Spin up a LocalCluster with the specified configuration if not already running.

        """
        if self.active:
            self.logger.warning("Cluster already running at %s, skipping reconfiguration.", self._address)
            return

        self.logger.debug(
            "Cluster configuration — nthreads: %d, nworkers: %d, memory_limit: %s",
            nthreads,
            nworkers,
            mem_limit,
        )

        self._cluster = LocalCluster(
            threads_per_worker=nthreads,
            n_workers=nworkers,
            memory_limit=mem_limit,
            silence_logs=logging.ERROR,
        )
        self._address = self._cluster.scheduler_address
        self.logger.info(
            "Initialized dask cluster at %s with %d workers.",
            self._address,
            len(self._cluster.workers),
        )

        self._configure_timeouts(connect_timeout=connect_timeout, tcp_timeout=tcp_timeout)

    def close(self):
        """Shut down the cluster if running. Safe to call even if never started."""
        if not self.active:
            self.logger.debug("No active cluster to close.")
            return

        self.logger.info("Closing dask cluster at %s.", self._address)
        self._cluster.close()
        self._cluster = None
        self._address = None

    def _configure_timeouts(self, connect_timeout: float = None, tcp_timeout: float = None):
        """Set Dask timeout environment variables if not already set.

        Args:
            cluster_config (dict): Cluster configuration dictionary loaded from YAML.
        """
        timeouts = {
            "DASK_DISTRIBUTED__COMM__TIMEOUTS__CONNECT": connect_timeout,
            "DASK_DISTRIBUTED__COMM__TIMEOUTS__TCP": tcp_timeout,
        }
        for env_var, value in timeouts.items():
            if value and env_var not in os.environ:
                os.environ[env_var] = f"{value}s"
                self.logger.debug("Set %s to %s", env_var, os.environ[env_var])

    # this can be used for with statement if needed in the future
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False  # do not suppress exceptions
