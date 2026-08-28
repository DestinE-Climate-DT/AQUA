"""Centralised manager for dask cluster in AQUA"""

import logging
import os

import dask
from dask.distributed import Client, LocalCluster

from aqua.core.logger import log_configure


class DaskCluster:
    """Manages the lifecycle of a Dask LocalCluster for parallel diagnostic execution."""

    def __init__(self, loglevel: str = "WARNING"):
        self.loglevel = loglevel
        self.logger = log_configure(log_level=loglevel, log_name="ClusterManager")
        self._cluster = None
        self._client = None

    @property
    def address(self):
        """Return the scheduler address, or None if the cluster is not running."""
        return self._cluster.scheduler_address if self.cluster_active else None

    @property
    def cluster_active(self):
        """Return True if the cluster is active."""
        return self._cluster is not None

    @property
    def client_active(self):
        """Return True if the client is active."""
        return self._client is not None

    @property
    def client(self):
        """Return the client instance, or None if not activated."""
        return self._client

    def setup(
        self,
        nworkers: int,
        nthreads: int,
        mem_limit: str = None,
        tmpdir: str = None,
        connect_timeout: float = None,
        tcp_timeout: float = None,
        **kwargs,
    ):
        """
        Setup a LocalCluster with the specified configuration if not already running.
        Does not activate the client - call activate_client() explicitly if needed.

        Args:
            nworkers (int): Number of dask workers to start.
            nthreads (int): Number of dask threads per worker.
            mem_limit (str): Memory limit per worker.
            tmpdir (str, optional): Temporary directory for Dask worker files.
            connect_timeout (float, optional): Connection timeout for Dask communications.
            tcp_timeout (float, optional): TCP timeout for Dask communications.

        """
        if self.cluster_active:
            self.logger.warning("Cluster already running at %s, skipping reconfiguration.", self.address)
            return

        self.logger.debug(
            "Cluster configuration — nthreads: %d, nworkers: %d, memory_limit: %s, tmpdir: %s",
            nthreads,
            nworkers,
            mem_limit,
            tmpdir,
        )

        # Configure Dask temporary directory if provided
        if tmpdir:
            dask.config.set({"temporary_directory": tmpdir})
            self.logger.info("Dask temporary directory set to: %s", tmpdir)

        # configure environment variables for Dask timeouts if provided
        # TODO: this permanently sets environment variables, which may not be ideal for all use cases.
        # Consider a more flexible approach in the future.
        self._configure_timeouts(connect_timeout=connect_timeout, tcp_timeout=tcp_timeout)

        # spinup cluster
        self._cluster = LocalCluster(
            threads_per_worker=nthreads, n_workers=nworkers, memory_limit=mem_limit, silence_logs=logging.ERROR, **kwargs
        )
        self.logger.info(
            "Initialized dask cluster at %s with %d workers.",
            self.address,
            len(self._cluster.workers),
        )

    def activate_client(self):
        """
        Create and activate the Dask Client for direct computation in this process.
        Must be called after setup() to enable Dask operations to use this cluster.
        """
        if not self.cluster_active:
            raise RuntimeError("Cannot activate client: cluster not setup. Call setup() first.")

        if self._client:
            self.logger.warning("Client already active for cluster %s", self.address)
            return

        self._client = Client(self._cluster)
        self.logger.info("Client activated for cluster at %s", self.address)

    def close(self):
        """Shut down the client (if exists) and cluster. Safe to call even if never started."""
        if not self.cluster_active:
            self.logger.debug("No active cluster to close.")
            return

        # Close client first if active
        if self._client:
            self.logger.info("Shutting down dask client.")
            self._client.shutdown()
            self._client = None

        # Then close cluster
        self.logger.info("Closing dask cluster at %s.", self.address)
        self._cluster.close()
        self._cluster = None

    def _configure_timeouts(self, connect_timeout: float = None, tcp_timeout: float = None):
        """
        Set Dask timeout environment variables

        Args:
            connect_timeout (float, optional): Connection timeout for Dask communications.
            tcp_timeout (float, optional): TCP timeout for Dask communications.
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
