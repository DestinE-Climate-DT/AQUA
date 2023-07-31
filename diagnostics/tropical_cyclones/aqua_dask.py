import dask
from dask.distributed import Client, LocalCluster
from aqua.logger import log_configure


class AquaDask():

    @property
    def dask(self):
        """Check if dask is needed"""
        return self.nproc > 1

    def __init__(self, nproc, loglevel='info'):

        self.nproc = nproc
        self.cluster = None
        self.client = None
        self.logger = log_configure(loglevel, 'AQUADASK')

    def set_dask(self):
        """
        Set up dask cluster
        """
        if self.dask:  # self.nproc > 1
            self.logger.info(
                'Setting up dask cluster with %s workers', self.nproc)
            # dask.config.set({'temporary_directory': self.tmpdir})
            # self.logger.info('Temporary directory: %s', self.tmpdir)
            self.cluster = LocalCluster(n_workers=self.nproc,
                                        threads_per_worker=1)
            self.client = Client(self.cluster)
        else:
            self.client = None
            dask.config.set(scheduler='synchronous')

    def close_dask(self):
        """
        Close dask cluster
        """
        if self.dask:  # self.nproc > 1
            self.client.shutdown()
            self.cluster.close()
            self.logger.info('Dask cluster closed')
