#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
AQUA tool to evalute speedo of some methods
'''

import argparse
import sys
from timeit import timeit
import dask
from dask.distributed import Client, LocalCluster
from aqua import Reader
from aqua import __version__ as version
from aqua.logger import log_configure

print('AQUA version is: ' + version)

def parse_arguments(arguments):
    """
    Parse command line arguments for the LRA CLI
    """

    parser = argparse.ArgumentParser(description='AQUA Benchmarker')
    parser.add_argument('-w', '--workers', type=str, default='1',
                        help='number of dask workers')
    parser.add_argument('-l', '--loglevel', type=str, default='WARNING',
                        help='log level [default: WARNING]')


    return parser.parse_args(arguments)


class Benchmarker():
    """
    A class for benchmarking tasks.

    Attributes:
        model (str): The model to be benchmarked.
        exp (str): The experiment to be performed.
        source (str): The source of the benchmark data.
        nproc (int): The number of processes to be used.
        nrepeat (int): The number of times to repeat the benchmark.
        loglevel (str): The log level for logging messages.
        logger (Logger): The logger object for logging messages.
        cluster (LocalCluster): The dask cluster object.
        client (Client): The dask client object.
    """

    @property
    def dask(self):
        """Check if dask is needed"""
        return self.nproc > 1

    def __init__(self, model=None, exp=None, source=None,
                 nproc=1, nrepeat=5, loglevel='WARNING'):
        """
        Initialize the Benchmarker object.

        Args:
            model (str, optional): The model to be benchmarked.
            exp (str, optional): The experiment to be performed.
            source (str, optional): The source of the benchmark data.
            nproc (int, optional): The number of processes to be used.
            nrepeat (int, optional): The number of times to repeat the benchmark.
            loglevel (str, optional): The log level for logging messages.
        """
        self.logger = log_configure(loglevel, 'daske')
        self.loglevel = loglevel
        self.nproc = nproc
        self.cluster = None
        self.client = None
        self.model = model
        self.exp = exp
        self.source = source
        self.nrepeat = nrepeat

    def set_dask(self):
        """
        Set up dask cluster.
        """
        if self.dask:  # self.nproc > 1
            self.logger.info('Setting up dask cluster with %s workers', self.nproc)
            #dask.config.set({'temporary_directory': self.tmpdir})
            #self.logger.info('Temporary directory: %s', self.tmpdir)
            self.cluster = LocalCluster(n_workers=self.nproc,
                                        threads_per_worker=1)
            self.client = Client(self.cluster)
        else:
            self.client = None
            dask.config.set(scheduler='synchronous')

    def close_dask(self):
        """
        Close dask cluster.
        """
        if self.dask:  # self.nproc > 1
            self.client.shutdown()
            self.cluster.close()
            self.logger.info('Dask cluster closed')

    def benchmark_reader(self):
        """
        Benchmark the reader.
        """
        self.logger.info('Benchmarking reader')
        single = timeit(lambda: Reader(self.model, self.exp, self.source), number=self.nrepeat)
        return round(single / self.nrepeat, 1)
    
    def benchmark_fldmean(self, tsteps=100):
        """
        Benchmark the fldmean method.
        """
        self.logger.info('Benchmarking fldmean')
        reader = Reader(self.model, self.exp, self.source)
        single = timeit(lambda: reader.retrieve(var='2t').isel(time=slice(0,tsteps)).aqua.fldmean().compute(),
                                number=self.nrepeat)
        return round(single / self.nrepeat, 1)

    def benchmark_regrid(self, tsteps=50):
        """
        Benchmark the regrid method.
        """
        self.logger.info('Benchmarking regrid')
        reader = Reader(self.model, self.exp, self.source, regrid='r100')
        single = timeit(lambda: reader.retrieve(var='2t').isel(time=slice(0,tsteps)).aqua.regrid().compute(),
                                number=self.nrepeat)
        return round(single / self.nrepeat, 1)


if __name__ == '__main__':

    args = parse_arguments(sys.argv[1:])
    logger = log_configure(args.loglevel, 'benchmarker')
    model = 'ICON'
    exp = 'historical-1990'
    source = 'hourly-hpz10-atm2d'

    # create benchmarker object
    Bench = Benchmarker(model = model, exp = exp, source = source,
                        nproc = int(args.workers), loglevel = args.loglevel)
    
    # create a list of methods and their names
    methods = [(Bench.benchmark_reader, 'Reader initialization'),
               (Bench.benchmark_fldmean, 'Fldmean'),
               (Bench.benchmark_regrid, 'Regrid')]

    # loop over the methods
    for method, name in methods:
        Bench.set_dask()
        time = method()
        Bench.close_dask()
        print(f"{name} took on average {time} seconds over {Bench.nrepeat} runs")

    print('All benchmarks done')

