"""OPA generator class to support OPA creation"""

import os
import copy
import warnings
import dask
import numpy as np
from dask.distributed import Client, LocalCluster
from one_pass.opa import Opa
from aqua.logger import log_configure
from aqua.util import create_folder, load_yaml, dump_yaml
from aqua.util import get_config_dir, get_machine
from aqua.reader import Reader


# Filter the specific warning
warnings.filterwarnings(
    "ignore",
    message="Converting non-nanosecond precision datetime values to nanosecond precision",
    category=UserWarning
)


class OPAgenerator():

    """This class serves as wrapper of the OPA to be used within AQUA"""

    @property
    def dask(self):
        """Check if dask is needed"""
        return self.nproc > 1

    def __init__(self,
                 model=None, exp=None, source=None, zoom=None,
                 var=None, vars=None, frequency=None,
                 checkpoint=True, stream_step=5,
                 outdir=None, tmpdir=None, configdir=None,
                 loglevel=None, overwrite=False, definitive=False,
                 nproc=1):

        """
        Initialize the LRA_Generator class

        Args:
            model (string):          The model name from the catalog
            exp (string):            The experiment name from the catalog
            source (string):         The sourceid name from the catalog
            zoom (int):              Healpix level of zoom
            var (str, list):         Variable(s) to be processed,vars in a synonim
            frequency (string):      The target frequency for averaging the OPA
            checkpoint (bool, opt):  Whether OPA should use or not checkpointing
            stream_step (int, opt):  How many days OPA should load at once
            outdir (string):         Where the LRA is
            tmpdir (string):         Where to store temporary files,
                                     default is None.
                                     Necessary for dask.distributed
            configdir (string):      Configuration directory where the catalog 
                                     are found
            loglevel (string, opt):  Logging level
            overwrite (bool, opt):   True to overwrite existing files in LRA,
                                     default is False
            definitive (bool, opt):  True to create the output file,
                                     False to just explore the reader
                                     operations, default is False
            nproc (int, opt):        Number of dask workers to use. default is 1
        """

        self.logger = log_configure(loglevel, 'opa_generator')
        self.loglevel = loglevel
        self.checkpoint = checkpoint
        self.stream_step = stream_step

        self.overwrite = overwrite
        if self.overwrite:
            self.logger.info('File will be overwritten if already existing.')

        self.definitive = definitive
        if not self.definitive:
            self.logger.warning('IMPORTANT: no file will be created, this is a dry run')

        if model:
            self.model = model
        else:
            raise KeyError('Please specify model.')

        if exp:
            self.exp = exp
        else:
            raise KeyError('Please specify experiment.')

        if source:
            self.source = source
        else:
            raise KeyError('Please specify source.')

        self.zoom = zoom
        if zoom is not None:
            self.logger.info('Zoom level set at: %s', str(zoom))

        if not configdir:
            self.configdir = get_config_dir()
        else:
            self.configdir = configdir
        self.machine = get_machine(self.configdir)

        # Initialize variable(s)
        self.var = None
        if vars:
            self.var = vars
        else:
            self.var = var
        if not self.var:
            raise KeyError('Please specify variable string or list.')
        self.logger.warning('Variable(s) to be processed: %s', self.var)
        if isinstance(self.var, str):
            self.var = [self.var]

        self.frequency = frequency

        self.outdir = os.path.join(outdir, self.model, self.exp)
        if self.frequency:
            self.outdir = os.path.join(self.outdir, self.frequency)

        self.tmpdir = tmpdir
        # self.checkpoint = os.path.join(self.tmpdir, "checkpoint.pickle")

        create_folder(self.outdir, loglevel=self.loglevel)
        create_folder(self.tmpdir, loglevel=self.loglevel)

        self.nproc = int(nproc)
        self.opa_dict = None
        self.checkpoint_file = None
        self.reader = None
        self.timedelta = 60
        self.entry_name = f'tmp-opa-{self.frequency}'

        if self.nproc > 1:
            self.logger.info('Running dask.distributed with %s workers',
                             self.nproc)

        self.client = None
        self.cluster = None

    def retrieve(self):
        """
        Retrieve data from the catalog
        """
        self.logger.info('Accessing catalog for %s-%s-%s...',
                         self.model, self.exp, self.source)

        # Initialize the reader
        self.reader = Reader(model=self.model, exp=self.exp,
                             source=self.source, zoom=self.zoom, 
                             configdir=self.configdir,
                             loglevel=self.loglevel)

        self.logger.info('Getting the timedelta to inform the OPA...')
        self._get_timedelta()

    def _get_timedelta(self):
        """
        Get the timedelta from the catalog
        """

        data = self.reader.retrieve(var=self.var)
        time_diff = np.diff(data.time.values).astype('timedelta64[m]')
        self.timedelta = time_diff[0].astype(int)
        self.logger.info('Timedelta is %s minutes', str(self.timedelta))

    def _configure_opa(self, var):
        """
        Set up the OPA
        """

        self.opa_dict = {
                         "stat": "mean",
                         "stat_freq": self.frequency,
                         "output_freq": "monthly",
                         "time_step": self.timedelta,
                         "variable": var,
                         "save": True,
                         "checkpoint": self.checkpoint,
                         "out_filepath": self.outdir,
                         "checkpoint_filepath": self.tmpdir
        }

        return Opa(self.opa_dict)

    def generate_opa(self):
        """
        Run the actual computation of the OPA looping on the dataset
        and on the variables
        """

        self._set_dask()
        for variable in self.var:
            self.logger.warning('Setting up OPA at %s frequency for variable %s...',
                                self.frequency, variable)

            self.logger.warning('Initializing the OPA')
            opa_mean = self._configure_opa(variable)

             # get info on the checkpoint file
            if self.checkpoint:
                self.checkpoint_file = opa_mean.checkpoint_file
            # self.checkpoint_file = opa_mean.checkpoint_file
            # self.remove_checkpoint()
            print(vars(opa_mean))

            self.logger.warning('Initializing the streaming generator...')
            self.reader.reset_stream()
            data_gen = self.reader.retrieve(streaming_generator=True,
                                            stream_step=self.stream_step,
                                            stream_unit='days')

            for data in data_gen:
                self.logger.info(f"start_date: {data.time[0].values} stop_date: {data.time[-1].values}")
                if self.definitive:
                    opa_mean.compute(data[variable])

        self._close_dask()

    def create_catalog_entry(self):
        """
        Create an entry in the catalog for the LRA in both source and
        regrid yaml
        """

        self.logger.warning('Creating catalog entry %s %s %s',
                            self.model, self.exp, self.entry_name)

        # define the block to be uploaded into the catalog
        block_cat = {
            'driver': 'netcdf',
            'args': {
                'urlpath': os.path.join(self.outdir, f'*{self.frequency}_mean.nc'),
                'chunks': {},
                'xarray_kwargs': {
                    'decode_times': True
                }
            }
        }

        # find the catalog of my experiment
        catalogfile = os.path.join(self.configdir, 'machines', self.machine,
                                   'catalog', self.model, self.exp+'.yaml')

        # load, add the block and close
        cat_file = load_yaml(catalogfile)
        cat_file['sources'][self.entry_name] = block_cat
        dump_yaml(outfile=catalogfile, cfg=cat_file)

        # find the regrid of my experiment
        regridfile = os.path.join(self.configdir, 'machines', self.machine,
                                  'regrid.yaml')
        cat_file = load_yaml(regridfile)
        dictexp = cat_file['source_grids'][self.model][self.exp]
        if self.source in dictexp:
            regrid_entry = dictexp[self.source]
        elif 'default' in dictexp:
            self.logger.warning('No entry found for source %s, assuming the default', self.source)
            regrid_entry = dictexp['default']
        else:
            raise KeyError('Cannot find experiment information regrid file')

        cat_file['source_grids'][self.model][self.exp][self.entry_name] = copy.deepcopy(regrid_entry)

        dump_yaml(outfile=regridfile, cfg=cat_file)

    def _remove_catalog_entry(self):
        """Remove the entries"""

        self.logger.warning('Removing catalog entry %s %s %s',
                            self.model, self.exp, self.entry_name)

        # find the catalog of my experiment
        catalogfile = os.path.join(self.configdir, 'machines', self.machine,
                                   'catalog', self.model, self.exp+'.yaml')
        cat_file = load_yaml(catalogfile)
        if self.entry_name in cat_file['sources']:
            del cat_file['sources'][self.entry_name]
        dump_yaml(outfile=catalogfile, cfg=cat_file)

        # find the regrid of my experiment
        regridfile = os.path.join(self.configdir, 'machines', self.machine,
                                  'regrid.yaml')
        cat_file = load_yaml(regridfile)
        if self.entry_name in cat_file['source_grids'][self.model][self.exp]:
            del cat_file['source_grids'][self.model][self.exp][self.entry_name]
        dump_yaml(outfile=regridfile, cfg=cat_file)

    def _remove_checkpoint(self):
        """Be sure that the checkpoint is removed"""

        if os.path.exists(self.checkpoint_file):
            self.logger.warning('Removing checkpoint file %s ',
                                self.checkpoint_file)
            os.remove(self.checkpoint_file)

    def _remove_data(self):
        """Clean the OPA data which are no longer necessary"""

        for file_name in os.listdir(self.outdir):
            if file_name.endswith('.nc'):
                file_path = os.path.join(self.outdir, file_name)
                self.logger.warning('Removing file %s ', file_path)
                os.remove(file_path)

    def clean(self):
        """Clean after a OPA run"""

        self._remove_catalog_entry()
        if self.checkpoint:
            self._remove_checkpoint()
        self._remove_data()

    def _set_dask(self):
        """
        Set up dask cluster
        """
        if self.dask:  # self.nproc > 1
            self.logger.info('Setting up dask cluster with %s workers',
                             self.nproc)
            dask.config.set({'temporary_directory': self.tmpdir})
            self.logger.info('Temporary directory: %s', self.tmpdir)
            self.cluster = LocalCluster(n_workers=self.nproc,
                                        threads_per_worker=1)
            self.client = Client(self.cluster)
        else:
            self.logger.info('Dask is disabled...')
            dask.config.set(scheduler='synchronous')

    def _close_dask(self):
        """
        Close dask cluster
        """
        if self.dask:  # self.nproc > 1
            self.client.shutdown()
            self.cluster.close()
            self.logger.info('Dask cluster closed')
