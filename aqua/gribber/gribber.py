import os
import subprocess
from glob import glob
import yaml
from aqua.logger import log_configure
from aqua.util import load_yaml, create_folder
from aqua.util import get_config_dir, get_machine
from aqua.reader import Reader


class Gribber():
    """
    Class to generate a JSON file from a GRIB file.
    """

    def __init__(self,
                 model=None, exp=None, source=None,
                 nprocs=1,
                 dir={'datadir': None,
                      'tmpdir': None,
                      'jsondir': None,
                      'configdir': None},
                 loglevel=None,
                 overwrite=False
                 ) -> None:
        """
        Initialize class.

        Parameters
        ----------
        model : str
            Model name
        exp : str
            Experiment name
        source : str
            Source name
        nprocs : int, optional
            Number of processors, by default 1
        dir : dict, optional
            Dictionary with directories
            data: data directory
            tmp: temporary directory
            json: JSON directory (output)
            configdir: catalog directory to update
        loglevel : str, optional
            Log level, by default None
        overwrite : bool, optional
            Overwrite JSON file and indices if they exist, by default False

        Methods
        -------
        create_entry()
            Create catalog entry.

        _check_steps()
            Check which steps have to be performed.

        _check_dir()
            Check if directories exist.

        _check_indices()
            Check if indices exist.

        _check_json()
            Check if JSON file exists.

        _check_catalog()
            Check if catalog file exists.

        _create_symlinks()
            Create symlinks to GRIB files.

        _create_indices()
            Create indices for GRIB files.

        _create_json()
            Create JSON file.

        _create_catalog_entry()
            Create catalog entry.

        help()
            Print help.
        """
        self.logger = log_configure(loglevel, 'gribber')
        self.overwrite = overwrite

        if model:
            self.model = model
        else:
            raise Exception('Please specify model.')

        if exp:
            self.exp = exp
        else:
            raise Exception('Please specify experiment.')

        if source:
            self.source = source
        else:
            raise Exception('Please specify source.')

        self.nprocs = nprocs

        # Create folders from dir dictionary, default outside of class
        self.dir = dir
        self._check_dir()

        self.datadir = self.dir['datadir']
        self.tmpdir = os.path.join(self.dir['tmpdir'], self.exp)
        self.jsondir = os.path.join(self.dir['jsondir'], self.exp)
        if not self.dir['configdir']:
            self.configdir = get_config_dir()
        else:
            self.configdir = self.dir['configdir']
        self.machine = get_machine(self.configdir)

        self.logger.info(f"Data directory: {self.datadir}")
        self.logger.info(f"JSON directory: {self.jsondir}")
        self.logger.info(f"Catalog directory: {self.catalogdir}")

        # Get gribtype and tgt_json from source
        self.gribtype = self.source.split('_')[0]
        self.tgt_json = self.source.split('_')[1]
        self.indices = None

        # Get gribfiles wildcard from gribtype
        self.gribfiles = self.gribtype + '????+*'
        self.logger.info(f"Gribfile wildcard: {self.gribfiles}")

        # Get catalog filename
        self.catalogfile = os.path.join(self.configdir, self.machine,
                                        'catalog', self.model,
                                        self.exp+'.yaml')
        self.logger.warning(f"Catalog file: {self.catalogfile}")

        # Get JSON filename
        self.jsonfile = os.path.join(self.jsondir, self.tgt_json+'.json')
        self.logger.warning(f"JSON file: {self.jsonfile}")

        self.flag = [False, False, False]
        self._check_steps()

    def create_entry(self):
        """
        Create catalog entry.
        """
        # Create folders
        for item in [self.tmpdir, self.jsondir]:
            create_folder(item, loglevel=self.logger.level)

        # Create symlinks to GRIB files
        self._create_symlinks()

        # Create indices for GRIB files
        if self.flag[0]:
            self._create_indices()

        # Create JSON file
        if self.flag[1]:
            self._create_json()

        # Create catalog entry
        self._create_catalog_entry()

    def check_entry(self):
        """
        Check if catalog entry works.
        """
        self.reader = Reader(model=self.model, exp=self.exp,
                             source=self.source, configdir=self.configdir,
                             loglevel=self.logger.level)

        data = self.reader.retrieve(fix=False)
        assert len(data) > 0

    def _check_steps(self):
        """
        Check if indices and JSON file have to be created.
        Check if catalog file exists.

        Updates:
            flag: list
                List with flags for indices, JSON file and catalog file.
        """
        # Check if indices have to be created
        # True if indices have to be created,
        # False otherwise
        self.flag[0] = self._check_indices()

        # Check if JSON file has to be created
        # True if JSON file has to be created,
        # False otherwise
        self.flag[1] = self._check_json()

        # Check if catalog file exists
        # True if catalog file exists,
        # False otherwise
        self.flag[2] = self._check_catalog()

    def _check_dir(self):
        """
        Check if dir dictionary contains None values.
        If None values are found, raise Exception.
        """
        for key in self.dir:
            if self.dir[key] is None:
                raise Exception(f'Directory {key} is None:\
                                check your configuration file!')

    def _check_indices(self):
        """
        Check if indices already exist.

        Returns:
            bool: True if indices have to be created, False otherwise.
        """
        self.logger.info("Checking if indices already exist...")
        if len(glob(os.path.join(self.tmpdir, '*.index'))) > 0:
            if self.overwrite:
                self.logger.warning("Indices already exist. Removing them...")
                for file in glob(os.path.join(self.tmpdir, '*.index')):
                    os.remove(file)
                return True
            else:
                self.logger.warning("Indices already exist.")
                return False
        else:  # Indices do not exist
            return True

    def _check_json(self):
        """
        Check if JSON file already exists.

        Returns:
            bool: True if JSON file has to be created, False otherwise.
        """
        self.logger.info("Checking if JSON file already exists...")
        if os.path.exists(self.jsonfile):
            if self.overwrite:
                self.logger.warning("JSON file already exists. Removing it...")
                os.remove(self.jsonfile)
                return True
            else:
                self.logger.warning("JSON file already exists.")
                return False
        else:  # JSON file does not exist
            return True

    def _check_catalog(self):
        """
        Check if catalog entry already exists.

        Returns:
            bool: True if catalog file exists, False otherwise.
        """
        self.logger.info("Checking if catalog file already exists...")
        if os.path.exists(self.catalogfile):
            self.logger.warning(f"Catalog file {self.catalogfile} already exists.")
            return True
        else:  # Catalog file does not exist
            self.logger.warning(f"Catalog file {self.catalogfile} does not exist.")
            self.logger.warning("It will be generated.")
            return False

    def _create_symlinks(self):
        """
        Create symlinks to GRIB files.
        """
        self.logger.info("Creating symlinks...")
        self.logger.info(f"Searching in {self.datadir}...")
        self.logger.info(os.path.join(self.datadir, self.gribfiles))
        try:
            for file in glob(os.path.join(self.datadir, self.gribfiles)):
                try:
                    os.symlink(file, os.path.join(self.tmpdir,
                               os.path.basename(file)))
                except FileExistsError:
                    self.logger.info(f"File {file} already exists in {self.tmpdir}")
        except FileNotFoundError:
            self.logger.error(f"Directory {self.datadir} not found.")

    def _create_indices(self):
        """
        Create indices for GRIB files.
        """
        self.logger.info("Creating GRIB indices...")

        # to be improved without using subprocess
        cmd = ['gribscan-index', '-n', str(self.nprocs)] +\
            glob(os.path.join(self.tmpdir, self.gribfiles))
        self.indices = subprocess.run(cmd)
        self.logger.info(self.indices)

    def _create_json(self):
        """
        Create JSON file.
        """
        self.logger.info("Creating JSON file...")

        #  to be improved without using subprocess
        cmd = ['gribscan-build', '-o', self.jsondir, '--magician', 'ifs',
               '--prefix', self.datadir + '/'] +\
            glob(os.path.join(self.tmpdir, '*index'))
        #  json = subprocess.run(cmd)
        subprocess.run(cmd)

    def _create_catalog_entry(self):
        """
        Create or update catalog file
        Updates both the main.yaml and the catalog file.
        """

        # Generate blocks to be added to the catalog file
        # Catalog file
        block_cat = {
            'driver': 'zarr',
            'args': {
                'consolidated': False,
                'urlpath': 'reference::' + os.path.join(self.jsondir,
                                                        self.tgt_json+'.json')
            }
        }
        self.logger.info("Block to be added to catalog file:")
        self.logger.info(block_cat)

        # Main catalog file
        block_main = {
            '{self.source}': {
                'description': self.description,
                'driver': 'yaml_file_cat',
                'args': {
                    'path': '{{CATALOG_DIR}}/{self.source}.yaml'
                }
            }
        }
        self.logger.info("Block to be added to main catalog file:")
        self.logger.info(block_main)

        if self.flag[2]:  # Catalog file exists
            cat_file = load_yaml(self.catalogfile)

            # Check if source already exists
            if self.source in cat_file['sources'].keys():
                if self.overwrite:
                    self.logger.warning(f"Source {self.source} already exists\
                        in {self.catalogfile}. Replacing it...")
                    cat_file['sources'][self.source] = block_cat
                else:
                    self.logger.warning(f"Source {self.source} already exists\
                        in {self.catalogfile}. Skipping...")
                    return
        else:  # Catalog file does not exist
            # default dict for zarr
            cat_file = {'plugins': {'source': [{'module': 'intake_xarray'},
                                             {'module': 'gribscan'}]}}
            cat_file['sources'] = {}
            cat_file['sources'][self.source] = block_cat

        # Write catalog file
        with open(self.catalogfile, 'w') as f:
            yaml.dump(cat_file, f, sort_keys=False)

        # Write main catalog file
        mainfilepath = os.path.join(self.catalogdir, 'main.yaml')
        main_file = load_yaml(mainfilepath)

        # Check if source already exists
        if self.source in main_file['sources'].keys():
            if self.overwrite:
                self.logger.warning(f"Source {self.source} already exists\
                    in {mainfilepath}. Replacing it...")
                main_file['sources'][self.source] = block_main
            else:
                self.logger.warning(f"Source {self.source} already exists\
                    in {mainfilepath}. Skipping...")
                return
        else:  # Source does not exist
            main_file['sources'][self.source] = block_main

    def help(self):
        """
        Print help message.
        """
        print("Gribber class:")
        print("  model: model name")
        print("  exp: experiment name")
        print("  source: source name")
        print("  nprocs: number of processors (default: 1)")
        print("  loglevel: logging level (default: WARNING)")
        print("  overwrite: overwrite existing files (default: False)")
        print("  dir: dictionary with directories")
        print("     datadir: data directory")
        print("     tmpdir: temporary directory")
        print("     jsondir: JSON directory")
        print("     catalogdir: catalog directory")
