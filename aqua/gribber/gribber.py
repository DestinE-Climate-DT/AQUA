"""
Gribber module for integrate gribscan within AQUA
"""

import os
import subprocess
from glob import glob
from aqua.logger import log_configure
from aqua.util import load_yaml, dump_yaml, create_folder
from aqua.util import ConfigPath
from aqua.reader import Reader


class Gribber():
    """
    Class to generate a JSON file from a GRIB file.
    """

    def __init__(self,
                 model=None, exp=None, source=None,
                 nprocs=1,
                 dirdict={'datadir': None,
                          'tmpdir': None,
                          'jsondir': None,
                          'configdir': None},
                 description=None,
                 loglevel=None,
                 overwrite=False,
                 search=False,
                 ) -> None:
        """
        Initialize the Gribber class.

        Args:
            model (str, optional):          Model name. Defaults to None.
            exp (str, optional):            Experiment name. Defaults to None.
            source (str, optional):         Source name. Defaults to None.
            nprocs (int, optional):         Number of processors.
                                            Defaults to 1.
            dirdict (dict, optional):       Dictionary with directories:
                                                data: data directory
                                                tmp: temporary directory
                                                json: JSON directory (output)
                                                configdir: catalog directory
                                                to update
                                            Defaults to {'datadir': None,
                                                         'tmpdir': None,
                                                         'jsondir': None,
                                                         'configdir': None}.
            description (str, optional):    Description of the experiment.
                                            Defaults to None.
            loglevel (str, optional):       Log level. Defaults to None.
            overwrite (bool, optional):     Overwrite JSON file and indices if
                                            they exist. Defaults to False.
            search (bool, optional):        Search for generic names of files.
                                            Defaults to False.

        Methods:
            Only private methods are listed here.
            _check_steps():          Check which steps have to be performed.
            _check_dir():            Check if directories exist.
            _check_indices():        Check if indices exist.
            _check_json():           Check if JSON file exists.
            _check_catalog():        Check if catalog file exists.
            _create_symlinks():      Create symlinks to GRIB files.
            _create_indices():       Create indices for GRIB files.
            _create_json():          Create JSON file.
            _create_catalog_entry(): Create catalog entry.
        """
        self.logger = log_configure(loglevel, 'gribber')
        self.overwrite = overwrite

        if model:
            self.model = model
            if self.model != "IFS":
                self.logger.warning("Other models than IFS are experimental.")
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

        self.nprocs = nprocs

        self.description = description
        self.search = search

        # Create folders from dir dictionary, default outside of class
        self.dir = dirdict
        self._check_dir()

        self.datadir = self.dir['datadir']
        self.tmpdir = os.path.join(self.dir['tmpdir'], self.model,
                                   self.exp)
        self.jsondir = os.path.join(self.dir['jsondir'], self.model,
                                    self.exp)
        self.logger.info("JSON directory: %s", self.jsondir)

        Configurer = ConfigPath(self.dir['configdir'])
        self.configdir = Configurer.configdir
        self.machine = Configurer.machine

        self.logger.info("Data directory: %s", self.datadir)
        self.logger.info("JSON directory: %s", self.jsondir)
        self.logger.info("Catalog directory: %s", self.configdir)

        # Get gribtype and tgt_json from source
        if not self.search:
            self.gribtype = self.source.split('_')[0]
            self.tgt_json = self.source.split('_')[1]
            self.logger.info("json file will be named as %s.",
                             self.tgt_json)
        else:
            self.tgt_json = self.source
            self.logger.info("json file will be named as %s.",
                             self.source)
        self.indices = None

        # Get gribfiles wildcard from gribtype
        if not self.search:
            self.gribfiles = self.gribtype + '????+*'
            self.logger.info("Gribfile wildcard: %s", self.gribfiles)
        else:
            self.logger.warning("Search for generic names of files.")
            format = ".data"
            self.logger.warning("Search for files with format: %s", format)
            self.logger.info("Restart the gribber if you want to change format.")
            self.gribfiles = '*' + format

        # Get catalog filename
        self.catalogdir = os.path.join(self.configdir, 'machines',
                                       self.machine, 'catalog',
                                       self.model)
        self.catalogfile = os.path.join(self.catalogdir, self.exp + '.yaml')
        self.logger.warning("Catalog file: %s", self.catalogfile)

        # Get JSON filename
        self.jsonfile = os.path.join(self.jsondir, self.tgt_json + '.json')
        self.logger.warning("JSON file: %s", self.jsonfile)

        self.flag = [False, False, False]
        self._check_steps()

    def create_entry(self, loglevel=None):
        """
        Create catalog entry.
        """
        if not loglevel:
            loglevel = 'WARNING'
        # Create folders
        for item in [self.tmpdir, self.jsondir]:
            create_folder(item, loglevel=loglevel)

        # Create symlinks to GRIB files
        self._create_symlinks()

        # Create indices for GRIB files
        if self.flag[0]:
            self._create_indices()

        # Create JSON file
        if self.flag[1]:
            self._create_json()
            if not os.path.exists(self.jsonfile):
                self.logger.error("Gribscan has created different json filename!")
                self.logger.error("Please check and modify the catalog files accordingly")

        # Create catalog entry
        self._create_catalog_entry()

        self._create_main_catalog()

        self._create_regrid_entry()

    def check_entry(self):
        """
        Check if catalog entry works.
        """
        self.reader = Reader(model=self.model, exp=self.exp,
                             source=self.source, configdir=self.configdir,
                             loglevel=self.logger.level, fix=False)

        data = self.reader.retrieve()
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
                raise KeyError(f'Directory {key} is None:\
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
        self.logger.info(f"Checking if JSON file {self.jsonfile} already exists...")
        if os.path.exists(self.jsonfile):
            if self.overwrite:
                self.logger.warning("JSON file already exists. Removing it...")
                os.remove(self.jsonfile)
                return True
            else:
                self.logger.warning("JSON file already exists.")
                self.logger.info("It will not be generated.")
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
            self.logger.warning("Catalog file %s already exists.",
                                self.catalogfile)
            return True
        else:  # Catalog file does not exist
            self.logger.warning("Catalog file %s does not exist.",
                                self.catalogfile)
            self.logger.warning("It will be generated.")
            return False

    def _create_symlinks(self):
        """
        Create symlinks to GRIB files.
        """
        self.logger.info("Creating symlinks...")
        self.logger.info("Searching in %s...", self.datadir)
        self.logger.info(os.path.join(self.datadir, self.gribfiles))
        try:
            for file in glob(os.path.join(self.datadir, self.gribfiles)):
                try:
                    os.symlink(file, os.path.join(self.tmpdir,
                               os.path.basename(file)))
                except FileExistsError:
                    self.logger.info("File %s already exists in %s", file,
                                     self.tmpdir)
        except FileNotFoundError:
            self.logger.error("Directory %s not found.", self.datadir)

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
        self.logger.info(f"Creating JSON file {self.jsonfile}...")

        if self.model != 'IFS':
            self.logger.warning("Model %s is experimental.", self.model)
            self.logger.warning("JSON file may have a different name.")

        #  to be improved without using subprocess
        if self.model == 'IFS':
            cmd = ['gribscan-build', '-o', self.jsondir, '--magician', 'ifs',
                   '--prefix', self.datadir + '/'] +\
                glob(os.path.join(self.tmpdir, '*index'))
        else:
            self.logger.warning("Model %s is experimental.", self.model)
            cmd = ['gribscan-build', '-o', self.jsondir,
                   '--prefix', self.datadir + '/'] +\
                glob(os.path.join(self.tmpdir, '*index'))
        subprocess.run(cmd)

    def _create_catalog_entry(self):
        """
        Create or update catalog file
        """

        # Generate blocks to be added to the catalog file
        # Catalog file
        block_cat = {
            'driver': 'zarr',
            'args': {
                'consolidated': False,
                'urlpath': 'reference::' + os.path.join(self.jsondir,
                                                        self.tgt_json + '.json')
            }
        }
        self.logger.info("Block to be added to catalog file:")
        self.logger.info(block_cat)

        if self.flag[2]:  # Catalog file exists
            cat_file = load_yaml(self.catalogfile)

            # Check if source already exists
            if self.source in cat_file['sources'].keys():
                if self.overwrite:
                    self.logger.warning(f"Source {self.source} already exists\
                        in {self.catalogfile}. Replacing it...")
                    cat_file['sources'][self.source] = block_cat
                else:
                    self.logger.warning(f"Source {self.source} already exists in {self.catalogfile}. Skipping...")
                return
        else:  # Catalog file does not exist
            # default dict for zarr
            cat_file = {'plugins': {'source': [{'module': 'intake_xarray'},
                                               {'module': 'gribscan'}]}}
            cat_file['sources'] = {}
            cat_file['sources'][self.source] = block_cat

            # Create folder if needed
            create_folder(folder=self.catalogdir)

        # Write catalog file
        dump_yaml(outfile=self.catalogfile, cfg=cat_file)

    def _create_main_catalog(self):
        """
        Updates the main.yaml file.
        """

        if not self.description:
            self.description = self.exp + ' data'

        # Main catalog file
        block_main = {
            self.exp: {
                'description': self.description,
                'driver': 'yaml_file_cat',
                'args': {
                    'path': '{{CATALOG_DIR}}/' + self.exp + '.yaml'
                }
            }
        }
        self.logger.info("Block to be added to main catalog file:")
        self.logger.info(block_main)

        # Write main catalog file
        mainfilepath = os.path.join(self.configdir, 'machines', self.machine,
                                    'catalog', self.model, 'main.yaml')
        main_file = load_yaml(mainfilepath)

        # Check if source already exists
        if self.exp in main_file['sources'].keys():
            if self.overwrite:
                self.logger.warning(f"Source {self.exp} already exists\
                    in {mainfilepath}. Replacing it...")
                main_file['sources'][self.source] = block_main
            else:
                self.logger.warning(f"Source {self.exp} already exists\
                    in {mainfilepath}. Skipping...")
            return
        else:  # Source does not exist
            main_file['sources'] = block_main

        # Write catalog file
        dump_yaml(outfile=mainfilepath, cfg=main_file)

    def _create_regrid_entry(self):
        """
        Updates the regrid.yaml file.

        At the moment this is only a placeholder for a possible function.
        """

        self.logger.warning("Update regrid.yaml has to be done manually.")

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
