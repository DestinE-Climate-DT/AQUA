"""The main AQUA Reader class"""

import os
import re

import types
import shutil
import intake
import intake_esm

import xarray as xr

# from metpy.units import units, DimensionalityError

import smmregrid as rg

from aqua.util import load_yaml, load_multi_yaml
from aqua.util import ConfigPath, area_selection
from aqua.logger import log_configure, log_history
from aqua.util import flip_lat_dir, find_vert_coord
import aqua.gsv

from .streaming import Streaming
from .fixer import FixerMixin
from .regrid import RegridMixin
from .timmean import TimmeanMixin
from .reader_utils import check_catalog_source, group_shared_dims, set_attrs
from .reader_utils import configure_masked_fields

# default spatial dimensions and vertical coordinates
default_space_dims = ['i', 'j', 'x', 'y', 'lon', 'lat', 'longitude',
                      'latitude', 'cell', 'cells', 'ncells', 'values',
                      'value', 'nod2', 'pix', 'elem']

# default vertical dimension
default_vertical_dims = ['nz1', 'nz', 'level', 'height']

# set default options for xarray
xr.set_options(keep_attrs=True)

class Reader(FixerMixin, RegridMixin, TimmeanMixin):
    """General reader for NextGEMS data."""

    instance = None  # Used to store the latest instance of the class

    def __init__(self, model=None, exp=None, source=None, fix=True,
                 regrid=None, regrid_method=None, zoom=None,
                 areas=True, datamodel=None,
                 streaming=False, stream_generator=False,
                 startdate=None, enddate=None,
                 rebuild=False, loglevel=None, nproc=4,
                 aggregation=None, chunks=None):
        """
        Initializes the Reader class, which uses the catalog
        `config/config.yaml` to identify the required data.

        Args:
            model (str, optional): Model ID. Defaults to "ICON".
            exp (str, optional): Experiment ID. Defaults to "tco2559-ng5".
            source (str, optional): Source ID. Defaults to None.
            regrid (str, optional): Perform regridding to grid `regrid`, as defined in `config/regrid.yaml`. Defaults to None.
            regrid_method (str, optional): CDO Regridding regridding method. Read from grid configuration.
                                           If not specified anywhere, using "ycon".
            fix (bool, optional): Activate data fixing
            zoom (int): healpix zoom level. (Default: None)
            areas (bool, optional): Compute pixel areas if needed. Defaults to True.
            datamodel (str, optional): Destination data model for coordinates, overrides the one in fixes.yaml.
                                       Defaults to None.
            streaming (bool, optional): If to retrieve data in a streaming mode. Defaults to False.
            stream_generator (bool, optional): if to return a generator object for data streaming. Defaults to False
            startdate (str, optional): The starting date for reading/streaming the data (e.g. '2020-02-25'). Defaults to None.
            enddate (str, optional): The final date for reading/streaming the data (e.g. '2020-03-25'). Defaults to None.
            rebuild (bool, optional): Force rebuilding of area and weight files. Defaults to False.
            loglevel (str, optional): Level of logging according to logging module.
                                      Defaults to log_level_default of loglevel().
            nproc (int,optional): Number of processes to use for weights generation. Defaults to 4.
            aggregation (str, optional): the streaming frequency in pandas style (1M, 7D etc. or 'monthly', 'daily' etc.)
            chunks (str or dict, optional): chunking to be used for GSV access.
                                              Defaults to None (using default from catalogue, recommended).
                                              If it is a string time chunking is assumed.
                                              If it is a dictionary the keys 'time' and 'vertical' are looked for.
                                              Time chunking can be one of S (step), 10M, 15M, 30M, h, 1h, 3h, 6h, D, 5D, W, M, Y.
                                              Vertical chunking is expressed as the number of vertical levels to be used.
                                              
        Returns:
            Reader: A `Reader` class object.
        """

        Reader.instance = self  # record the latest instance of the class (used for accessor)

        # define the internal logger
        self.loglevel = loglevel
        self.logger = log_configure(log_level=self.loglevel, log_name='Reader')

        self.exp = exp
        self.model = model
        self.regrid_method = regrid_method
        self.nproc = nproc
        self.vert_coord = None
        self.deltat = 1
        self.aggregation = aggregation
        self.chunks = chunks

        self.grid_area = None
        self.src_grid_area = None
        self.dst_grid_area = None

        if stream_generator:  # Stream generator also implies streaming
            streaming = True

        if streaming:
            self.streamer = Streaming(startdate=startdate,
                                      enddate=enddate,
                                      aggregation=aggregation,
                                      loglevel=self.loglevel)
            # Export streaming methods TO DO: probably useless
            self.reset_stream = self.streamer.reset
            self.stream = self.streamer.stream

        self.stream_generator = stream_generator
        self.streaming = streaming

        self.startdate = startdate
        self.enddate = enddate

        self.previous_data = None  # used for FDB iterator fixing

        # define configuration file and paths
        Configurer = ConfigPath()
        self.configdir = Configurer.configdir
        self.machine = Configurer.machine

        # get configuration from the machine
        self.catalog_file, self.fixer_folder, self.config_file = (
            Configurer.get_reader_filenames())
        self.cat = intake.open_catalog(self.catalog_file)

        # check source existence
        self.source = check_catalog_source(self.cat, self.model, self.exp,
                                           source, name="catalog")

        # check that you defined zoom in a correct way
        self.zoom = self._check_zoom(zoom)

        if self.zoom:
            self.esmcat = self.cat[self.model][self.exp][self.source](zoom=self.zoom)
        else:
            self.esmcat = self.cat[self.model][self.exp][self.source]

        # get fixes dictionary and find them
        self.fix = fix  # fix activation flag
        self.fixer_name = self.esmcat.metadata.get('fixer_name', None)

        # case to disable automatic fix
        if self.fixer_name is False:
            self.logger.warning('A False flag is specified in fixer_name metadata, disabling fix!')
            self.fix = False
        
        if self.fix:
            self.fixes_dictionary = load_multi_yaml(self.fixer_folder, loglevel=self.loglevel)
            self.fixes = self.find_fixes()  # find fixes for this model/exp/source
            self.dst_datamodel = datamodel
            # Default destination datamodel
            # (unless specified in instantiating the Reader)
            if not self.dst_datamodel:
                self.dst_datamodel = self.fixes_dictionary["defaults"].get("dst_datamodel", None)

        # Store the machine-specific CDO path if available
        cfg_base = load_yaml(self.config_file)
        self.cdo = self._set_cdo(cfg_base)

        # load and check the regrid
        if regrid or areas:

            # loading the grid defintion file
            main_file = os.path.join(self.configdir, 'aqua-grids.yaml')
            machine_file = os.path.join(self.configdir, 'machines', self.machine, 'catalog.yaml')
            cfg_regrid = load_multi_yaml(filenames=[main_file, machine_file],
                                         definitions="paths",
                                         loglevel=self.loglevel)

            # define grid names
            self.src_grid_name = self.esmcat.metadata.get('source_grid_name')
            if self.src_grid_name is not None:
                self.logger.info('Grid metadata is %s', self.src_grid_name)
            else: 
                self.logger.warning('Grid metadata is not defined. Regridding capabilities might not work.')
            self.dst_grid_name = regrid

            # configure all the required elements
            self._configure_coords(cfg_regrid)

        # generate source areas
        if areas:
            self._generate_load_src_area(cfg_regrid, rebuild)

        # configure regridder and generate weights
        if regrid:
            self._regrid_configure(cfg_regrid)
            self._load_generate_regrid_weights(cfg_regrid, rebuild)

        # generate destination areas
        if areas and regrid:
            self._generate_load_dst_area(cfg_regrid, rebuild)

    def _set_cdo(self, cfg_base):
        """
        Check information on CDO to set the correct version

        Arguments:
            cfg_base (dict): the configuration dictionary

        Returns:
            The path to the CDO executable
        """

        cdo = cfg_base["cdo"].get(self.machine, None)
        if not cdo:
            cdo = shutil.which("cdo")
            if cdo:
                self.logger.debug("Found CDO path: %s", cdo)
            else:
                self.logger.error("CDO not found in path: Weight and area generation will fail.")
        else:
            self.logger.debug("Using CDO from config: %s", cdo)

        return cdo

    def _load_generate_regrid_weights(self, cfg_regrid, rebuild):

        """
        Generated and load the regrid weights for all the vertical coordinates available
        This is done by looping on vetical coordinates, defining a template and calling
        the correspondet smmregrid function

        Args:
            cfg_regrid (dict): dictionary with the grid definitions
            rebuild (bool): true/false flag to trigger recomputation of areas

        Returns:
            Define in the class object the smmregridder object
        """

        self.weightsfile = {}
        self.weights = {}
        self.regridder = {}

        source_grid = cfg_regrid['grids'][self.src_grid_name]
        sgridpath = source_grid.get("path", None)

        # List of vertical coordinates or 2d to iterate over
        if sgridpath:
            if isinstance(sgridpath, dict):
                vclist = sgridpath.keys()
            else:
                vclist = self.vert_coord
        else:
            vclist = self.vert_coord

        for vc in vclist:
            # compute correct filename ending
            levname = vc if vc == "2d" or vc == "2dm" else f"3d-{vc}"

            if sgridpath:
                template_file = cfg_regrid["weights"]["template_grid"].format(sourcegrid=self.src_grid_name,
                                                                              method=self.regrid_method,
                                                                              targetgrid=self.dst_grid_name,
                                                                              level=levname)
            else:
                template_file = cfg_regrid["weights"]["template_default"].format(model=self.model,
                                                                                 exp=self.exp,
                                                                                 source=self.source,
                                                                                 method=self.regrid_method,
                                                                                 targetgrid=self.dst_grid_name,
                                                                                 level=levname)
            # add the zoom level in the template file
            if self.zoom is not None:
                template_file = re.sub(r'\.nc',
                                       '_z' + str(self.zoom) + r'\g<0>',
                                       template_file)

            self.weightsfile.update({vc: os.path.join(
                cfg_regrid["paths"]["weights"],
                template_file)})

            # If weights do not exist, create them
            if rebuild or not os.path.exists(self.weightsfile[vc]):
                if os.path.exists(self.weightsfile[vc]):
                    os.unlink(self.weightsfile[vc])
                self._make_weights_file(self.weightsfile[vc], source_grid,
                                        cfg_regrid, regrid=self.dst_grid_name,
                                        vert_coord=vc, extra=[],
                                        zoom=self.zoom, method=self.regrid_method,
                                        original_grid_size=self.grid_area.size,
                                        nproc = self.nproc)


            self.weights.update({vc: xr.open_mfdataset(self.weightsfile[vc])})
            vc2 = None if vc == "2d" or vc == "2dm" else vc
            self.regridder.update({vc: rg.Regridder(weights=self.weights[vc],
                                                    vert_coord=vc2,
                                                    space_dims=default_space_dims)})

    def _configure_coords(self, cfg_regrid):
        """
        Define the horizontal and vertical coordinates to be used by areas and regrid.
        Horizontal coordinates are guessed from a predefined list.
        Vertical coordinates are read from the grid file.

        Args:
            cfg_regrid (dict): dictionary with the grid definitions

        Returns:
            Defined into the class object space and vert cordinates
        """

        if self.src_grid_name not in cfg_regrid['grids']:
            raise KeyError(f'Source grid {self.src_grid_name} does not exist in aqua-grid.yaml!')

        source_grid = cfg_regrid['grids'][self.src_grid_name]

        # get values from files
        vert_coord = source_grid.get("vert_coord", None)
        space_coord = source_grid.get("space_coord", None)

        # guessing space coordinates
        self.src_space_coord, self.vert_coord = self._guess_coords(space_coord, vert_coord,
                                                                   default_space_dims,
                                                                   default_vertical_dims)
        self.logger.debug("Space coords are %s", self.src_space_coord)
        self.logger.debug("Vert coords are %s", self.vert_coord)

        # Normalize vert_coord to list
        if not isinstance(self.vert_coord, list):
            self.vert_coord = [self.vert_coord]

        self.support_dims = source_grid.get("support_dims", [])  # TODO do we use this?
        self.space_coord = self.src_space_coord

    def _regrid_configure(self, cfg_regrid):
        """
        Configure all the different steps need for the regridding computation
        1) define the masked variables 2) load the source grid file for the different vert grids
        3) define regrid_method to be passed to CDO

        Args:
            cfg_regrid (dict): dictionary with the grid definitions

        Returns:
            All the required class definition to run the regridding later on
        """

        source_grid = cfg_regrid['grids'][self.src_grid_name]

        # define which variables has to be masked
        self.masked_attr, self.masked_vars = configure_masked_fields(source_grid)

        # set target grid coordinates
        self.dst_space_coord = ["lon", "lat"]

        # Expose grid information for the source as a dictionary of
        # open xarrays
        sgridpath = source_grid.get("path", None)
        if sgridpath:
            if isinstance(sgridpath, dict):
                self.src_grid = {}
                for k, v in sgridpath.items():
                    self.src_grid.update({k: xr.open_dataset(v.format(zoom=self.zoom),
                                                             decode_times=False)})
            else:
                if self.vert_coord:
                    self.src_grid = {self.vert_coord[0]: xr.open_dataset(sgridpath.format(zoom=self.zoom),
                                                                         decode_times=False)}
                else:
                    self.src_grid = {"2d": xr.open_dataset(sgridpath.format(zoom=self.zoom),
                                                           decode_times=False)}
        else:
            self.src_grid = None

        # if regrid method is not defined, read from the grid and use "ycon" as default
        default_regrid_method = "ycon"
        if self.regrid_method is None:
            self.regrid_method = source_grid.get("regrid_method", default_regrid_method)
        else:
            self.regrid_method = self.regrid_method

        if self.regrid_method is not default_regrid_method:
            self.logger.info("Regrid method: %s", self.regrid_method)

    def _generate_load_dst_area(self, cfg_regrid, rebuild):
        """
        Generate and load area file for the destination grid.

        Arguments:
            cfg_regrid (dict): dictionary with the grid definitions
            rebuild (bool): true/false flag to trigger recomputation of areas

        Returns:
            the destination area file loaded as xarray dataset and stored in the class object
        """

        self.dst_areafile = os.path.join(
                cfg_regrid["paths"]["areas"],
                cfg_regrid["areas"]["template_grid"].format(grid=self.dst_grid_name))

        if rebuild or not os.path.exists(self.dst_areafile):
            if os.path.exists(self.dst_areafile):
                os.unlink(self.dst_areafile)
            grid = cfg_regrid["grids"][self.dst_grid_name]
            self._make_dst_area_file(self.dst_areafile, grid)

        # open the area file and possibily fix it
        self.dst_grid_area = xr.open_mfdataset(self.dst_areafile).cell_area
        if self.fix:
            self.dst_grid_area = self._fix_area(self.dst_grid_area)

    def _generate_load_src_area(self, cfg_regrid, rebuild):
        """
        Generate and load area file for the source grid.

        Arguments:
            cfg_regrid (dict): dictionary with the grid definitions
            rebuild (bool): true/false flag to trigger recomputation of areas

        Returns:
            the source area file loaded as xarray dataset and stored in the class object
        """

        source_grid = cfg_regrid['grids'][self.src_grid_name]
        sgridpath = source_grid.get("path", None)

        if sgridpath:
            template_file = cfg_regrid["areas"]["template_grid"].format(grid=self.src_grid_name)
        else:
            template_file = cfg_regrid["areas"]["template_default"].format(model=self.model,
                                                                           exp=self.exp,
                                                                           source=self.source)
        # add the zoom level in the template file
        if self.zoom is not None:
            template_file = re.sub(r'\.nc',
                                   '_z' + str(self.zoom) + r'\g<0>',
                                   template_file)

        self.src_areafile = os.path.join(
            cfg_regrid["paths"]["areas"],
            template_file)

        # If source areas do not exist, create them
        if rebuild or not os.path.exists(self.src_areafile):
            if os.path.exists(self.src_areafile):
                os.unlink(self.src_areafile)

            # Another possibility: was a "cellarea" file provided in regrid.yaml?
            cellareas = source_grid.get("cellareas", None)
            cellarea_var = source_grid.get("cellarea_var", None)
            if cellareas and cellarea_var:
                self.logger.info("Using cellareas file provided in aqua-grids.yaml")
                xr.open_mfdataset(cellareas)[cellarea_var].rename("cell_area").squeeze().to_netcdf(self.src_areafile)
            else:
                self._make_src_area_file(self.src_areafile, source_grid,
                                         gridpath=cfg_regrid["cdo-paths"]["download"],
                                         icongridpath=cfg_regrid["cdo-paths"]["icon"],
                                         zoom=self.zoom)

        self.src_grid_area = xr.open_mfdataset(self.src_areafile).cell_area

        self.grid_area = self.src_grid_area
        if self.fix:
            self.grid_area = self._fix_area(self.grid_area)

    def retrieve(self, var=None, level=None,
                 startdate=None, enddate=None,
                 history=True, sample=False):
        """
        Perform a data retrieve.

        Arguments:
            var (str, list): the variable(s) to retrieve. Defaults to None. If None, all variables are retrieved.
            level (list, float, int): Levels to be read, overriding default in catalogue source (only for FDB) .
            startdate (str): The starting date for reading/streaming the data (e.g. '2020-02-25'). Defaults to None.
            enddate (str): The final date for reading/streaming the data (e.g. '2020-03-25'). Defaults to None.
            history (bool): If you want to add to the metadata history information about retrieve. Defaults to True.
            sample (bool): read only one default variable (used only if var is not specified). Defaults to False.

        Returns:
            A xarray.Dataset containing the required data.
        """

        # Streaming emulator require these to be defined in __init__
        if (self.streaming and not self.stream_generator) and (startdate or enddate):
            raise KeyError("In case of streaming=true the arguments startdate/enddate have to be specified when initializing the class.")  # noqa E501

        if not startdate:  # In case the streaming startdate is used also for FDB copy it
            startdate = self.startdate
        if not enddate:  # In case the streaming startdate is used also for FDB copy it
            enddate = self.enddate

        # get loadvar
        if var:
            if isinstance(var, str) or isinstance(var, int):
                var = str(var).split()  # conversion to list guarantees that a Dataset is produced
            self.logger.info("Retrieving variables: %s", var)
            loadvar = self.get_fixer_varname(var) if self.fix else var
        else:
            # If we are retrieving from fdb we have to specify the var
            if isinstance(self.esmcat, aqua.gsv.intake_gsv.GSVSource):

                metadata = self.esmcat.metadata
                if metadata:
                    loadvar = metadata.get('variables')
                    
                    if loadvar is None:
                        loadvar = [self.esmcat._request['param']]  # retrieve var from catalogue
        
                    if not isinstance(loadvar, list):
                        loadvar = [loadvar]

                    if sample:
                        #self.logger.debug("FDB source sample reading, selecting only one variable")
                        loadvar = [loadvar[0]]

                    self.logger.debug("FDB source: loading variables as %s", loadvar)

            else:
                loadvar = None

        fiter = False
        ffdb = False
        # If this is an ESM-intake catalogue use first dictionary value,
        if isinstance(self.esmcat, intake_esm.core.esm_datastore):
            data = self.reader_esm(self.esmcat, loadvar)
        # If this is an fdb entry
        elif isinstance(self.esmcat, aqua.gsv.intake_gsv.GSVSource):
            data = self.reader_fdb(self.esmcat, loadvar, startdate, enddate,
                                   dask=(not self.stream_generator), level=level)
            fiter = self.stream_generator  # this returs an iterator unless dask is set
            ffdb = True  # These data have been read from fdb
        else:
            data = self.reader_intake(self.esmcat, var, loadvar)  # Returns a generator object

        # if retrieve history is required (disable for retrieve_plain)
        if history:
            if ffdb:
                fkind = "FDB"
            else:
                fkind = "file from disk"
            data = log_history(data, f"Retrieved from {self.model}_{self.exp}_{self.source} using {fkind}")

        if self.fix:
            data = self.fixer(data, var)

        if not ffdb:  # FDB sources already have the index, already selected levels
            data = self._index_and_level(data, level=level)  # add helper index, select levels (optional)

        # log an error if some variables have no units
        if isinstance(data, xr.Dataset) and self.fix:
            for var in data.data_vars:
                if not hasattr(data[var], 'units'):
                    self.logger.error('Variable %s has no units!', var)

        if not fiter:  # This is not needed if we already have an iterator
            if self.streaming:
                if self.stream_generator:
                    data = self.streamer.generator(data, startdate=startdate, enddate=enddate)
                else:
                    data = self.streamer.stream(data)
            elif startdate and enddate and not ffdb:  # do not select if data come from FDB (already done)
                data = data.sel(time=slice(startdate, enddate))

        if isinstance(data, xr.Dataset):
            data.aqua.set_default(self)  # This links the dataset accessor to this instance of the Reader class

        return data

    def _index_and_level(self, data, level=None):
        """
        Add a helper idx_3d coordinate to the data and select levels if provided

        Arguments:
            data (xr.Dataset):  the input xarray.Dataset
            level (list, int):  levels to be selected. Defaults to None.

        Returns:
            A xarray.Dataset containing the data with the idx_3d coordinate.
        """

        if self.vert_coord:
            vert_coord = [coord for coord in self.vert_coord if coord not in ["2d", "2dm"]]  # filter out 2d stuff
        else:
            vert_coord = []

        for dim in vert_coord:  # Add a helper index to the data
            if dim in data.coords:
                idx = list(range(0, len(data.coords[dim])))
                data = data.assign_coords(**{f"idx_{dim}": (dim, idx)})
        
        if level:
            if not isinstance(level, list):
                level = [level]
            if not vert_coord:  # try to find a vertical coordinate
                vert_coord = find_vert_coord(data)
            if vert_coord:
                if len(vert_coord) > 1:
                    self.logger.warning("Found more than one vertical coordinate, using the first one: %s", vert_coord[0])
                data = data.sel(**{vert_coord[0]: level})
                data = log_history(data, f"Selecting levels {level} from vertical coordinate {vert_coord[0]}")
            else:
                self.logger.error("Levels selected but no vertical coordinate found in data!")

        return data

    def set_default(self):
        """Sets this reader as the default for the accessor."""

        Reader.instance = self  # Refresh the latest reader instance used

    def regrid(self, data):
        """Call the regridder function returning container or iterator"""

        if isinstance(data, types.GeneratorType):
            return self._regridgen(data)
        else:
            return self._regrid(data)

    def _regridgen(self, data):
        for ds in data:
            yield self._regrid(ds)

    def _regrid(self, datain):
        """
        Perform regridding of the input dataset.

        Arguments:
            data (xr.Dataset):  the input xarray.Dataset
        Returns:
            A xarray.Dataset containing the regridded data.
        """

        # Check if original lat has been flipped and in case flip back, returns a deep copy in that case
        data = flip_lat_dir(datain)

        data = data.copy()  # make a copy to avoid modifying the original dataset/dataarray

        # Scan the variables:
        # if a 2d one is found but this was not expected, then assume that it comes from a 3d one
        # and expand it along the vertical dimension.
        # This works only if only one variable was selected,
        # else the information on which variable was using which dimension is lost.
        expand_list = []
        if self.vert_coord and "2d" not in self.vert_coord:
            if isinstance(data, xr.Dataset):
                for var in data:
                    # check if none of the dimensions of var is in self.vert_coord
                    if not list(set(data[var].dims) & set(self.vert_coord)):
                        # find list of coordinates that start with idx_ in their name
                        idx = [coord for coord in data[var].coords if coord.startswith("idx_")]
                        if idx:  # found coordinates starting with idx_, use first one to expand var
                            coord_exp = idx[0][4:]  # remove idx_ from the name of the first one (there should be only one)
                            if coord_exp in data[var].coords:
                                expand_list.append(var)
                if expand_list:
                    for var in expand_list:
                        data[var] = data[var].expand_dims(dim=coord_exp, axis=1)
                    self.logger.debug(f"Expanding variables {expand_list} with vertical dimension {coord_exp}")
                    if len(idx) > 1:
                        self.logger.warning("Found more than one idx_ coordinate for expanded variables, did you select slices of multiple vertical coordinates? Results may not be correct.")

            else:  # assume DataArray
                if not list(set(data.dims) & set(self.vert_coord)):
                    idx = [coord for coord in data.coords if coord.startswith("idx_")]
                    if idx:
                        if len(idx) > 1:
                            self.logger.warning("Found more than one idx_ coordinate, did you select slices of multiple vertical coordinates? Results may not be correct.")
                        coord_exp = idx[0][4:]
                        if coord_exp in data.coords:
                            data = data.expand_dims(dim=coord_exp, axis=1)
                            self.logger.debug(f"Expanding variable {data.name} with vertical dimension {coord_exp}")
                            expand_list = [data.name]

        if self.vert_coord == ["2d"]:
            datadic = {"2d": data}
        else:
            self.logger.debug("Grouping variables that share the same dimension")
            self.logger.debug("Vert coord: %s", self.vert_coord)
            self.logger.debug("masked_att: %s", self.masked_attr)
            self.logger.debug("masked_vars: %s", self.masked_vars)

            datadic = group_shared_dims(data, self.vert_coord, others="2d",
                                        masked="2dm", masked_att=self.masked_attr,
                                        masked_vars=self.masked_vars)

        # Iterate over list of groups of variables, regridding them separately
        out = []
        for vc, dd in datadic.items():
            if isinstance(dd, xr.Dataset):
                self.logger.debug(f"Using vertical coordinate {vc}: {list(dd.data_vars)}")
            else:
                self.logger.debug(f"Using vertical coordinate {vc}: {dd.name}")            

            # remove extra coordinates starting with idx_ (if any)
            # to make the regridder work correctly with multiple helper indices
            for coord in dd.coords:
                if coord.startswith("idx_") and coord != f"idx_{vc}":
                    dd = dd.drop_vars(coord)
                    if isinstance(dd, xr.Dataset):
                        self.logger.debug(f"Dropping {coord} from {list(dd.data_vars)}")
                    else:
                        self.logger.debug(f"Dropping {coord} from {dd.name}")

            out.append(self.regridder[vc].regrid(dd))

        if len(out) > 1:
            out = xr.merge(out)
        else:
            # If this was a single dataarray
            out = out[0]

        # If we expanded some variables, squeeze them back
        if expand_list:
            out = out.squeeze(dim=coord_exp)

        # set regridded attribute to 1 for all vars
        out = set_attrs(out, {"regridded": 1})

        # set these two to the target grid
        # (but they are actually not used so far)
        self.grid_area = self.dst_grid_area
        self.space_coord = ["lon", "lat"]

        out.aqua.set_default(self)  # This links the dataset accessor to this instance of the Reader class

        out = log_history(out, f"Regrid from {self.src_grid_name} to {self.dst_grid_name}")

        return out


    def _check_if_regridded(self, data):
        """
        Checks if a dataset or Datarray has been regridded.

        Arguments:
            data (xr.DataArray or xarray.DataDataset):  the input data
        Returns:
            A boolean value
        """

        if isinstance(data, xr.Dataset):
            att = list(data.data_vars.values())[0].attrs
        else:
            att = data.attrs

        return att.get("regridded", False)

    def fldmean(self, data, lon_limits=None, lat_limits=None, **kwargs):
        """
        Perform a weighted global average.
        If a subset of the data is provided, the average is performed only on the subset.

        Arguments:
            data (xr.DataArray or xarray.DataDataset):  the input data
            lon_limits (list, optional):  the longitude limits of the subset
            lat_limits (list, optional):  the latitude limits of the subset

        Kwargs:
            - box_brd (bool,opt): choose if coordinates are comprised or not in area selection.
                                  Default is True

        Returns:
            the value of the averaged field
        """

        # If these data have been regridded we should use
        # the destination grid info
        if self._check_if_regridded(data):
            space_coord = self.dst_space_coord
            grid_area = self.dst_grid_area
        else:
            space_coord = self.src_space_coord
            grid_area = self.src_grid_area

        if lon_limits is not None or lat_limits is not None:
            data = area_selection(data, lon=lon_limits, lat=lat_limits,
                                  loglevel=self.loglevel, **kwargs)

        # check if coordinates are aligned
        try:
            xr.align(grid_area, data, join='exact')
        except ValueError as err:
            # check in the dimensions what is wrong
            for coord in self.grid_area.coords:

                xcoord = data.coords[coord]
                # HACK to solve minor issue in xarray
                # check https://github.com/oloapinivad/AQUA/pull/397 for further info
                if len(xcoord.coords) > 1:
                    self.logger.warning('Issue found in %s, removing spurious coordinates', coord)
                    drop_coords = [koord for koord in xcoord.coords if koord != coord]
                    xcoord = xcoord.drop_vars(drop_coords)

                # option1: shape different
                if len(self.grid_area[coord]) != len(xcoord):
                    raise ValueError(f'{coord} has different shape between area files and your dataset.'
                                     'If using the LRA, try setting the regrid=r100 option') from err
                # shape are ok, but coords are different
                if not self.grid_area[coord].equals(xcoord):
                    # if they are fine when sorted, there is a sorting mismatch
                    if self.grid_area[coord].sortby(coord).equals(xcoord.sortby(coord)):
                        self.logger.warning('%s is sorted in different way between area files and your dataset. Flipping it!',
                                            coord)
                        self.grid_area = self.grid_area.reindex({coord: list(reversed(self.grid_area[coord]))})
                        # raise ValueError(f'{coord} is sorted in different way between area files and your dataset.') from err
                    # something else
                    else:
                        raise ValueError(f'{coord} has a mismatch in coordinate values!') from err

        out = data.weighted(weights=grid_area.fillna(0)).mean(dim=space_coord)

        out.aqua.set_default(self)  # This links the dataset accessor to this instance of the Reader class

        log_history(data, f"Spatially averaged from {self.src_grid_name} grid")

        return out

    def _check_zoom(self, zoom):
        """
        Function to check if the zoom parameter is included in the metadata of
        the source and performs a few safety checks.
        It could be extended to any other metadata flag.

        Arguments:
            zoom (int): the zoom level to be checked

        Returns:
            zoom after check has been processed
        """

        # safe check for zoom into the catalog parameters (at exp level)
        shortcat = self.cat[self.model][self.exp]
        metadata1 = 'zoom' in shortcat.metadata.get('parameters', {}).keys()

        # check at source level (within the parameters)
        # metadata2 = 'zoom' in shortcat[self.source].metadata.get('parameters', {}).keys()
        checkentry = shortcat[self.source].describe()['user_parameters']
        if len(checkentry) > 0:
            metadata2 = 'zoom' in checkentry[0]['name']
        else:
            metadata2 = False

        # combine the two flags
        metadata = metadata1 or metadata2
        if zoom is None:
            if metadata:
                self.logger.warning('No zoom specified but the source requires it, setting zoom=0')
                return 0
            return zoom

        if zoom is not None:
            if metadata:
                return zoom

            self.logger.warning('%s %s %s has not zoom option, disabling zoom=None',
                                self.model, self.exp, self.source)
            return None

    def vertinterp(self, data, levels=None, vert_coord='plev', units=None,
                   method='linear'):
        """
        A basic vertical interpolation based on interp function
        of xarray within AQUA. Given an xarray object, will interpolate the
        vertical dimension along the vert_coord.
        If it is a Dataset, only variables with the required vertical
        coordinate will be interpolated.

        Args:
            data (DataArray, Dataset): your dataset
            levels (float, or list): The level you want to interpolate the vertical coordinate
            units (str, optional, ): The units of your vertical axis. Default 'Pa'
            vert_coord (str, optional): The name of the vertical coordinate. Default 'plev'
            method (str, optional): The type of interpolation method supported by interp()

        Return
            A DataArray or a Dataset with the new interpolated vertical dimension
        """

        if levels is None:
            raise KeyError('Levels for interpolation must be specified')

        # error if vert_coord is not there
        if vert_coord not in data.coords:
            raise KeyError(f'The vert_coord={vert_coord} is not in the data!')

        # if you not specified the units, guessing from the data
        if units is None:
            if hasattr(data[vert_coord], 'units'):
                self.logger.warning('Units of vert_coord=%s has not defined, reading from the data', vert_coord)
                units = data[vert_coord].units
            else:
                raise ValueError('Original dataset has not unit on the vertical axis, failing!')

        if isinstance(data, xr.DataArray):
            final = self._vertinterp(data=data, levels=levels, units=units,
                                     vert_coord=vert_coord, method=method)

        elif isinstance(data, xr.Dataset):
            selected_vars = [da for da in data.data_vars if vert_coord in data[da].coords]
            final = data[selected_vars].map(self._vertinterp, keep_attrs=True,
                                            levels=levels, units=units,
                                            vert_coord=vert_coord, method=method)
        else:
            raise ValueError('This is not an xarray object!')

        final = log_history(final, f"Interpolated from original levels {data[vert_coord].values} {data[vert_coord].units} to level {levels} using {method} method.")

        final.aqua.set_default(self)  # This links the dataset accessor to this instance of the Reader class

        return final

    def _vertinterp(self, data, levels=None, units='Pa', vert_coord='plev', method='linear'):

        # verify units are good
        if data[vert_coord].units != units:
            self.logger.warning('Converting vert_coord units to interpolate from %s to %s',
                                data[vert_coord].units, units)
            data = data.metpy.convert_coordinate_units(vert_coord, units)

        # very simple interpolation
        final = data.interp({vert_coord: levels}, method=method)

        return final

    def detrend(self, data, dim="time", degree=1, skipna=True):
        """
        A basic detrending routine based on polyfit and polyval xarray functions
        within AQUA. Given an xarray object, will provide the detrended timeseries,
        by default working along time coordinate
        If it is a Dataset, only variables with the required
        coordinate will be detrended.

        Args:
            data (DataArray, Dataset): your dataset
            dim (str): The dimension along which apply detrending
            degree (str, optional): The degree of the polinominal fit. Default is 1, i.e. linear detrend
            skinpna (bool, optional): skip or not the NaN

        Return
            A detrended DataArray or a Dataset 
        """

        if isinstance(data, xr.DataArray):
            final = self._detrend(data=data, dim=dim, degree=degree, skipna=skipna)

        elif isinstance(data, xr.Dataset):
            selected_vars = [da for da in data.data_vars if dim in data[da].coords]
            final = data[selected_vars].map(self._detrend, keep_attrs=True, 
                                            dim=dim, degree=degree, skipna=skipna)
        else:
            raise ValueError('This is not an xarray object!')

        final = log_history(final, f"Detrended with polynominal of order {degree} along {dim} dimension")

        # This links the dataset accessor to this instance of the Reader class
        final.aqua.set_default(self)

        return final

    def _detrend(self, data, dim="time", degree=1, skipna=True):
        """
        Detrend a DataArray along a single dimension.
        Taken from https://ncar.github.io/esds/posts/2022/dask-debug-detrend/
        According to the post, current implementation is not the most efficient one.
        """

        # calculate polynomial coefficients
        p = data.polyfit(dim=dim, deg=degree, skipna=skipna)
        # evaluate trend
        fit = xr.polyval(data[dim], p.polyfit_coefficients)
        # remove the trend
        return data - fit

    def reader_esm(self, esmcat, var):
        """Reads intake-esm entry. Returns a dataset."""
        cdf_kwargs = esmcat.metadata.get('cdf_kwargs', {"chunks": {"time": 1}})
        query = esmcat.metadata['query']
        if var:
            query_var = esmcat.metadata.get('query_var', 'short_name')
            # Convert to list if not already
            query[query_var] = var.split() if isinstance(var, str) else var
        subcat = esmcat.search(**query)
        data = subcat.to_dataset_dict(cdf_kwargs=cdf_kwargs,
                                      # zarr_kwargs=dict(consolidated=True),
                                      # decode_times=True,
                                      # use_cftime=True)
                                      progressbar=False
                                      )
        return list(data.values())[0]

    def reader_fdb(self, esmcat, var, startdate, enddate, dask=False, level=None):
        """
        Read fdb data. Returns an iterator or dask array.
        Args:
            esmcat (intake catalogue): the intake catalogue to read
            var (str): the shortname of the variable to retrieve
            startdate (str): a starting date and time in the format YYYYMMDD:HHTT
            enddate (str): an ending date and time in the format YYYYMMDD:HHTT
            dask (bool): return directly a dask array instead of an iterator
            level (list, float, int): level to be read, overriding default in catalogue
        Returns:
            An xarray.Dataset or an iterator over datasets
        """

        if level and not isinstance(level, list):
            level = [level]

        if dask:
            if self.chunks:  # if the chunking option is specified override that from the catalogue
                data = esmcat(startdate=startdate, enddate=enddate, var=var, level=level,
                              chunks=self.chunks,
                              logging=True, loglevel=self.loglevel).to_dask()
            else:
                data = esmcat(startdate=startdate, enddate=enddate, var=var, level=level,
                              logging=True, loglevel=self.loglevel).to_dask()
        else:
            if self.aggregation:  # covers special case: if GSV source and stream_generator then aggregation overrides chunks if specified
                chunks = self.aggregation
            else:
                chunks = self.chunks
            if chunks:
                data = esmcat(startdate=startdate, enddate=enddate, var=var, level=level,
                              chunks=chunks,
                              logging=True, loglevel=self.loglevel).read_chunked()
            else:
                data = esmcat(startdate=startdate, enddate=enddate, var=var, level=level,
                              logging=True, loglevel=self.loglevel).read_chunked()

        return data

    def reader_intake(self, esmcat, var, loadvar, keep="first"):
        """
        Read regular intake entry. Returns dataset.

        Args:
            esmcat (intake.catalog.Catalog): your catalog
            var (list or str): Variable to load
            loadvar (list of str): List of variables to load
            keep (str, optional): which duplicate entry to keep ("first" (default), "last" or None)

        Returns:
            Dataset
        """

        data = esmcat.to_dask()

        if loadvar:
            if all(element in data.data_vars for element in loadvar):
                data = data[loadvar]
            else:
                try:
                    data = data[var]
                    self.logger.warning("You are asking for var %s but the fixes definition requires %s, which is not there.", var, loadvar)
                    self.logger.warning("Retrieving %s, but it would be safer to run with fix=False or to correct the fixes", var)
                except Exception as e:
                    raise KeyError("You are asking for variables which we cannot find in the catalog!") from e

        # check for duplicates
        if 'time' in data.coords:
            len0 = len(data.time)
            data = data.drop_duplicates(dim='time', keep=keep)
            if len(data.time) != len0:
                self.logger.warning("Duplicate entries found along the time axis, keeping the %s one.", keep)

        return data

    def stream(self, data, startdate=None, enddate=None, aggregation=None,
               timechunks=None, reset=False):
        """
        Stream a dataset chunk using the startdate, enddate, and aggregation parameters defined in the constructor.
        This operation utilizes the 'stream' method from the Streaming class.
        It first checks if the Streaming class has been initialized; if not, it initializes the class.

        Arguments:
            data (xr.Dataset):      the input xarray.Dataset
            startdate (str): the starting date for streaming the data (e.g. '2020-02-25') (None)
            enddate (str): the ending date for streaming the data (e.g. '2021-01-01') (None)
            aggregation (str): the streaming frequency in pandas style (1M, 7D etc.)
            timechunks (DataArrayResample, optional): a precomputed chunked time axis
            reset (bool, optional): reset the streaming

        Returns:
            A xarray.Dataset containing the subset of the input data that has been streamed.
        """
        if not hasattr(self, 'streamer'):
            self.streamer = Streaming(startdate=startdate,
                                      enddate=enddate,
                                      aggregation=aggregation)
            self.stream = self.streamer.stream

        stream_data = self.stream(data,
                                  startdate=startdate,
                                  enddate=enddate,
                                  aggregation=aggregation,
                                  timechunks=timechunks,
                                  reset=reset)

        stream_data.aqua.set_default(self)  # This links the dataset accessor to this instance of the Reader class

        return stream_data

    def info(self):
        """Prints info about the reader"""
        print("Reader for model %s, experiment %s, source %s" %
              (self.model, self.exp, self.source))

        if isinstance(self.esmcat, aqua.gsv.intake_gsv.GSVSource):
            if "expver" in self.esmcat._request.keys():
                print("  This experiment has expID %s" % self.esmcat._request['expver'])

        metadata = self.esmcat.metadata

        if self.fix:
            print("Data fixing is active:")
            if "fixer_name" in metadata.keys():
                print("  Fixer name is %s" % metadata["fixer_name"])
            else:
                # TODO: to be removed when all the catalogues are updated
                print("  Fixes: %s" % self.fixes)

        if self.dst_grid_name:
            print("Regridding is active:")
            print("  Target grid is %s" % self.dst_grid_name)
            print("  Regridding method is %s" % self.regrid_method)

        print("Metadata:")
        for k, v in metadata.items():
            print("  %s: %s" % (k, v))

        if isinstance(self.esmcat, aqua.gsv.intake_gsv.GSVSource):
            print("GSV request for this source:")
            for k, v in self.esmcat._request.items():
                if k not in ["time", "param", "step", "expver"]:
                    print("  %s: %s" % (k, v))
