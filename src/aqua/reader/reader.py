"""The main AQUA Reader class"""

from contextlib import contextmanager
import intake_esm
import intake_xarray
import xarray as xr
from metpy.units import units

from smmregrid import GridInspector

from aqua.util import load_multi_yaml, files_exist, to_list
from aqua.util import ConfigPath, area_selection, find_vert_coord
from aqua.logger import log_configure, log_history
from aqua.exceptions import NoDataError, NoRegridError
from aqua.version import __version__ as aqua_version
from aqua.regridder import Regridder
from aqua.timstat import TimStat
from aqua.data_model import counter_reverse_coordinate
import aqua.gsv

from .streaming import Streaming
from .fixer import FixerMixin
from .reader_utils import set_attrs

# set default options for xarray
xr.set_options(keep_attrs=True)

class Reader(FixerMixin):
    """General reader for climate data."""

    instance = None  # Used to store the latest instance of the class

    def __init__(self, model=None, exp=None, source=None, catalog=None,
                 fix=True,
                 regrid=None, regrid_method=None,
                 areas=True, datamodel=None,
                 streaming=False,
                 startdate=None, enddate=None,
                 rebuild=False, loglevel=None, nproc=4,
                 aggregation=None, chunks=None,
                 preproc=None, convention='eccodes',
                 **kwargs):
        """
        Initializes the Reader class, which uses the catalog
        `config/config.yaml` to identify the required data.

        Args:
            model (str): Model ID. Mandatory
            exp (str): Experiment ID. Mandatory.
            source (str): Source ID. Mandatory
            catalog (str, optional): Catalog where to search for the triplet.  Default to None will allow for autosearch in
                                     the installed catalogs.
            regrid (str, optional): Perform regridding to grid `regrid`, as defined in `config/regrid.yaml`. Defaults to None.
            regrid_method (str, optional): CDO Regridding regridding method. Read from grid configuration.
                                           If not specified anywhere, using "ycon".
            fix (bool, optional): Activate data fixing
            areas (bool, optional): Compute pixel areas if needed. Defaults to True.
            datamodel (str, optional): Destination data model for coordinates, overrides the one in fixes.yaml.
                                       Defaults to None.
            streaming (bool, optional): If to retrieve data in a streaming mode. Defaults to False.
            startdate (str, optional): The starting date for reading/streaming the data (e.g. '2020-02-25'). Defaults to None.
            enddate (str, optional): The final date for reading/streaming the data (e.g. '2020-03-25'). Defaults to None.
            rebuild (bool, optional): Force rebuilding of area and weight files. Defaults to False.
            loglevel (str, optional): Level of logging according to logging module.
                                      Defaults to log_level_default of loglevel().
            nproc (int, optional): Number of processes to use for weights generation. Defaults to 4.
            aggregation (str, optional): the streaming frequency in pandas style (1M, 7D etc. or 'monthly', 'daily' etc.)
                                         Defaults to None (using default from catalog, recommended).
            chunks (str or dict, optional): chunking to be used for GSV access.
                                            Defaults to None (using default from catalog, recommended).
                                            If it is a string time chunking is assumed.
                                            If it is a dictionary the keys 'time' and 'vertical' are looked for.
                                            Time chunking can be one of S (step), 10M, 15M, 30M, h, 1h, 3h, 6h, D, 5D, W, M, Y.
                                            Vertical chunking is expressed as the number of vertical levels to be used.
            preproc (function, optional): a function to be applied to the dataset when retrieved. Defaults to None.
            convention (str, optional): convention to be used for reading data. Defaults to 'eccodes'.
                                        (Only one supported so far)

        Keyword Args:
            engine (str, optional): Engine to be used for GSV retrieval: 'polytope' or 'fdb'. Defaults to 'fdb'. 
            zoom (int, optional): HEALPix grid zoom level (e.g. zoom=10 is h1024). Allows for multiple gridname definitions.
            realization (int, optional): The ensemble realization number, included in the output filename.
            **kwargs: Additional arbitrary keyword arguments to be passed as additional parameters to the intake catalog entry.

        Returns:
            Reader: A `Reader` class object.
        """

        Reader.instance = self  # record the latest instance of the class (used for accessor)

        # define the internal logger
        self.loglevel = loglevel
        self.logger = log_configure(log_level=self.loglevel, log_name='Reader')

        self.exp = exp
        self.model = model
        self.source = source
        # these infos are used by the regridder to correct define areas/weights name
        reader_kwargs = {'model': model, 'exp': exp, 'source': source}
        self.regrid_method = regrid_method
        self.nproc = nproc
        self.vert_coord = None
        self.deltat = 1  # time in seconds to be used for cumulated variables unit convrersion
        self.time_correction = False  # extra flag for correction data with cumulation time on monthly timescale
        self.aggregation = aggregation
        self.chunks = chunks

        # Preprocessing function
        self.preproc = preproc

        self.grid_area = None
        self.src_grid_area = None
        self.tgt_grid_area = None

        if streaming:
            self.streamer = Streaming(startdate=startdate,
                                      enddate=enddate,
                                      aggregation=aggregation,
                                      loglevel=self.loglevel)
            # Export streaming methods TO DO: probably useless
            self.reset_stream = self.streamer.reset
            self.stream = self.streamer.stream
        self.streaming = streaming

        self.startdate = startdate
        self.enddate = enddate

        self.sample_data = None  # used to avoid multiple calls of retrieve_plain

        # define configuration file and paths
        configurer = ConfigPath(catalog=catalog, loglevel=loglevel)
        self.configdir = configurer.configdir
        self.machine = configurer.get_machine()
        self.config_file = configurer.config_file
        self.cat, self.catalog_file, self.machine_file = configurer.deliver_intake_catalog(
            catalog=catalog, model=model, exp=exp, source=source)
        self.fixer_folder, self.grids_folder = configurer.get_reader_filenames()

        # deduce catalog name
        self.catalog = self.cat.name

        # machine dependent catalog path
        machine_paths, intake_vars = configurer.get_machine_info()

        # load the catalog
        aqua.gsv.GSVSource.first_run = True  # Hack needed to avoid double checking of paths (which would not work if on another machine using polytope)
        self.expcat = self.cat(**intake_vars)[self.model][self.exp]  # the top-level experiment entry
        self.esmcat = self.expcat[self.source](**kwargs) 

        # Manual safety check for netcdf sources (see #943), we output a more meaningful error message
        if isinstance(self.esmcat, intake_xarray.netcdf.NetCDFSource):
            if not files_exist(self.esmcat.urlpath):
                raise NoDataError(f"No NetCDF files available for {self.model} {self.exp} {self.source}, please check the urlpath: {self.esmcat.urlpath}")  # noqa E501

        # store the kwargs for further usage
        self.kwargs = self._check_kwargs_parameters(kwargs, intake_vars)

        # extend the unit registry
        units_extra_definition()
        # Get fixes dictionary and find them
        self.fix = fix  # fix activation flag
        self.fixer_name = self.esmcat.metadata.get('fixer_name', None)
        self.convention = convention
        if self.convention is not None and self.convention != 'eccodes':
            raise ValueError(f"Convention {self.convention} not supported, only 'eccodes' is supported so far.")

        # case to disable automatic fix
        if self.fixer_name is False:
            self.logger.warning('A False flag is specified in fixer_name metadata, disabling fix!')
            self.fix = False

        if self.fix:
            self.fixes_dictionary = load_multi_yaml(self.fixer_folder, loglevel=self.loglevel)
            self.fixes = self.find_fixes()  # find fixes for this model/exp/source
            self.tgt_datamodel = datamodel
            # Default destination datamodel
            # (unless specified in instantiating the Reader)
            if not self.tgt_datamodel:
                self.tgt_datamodel = self.fixes_dictionary["defaults"].get("dst_datamodel", None)

        # define grid names
        self.src_grid_name = self.esmcat.metadata.get('source_grid_name')
        self.tgt_grid_name = regrid

        # init the regridder and the areas
        self._configure_regridder(machine_paths, regrid=regrid, areas=areas,
                                  rebuild=rebuild, reader_kwargs=reader_kwargs)

    def _configure_regridder(self, machine_paths, regrid=False, areas=False,
                             rebuild=False, reader_kwargs=None):
        """
        Configure the regridder and generate areas and weights.

        Arguments:
            machine_paths (dict): The machine specific paths. Used to configure regridder file paths
            regrid (bool): If regrid is required. Defaults to False.
            areas (bool): If areas are required. Defaults to False.
            rebuild (bool): If weights and areas should be rebuilt. Defaults to False.
            reader_kwargs (dict): The reader kwargs.
        """

        # load and check the regrid
        if regrid or areas:

            # create the configuration dictionary
            cfg_regrid = load_multi_yaml(folder_path=self.grids_folder,
                                         definitions=machine_paths['paths'],
                                         loglevel=self.loglevel)
            cfg_regrid = {**machine_paths, **cfg_regrid}

            if self.src_grid_name is None:
                self.logger.warning('Grid metadata is not defined. Trying to access the real data')
                data = self._retrieve_plain()
                self.regridder = Regridder(cfg_regrid, data=data, loglevel=self.loglevel)
            else:
                self.logger.info('Grid metadata is %s', self.src_grid_name)
                self.regridder = Regridder(cfg_regrid, src_grid_name=self.src_grid_name, 
                                           loglevel=self.loglevel)

                if self.regridder.error:
                    self.logger.warning('Issues in the Regridder() init: trying with data')
                    data = self._retrieve_plain()
                    self.regridder = Regridder(cfg_regrid, src_grid_name=self.src_grid_name, 
                                               data=data, loglevel=self.loglevel)

            # export src space coord and vertical coord
            self.src_space_coord = self.regridder.src_horizontal_dims
            self.vert_coord = self.regridder.src_vertical_dim

            # TODO: it is likely there are other cases where we need to disable regrid.
            if not self.regridder.cdo:
                areas = False
                regrid = False

        # generate source areas
        if areas:
            # generate source areas and expose them in the reader
            self.src_grid_area = self.regridder.areas(rebuild=rebuild, reader_kwargs=reader_kwargs)
            if self.fix:
                # TODO: this should include the latitudes flipping fix.
                # TODO: No check is done on the areas coords vs data coords
                self.src_grid_area = self._fix_area(self.src_grid_area)

        # configure regridder and generate weights
        if regrid:
            # generate weights and init the SMMregridder
            self.regridder.weights(
                rebuild=rebuild,
                tgt_grid_name=self.tgt_grid_name,
                regrid_method=self.regrid_method,
                reader_kwargs=reader_kwargs)

        # generate destination areas, expost them and the associated space coordinates
        if areas and regrid:
            self.tgt_grid_area = self.regridder.areas(tgt_grid_name=self.tgt_grid_name, rebuild=rebuild)
            if self.fix:
                # TODO: this should include the latitudes flipping fix
                self.tgt_grid_area = self._fix_area(self.tgt_grid_area)
            self.tgt_space_coord = self.regridder.tgt_horizontal_dims

        # activste time statistics
        self.timemodule = TimStat(loglevel=self.loglevel)

    def retrieve(self, var=None, level=None,
                 startdate=None, enddate=None,
                 history=True, sample=False):
        """
        Perform a data retrieve.

        Arguments:
            var (str, list): the variable(s) to retrieve. Defaults to None. If None, all variables are retrieved.
            level (list, float, int): Levels to be read, overriding default in catalog source (only for FDB) .
            startdate (str): The starting date for reading/streaming the data (e.g. '2020-02-25'). Defaults to None.
            enddate (str): The final date for reading/streaming the data (e.g. '2020-03-25'). Defaults to None.
            history (bool): If you want to add to the metadata history information about retrieve. Defaults to True.
            sample (bool): read only one default variable (used only if var is not specified). Defaults to False.

        Returns:
            A xarray.Dataset containing the required data.
        """

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
                        loadvar = [self.esmcat._request['param']]  # retrieve var from catalog

                    if not isinstance(loadvar, list):
                        loadvar = [loadvar]

                    if sample:
                        # self.logger.debug("FDB source sample reading, selecting only one variable")
                        loadvar = [loadvar[0]]

                    self.logger.debug("FDB source: loading variables as %s", loadvar)

            else:
                loadvar = None

        ffdb = False
        # If this is an ESM-intake catalog use first dictionary value,
        if isinstance(self.esmcat, intake_esm.core.esm_datastore):
            data = self.reader_esm(self.esmcat, loadvar)
        # If this is an fdb entry
        elif isinstance(self.esmcat, aqua.gsv.intake_gsv.GSVSource):
            data = self.reader_fdb(self.esmcat, loadvar, startdate, enddate,
                                   dask=True, level=level)
            ffdb = True  # These data have been read from fdb
        else:
            data = self.reader_intake(self.esmcat, var, loadvar)

        # if retrieve history is required (disable for retrieve_plain)
        if history:
            if ffdb:
                fkind = "FDB"
            else:
                fkind = "file from disk"
            data = log_history(data, f"Retrieved from {self.model}_{self.exp}_{self.source} using {fkind}")


        if not ffdb:  # FDB sources already have the index, already selected levels
            data = self._add_index(data)  # add helper index
            data = self._select_level(data, level=level)  # select levels (optional)

        if self.fix:
            data = self.fixer(data, var)

        # log an error if some variables have no units
        if isinstance(data, xr.Dataset) and self.fix:
            for variable in data.data_vars:
                if not hasattr(data[variable], 'units'):
                    self.logger.warning('Variable %s has no units!', variable)

        if self.streaming:
            data = self.streamer.stream(data)
        elif startdate and enddate and not ffdb:  # do not select if data come from FDB (already done)
            data = data.sel(time=slice(startdate, enddate))

        if isinstance(data, xr.Dataset):
            data.aqua.set_default(self)  # This links the dataset accessor to this instance of the Reader class

        # Preprocessing function
        if self.preproc:
            data = self.preproc(data)

        # Add info metadata in each dataset
        info_metadata = {'AQUA_model': self.model, 'AQUA_exp': self.exp,
                         'AQUA_source': self.source, 'AQUA_catalog': self.catalog,
                         'AQUA_version': aqua_version}
        data = set_attrs(data, info_metadata)

        return data

    def _add_index(self, data):

        """
        Add a helper idx_{dim3d} coordinate to the data to be used for level selection

        Arguments:
            data (xr.Dataset):  the input xarray.Dataset

        Returns:
            A xarray.Dataset containing the data with the idx_dim{3d} coordinate.
        """

        if self.vert_coord:
            for dim in to_list(self.vert_coord):
                if dim in data.coords:
                    idx = list(range(0, len(data.coords[dim])))
                    data = data.assign_coords(**{f"idx_{dim}": (dim, idx)})
        return data

    def _select_level(self, data, level=None):
        """
        Select levels if provided. It is based on self.vert_coord but it extends the feature 
        to atmospheric levels, so it should not be considered as the same vertical coordinate

        Arguments:
            data (xr.Dataset):  the input xarray.Dataset
            level (list, int):  levels to be selected. Defaults to None.

        Returns:
            A xarray.Dataset containing the data with the selected levels.
        """

        # return if no levels are selected
        if not level:
            return data

        # find the vertical coordinate, which can be the smmregrid one or 
        # any other with a dimension compatible (Pa, cm, etc)
        full_vert_coord = find_vert_coord(data) if not self.vert_coord else self.vert_coord

        # return if no vertical coordinate is found
        if not full_vert_coord:
            self.logger.error("Levels selected but no vertical coordinate found in data!")
            return data
        
        # ensure that level is a list
        level = to_list(level)

        # do the selection on the first vertical coordinate found
        if len(full_vert_coord) > 1:
            self.logger.warning(
                "Found more than one vertical coordinate, using the first one: %s", 
                full_vert_coord[0])
        data = data.sel(**{full_vert_coord[0]: level})
        data = log_history(data, f"Selecting levels {level} from vertical coordinate {full_vert_coord[0]}")
        return data

    def set_default(self):
        """Sets this reader as the default for the accessor."""

        Reader.instance = self  # Refresh the latest reader instance used

    def regrid(self, data):
        """Call the regridder function returning container or iterator"""

        if self.tgt_grid_name is None:
            raise NoRegridError('regrid has not been initialized in the Reader, cannot perform any regrid.')
        
        data = counter_reverse_coordinate(data)

        out = self.regridder.regrid(data)

        # set regridded attribute to 1 for all vars
        out = set_attrs(out, {"AQUA_regridded": 1})
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

        return att.get("AQUA_regridded", False)

    # def _clean_spourious_coords(self, data, name=None):
    #     """
    #     Remove spurious coordinates from an xarray DataArray or Dataset.

    #     This function identifies and removes unnecessary coordinates that may
    #     be incorrectly associated with spatial coordinates, such as a time
    #     coordinate being linked to latitude or longitude.

    #     Parameters:
    #     ----------
    #     data : xarray.DataArray or xarray.Dataset
    #         The input data object from which spurious coordinates will be removed.

    #     name : str, optional
    #         An optional name or identifier for the data. This will be used in
    #         warning messages to indicate which dataset the issue pertains to.

    #     Returns:
    #     -------
    #     xarray.DataArray or xarray.Dataset
    #         The cleaned data object with spurious coordinates removed.
    #     """

    #     drop_coords = set()
    #     for coord in list(data.coords):
    #         if len(data[coord].coords)>1:
    #             drop_coords.update(koord for koord in data[coord].coords if koord != coord)
    #     if not drop_coords:
    #         return data
    #     self.logger.warning('Issue found in %s, removing %s coordinates',
    #                             name, list(drop_coords))
    #     return data.drop_vars(drop_coords)

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
            space_coord = self.tgt_space_coord
            grid_area = self.tgt_grid_area.cell_area
        else:
            space_coord = self.src_space_coord
            grid_area = self.src_grid_area.cell_area

        if lon_limits is not None or lat_limits is not None:
            data = area_selection(data, lon=lon_limits, lat=lat_limits,
                                  loglevel=self.loglevel, **kwargs)
        self.logger.debug('Space coordinates are %s', space_coord)
        # cleaning coordinates which have "multiple" coordinates in their own definition
        # grid_area = self._clean_spourious_coords(grid_area, name = "area")
        # data = self._clean_spourious_coords(data, name = "data")

        # HAVE TO ADD AN ERROR IF AREAS HAVE NOT THE SAME AREAS AS DATA

        # check if coordinates are aligned
        try:
            xr.align(grid_area, data, join='exact')
        except ValueError as err:
            # check in the dimensions what is wrong
            for coord in grid_area.coords:
                if coord in space_coord:
                    xcoord = data.coords[coord]

                    # first case: shape different
                    if len(grid_area[coord]) != len(xcoord):
                        raise ValueError(f'{coord} has different shape between area files and your dataset.'
                                         'If using the LRA, try setting the regrid=r100 option') from err
                    # shape are ok, but coords are different
                    if not grid_area[coord].equals(xcoord):
                        # if they are fine when sorted, there is a sorting mismatch
                        if grid_area[coord].sortby(coord).equals(xcoord.sortby(coord)):
                            self.logger.warning('%s is sorted in different way between area files and your dataset. Flipping it!', # noqa E501
                                                coord)
                            grid_area = grid_area.reindex({coord: list(reversed(grid_area[coord]))})
                        else:
                            raise ValueError(f'{coord} has a mismatch in coordinate values!') from err

        out = data.weighted(weights=grid_area.fillna(0)).mean(dim=space_coord)

        out.aqua.set_default(self)  # This links the dataset accessor to this instance of the Reader class

        log_history(data, f"Spatially averaged from {self.src_grid_name} grid")

        return out

    def _check_kwargs_parameters(self, main_parameters, intake_parameters):
        """
        Function to check if which parameters are included in the metadata of
        the source and performs a few safety checks.

        Args:
            main_parameters: get them from kwargs
            intake_parameters: get them from catalog machine specific file

        Returns:
            kwargs after check has been processed
        """
        # join the kwargs
        parameters = {**main_parameters, **intake_parameters}

        # remove null kwargs
        parameters = {key: value for key, value in parameters.items() if value is not None}

        user_parameters = self.esmcat.describe().get('user_parameters')
        if user_parameters is not None:
            if parameters is None:
                parameters = {}

            for param in user_parameters:
                if param['name'] not in parameters:
                    self.logger.warning('%s parameter is required but is missing, setting to default %s',
                                        param['name'], param['default'])
                    parameters[param['name']] = param['default']

        return parameters

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

        final = log_history(
            final, f"Interpolated from original levels {data[vert_coord].values} {data[vert_coord].units} to level {levels} using {method} method.") # noqa E501

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
        Read fdb data. Returns a dask array.

        Args:
            esmcat (intake catalog): the intake catalog to read
            var (str, int or list): the variable(s) to read
            startdate (str): a starting date and time in the format YYYYMMDD:HHTT
            enddate (str): an ending date and time in the format YYYYMMDD:HHTT
            dask (bool): return directly a dask array
            level (list, float, int): level to be read, overriding default in catalog

        Returns:
            An xarray.Dataset 
        """
        # Var can be a list or a single one of these cases:
        # - an int, in which case it is a paramid
        # - a str, in which case it is a short_name that needs to be matched with the paramid
        # - a list (in this case I may have a list of lists) if fix=True and the original variable
        #   found a match in the source field of the fixer dictionary
        request = esmcat._request
        var = to_list(var)
        var_match = []

        fdb_var = esmcat.metadata.get('variables', None)
        # This is a fallback for the case in which no 'variables' metadata is defined
        # It is a backward compatibility feature and it may be removed in the future
        # We need to access with describe because the 'param' element is not a class
        # attribute. I would not add it since it is a deprecated feature
        if fdb_var is None:
            self.logger.warning("No 'variables' metadata defined in the catalog, this is deprecated!")
            fdb_var = esmcat.describe()["args"]["request"]["param"]
            fdb_var = to_list(fdb_var)

        # We avoid the following loop if the user didn't specify any variable
        # We make sure this is the case by checking that var is the same as fdb_var
        # If we need to loop, two cases may arise:
        # 1. fix=True: if elem is a paramid we try to match it with the list on fdb_var
        #              if is a str we scan in the fixer dictionary if there is a match
        #              and we use the paramid listed in the source block to match with fdb_var
        #              As a final fallback, if the scan fails, we use the initial str as a match
        #              letting eccodes itself to find the paramid (this may lead to errors)
        # 2. fix=False: we just scan the list of variables requested by the user.
        #               For paramids we do as case 1, while for str we just do as in the fallback
        #               option defined in case 1
        # We're trying to set the if/else by int vs str and then eventually by the fix option
        # We store the fixer_dict once for all for semplicity of the if case.
        if self.fix is True:
            fixer_dict = self.fixes.get('vars', {})
            if fixer_dict == {}:
                self.logger.debug("No 'vars' block in the fixer, guessing variable names base on ecCodes")
        if var != fdb_var:
            for element in var:
                # We catch also the case where we ask for var='137' but we know that is a paramid
                if isinstance(element, int) or (isinstance(element, str) and element.isdigit()):
                    element = int(element) if isinstance(element, str) else element
                    element = to_list(element)
                    match = list(set(fdb_var) & set(element))
                    if match and len(match) == 1:
                        var_match.append(match[0])
                    elif match and len(match) > 1:
                        self.logger.warning('Multiple matches found for %s, using the first one', element)
                        var_match.append(match[0])
                    else:
                        self.logger.warning('No match found for %s, skipping it', element)
                elif isinstance(element, str):
                    if self.fix is True:
                        if element in fixer_dict:
                            src_element = fixer_dict[element].get('source', None)
                            derived_element = fixer_dict[element].get('derived', None)
                            if derived_element is not None or src_element is None:  # We let eccodes to find the paramid
                                var_match.append(derived_element)
                            else:  # src_element is not None and it is not a derived variable
                                match = list(set(fdb_var) & set(src_element))
                                if match and len(match) == 1:
                                    var_match.append(match[0])
                                elif match and len(match) > 1:
                                    self.logger.warning('Multiple paramids found for %s: %s, using: %s',
                                                        element, match, match[0])
                                    var_match.append(match[0])
                                else:
                                    self.logger.warning('No match found for %s, using eccodes to find the paramid',
                                                        element)
                                    var_match.append(element)
                    else:
                        var_match.append(element)
                elif isinstance(element, list):
                    if self.fix is False:
                        raise ValueError(f"Var {element} is a list and fix is False, this is not allowed")
                    match = list(set(fdb_var) & set(element))
                    if match and len(match) == 1:
                        var_match.append(match[0])
                    elif match and len(match) > 1:
                        self.logger.warning('Multiple matches found for %s, using the first one', element)
                        var_match.append(match[0])
                    else:
                        self.logger.error('No match found for %s, skipping it', element)
                else:  # Something weird is happening, we may want to have a raise instead
                    self.logger.error("Element %s is not a valid type, skipping it", element)
        else:  # There is no need to scan the list of variables, total match
            var_match = var

        if var_match == []:
            self.logger.error("No match found for the variables you are asking for!")
            self.logger.error("Please be sure the metadata 'variables' is defined in the catalog")
            var_match = var
        else:
            self.logger.debug("Found variables: %s", var_match)

        var = var_match
        self.logger.debug("Requesting variables: %s", var)

        if level and not isinstance(level, list):
            level = [level]

        # for streaming emulator
        if self.aggregation and not dask:
            chunks = self.aggregation
        else:
            chunks = self.chunks

        if isinstance(chunks, dict):
            if self.aggregation and not chunks.get('time'):
                chunks['time'] = self.aggregation
            if self.streaming and not self.aggregation:
                self.logger.warning(
                    "Aggregation is not set, using default time resolution for streaming. If you are asking for a longer chunks['time'] for GSV access, please set a suitable aggregation value") # noqa E501

        if dask:
            if chunks:  # if the chunking or aggregation option is specified override that from the catalog
                data = esmcat(request=request, startdate=startdate, enddate=enddate, var=var, level=level,
                              chunks=chunks, logging=True, loglevel=self.loglevel).to_dask()
            else:
                data = esmcat(request=request, startdate=startdate, enddate=enddate, var=var, level=level,
                              logging=True, loglevel=self.loglevel).to_dask()
        else:
            if chunks:
                data = esmcat(request=request, startdate=startdate, enddate=enddate, var=var, level=level,
                              chunks=chunks, logging=True, loglevel=self.loglevel).read_chunked()
            else:
                data = esmcat(request=request, startdate=startdate, enddate=enddate, var=var, level=level,
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
        # The coder introduces the possibility to specify a time decoder for the time axis
        if 'time_coder' in esmcat.metadata:
            coder = xr.coders.CFDatetimeCoder(time_unit=esmcat.metadata['time_coder'])
            esmcat.xarray_kwargs.update({'decode_times': coder})

        data = esmcat.to_dask()

        if loadvar:
            loadvar = to_list(loadvar)
            loadvar_match = []
            for element in loadvar:
                # Having to do a list comprehension we want to be sure that the element is a list
                element = to_list(element)
                match = list(set(data.data_vars) & set(element))

                if match:
                    loadvar_match.append(match[0])
                else:
                    self.logger.warning('No match found for %s', element)
            loadvar = loadvar_match

            if all(element in data.data_vars for element in loadvar):
                data = data[loadvar]
            else:
                try:
                    data = data[var]
                    self.logger.warning("You are asking for var %s but the fixes definition requires %s, which is not there.",
                                        var, loadvar)
                    self.logger.warning("Retrieving %s, but it would be safer to run with fix=False or to correct the fixes",
                                        var)
                except Exception as e:
                    raise KeyError("You are asking for variables which we cannot find in the catalog!") from e

        # check for duplicates
        if 'time' in data.coords:
            len0 = len(data.time)
            data = data.drop_duplicates(dim='time', keep=keep)
            if len(data.time) != len0:
                self.logger.warning("Duplicate entries found along the time axis, keeping the %s one.", keep)

        return data

    @contextmanager
    def _temporary_attrs(self, **kwargs):
        """Temporarily override Reader attributes, restoring them afterward."""
        original_values = {key: getattr(self, key) for key in kwargs}
        try:
            for key, value in kwargs.items():
                setattr(self, key, value)
            yield
        finally:
            for key, value in original_values.items():
                setattr(self, key, value)

    def _retrieve_plain(self, *args, **kwargs):
        """
        Retrieve data without any additional processing.
        Making use of GridInspector, provide a sample data which has minimum
        size by subselecting along variables and time dimensions 

        Args:
            *args: arguments to be passed to retrieve
            **kwargs: keyword arguments to be passed to retrieve

        Returns:
            A xarray.Dataset containing the required miminal sample data.
        """
        if self.sample_data is not None:
            self.logger.debug('Sample data already availabe, avoid _retrieve_plain()')
            return self.sample_data

        # Temporarily disable unwanted settings
        with self._temporary_attrs(aggregation=None, chunks=None, fix=False, streaming=False,
                                   startdate=None, enddate=None, preproc=None):
            self.logger.debug('Getting sample data through _retrieve_plain()...')
            data = self.retrieve(history=False, *args, **kwargs)

        self.sample_data = self._grid_inspector(data)
        return self.sample_data

    def _grid_inspector(self, data):
        """
        Use smmregrid GridInspector to get minimal sample data

        Args:
            data (xarray.Dataset): input data

        Returns:
            A xarray.Dataset containing the required miminal sample data.
        """

        # this could be a method of the GridInspector class
        def get_gridtype_attr(gridtypes, attr):
            """Helper compact tool to extra gridtypes information"""
            out = []
            for gridtype in gridtypes:
                value = getattr(gridtype, attr, None)
                if isinstance(value, (list, tuple)):
                    out.extend(value)
                elif isinstance(value, dict):
                    out.extend(value.keys())
                elif isinstance(value, str):
                    out.append(value)

            return list(dict.fromkeys(out))

        # get gridtypes from smrregird
        gridtypes = GridInspector(data).get_grid_info()

        # get info on time dimensions and variables
        minimal_variables = get_gridtype_attr(gridtypes, 'variables')
        minimal_time = get_gridtype_attr(gridtypes, 'time_dims')

        if minimal_variables:
            self.logger.debug('Variables found: %s', minimal_variables)
            data = data[minimal_variables]
        if minimal_time:
            self.logger.debug('Time dimensions found: %s', minimal_time)
            data = data.isel({t: 0 for t in minimal_time})
        return data

    def info(self):
        """Prints info about the reader"""
        print(f"Reader for model {self.model}, experiment {self.exp}, source {self.source}")

        if isinstance(self.esmcat, aqua.gsv.intake_gsv.GSVSource):
            if "expver" in self.esmcat._request.keys():
                print(f"  This experiment has expID {self.esmcat._request['expver']}")

        metadata = self.esmcat.metadata

        if self.fix:
            print("Data fixing is active:")
            if "fixer_name" in metadata.keys():
                print(f"  Fixer name is {metadata['fixer_name']}")
            else:
                # TODO: to be removed when all the catalogs are updated
                print(f"  Fixes: {self.fixes}")

        if self.tgt_grid_name:
            print("Regridding is active:")
            print(f"  Target grid is {self.tgt_grid_name}")

        print("Metadata:")
        for k, v in metadata.items():
            print(f"  {k}: {v}")

        if isinstance(self.esmcat, aqua.gsv.intake_gsv.GSVSource):
            print("GSV request for this source:")
            for k, v in self.esmcat._request.items():
                if k not in ["time", "param", "step", "expver"]:
                    print("  %s: %s" % (k, v))

    def timstat(self, data, stat, freq=None, exclude_incomplete=False,
             time_bounds=False, center_time=False):
        """
        Time averaging wrapper which is calling the timstat module

        Args:
            data (xr.DataArray or xarray.Dataset):  the input data
            stat (str):  the statistical function to be applied
            freq (str):  the frequency of the time average
            exclude_incomplete (bool):  exclude incomplete time averages
            time_bounds (bool):  produce time bounds after averaging
            center_time (bool):  center time for averaging
        """

        data = self.timemodule.timstat(
            data, stat=stat, freq=freq,
            exclude_incomplete=exclude_incomplete,
            time_bounds=time_bounds,
            center_time=center_time)
        data.aqua.set_default(self) #accessor linking
        return data
    
    def timmean(self, data, **kwargs):
        return self.timstat(data, stat='mean', **kwargs)

    def timmax(self, data, **kwargs):
        return self.timstat(data, stat='max', **kwargs)
    
    def timmin(self, data, **kwargs):
       return self.timstat(data, stat='min', **kwargs)
    
    def timstd(self, data, **kwargs):
       return self.timstat(data, stat='std', **kwargs)

def units_extra_definition():
    """Add units to the pint registry"""

    # special units definition
    # needed to work with metpy 1.4.0 see
    # https://github.com/Unidata/MetPy/issues/2884
    units._on_redefinition = 'ignore'
    units.define('fraction = [] = Fraction = frac')
    units.define('psu = 1e-3 frac')
    units.define('PSU = 1e-3 frac')
    units.define('Sv = 1e+6 m^3/s')
    units.define("North = degrees_north = degreesN = degN")
    units.define("East = degrees_east = degreesE = degE")
