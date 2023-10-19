"""The main AQUA Reader class"""

import os
import re
import io
import sys

import types
import tempfile
import shutil
import intake
import intake_esm

import xarray as xr

# from metpy.units import units, DimensionalityError
import numpy as np
import smmregrid as rg

from aqua.util import load_yaml, load_multi_yaml
from aqua.util import ConfigPath, area_selection
from aqua.logger import log_configure, log_history, log_history_iter
from aqua.util import check_chunk_completeness, frequency_string_to_pandas
from aqua.util import flip_lat_dir
import aqua.gsv

from .streaming import Streaming
from .fixer import FixerMixin
from .regrid import RegridMixin
from .reader_utils import check_catalog_source, group_shared_dims, set_attrs


# default spatial dimensions and vertical coordinates
default_space_dims = ['i', 'j', 'x', 'y', 'lon', 'lat', 'longitude',
                      'latitude', 'cell', 'cells', 'ncells', 'values',
                      'value', 'nod2', 'pix', 'elem']


# set default options for xarray
xr.set_options(keep_attrs=True)


class Reader(FixerMixin, RegridMixin):
    """General reader for NextGEMS data."""

    def __init__(self, model=None, exp=None, source=None, freq=None, fix=True,
                 regrid=None, method="ycon", zoom=None, configdir=None,
                 areas=True,  # pylint: disable=W0622
                 datamodel=None, streaming=False, stream_step=1, stream_unit='steps',
                 stream_startdate=None, rebuild=False, loglevel=None, nproc=4, aggregation=None,
                 verbose=False, exclude_incomplete=False,
                 buffer=None):
        """
        Initializes the Reader class, which uses the catalog
        `config/config.yaml` to identify the required data.

        Args:
            model (str, optional): Model ID. Defaults to "ICON".
            exp (str, optional): Experiment ID. Defaults to "tco2559-ng5".
            source (str, optional): Source ID. Defaults to None.
            regrid (str, optional): Perform regridding to grid `regrid`, as defined in `config/regrid.yaml`. Defaults to None.
            method (str, optional): Regridding method. Defaults to "ycon".
            fix (bool, optional): Activate data fixing
            zoom (int): healpix zoom level. (Default: None)
            configdir (str, optional): Folder where the config/catalog files are located. Defaults to None.
            areas (bool, optional): Compute pixel areas if needed. Defaults to True.
            var (str or list, optional): Variable(s) to extract; "vars" is a synonym. Defaults to None.
            datamodel (str, optional): Destination data model for coordinates, overrides the one in fixes.yaml. Defaults to None.
            freq (str, optional): Frequency of the time averaging. Valid values are monthly, daily, yearly. Defaults to None.
            streaming (bool, optional): If to retrieve data in a streaming mode. Defaults to False.
            stream_step (int, optional): The number of time steps to stream the data by. Defaults to 1.
            stream_unit (str, optional): The unit of time to stream the data by (e.g. 'hours', 'days', 'months', 'years'). Defaults to 'steps'.
            stream_startdate (str, optional): The starting date for streaming the data (e.g. '2020-02-25'). Defaults to None.
            rebuild (bool, optional): Force rebuilding of area and weight files. Defaults to False.
            loglevel (str, optional): Level of logging according to logging module. Defaults to log_level_default of loglevel().
            nproc (int,optional): Number of processes to use for weights generation. Defaults to 16.
            aggregation (str, optional): aggregation/chunking to be used for GSV access (e.g. D, M, Y). Defaults to None (using default from catalogue, recommended).
            verbose (bool, optional): if to print to screen additional info (used only for FDB access at the moment)
            exclude_incomplete (bool, optional): when using timmean() method, remove incomplete chunk from averaging. Default to False. 
            buffer (str or bool, optional): buffering of FDB/GSV streams in a temporary directory specified by the keyword. The result will be a dask array and not an iterator. Can be simply a boolean True for memory buffering.

        Returns:
            Reader: A `Reader` class object.
        """

        # define the internal logger
        self.loglevel = loglevel
        self.logger = log_configure(log_level=self.loglevel, log_name='Reader')

        self.exp = exp
        self.model = model
        self.targetgrid = regrid
        self.nproc = nproc
        self.freq = freq
        self.vert_coord = None
        self.deltat = 1
        self.aggregation = aggregation
        self.verbose = verbose
        self.exclude_incomplete = exclude_incomplete
        extra = []

        self.grid_area = None
        self.src_grid_area = None
        self.dst_grid_area = None

        self.streaming = streaming
        self.streamer = Streaming(stream_step=stream_step,
                                  stream_unit=stream_unit,
                                  stream_startdate=stream_startdate,
                                  loglevel=self.loglevel)

        # Export streaming methods TO DO: probably useless
        self.reset_stream = self.streamer.reset_stream
        self.stream = self.streamer.stream
        self.stream_generator = self.streamer.stream_generator

        self.previous_data = None  # used for FDB iterator fixing

        if buffer and buffer is not True:  # optional FDB buffering
            if not os.path.isdir(buffer):
                raise ValueError("The directory specified by buffer must exist.") 
            self.buffer = tempfile.TemporaryDirectory(dir=buffer)
        elif buffer is True:
            self.buffer = True
        else:
            self.buffer = None

        # define configuration file and paths
        Configurer = ConfigPath(configdir=configdir)
        self.configdir = Configurer.configdir
        self.machine = Configurer.machine

        # get configuration from the machine
        self.catalog_file, self.regrid_file, self.fixer_folder, self.config_file = (
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
        self.fix = fix # fix activation flag
        if self.fix:
            self.fixes_dictionary = load_multi_yaml(self.fixer_folder)
            self.fixes = self.find_fixes() # find fixes for this model/exp/source

        # Store the machine-specific CDO path if available
        cfg_base = load_yaml(self.config_file)
        self.cdo = cfg_base["cdo"].get(self.machine, None)
        if not self.cdo:
            self.cdo = shutil.which("cdo")
            if self.cdo:
                self.logger.debug("Found CDO path: %s", self.cdo)
            else:
                self.logger.error("CDO not found in path: Weight and area generation will fail.")
        else:
            self.logger.debug("Using CDO from config: %s", self.cdo)

        if self.fix:
            self.dst_datamodel = datamodel
            # Default destination datamodel
            # (unless specified in instantiating the Reader)
            if not self.dst_datamodel:
                self.dst_datamodel = self.fixes_dictionary["defaults"].get("dst_datamodel", None)

        # load and check the regrid
        if regrid or areas:
            # New load of regrid.yaml split in multiples folders
            main_file = os.path.join(self.configdir, 'aqua-grids.yaml')
            machine_file = os.path.join(self.configdir, 'machines', self.machine, 'regrid.yaml')

            cfg_regrid = load_multi_yaml(filenames=[main_file, machine_file],
                                         definitions="paths",
                                         loglevel=self.loglevel)

            source_grid_id = check_catalog_source(cfg_regrid["sources"],
                                                  self.model, self.exp,
                                                  self.source, name='regrid')
            source_grid = cfg_regrid['grids'][cfg_regrid['sources'][self.model][self.exp][source_grid_id]]
            source_grid_name = cfg_regrid['sources'][self.model][self.exp][source_grid_id]

            # Normalize vert_coord to list
            self.vert_coord = source_grid.get("vert_coord", "2d")  # If not specified we assume that this is only a 2D case

            if not isinstance(self.vert_coord, list):
                self.vert_coord = [self.vert_coord]

            self.masked_att = source_grid.get("masked", None)  # Optional selection of masked variables
            self.masked_vars = source_grid.get("masked_vars", None)  # Optional selection of masked variables

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

            self.src_space_coord = source_grid.get("space_coord", None)
            if self.src_space_coord is None:
                    self.src_space_coord = self._guess_space_coord(default_space_dims)

            self.support_dims = source_grid.get("support_dims", [])
            self.space_coord = self.src_space_coord

        if regrid:
            self.dst_space_coord = ["lon", "lat"]

            self.weightsfile = {}
            self.weights = {}
            self.regridder = {}

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
                    template_file = cfg_regrid["weights"]["template_grid"].format(sourcegrid=source_grid_name,
                                                                                  method=method,
                                                                                  targetgrid=regrid,
                                                                                  level=levname)   
                else: 
                    template_file = cfg_regrid["weights"]["template_default"].format(model=model,
                                                                                   exp=exp,
                                                                                   source=source,
                                                                                   method=method,
                                                                                   targetgrid=regrid,
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
                                            cfg_regrid, regrid=regrid,
                                            vert_coord=vc, extra=extra,
                                            zoom=self.zoom, method=method)

                self.weights.update({vc: xr.open_mfdataset(self.weightsfile[vc])})
                vc2 = None if vc == "2d" or vc == "2dm" else vc
                self.regridder.update({vc: rg.Regridder(weights=self.weights[vc],
                                                        vert_coord=vc2,
                                                        space_dims=default_space_dims)})

        if areas:
            if sgridpath:
                template_file = cfg_regrid["areas"]["template_grid"].format(grid = source_grid_name)
            else:
                template_file = cfg_regrid["areas"]["template_default"].format(model=model,
                                                                               exp=exp,
                                                                               source=source)                                                                                   
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
                # Another possibility: was a "cellarea" file provided in regrid.yaml?
                cellareas = source_grid.get("cellareas", None)
                cellarea_var = source_grid.get("cellarea_var", None)
                if cellareas and cellarea_var:
                    self.logger.warning("Using cellareas file provided in regrid.yaml")
                    xr.open_mfdataset(cellareas)[cellarea_var].rename("cell_area").squeeze().to_netcdf(self.src_areafile)
                else:
                    # We have to reconstruct it
                    if os.path.exists(self.src_areafile):
                        os.unlink(self.src_areafile)
                    self._make_src_area_file(self.src_areafile, source_grid,
                                             gridpath=cfg_regrid["cdo-paths"]["download"],
                                             icongridpath=cfg_regrid["cdo-paths"]["icon"],
                                             zoom=self.zoom)

            self.src_grid_area = xr.open_mfdataset(self.src_areafile).cell_area

            if regrid:
                self.dst_areafile = os.path.join(
                    cfg_regrid["paths"]["areas"],
                    cfg_regrid["areas"]["template_grid"].format(grid=self.targetgrid))

                if rebuild or not os.path.exists(self.dst_areafile):
                    if os.path.exists(self.dst_areafile):
                        os.unlink(self.dst_areafile)
                    grid = cfg_regrid["grids"][regrid]
                    self._make_dst_area_file(self.dst_areafile, grid)

                self.dst_grid_area = xr.open_mfdataset(self.dst_areafile).cell_area
                if self.fix:
                    self.dst_grid_area = self._fix_area(self.dst_grid_area)

            self.grid_area = self.src_grid_area
            if self.fix:
                self.grid_area = self._fix_area(self.grid_area)

    def retrieve(self, regrid=False, timmean=False,
                 apply_unit_fix=True, var=None, vars=None,
                 streaming=False, stream_step=None, stream_unit=None,
                 stream_startdate=None, streaming_generator=False,
                 startdate=None, enddate=None):
        """
        Perform a data retrieve.

        Arguments:
            regrid (bool):              if to regrid the retrieved data
                                        Defaults to False
            timmean (bool):             if to average the retrieved data
                                        Defaults to False
            apply_unit_fix (bool):      if to already adjust units by
                                        multiplying by a factor or adding
                                        an offset (this can also be done later
                                        with the `apply_unit_fix` method).
                                        Defaults to True
            var (str, list):            the variable(s) to retrieve.
                                        Defaults to None
                                        vars is a synonym.
                                        if None, all variables are retrieved
            streaming (bool):           if to retreive data in a streaming
                                        mode. Defaults to False
            streaming_generator (bool): if to return a generator object for
                                        data streaming. Defaults to False
            stream_step (int):          the number of time steps to stream the
                                        data by. Defaults to None
            stream_unit (str):          the unit of time to stream the data
                                        by (e.g. 'hours', 'days', 'months',
                                        'years'). Defaults to None
            stream_startdate (str):     the starting date for streaming the
                                        data (e.g. '2020-02-25').
                                        Defaults to None
        Returns:
            A xarray.Dataset containing the required data.
        """

        if stream_startdate:  # In case the streaming startdate is used also for FDB copy it
            startdate = stream_startdate

        if vars:
            var = vars

        # get loadvar
        if var:
            if isinstance(var, str):  # conversion to list guarantees that a Dataset is produced
                var = var.split()
            self.logger.info("Retrieving variables: %s", var)
            loadvar = self.get_fixer_varname(var) if self.fix else var
        else:
            if isinstance(self.esmcat, aqua.gsv.intake_gsv.GSVSource):  # If we are retrieving from fdb we have to specify the var
                var = [self.esmcat._request['param']]  # retrieve var from catalogue

                self.logger.info(f"FDB source, setting default variable to {var[0]}")
                loadvar = self.get_fixer_varname(var) if self.fix else var
            else:
                loadvar = None

        fiter = False
        # If this is an ESM-intake catalogue use first dictionary value,
        if isinstance(self.esmcat, intake_esm.core.esm_datastore):
            data = self.reader_esm(self.esmcat, loadvar)
        # If this is an fdb entry
        elif isinstance(self.esmcat, aqua.gsv.intake_gsv.GSVSource):
            data = self.reader_fdb(self.esmcat, loadvar, startdate, enddate, dask=(not streaming_generator))
            fiter = streaming_generator  # this returs an iterator unless dask is set
        else:
            data = self.reader_intake(self.esmcat, var, loadvar)  # Returns a generator object

            if var:
                if all(element in data.data_vars for element in loadvar):
                    data = data[loadvar]
                else:
                    try:
                        data = data[var]
                        self.logger.warning(f"You are asking for var {var} which is already fixed from {loadvar}.")
                        self.logger.warning(f"Would be safer to run with fix=False")
                    except:
                        raise KeyError("You are asking for variables which we cannot find in the catalog!")

        data = log_history_iter(data, "retrieved by AQUA retriever")

        # sequence which should be more efficient: decumulate - averaging - regridding - fixing

        if self.targetgrid and regrid:
            data = self.regrid(data)
            self.grid_area = self.dst_grid_area

        if self.fix:   # Do not change easily this order. The fixer assumes to be after regridding
            data = self.fixer(data, var, apply_unit_fix=apply_unit_fix)

        if self.freq and timmean:
            data = self.timmean(data, exclude_incomplete=self.exclude_incomplete)

        if fiter and self.buffer:  # We prefer an xarray, let's buffer everything
            if self.buffer is True:  # we did not provide a buffer path, use an xarray in memory
                data = self.buffer_mem(data)
            else:
                data = self.buffer_iter(data)
            fiter = False

        if not fiter:
            # This is not needed if we already have an iterator
            if streaming or self.streaming or streaming_generator:
                if streaming_generator:
                    data = self.streamer.stream_generator(data, stream_step=stream_step,
                                                          stream_unit=stream_unit,
                                                          stream_startdate=stream_startdate)
                else:
                    data = self.streamer.stream(data, stream_step=stream_step,
                                                stream_unit=stream_unit,
                                                stream_startdate=stream_startdate)

        # safe check that we provide only what exactly asked by var
        # if var:
        #    data = data[var]

        return data

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

        data = flip_lat_dir(datain)  # Check if original lat has been flipped and in case flip back, returns a deep copy in that case

        if self.vert_coord == ["2d"]:
            datadic = {"2d": data}
        else:
            self.logger.debug("Grouping variables that share the same dimension")
            self.logger.debug("Vert coord: %s", self.vert_coord)
            self.logger.debug("masked_att: %s", self.masked_att)
            self.logger.debug("masked_vars: %s", self.masked_vars)

            datadic = group_shared_dims(data, self.vert_coord, others="2d",
                                        masked="2dm", masked_att=self.masked_att,
                                        masked_vars=self.masked_vars)

        # Iterate over list of groups of variables, regridding them separately
        out = []
        for vc, dd in datadic.items():
            out.append(self.regridder[vc].regrid(dd))

        if len(out) > 1:
            out = xr.merge(out)
        else:
            # If this was a single dataarray
            out = out[0]

        # set regridded attribute to 1 for all vars
        out = set_attrs(out, {"regridded": 1})

        # set these two to the target grid
        # (but they are actually not used so far)
        self.grid_area = self.dst_grid_area
        self.space_coord = ["lon", "lat"]

        log_history(out, "regridded by AQUA regridder")
        return out
    

    def timmean(self, data, freq=None, exclude_incomplete=False, time_bounds=False):
        """Call the timmean function returning container or iterator"""
        if isinstance(data, types.GeneratorType):
            return self._timmeangen(data, freq, exclude_incomplete, time_bounds)
        else:
            return self._timmean(data, freq, exclude_incomplete, time_bounds)


    def _timmeangen(self, data, freq=None, exclude_incomplete=False, time_bounds=False):
        for ds in data:
            yield self._timmean(ds, freq, exclude_incomplete, time_bounds)


    def _timmean(self, data, freq=None, exclude_incomplete=None, time_bounds=False):
        """
        Perform daily and monthly averaging

        Arguments:
            data (xr.Dataset):  the input xarray.Dataset
            freq (str):         the frequency of the time averaging.
                                Valid values are monthly, daily, yearly. Defaults to None.
            exclude_incomplete (bool):  Check if averages is done on complete chunks, and remove from the output
                                        chunks which have not all the expected records. If None, using from Reader
            time_bound (bool):  option to create the time bounds
        Returns:
            A xarray.Dataset containing the time averaged data.
        """

        if freq is None:
            freq = self.freq
        
        if exclude_incomplete is None:
            exclude_incomplete = self.exclude_incomplete

        resample_freq = frequency_string_to_pandas(freq)

        try:
            # resample
            self.logger.info('Resampling to %s frequency...', str(resample_freq))
            out = data.resample(time=resample_freq).mean()
        except ValueError as exc:
            raise ValueError('Cant find a frequency to resample, aborting!') from exc
        
        # set time as the first timestamp of each month/day according to the sampling frequency
        out['time'] = out['time'].to_index().to_period(resample_freq).to_timestamp().values

        if exclude_incomplete:
            boolean_mask = check_chunk_completeness(data, resample_frequency=resample_freq)
            out = out.where(boolean_mask, drop=True)

        # check time is correct
        if np.any(np.isnat(out.time)):
            raise ValueError('Resampling cannot produce output for all frequency step, is your input data correct?')

        log_history(out, f"resampled to frequency {resample_freq} by AQUA timmean")

        # add a variable to create time_bounds
        if time_bounds:
            resampled = data.time.resample(time=resample_freq)
            time_bnds = xr.concat([resampled.min(),  resampled.max()], dim='bnds').transpose()
            time_bnds['time'] = out.time
            time_bnds.name = 'time_bnds'
            out = xr.merge([out, time_bnds])
            if np.any(np.isnat(out.time_bnds)):
                raise ValueError('Resampling cannot produce output for all time_bnds step!')
            log_history(out, "time_bnds added by by AQUA timmean")
       
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
                #HACK to solve minor issue in xarray
                # check https://github.com/oloapinivad/AQUA/pull/397 for further info
                if len(xcoord.coords)>1:
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
                        self.logger.warning('%s is sorted in different way between area files and your dataset. Flipping it!', coord)
                        self.grid_area = self.grid_area.reindex({coord: list(reversed(self.grid_area[coord]))})
                        # raise ValueError(f'{coord} is sorted in different way between area files and your dataset.') from err
                    # something else
                    else:
                        raise ValueError(f'{coord} has a mismatch in coordinate values!') from err

        out = data.weighted(weights=grid_area.fillna(0)).mean(dim=space_coord)

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


    def reader_fdb(self, esmcat, var, startdate, enddate, dask=False):
        """
        Read fdb data. Returns an iterator or dask array.
        Args:
            esmcat (intake catalogue): the intake catalogue to read
            var (str): the shortname of the variable to retrieve
            startdate (str): a starting date and time in the format YYYYMMDD:HHTT
            enddate (str): an ending date and time in the format YYYYMMDD:HHTT
            dask (bool): return directly a dask array instead of an iterator
        Returns:
            An xarray.Dataset or an iterator over datasets
        """

        if dask:
            if self.aggregation:
                data = esmcat(startdate=startdate, enddate=enddate, var=var,
                              aggregation=self.aggregation,
                              logging = True, verbose=self.verbose).to_dask()
            else:
                data =esmcat(startdate=startdate, enddate=enddate, var=var,
                             logging = True, verbose=self.verbose).to_dask()
        else:
            if self.aggregation:
                data = esmcat(startdate=startdate, enddate=enddate, var=var,
                              aggregation=self.aggregation,
                              logging=True, verbose=self.verbose).read_chunked()
            else:
                data = esmcat(startdate=startdate, enddate=enddate, var=var,
                              logging=True, verbose=self.verbose).read_chunked()

        return data

    def reader_intake(self, esmcat, var, loadvar, keep="first"):
        """
        Read regular intake entry. Returns dataset.

        Args:
            esmcat (intake.catalog.Catalog): your catalog
            var (str): Variable to load
            loadvar (list of str): List of variables to load
            keep (str, optional): which duplicate entry to keep ("first" (default), "last" or None)

        Returns:
            Dataset
        """

        if loadvar:
            data = esmcat.to_dask()
            if all(element in data.data_vars for element in loadvar):
                data = data[loadvar]
            else:
                try:
                    data = data[var]
                    self.logger.warning("You are asking for var %s which is already fixed from %s.", var, loadvar)
                    self.logger.warning("It would be safer to run with fix=False")
                except:
                    raise KeyError("You are asking for variables which we cannot find in the catalog!")
        else:
            data = esmcat.to_dask()

        # check for duplicates
        if 'time' in data.coords:
            len0 = len(data.time)
            data = data.drop_duplicates(dim='time', keep=keep)
            if len(data.time) != len0:
                self.logger.warning("Duplicate entries found along the time axis, keeping the %s one.", keep)

        return data

    def buffer_iter(self, data):
        """
        Buffers an iterator object into a temporary directory
        Args:
            data (iterator over xarray.Dataset): the data to be buffered

        Returns:
            A xarray.Dataset pointing to the buffered data
        """

        self.logger.info("Buffering iterator to: %s", self.buffer.name)
        niter =0
        for dd in data:
            dd.to_netcdf(f"{self.buffer.name}/iter{niter}.nc")
            niter = niter + 1

        return xr.open_mfdataset(f"{self.buffer.name}/iter*.nc")
    
    def buffer_mem(self, data):
        """
        Buffers (reads) an iterator object directly into a dataset
        Args:
            data (iterator over xarray.Dataset): the data to be buffered

        Returns:
            A xarray.Dataset
        """

        self.logger.info("Buffering iterator to memory")
        ds = next(data)  # get the first one
        try: 
            for dd in data:
                ds = xr.concat([ds, dd], dim="time")
        except StopIteration:
            pass  # The iterator has finished, we are done

        return ds
