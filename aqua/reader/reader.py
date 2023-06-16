"""The main AQUA Reader class"""

import os
import sys
import re

import types
import intake
import intake_esm

import xarray as xr

# from metpy.units import units, DimensionalityError
import numpy as np
import smmregrid as rg

from aqua.util import load_yaml, load_multi_yaml
from aqua.util import get_reader_filenames, get_config_dir, get_machine
from aqua.util import log_history, log_history_iter
from aqua.logger import log_configure
import aqua.gsv

from .streaming import Streaming
from .fixer import FixerMixin
from .regrid import RegridMixin
from .reader_utils import check_catalog_source, group_shared_dims, set_attrs


# default spatial dimensions and vertical coordinates
default_space_dims = ['i', 'j', 'x', 'y', 'lon', 'lat', 'longitude', 'latitude',
                      'cell', 'cells', 'ncells', 'values', 'value', 'nod2', 'pix', 'elem']


# set default options for xarray
xr.set_options(keep_attrs=True)


class Reader(FixerMixin, RegridMixin):
    """General reader for NextGEMS data."""

    def __init__(self, model="ICON", exp="tco2559-ng5", source=None, freq=None,
                 regrid=None, method="ycon", zoom=None, configdir=None,
                 areas=True,  # pylint: disable=W0622
                 datamodel=None, streaming=False, stream_step=1, stream_unit='steps',
                 stream_startdate=None, rebuild=False, loglevel=None, nproc=4):
        """
        Initializes the Reader class, which uses the catalog `config/config.yaml` to identify the required data.

        Args:
            model (str, optional): Model ID. Defaults to "ICON".
            exp (str, optional): Experiment ID. Defaults to "tco2559-ng5".
            source (str, optional): Source ID. Defaults to None.
            regrid (str, optional): Perform regridding to grid `regrid`, as defined in `config/regrid.yaml`. Defaults to None.
            method (str, optional): Regridding method. Defaults to "ycon".
            zoom (int):             healpix zoom level. (Default: None)
            configdir (str, optional): Folder where the config/catalog files are located. Defaults to None.
            areas (bool, optional): Compute pixel areas if needed. Defaults to True.
            var (str or list, optional): Variable(s) to extract; "vars" is a synonym. Defaults to None.
            datamodel (str, optional): Destination data model for coordinates, overrides the one in fixes.yaml. Defaults to None.
            streaming (bool, optional): If to retrieve data in a streaming mode. Defaults to False.
            stream_step (int, optional): The number of time steps to stream the data by. Defaults to 1.
            stream_unit (str, optional): The unit of time to stream the data by (e.g. 'hours', 'days', 'months', 'years'). Defaults to 'steps'.
            stream_startdate (str, optional): The starting date for streaming the data (e.g. '2020-02-25'). Defaults to None.
            rebuild (bool, optional): Force rebuilding of area and weight files. Defaults to False.
            loglevel (str, optional): Level of logging according to logging module. Defaults to log_level_default of loglevel().
            nproc (int,optional): Number of processes to use for weights generation. Defaults to 16.

        Returns:
            Reader: A `Reader` class object.
        """

        # define the internal logger
        self.logger = log_configure(log_level=loglevel, log_name='Reader')

        self.exp = exp
        self.model = model
        self.targetgrid = regrid
        self.nproc = nproc
        self.freq = freq
        self.vert_coord = None
        self.deltat = 1
        extra = []

        self.grid_area = None
        self.src_grid_area = None
        self.dst_grid_area = None

        self.streaming = streaming
        self.streamer = Streaming(stream_step=stream_step,
                                  stream_unit=stream_unit,
                                  stream_startdate=stream_startdate,
                                  loglevel=loglevel)
        # Export streaming methods
        self.reset_stream = self.streamer.reset_stream
        self.stream = self.streamer.stream
        self.stream_generator = self.streamer.stream_generator

        if not configdir:
            self.configdir = get_config_dir()
        else:
            self.configdir = configdir
        self.machine = get_machine(self.configdir)

        # get configuration from the machine
        self.catalog_file, self.regrid_file, self.fixer_folder, self.config_file = (
            get_reader_filenames(self.configdir, self.machine))
        self.cat = intake.open_catalog(self.catalog_file)

        # check source existence
        self.source = check_catalog_source(self.cat, self.model, self.exp, source, name="catalog")

        # check that you defined zoom in a correct way
        self.zoom = self._check_zoom(zoom)

        # get fixes dictionary and find them
        self.fixes_dictionary = load_multi_yaml(self.fixer_folder)
        self.fixes = self.find_fixes()

        # Store the machine-specific CDO path if available
        cfg_base = load_yaml(self.config_file)
        self.cdo = cfg_base["cdo"].get(self.machine, "cdo")

        # load and check the regrid
        cfg_regrid = load_yaml(self.regrid_file)
        source_grid_id = check_catalog_source(cfg_regrid["source_grids"],
                                              self.model, self.exp, source, name='regrid')
        source_grid = cfg_regrid["source_grids"][self.model][self.exp][source_grid_id]

        # Normalize vert_coord to list
        self.vert_coord = source_grid.get("vert_coord", "2d")  # If not specified we assume that this is only a 2D case

        if not isinstance(self.vert_coord, list):
            self.vert_coord = [self.vert_coord]

        self.masked_att = source_grid.get("masked", None)  # Optional selection of masked variables
        self.masked_vars = source_grid.get("masked_vars", None)  # Optional selection of masked variables

        # Expose grid information for the source as a dictionary of open xarrays
        sgridpath = source_grid.get("path", None)
        if sgridpath:
            if isinstance(sgridpath, dict):
                self.src_grid = {}
                for k, v in sgridpath.items():
                    self.src_grid.update({k: xr.open_dataset(v.format(zoom=self.zoom), decode_times=False)})
            else:
                if self.vert_coord:
                    self.src_grid = {self.vert_coord[0]: xr.open_dataset(sgridpath.format(zoom=self.zoom), decode_times=False)}
                else:
                    self.src_grid = {"2d": xr.open_dataset(sgridpath.format(zoom=self.zoom), decode_times=False)}
        else:
            self.src_grid = None

        self.dst_datamodel = datamodel
        # Default destination datamodel (unless specified in instantiating the Reader)
        if not self.dst_datamodel:
            self.dst_datamodel = self.fixes_dictionary["defaults"].get("dst_datamodel", None)

        self.src_space_coord = source_grid.get("space_coord", None)
        self.space_coord = self.src_space_coord
        self.dst_space_coord = ["lon", "lat"]

        if regrid:

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

                template_file = cfg_regrid["weights"]["template"].format(model=model,
                                                                         exp=exp,
                                                                         method=method,
                                                                         target=regrid,
                                                                         source=self.source,
                                                                         level=levname)

                # add the zoom level in the template file (same as done in areas)
                if self.zoom is not None:
                    template_file = re.sub(r'\.nc', '_z' + str(self.zoom) + r'\g<0>', template_file)

                self.weightsfile.update({vc: os.path.join(
                    cfg_regrid["weights"]["path"],
                    template_file)})

                # If weights do not exist, create them
                if rebuild or not os.path.exists(self.weightsfile[vc]):
                    if os.path.exists(self.weightsfile[vc]):
                        os.unlink(self.weightsfile[vc])
                    self._make_weights_file(self.weightsfile[vc], source_grid,
                                            cfg_regrid, regrid=regrid, vert_coord=vc,
                                            extra=extra, zoom=self.zoom, method=method)

                self.weights.update({vc: xr.open_mfdataset(self.weightsfile[vc])})
                vc2 = None if vc == "2d" or vc == "2dm" else vc
                self.regridder.update({vc: rg.Regridder(weights=self.weights[vc], vert_coord=vc2, space_dims=default_space_dims)})

        if areas:

            template_file = cfg_regrid["areas"]["src_template"].format(model=model, exp=exp, source=self.source)

            # add the zoom level in the template file (same as done in weights)
            if self.zoom is not None:
                template_file = re.sub(r'\.nc', '_z' + str(self.zoom) + r'\g<0>', template_file)

            self.src_areafile = os.path.join(
                cfg_regrid["areas"]["path"],
                template_file)

            # If source areas do not exist, create them
            if rebuild or not os.path.exists(self.src_areafile):
                # Another possibility: was a "cellarea" file provided in regrid.yaml?
                cellareas = source_grid.get("cellareas", None)
                cellarea_var = source_grid.get("cellarea_var", None)
                if cellareas and cellarea_var:
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
                    cfg_regrid["areas"]["path"],
                    cfg_regrid["areas"]["dst_template"].format(grid=self.targetgrid))

                if rebuild or not os.path.exists(self.dst_areafile):
                    if os.path.exists(self.dst_areafile):
                        os.unlink(self.dst_areafile)
                    grid = cfg_regrid["target_grids"][regrid]
                    self._make_dst_area_file(self.dst_areafile, grid)

                self.dst_grid_area = xr.open_mfdataset(self.dst_areafile).cell_area

            self.grid_area = self.src_grid_area

    def retrieve(self, regrid=False, timmean=False,
                 fix=True, apply_unit_fix=True, var=None, vars=None,  # pylint: disable=W0622
                 streaming=False, stream_step=None, stream_unit=None,
                 stream_startdate=None, streaming_generator=False,
                 startdate=None, enddate=None):
        """
        Perform a data retrieve.

        Arguments:
            regrid (bool):          if to regrid the retrieved data (False)
            timmean (bool):         if to average the retrieved data (False)
            fix (bool):             if to perform a fix (var name, units, coord name adjustments) (True)
            apply_unit_fix (bool):  if to already adjust units by multiplying by a factor or adding
                                    an offset (this can also be done later with the `apply_unit_fix` method) (True)
            var (str, list):        the variable(s) to retrieve (None), vars is a synonym
                                    if None, all variables are retrieved
            streaming (bool):       if to retreive data in a streaming mode (False)
            streaming_generator (bool):  if to return a generator object for data streaming (False).
            stream_step (int):      the number of time steps to stream the data by (Default = 1)
            stream_unit (str):      the unit of time to stream the data by
                                    (e.g. 'hours', 'days', 'months', 'years') (None)
            stream_startdate (str): the starting date for streaming the data (e.g. '2020-02-25') (None)
        Returns:
            A xarray.Dataset containing the required data.
        """

        # this is done in the __init__
        # self.cat = intake.open_catalog(self.catalog_file)
        # Extract subcatalogue
        if self.zoom:
            esmcat = self.cat[self.model][self.exp][self.source](zoom=self.zoom)
        else:
            esmcat = self.cat[self.model][self.exp][self.source]

        if vars:
            var = vars

        # get loadvar
        if var:
            if isinstance(var, str):
                var = var.split()
            self.logger.info("Retrieving variables: %s", var)

            loadvar = self.get_fixer_varname(var) if fix else var
        else:
            loadvar = None

        fiter = False
        # If this is an ESM-intake catalogue use first dictionary value,
        if isinstance(esmcat, intake_esm.core.esm_datastore):
            data = self.reader_esm(esmcat, loadvar)
        # If this is an fdb entry
        elif isinstance(esmcat, aqua.gsv.intake_gsv.GSVSource):
            data = self.reader_fdb(esmcat, loadvar, startdate, enddate)
            fiter = True  # this returs an iterator
        else:
            data = self.reader_intake(esmcat, var, loadvar)  # Returns a generator object

        log_history_iter(data, "retrieved by AQUA retriever")

        # sequence which should be more efficient: decumulate - averaging - regridding - fixing

        # These do not work in the iterator case
        if not fiter:
            if self.freq and timmean:
                data = self.timmean(data)

        if self.targetgrid and regrid:
            data = self.regrid(data)
            self.grid_area = self.dst_grid_area
        if fix:
            data = self.fixer(data, apply_unit_fix=apply_unit_fix)  # fixer accepts also iterators

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

    def _regrid(self, data):
        """
        Perform regridding of the input dataset.

        Arguments:
            data (xr.Dataset):  the input xarray.Dataset
        Returns:
            A xarray.Dataset containing the regridded data.
        """

        if self.vert_coord == ["2d"]:
            datadic = {"2d": data}
        else:
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

        out = set_attrs(out, {"regridded": 1})  # set regridded attribute to 1 for all vars

        # set these two to the target grid (but they are actually not used so far)
        self.grid_area = self.dst_grid_area
        self.space_coord = ["lon", "lat"]

        log_history(out, "regridded by AQUA regridder")
        return out

    def timmean(self, data, freq=None):
        """
        Perform daily and monthly averaging

        Arguments:
            data (xr.Dataset):  the input xarray.Dataset
        Returns:
            A xarray.Dataset containing the regridded data.
        """

        if freq is None:
            freq = self.freq

        # translate frequency in pandas-style time
        if freq == 'monthly':
            resample_freq = '1M'
        elif freq == 'daily':
            resample_freq = '1D'
        elif freq == 'yearly':
            resample_freq = '1Y'
        else:
            resample_freq = freq

        try:
            # resample
            self.logger.info('Resamplig to %s frequency...', str(resample_freq))
            out = data.resample(time=resample_freq).mean()
            # for now, we set initial time of the averaging period following ECMWF standard
            # HACK: we ignore hours/sec to uniform the output structure
            proper_time = data.time.resample(time=resample_freq).min()
            out['time'] = np.array(proper_time.values, dtype='datetime64[h]')
        except ValueError:
            sys.exit('Cant find a frequency to resample, aborting!')

        # check for NaT
        if np.any(np.isnat(out.time)):
            self.logger.warning('Resampling cannot produce output for all frequency step, is your input data correct?')

        log_history(out, f"resampled to frequency {resample_freq} by AQUA timmean")
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

    def fldmean(self, data):
        """
        Perform a weighted global average.

        Arguments:
            data (xr.DataArray or xarray.DataDataset):  the input data
        Returns:
            the value of the averaged field
        """

        # If these data have been regridded we should use the destination grid info
        if self._check_if_regridded(data):
            space_coord = self.dst_space_coord
            grid_area = self.dst_grid_area
        else:
            space_coord = self.src_space_coord
            grid_area = self.src_grid_area

        # check if coordinates are aligned
        try:
            xr.align(grid_area, data, join='exact')
        except ValueError as err:
            # check in the dimensions what is wrong
            for coord in self.grid_area.coords:
                # option1: shape different
                if len(self.grid_area[coord]) != len(data.coords[coord]):
                    raise ValueError(f'{coord} has different shape between area files and your dataset.'
                                     'If using the LRA, try setting the regrid=r100 option') from err
                # shape are ok, but coords are different
                if not self.grid_area[coord].equals(data.coords[coord]):
                    # if they are fine when sorted, there is a sorting mismatch
                    if self.grid_area[coord].sortby(coord).equals(data.coords[coord].sortby(coord)):
                        raise ValueError(f'{coord} is sorted in different way between area files and your dataset.') from err
                    # something else
                    raise ValueError(f'{coord} has a mismatch in coordinate values!') from err

        out = data.weighted(weights=grid_area.fillna(0)).mean(dim=space_coord)

        return out

    def _check_zoom(self, zoom):

        """
        Function to check if the zoom parameter is included in the metadata of the
        source and performs a few safety checks.
        It could be extended to any other metadata flag.

        Arguments:
            zoom (integer):

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

    def vertinterp(self, data, levels=None, vert_coord='plev', units=None, method='linear'):
        """
        A basic vertical interpolation based on interp function
        of xarray within AQUA. Given an xarray object, will interpolate the vertical dimension along
        the vert_coord. If it is a Dataset, only variables with the required vertical coordinate
        will be interpolated

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
                                      zarr_kwargs=dict(consolidated=True),
                                      # decode_times=True,
                                      # use_cftime=True)
                                      progressbar=False
                                      )
        return list(data.values())[0]

    def reader_fdb(self, esmcat, var, startdate, enddate):
        """Read fdb data. Returns an iterator."""
        # These are all needed in theory

        if not enddate:
            enddate = startdate
        return esmcat(startdate=startdate, enddate=enddate, var=var).read_chunked()

    def reader_intake(self, esmcat, var, loadvar):
        """Read regular intake entry. Returns dataset."""
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
        return data
