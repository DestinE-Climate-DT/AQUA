"""The main AQUA Reader class"""

import os
import sys
import re

import intake
import intake_esm
import xarray as xr

# from metpy.units import units, DimensionalityError
import numpy as np
import smmregrid as rg

from aqua.util import load_yaml, load_multi_yaml
from aqua.util import get_reader_filenames, get_config_dir, get_machine
from aqua.util import log_history
from aqua.logger import log_configure

from .streaming import Streaming
from .fixer import FixerMixin
from .regrid import RegridMixin
from .reader_utils import check_catalog_source


class Reader(FixerMixin, RegridMixin):
    """General reader for NextGEMS data."""

    def __init__(self, model="ICON", exp="tco2559-ng5", source=None, freq=None,
                 regrid=None, method="ycon", zoom=None, configdir=None,
                 level=None, areas=True,  # pylint: disable=W0622
                 datamodel=None, streaming=False, stream_step=1, stream_unit='steps',
                 stream_startdate=None, rebuild=False, loglevel=None):
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
            level (int, optional): Level to extract if input data are 3D (starting from 0). Defaults to None.
            areas (bool, optional): Compute pixel areas if needed. Defaults to True.
            var (str or list, optional): Variable(s) to extract; "vars" is a synonym. Defaults to None.
            datamodel (str, optional): Destination data model for coordinates, overrides the one in fixes.yaml. Defaults to None.
            streaming (bool, optional): If to retrieve data in a streaming mode. Defaults to False.
            stream_step (int, optional): The number of time steps to stream the data by. Defaults to 1.
            stream_unit (str, optional): The unit of time to stream the data by (e.g. 'hours', 'days', 'months', 'years'). Defaults to 'steps'.
            stream_startdate (str, optional): The starting date for streaming the data (e.g. '2020-02-25'). Defaults to None.
            rebuild (bool, optional): Force rebuilding of area and weight files. Defaults to False.
            loglevel (str, optional): Level of logging according to logging module. Defaults to log_level_default of loglevel().

        Returns:
            Reader: A `Reader` class object.
        """

        # define the internal logger
        self.logger = log_configure(log_level=loglevel, log_name='Reader')

        self.exp = exp
        self.model = model
        self.targetgrid = regrid
        self.zoom = zoom
        self.freq = freq
        self.level = level
        self.vertcoord = None
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
        self.catalog_file, self.regrid_file, self.fixer_folder, self.config_file = get_reader_filenames(self.configdir, self.machine)
        self.cat = intake.open_catalog(self.catalog_file)

        # check source existence
        self.source = check_catalog_source(self.cat, self.model, self.exp, source, name="catalog")

        # safe check for zoom into the catalog parameters
        if 'zoom' in self.cat[self.model][self.exp].metadata.get('parameters', {}).keys() and self.zoom is None:

            self.logger.warning('No zoom specified but the source requires it, setting zoom=0')
            self.zoom = 0

        # get fixes dictionary and find them
        self.fixes_dictionary = load_multi_yaml(self.fixer_folder)
        self.fixes = self.find_fixes()

        # Store the machine-specific CDO path if available
        cfg_base = load_yaml(self.config_file)
        self.cdo = cfg_base["cdo"].get(self.machine, "cdo")

        # load and check the regrid
        cfg_regrid = load_yaml(self.regrid_file)
        source_grid_id = check_catalog_source(cfg_regrid["source_grids"], self.model, self.exp, source, name='regrid')
        source_grid = cfg_regrid["source_grids"][self.model][self.exp][source_grid_id]
        self.vertcoord = source_grid.get("vertcoord", None)  # Some more checks needed

        # Expose grid information for the source
        sgridpath = source_grid.get("path", None)
        if sgridpath:
            self.src_grid = xr.open_dataset(sgridpath.format(zoom=self.zoom), decode_times=False)
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
            if level is not None:
                if not self.vertcoord:
                    raise KeyError("You should specify a vertcoord key in regrid.yaml for this source to use levels.")
                extra = f"-sellevidx,{level+1} "

            if (level is None) and self.vertcoord:
                raise RuntimeError("This is a masked 3d source: you should specify a specific level.")

            template_file = cfg_regrid["weights"]["template"].format(model=model,
                                                         exp=exp,
                                                         method=method,
                                                         target=regrid,
                                                         source=self.source,
                                                         level=("2d" if level is None else level))

            # add the zoom level in the template file (same as done in areas)
            if self.zoom is not None:
                template_file = re.sub(r'\.nc', '_z' + str(self.zoom) + r'\g<0>', template_file)

            self.weightsfile = os.path.join(
                cfg_regrid["weights"]["path"],
                template_file)

            # If weights do not exist, create them
            if rebuild or not os.path.exists(self.weightsfile):
                if os.path.exists(self.weightsfile):
                    os.unlink(self.weightsfile)
                self._make_weights_file(self.weightsfile, source_grid,
                                        cfg_regrid, regrid=regrid,
                                        extra=extra, zoom=self.zoom)

            self.weights = xr.open_mfdataset(self.weightsfile)
            self.regridder = rg.Regridder(weights=self.weights)

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
                 stream_startdate=None, streaming_generator=False):
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
        #self.cat = intake.open_catalog(self.catalog_file)
        # Extract subcatalogue
        if self.zoom:
            esmcat = self.cat[self.model][self.exp][self.source](zoom=self.zoom)
        else:
            esmcat = self.cat[self.model][self.exp][self.source]

        if vars:
            var = vars

        # Extract data from cat.
        # If this is an ESM-intake catalogue use first dictionary value,
        # else extract directly a dask dataset
        if isinstance(esmcat, intake_esm.core.esm_datastore):
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
            data = list(data.values())[0]
        else:
            data = esmcat.to_dask()

            if var:
                # conversion to list guarantee that Dataset is produced
                if isinstance(var, str):
                    var = var.split()

                # get loadvar
                loadvar = self.get_fixer_varname(var) if fix else var

                if all(element in data.data_vars for element in loadvar):
                    data = data[loadvar]
                else:
                    raise KeyError("You are asking for variables which we cannot find in the catalog!")

        # select only a specific level when reading. Level coord names defined in regrid.yaml
        if self.level is not None:
            data = data.isel({self.vertcoord: self.level})

        log_history(data, "retrieved by AQUA retriever")

        # sequence which should be more efficient: decumulate - averaging - regridding - fixing
        #if decumulate:
            # data = data.map(self.decumulate, keep_attrs=True)
            # data = data.map(self.decumulate)
        if self.freq and timmean:
            data = self.timmean(data)
        if self.targetgrid and regrid:
            data = self.regridder.regrid(data)
            self.grid_area = self.dst_grid_area
        if fix:   # Do not change easily this order. The fixer assumes to be after regridding
            data = self.fixer(data, apply_unit_fix=apply_unit_fix)
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
        if var:
            data = data[var]

        return data

    def regrid(self, data):
        """
        Perform regridding of the input dataset.

        Arguments:
            data (xr.Dataset):  the input xarray.Dataset
        Returns:
            A xarray.Dataset containing the regridded data.
        """

        out = self.regridder.regrid(data)

        out.attrs["regridded"] = 1
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

    ## TODO: this is not used anymore, check if it can be deleted
    # def _check_if_accumulated_auto(self, data):
    #     """To check if a DataArray is accumulated.
    #     Arbitrary check on the first 20 timesteps"""

    #     # randomly pick a few timesteps from a gridpoint
    #     ndims = [dim for dim in data.dims if data[dim].size > 1][1:]
    #     pindex = {dim: 0 for dim in ndims}

    #     # extract the first 20 timesteps and do the derivative
    #     check = data.isel(pindex).isel(time=slice(None, 20)).diff(dim='time').values

    #     # check all derivative are positive or all negative
    #     condition = (check >= 0).all() or (check <= 0).all()

    #     return condition

    # def _check_if_accumulated(self, data):
    #     """To check if a DataArray is accumulated.
    #     On a list of variables defined by the GRIB names

    #     Args:
    #         data (xr.DataArray): field to be processed

    #     Returns:
    #         bool: True if decumulation is necessary, False if no
    #     """

    #     decumvars = ['tp', 'e', 'slhf', 'sshf',
    #                  'tsr', 'ttr', 'ssr', 'str',
    #                  'tsrc', 'ttrc', 'ssrc', 'strc',
    #                  'tisr', 'tprate', 'mer', 'tp', 'cp', 'lsp']

    #     if data.name in decumvars:
    #         return True
    #     else:
    #         return False

    # def decumulate(self, data, cumulation_time=None, check=True):
    #     """
    #     Test function to remove cumulative effect on IFS fluxes.
    #     Cumulation times are estimated from the intervals of the data, but
    #     can be specified manually

    #     Args:
    #         data (xr.DataArray):     field to be processed
    #         cumulation_time (float): optional, specific cumulation time
    #         check (bool):            if to check if the variable needs to be decumulated

    #     Returns:
    #         A xarray.DataArray where the cumulation time has been removed
    #     """
    #     if check:
    #         if not self._check_if_accumulated(data):
    #             return data

    #     # which frequency are the data?
    #     if not cumulation_time:
    #         cumulation_time = (data.time[1]-data.time[0]).values/np.timedelta64(1, 's')

    #     # get the derivatives
    #     deltas = data.diff(dim='time') / cumulation_time

    #     # add a first timestep empty to align the original and derived fields
    #     zeros = xr.zeros_like(data.isel(time=0))
    #     deltas = xr.concat([zeros, deltas], dim='time').transpose('time', ...)

    #     # universal mask based on the change of month (shifted by one timestep)
    #     mask = ~(data['time.month'] != data['time.month'].shift(time=1))
    #     mask = mask.shift(time=1, fill_value=False)

    #     # check which records are kept
    #     # print(data.time[~mask])

    #     # kaboom: exploit where
    #     clean = deltas.where(mask, data/cumulation_time)

    #     # remove the first timestep (no sense in cumulated)
    #     clean = clean.isel(time=slice(1, None))

    #     # rollback the time axis by half the cumulation time
    #     clean['time'] = clean.time - np.timedelta64(int(cumulation_time/2), 's')

    #     # WARNING: HACK FOR EVAPORATION
    #     # print(clean.units)
    #     if clean.units == 'm of water equivalent':
    #         clean.attrs['units'] = 'm'

    #     # use metpy units to divide by seconds
    #     new_units = (units(clean.units)/units('s'))

    #     # usual case for radiative fluxes
    #     try:
    #         clean.attrs['units'] = str(new_units.to('W/m^2').units)
    #     except DimensionalityError:
    #         clean.attrs['units'] = str(new_units.units)

    #     # add an attribute that can be later used to infer about decumulation
    #     clean.attrs['decumulated'] = 1

    #     return clean
