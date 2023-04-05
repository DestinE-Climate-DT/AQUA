"""The main AQUA Reader class"""

import os
import sys
import subprocess
import tempfile
import json
import warnings
import intake
import intake_esm
import xarray as xr
import pandas as pd
from metpy.units import units, DimensionalityError
import numpy as np
import smmregrid as rg
import cf2cdm
from aqua.util import load_yaml, _eval_formula, get_eccodes_attr
from aqua.util import get_reader_filenames, get_config_dir, get_machine
from aqua.util import log_history
from aqua.logger import log_configure


class Reader():
    """General reader for NextGEMS data (on Levante for now)"""

    def __init__(self, model="ICON", exp="tco2559-ng5", source=None, freq=None,
                 regrid=None, method="ycon", zoom=None, configdir=None,
                 level=None, areas=True, var=None, vars=None,  # pylint: disable=W0622
                 datamodel=None, streaming=False, stream_step=1, stream_unit=None,
                 stream_startdate=None, rebuild=False, loglevel=None):
        """
        The Reader constructor.
        It uses the catalog `config/config.yaml` to identify the required data.

        Arguments:
            model (str):            model ID
            exp (str):              experiment ID
            source (str):           source ID
            regrid (str):           perform regridding to grid `regrid`, as defined in `config/regrid.yaml` (None)
            method (str):           regridding method (ycon)
            zoom (int):             healpix zoom level
            configdir (str)         folder where the config/catalog files are located (config)
            level (int):            level to extract if input data are 3D (starting from 0)
            areas (bool):           compute pixel areas if needed (True)
            var (str, list):        variable(s) which we will extract; vars is a synonym (None)
            datamodel (str):        destination data model for coordinates, overrides the one in fixes.yaml (None)
            streaming (bool):       if to retreive data in a streaming mode (False)
            stream_step (int):      the number of time steps to stream the data by (Default = 1)
            stream_unit (str):      the unit of time to stream the data by
                                    (e.g. 'hours', 'days', 'months', 'years') (None)
            stream_startdate (str): the starting date for streaming the data (e.g. '2020-02-25') (None)
            rebuild (bool):         force rebuilding of area and weight files
            loglevel (string):      Level of logging according to logging module
                                    (default: log_level_default of loglevel())

        Returns:
            A `Reader` class object.
        """

        # define the internal logger
        self.logger = log_configure(log_level=loglevel, log_name='Reader')

        if vars:
            var = vars
        self.var = var
        self.exp = exp
        self.model = model
        self.targetgrid = regrid
        if (exp == "hpx") and not zoom:
            zoom = 9
        self.zoom = zoom
        self.freq = freq
        self.level = level
        self.vertcoord = None
        self.deltat = 1
        extra = []

        self.grid_area = None
        self.src_grid_area = None
        self.dst_grid_area = None

        self.stream_index = 0
        self.stream_date = None
        self.streaming = streaming
        self.stream_step = stream_step
        self.stream_unit = stream_unit
        self.stream_startdate = stream_startdate

        if not configdir:
            self.configdir = get_config_dir()
        else:
            self.configdir = configdir
        self.machine = get_machine(self.configdir)

        # get configuration from the machine
        self.catalog_file, self.regrid_file, self.fixer_file = get_reader_filenames(self.configdir, self.machine)
        self.cat = intake.open_catalog(self.catalog_file)

        # load and check the regrid
        cfg_regrid = load_yaml(self.regrid_file)
        source_grid_id = _check_catalog_source(cfg_regrid["source_grids"], self.model, self.exp, source, name='regrid')
        source_grid = cfg_regrid["source_grids"][self.model][self.exp][source_grid_id]

        self.dst_datamodel = datamodel
        # Default destination datamodel (unless specified in instantiating the Reader)
        if not self.dst_datamodel:
            fixes = load_yaml(self.fixer_file)
            self.dst_datamodel = fixes["defaults"].get("dst_datamodel", None)

        self.source = _check_catalog_source(self.cat, self.model, self.exp, source, name="catalog")

        self.src_space_coord = source_grid.get("space_coord", None)
        self.space_coord = self.src_space_coord
        self.dst_space_coord = ["lon", "lat"]

        if regrid:
            self.vertcoord = source_grid.get("vertcoord", None)  # Some more checks needed
            if level is not None:
                if not self.vertcoord:
                    raise KeyError("You should specify a vertcoord key in regrid.yaml for this source to use levels.")
                extra = f"-sellevidx,{level+1} "

            if (level is None) and self.vertcoord:
                raise RuntimeError("This is a masked 3d source: you should specify a specific level.")

            self.weightsfile = os.path.join(
                cfg_regrid["weights"]["path"],
                cfg_regrid["weights"]["template"].format(model=model,
                                                         exp=exp,
                                                         method=method,
                                                         target=regrid,
                                                         source=self.source,
                                                         level=("2d" if level is None else level)))

            # If weights do not exist, create them
            if rebuild or not os.path.exists(self.weightsfile):
                if os.path.exists(self.weightsfile):
                    os.unlink(self.weightsfile)
                self._make_weights_file(self.weightsfile, source_grid,
                                        cfg_regrid, regrid=regrid,
                                        extra=extra, zoom=zoom)

            self.weights = xr.open_mfdataset(self.weightsfile)
            self.regridder = rg.Regridder(weights=self.weights)

        if areas:
            self.src_areafile = os.path.join(
                cfg_regrid["areas"]["path"],
                cfg_regrid["areas"]["src_template"].format(model=model, exp=exp, source=self.source))

            # If source areas do not exist, create them
            if rebuild or not os.path.exists(self.src_areafile):
                if os.path.exists(self.src_areafile):
                    os.unlink(self.src_areafile)
                self._make_src_area_file(self.src_areafile, source_grid,
                                         gridpath=cfg_regrid["cdo-paths"]["download"],
                                         icongridpath=cfg_regrid["cdo-paths"]["icon"],
                                         zoom=zoom)

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

    def _make_dst_area_file(self, areafile, grid):
        """Helper function to create destination (regridded) area files."""

        self.logger.warning("Destination areas file not found: %s", areafile)
        self.logger.warning("Attempting to generate it ...")

        dst_extra = f"-const,1,{grid}"
        grid_area = self.cdo_generate_areas(source=dst_extra)

        # Make sure that grid areas contain exactly the same coordinates
        data = self.retrieve(fix=False)
        data = self.regridder.regrid(data.isel(time=0))
        grid_area = grid_area.assign_coords({coord: data.coords[coord] for coord in self.dst_space_coord})

        grid_area.to_netcdf(self.dst_areafile)
        self.logger.warning("Success!")

    def _make_src_area_file(self, areafile, source_grid,
                            gridpath="", icongridpath="", zoom=None):
        """Helper function to create source area files."""

        sgridpath = source_grid.get("path", None)
        if not sgridpath:
            # there is no source grid path at all defined in the regrid.yaml file:
            # let's reconstruct it from the file itself
            data = self.retrieve(fix=False)
            temp_file = tempfile.NamedTemporaryFile(mode='w')
            sgridpath = temp_file.name
            data.isel(time=0).to_netcdf(sgridpath)
        else:
            temp_file = None
            if zoom:
                sgridpath = sgridpath.format(zoom=9-zoom)

        self.logger.warning("Source areas file not found: %s", areafile)
        self.logger.warning("Attempting to generate it ...")
        self.logger.warning("Source grid: %s", sgridpath)
        src_extra = source_grid.get("extra", [])
        grid_area = self.cdo_generate_areas(source=sgridpath,
                                            gridpath=gridpath,
                                            icongridpath=icongridpath,
                                            extra=src_extra)
        # Make sure that the new DataArray uses the expected spatial dimensions
        grid_area = self._rename_dims(grid_area, self.src_space_coord)
        data = self.retrieve(fix=False)
        grid_area = grid_area.assign_coords({coord: data.coords[coord] for coord in self.src_space_coord})
        grid_area.to_netcdf(areafile)
        self.logger.warning("Success!")

    def _make_weights_file(self, weightsfile, source_grid, cfg_regrid, regrid=None, extra=None, zoom=None):
        """Helper function to produce weights file"""

        sgridpath = source_grid.get("path", None)
        if not sgridpath:
            # there is no source grid path at all defined in the regrid.yaml file:
            # let's reconstruct it from the file itself
            data = self.retrieve(fix=False)
            temp_file = tempfile.NamedTemporaryFile(mode='w')
            sgridpath = temp_file.name
            data.isel(time=0).to_netcdf(sgridpath)
        else:
            temp_file = None
            if zoom:
                sgridpath = sgridpath.format(zoom=9-zoom)

        self.logger.warning("Weights file not found: %s", weightsfile)
        self.logger.warning("Attempting to generate it ...")
        self.logger.warning("Source grid: %s", sgridpath)

        # hack to  pass a correct list of all options
        src_extra = source_grid.get("extra", [])
        if src_extra:
            if not isinstance(src_extra, list):
                src_extra = [src_extra]
        if extra:
            extra = [extra]
        else:
            extra = []
        extra = extra + src_extra
        weights = rg.cdo_generate_weights(source_grid=sgridpath,
                                          target_grid=cfg_regrid["target_grids"][regrid],
                                          method='ycon',
                                          gridpath=cfg_regrid["cdo-paths"]["download"],
                                          icongridpath=cfg_regrid["cdo-paths"]["icon"],
                                          extra=extra)
        weights.to_netcdf(weightsfile)
        self.logger.warning("Success!")

    def cdo_generate_areas(self, source, icongridpath=None, gridpath=None, extra=None):
        """
            Generate grid areas using CDO

            Args:
                source (xarray.DataArray or str): Source grid
                gridpath (str): where to store downloaded grids
                icongridpath (str): location of ICON grids (e.g. /pool/data/ICON)
                extra: command(s) to apply to source grid before weight generation (can be a list)

            Returns:
                :obj:`xarray.DataArray` with cell areas
        """

        # Make some temporary files that we'll feed to CDO
        area_file = tempfile.NamedTemporaryFile()

        if isinstance(source, str):
            sgrid = source
        else:
            source_grid_file = tempfile.NamedTemporaryFile()
            source.to_netcdf(source_grid_file.name)
            sgrid = source_grid_file.name

        # Setup environment
        env = os.environ
        if gridpath:
            env["CDO_DOWNLOAD_PATH"] = gridpath
        if icongridpath:
            env["CDO_ICON_GRIDS"] = icongridpath

        try:
            # Run CDO
            if extra:
                # make sure extra is a flat list if it is not already
                if not isinstance(extra, list):
                    extra = [extra]

                subprocess.check_output(
                    [
                        "cdo",
                        "-f", "nc4",
                        "gridarea",
                    ] + extra +
                    [
                        sgrid,
                        area_file.name,
                    ],
                    stderr=subprocess.PIPE,
                    env=env,
                )
            else:
                subprocess.check_output(
                    [
                        "cdo",
                        "-f", "nc4",
                        "gridarea",
                        sgrid,
                        area_file.name,
                    ],
                    stderr=subprocess.PIPE,
                    env=env,
                )

            areas = xr.load_dataset(area_file.name, engine="netcdf4")
            areas.cell_area.attrs['units'] = 'm2'
            areas.cell_area.attrs['standard_name'] = 'area'
            areas.cell_area.attrs['long_name'] = 'area of grid cell'
            return areas.cell_area

        except subprocess.CalledProcessError as err:
            # Print the CDO error message
            self.logger.critical(err.output.decode(), file=sys.stderr)
            raise

        finally:
            # Clean up the temporary files
            if not isinstance(source, str):
                source_grid_file.close()
            area_file.close()

    def retrieve(self, regrid=False, timmean=False, decumulate=False,
                 fix=True, apply_unit_fix=True, var=None, vars=None,  # pylint: disable=W0622
                 streaming=False, stream_step=1, stream_unit=None,
                 stream_startdate=None, streaming_generator=False):
        """
        Perform a data retrieve.

        Arguments:
            regrid (bool):          if to regrid the retrieved data (False)
            timmean (bool):         if to average the retrieved data (False)
            decumulate (bool):      if to remove the cumulation from data (False)
            fix (bool):             if to perform a fix (var name, units, coord name adjustments) (True)
            apply_unit_fix (bool):  if to already adjust units by multiplying by a factor or adding
                                    an offset (this can also be done later with the `apply_unit_fix` method) (True)
            var (str, list):  variable(s) which we will extract; vars is a synonym (None)
            streaming (bool):       if to retreive data in a streaming mode (False)
            streaming_generator (bool):  if to return a generator object for data streaming (False).
            stream_step (int):      the number of time steps to stream the data by (Default = 1)
            stream_unit (str):      the unit of time to stream the data by
                                    (e.g. 'hours', 'days', 'months', 'years') (None)
            stream_startdate (str): the starting date for streaming the data (e.g. '2020-02-25') (None)
        Returns:
            A xarray.Dataset containing the required data.
        """

        self.cat = intake.open_catalog(self.catalog_file)
        # Extract subcatalogue
        if self.zoom:
            esmcat = self.cat[self.model][self.exp][self.source](zoom=self.zoom)
        else:
            esmcat = self.cat[self.model][self.exp][self.source]

        if vars:
            var = vars
        if not var:
            var = self.var

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
            if var:
                # conversion to list guarantee that Dataset is produced
                if isinstance(var, str):
                    var = var.split()
                data = esmcat.to_dask()[var]

            else:
                data = esmcat.to_dask()

        # select only a specific level when reading. Level coord names defined in regrid.yaml
        if self.level is not None:
            data = data.isel({self.vertcoord: self.level})

        # sequence which should be more efficient: decumulate - averaging - regridding - fixing
        if decumulate:
            # data = data.map(self.decumulate, keep_attrs=True)
            data = data.map(self.decumulate)
        if self.freq and timmean:
            data = self.timmean(data)
        if self.targetgrid and regrid:
            data = self.regridder.regrid(data)
            self.grid_area = self.dst_grid_area
        if fix:   # Do not change easily this order. The fixer assumes to be after regridding
            data = self.fixer(data, apply_unit_fix=apply_unit_fix)
        if streaming or self.streaming or streaming_generator:
            if stream_step == 1:
                stream_step = self.stream_step
            if not stream_unit:
                stream_unit = self.stream_unit
            if not stream_startdate:
                stream_startdate = self.stream_startdate
            if streaming_generator:
                data = self.stream_generator(data, stream_step, stream_unit, stream_startdate)
            else:
                data = self.stream(data, stream_step, stream_unit, stream_startdate)

        log_history(data, "retrieved by AQUA fixer")
        return data

    def stream(self, data, stream_step=1, stream_unit=None, stream_startdate=None):
        """
        The stream method is used to stream data by either a specific time interval
        or by a specific number of samples. If the unit parameter is specified, the
        data is streamed by the specified unit and stream_step (e.g. 1 month).
        If the unit parameter is not specified, the data is streamed by stream_step
        steps of the original time resolution of input data.

        If the stream function is called a second time, it will return the subsequent
        chunk of data in the sequence. The function keeps track of the state of the
        streaming process through the use of internal attributes. This allows the user
        to stream through the entire dataset in multiple calls to the function,
        retrieving consecutive chunks of data each time.

        If stream_startdate is not specified, the method will use the first date in the dataset.

        Arguments:
            data (xr.Dataset):      the input xarray.Dataset
            stream_step  (int):     the number of time steps to stream the data by (Default = 1)
            stream_unit (str):      the unit of time to stream the data by
                                    (e.g. 'hours', 'days', 'months', 'years') (None)
            stream_startdate (str): the starting date for streaming the data
                                    (e.g. '2020-02-25') (None)
        Returns:
            A xarray.Dataset containing the subset of the input data that has been streamed.
        """
        if not self.stream_date:
            if stream_startdate:
                self.stream_date = pd.to_datetime(stream_startdate)
            else:
                self.stream_date = pd.to_datetime(data.time[0].values)

        if self.stream_index == 0 and stream_startdate:
            self.stream_index = data.time.to_index().get_loc(pd.to_datetime(stream_startdate))

        if stream_unit:
            start_date = self.stream_date
            stop_date = start_date + pd.DateOffset(**{stream_unit: stream_step})
            self.stream_date = stop_date
            return data.sel(time=slice(start_date, stop_date)).where(data.time != stop_date, drop=True)
        else:
            start_index = self.stream_index
            stop_index = start_index + stream_step
            self.stream_index = stop_index
            return data.isel(time=slice(start_index, stop_index))

    def reset_stream(self):
        """
        Reset the state of the streaming process.
        This means that if the stream function is called again after calling reset_stream,
        it will start streaming the input data from the beginning.
        """
        self.stream_index = 0
        self.stream_date = None

    def stream_generator(self, data, stream_step=1, stream_unit=None, stream_startdate=None):
        """
        The stream_generator method is designed to split data into smaller chunks of data for
        processing or analysis. It returns a generator object that yields the smaller chunks of data.
        The method can split the data based on either a specific time interval or by a specific number of samples.
        If the unit parameter is specified, the data is streamed by the specified unit and stream_step (e.g. 1 month).
        If the unit parameter is not specified, the data is streamed by stream_step steps of the original time
        resolution of input data.

        Arguments:
            data (xr.Dataset):      the input xarray.Dataset
            stream_step  (int):     the number of samples or time interval to stream the data by (Default = 1)
            stream_unit (str):      the unit of the time interval to stream the data by
                                    (e.g. 'hours', 'days', 'months', 'years') (None)
            stream_startdate (str): the starting date for streaming the data (e.g. '2020-02-25') (None)
        Returns:
            A generator object that yields the smaller chunks of data.
        """
        if stream_startdate:
            start_date = pd.to_datetime(stream_startdate)
        else:
            start_date = data.time[0].values
        if stream_unit:
            while start_date < data.time[-1].values:
                stop_date = pd.to_datetime(start_date) + pd.DateOffset(**{stream_unit: stream_step})
                yield data.sel(time=slice(start_date, stop_date)).where(data.time != stop_date, drop=True)
                start_date = stop_date
        if not stream_unit:
            start_index = data.time.to_index().get_loc(start_date)
            while start_index < len(data.time):
                stop_index = start_index + stream_step
                yield data.isel(time=slice(start_index, stop_index))
                start_index = stop_index

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

        log_history(out, "regridded by AQUA fixer")
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
        if freq == 'mon':
            resample_freq = '1M'
        elif freq == 'day':
            resample_freq = '1D'
        elif freq == 'yr':
            resample_freq = '1Y'
        else:
            resample_freq = freq

        try:
            # resample, and assign the correct time
            out = data.resample(time=resample_freq).mean()
            proper_time = data.time.resample(time=resample_freq).mean()
            out['time'] = proper_time.values
        except ValueError:
            sys.exit('Cant find a frequency to resample, aborting!')

        # check for NaT
        if np.any(np.isnat(out.time)):
            self.logger.warning('Resampling cannot produce output for all frequency step, is your input data correct?')

        log_history(out, f"resampled to frequency {resample_freq} by AQUA fixer")
        return out

    def _check_if_accumulated_auto(self, data):
        """To check if a DataArray is accumulated.
        Arbitrary check on the first 20 timesteps"""

        # randomly pick a few timesteps from a gridpoint
        ndims = [dim for dim in data.dims if data[dim].size > 1][1:]
        pindex = {dim: 0 for dim in ndims}

        # extract the first 20 timesteps and do the derivative
        check = data.isel(pindex).isel(time=slice(None, 20)).diff(dim='time').values

        # check all derivative are positive or all negative
        condition = (check >= 0).all() or (check <= 0).all()

        return condition

    def _check_if_accumulated(self, data):
        """To check if a DataArray is accumulated.
        On a list of variables defined by the GRIB names

        Args:
            data (xr.DataArray): field to be processed

        Returns:
            bool: True if decumulation is necessary, False if no
        """

        decumvars = ['tp', 'e', 'slhf', 'sshf',
                     'tsr', 'ttr', 'ssr', 'str',
                     'tsrc', 'ttrc', 'ssrc', 'strc',
                     'tisr', 'tprate', 'mer', 'tp', 'cp', 'lsp']

        if data.name in decumvars:
            return True
        else:
            return False

    def simple_decumulate(self, data, jump=None, keep_first=True):
        """
        Remove cumulative effect on IFS fluxes.

        Args:
            data (xr.DataArray):     field to be processed
            jump (str):              used to fix periodic jumps (a very specific NextGEMS IFS issue)
                                     Examples: jump='month' (the NextGEMS case), jump='day')
            keep_first (bool):       if to keep the first value as it is (True) or place a 0 (False)

        Returns:
            A xarray.DataArray where the cumulation has been removed
        """

        # get the derivatives
        deltas = data.diff(dim='time')

        # add a first timestep empty to align the original and derived fields
        if keep_first:
            zeros = data.isel(time=0)
        else:
            zeros = xr.zeros_like(data.isel(time=0))

        deltas = xr.concat([zeros, deltas], dim='time').transpose('time', ...)

        if jump:
            # universal mask based on the change of month (shifted by one timestep)
            mask = ~(data[f'time.{jump}'] != data[f'time.{jump}'].shift(time=1))
            mask = mask.shift(time=1, fill_value=False)

            # kaboom: exploit where
            deltas = deltas.where(mask, data)

            # remove the first timestep (no sense in cumulated)
            # clean = clean.isel(time=slice(1, None))

        # add an attribute that can be later used to infer about decumulation
        deltas.attrs['decumulated'] = 1

        log_history(deltas, "decumulated by AQUA fixer")
        return deltas

    def decumulate(self, data, cumulation_time=None, check=True):
        """
        Test function to remove cumulative effect on IFS fluxes.
        Cumulation times are estimated from the intervals of the data, but
        can be specified manually

        Args:
            data (xr.DataArray):     field to be processed
            cumulation_time (float): optional, specific cumulation time
            check (bool):            if to check if the variable needs to be decumulated

        Returns:
            A xarray.DataArray where the cumulation time has been removed
        """
        if check:
            if not self._check_if_accumulated(data):
                return data

        # which frequency are the data?
        if not cumulation_time:
            cumulation_time = (data.time[1]-data.time[0]).values/np.timedelta64(1, 's')

        # get the derivatives
        deltas = data.diff(dim='time') / cumulation_time

        # add a first timestep empty to align the original and derived fields
        zeros = xr.zeros_like(data.isel(time=0))
        deltas = xr.concat([zeros, deltas], dim='time').transpose('time', ...)

        # universal mask based on the change of month (shifted by one timestep)
        mask = ~(data['time.month'] != data['time.month'].shift(time=1))
        mask = mask.shift(time=1, fill_value=False)

        # check which records are kept
        # print(data.time[~mask])

        # kaboom: exploit where
        clean = deltas.where(mask, data/cumulation_time)

        # remove the first timestep (no sense in cumulated)
        clean = clean.isel(time=slice(1, None))

        # rollback the time axis by half the cumulation time
        clean['time'] = clean.time - np.timedelta64(int(cumulation_time/2), 's')

        # WARNING: HACK FOR EVAPORATION
        # print(clean.units)
        if clean.units == 'm of water equivalent':
            clean.attrs['units'] = 'm'

        # use metpy units to divide by seconds
        new_units = (units(clean.units)/units('s'))

        # usual case for radiative fluxes
        try:
            clean.attrs['units'] = str(new_units.to('W/m^2').units)
        except DimensionalityError:
            clean.attrs['units'] = str(new_units.units)

        # add an attribute that can be later used to infer about decumulation
        clean.attrs['decumulated'] = 1

        return clean

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

    def _get_spatial_sample(self, da, space_coord):
        """
        Selects a single spatial sample along the dimensions specified in `space_coord`.

        Arguments:
            da (xarray.DataArray):     Input data array to select the spatial sample from.
            space_coord (list of str): List of dimension names corresponding to the spatial coordinates to select.

        Returns:
            Data array containing a single spatial sample along the specified dimensions.
        """

        dims = list(da.dims)
        extra_dims = list(set(dims) - set(space_coord))
        da_out = da.isel({dim: 0 for dim in extra_dims})
        return da_out

    def _rename_dims(self, da, dim_list):
        """
        Renames the dimensions of a DataArray so that any dimension which is already
        in `dim_list` keeps its name, and the others are renamed to whichever other
        dimension name is in `dim_list`.
        If `da` has only one dimension with a name which is different from that in `dim_list`,
        it is renamed to that new name.
        If it has two coordinate names (e.g. "lon" and "lat") which appear also in `dim_list`,
        these are not touched.

        Parameters
        ----------
        da (xarray.DataArray):  The input DataArray to rename.
        dim_list (list of str): The list of dimension names to use.

        Returns
        -------
        xarray.DataArray
            A new DataArray with the renamed dimensions.
        """

        dims = list(da.dims)
        # Lisy of dims which are already there
        shared_dims = list(set(dims) & set(dim_list))
        # List of dims in B which are not in space_coord
        extra_dims = list(set(dims) - set(dim_list))
        # List of dims in da which are not in dim_list
        new_dims = list(set(dim_list) - set(dims))
        i = 0
        da_out = da
        for dim in extra_dims:
            if dim not in shared_dims:
                da_out = da.rename({dim: new_dims[i]})
                i += 1
        return da_out

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
        xr.align(grid_area, data, join='exact')

        out = data.weighted(weights=grid_area.fillna(0)).mean(dim=space_coord)

        return out

    def fixer(self, data, apply_unit_fix=False):
        """
        Perform fixes (var name, units, coord name adjustments) of the input dataset.

        Arguments:
            data (xr.Dataset):      the input dataset
            apply_unit_fix (bool):  if to perform immediately unit conversions (which requite a product or an addition).
                                    The fixer sets anyway an offset or a multiplicative factor in the data attributes.
                                    These can be applied also later with the method `apply_unit_fix`. (false)

        Returns:
            A xarray.Dataset containing the fixed data and target units, factors and offsets in variable attributes.
        """

        fixes = load_yaml(self.fixer_file)
        model = self.model
        exp = self.exp
        src = self.source

        # Default input datamodel
        src_datamodel = fixes["defaults"].get("src_datamodel", None)

        fixm = fixes["models"].get(model, None)
        if not fixm:
            self.logger.warning("No fixes defined for model %s", model)
            return data

        fixexp = fixm.get(exp, None)
        if not fixexp:
            fixexp = fixm.get('default', None)
            if not fixexp:
                self.logger.warning("No fixes defined for model %s, experiment %s", model, exp)
                return data

        fix = fixexp.get(src, None)
        if not fix:
            fix = fixexp.get('default', None)
            if not fix:
                self.logger.warning("No fixes defined for model %s, experiment %s, source %s", model, exp, src)
                return data

        self.deltat = fix.get("deltat", 1.0)
        jump = fix.get("jump", None)  # if to correct for a monthly accumulation jump

        fixd = {}
        varlist = {}
        variables = fix.get("vars", None)
        if variables:
            for var in variables:
                unit = None
                attributes = {}
                varname = var

                grib = variables[var].get("grib", None)
                # This is a grib variable, use eccodes to find attributes
                if grib:
                    # Get relevant eccodes attribues
                    attributes.update(get_eccodes_attr(var))
                    sn = attributes.get("shortName", None)
                    if (sn != '~') and (var != sn):
                        varname = sn
                        self.logger.info("Grib attributes for %s: %s", varname, attributes)

                varlist[var] = varname

                source = variables[var].get("source", None)
                # This is a renamed variable. This will be done at the end.
                if source:
                    if source not in data.variables:
                        continue
                    fixd.update({f"{source}": f"{varname}"})
                    log_history(data[source], "variable renamed by AQUA fixer")

                formula = variables[var].get("derived", None)
                # This is a derived variable, let's compute it and create the new variable
                if formula:
                    try:
                        data[varname] = _eval_formula(formula, data)
                        source = varname
                        attributes.update({"derived": formula})
                        self.logger.info("Derived %s from %s", var, formula)
                        log_history(data[source], "variable derived by AQUA fixer")
                    except KeyError:
                        # The variable could not be computed, let's skip it
                        continue

                # Get extra attributes if any
                attributes.update(variables[var].get("attributes", {}))

                # update attributes
                if attributes:
                    for att, value in attributes.items():
                        # Already adjust all attributes but not yet units
                        if att == "units":
                            unit = value
                        else:
                            data[source].attrs[att] = value

                # Override destination units
                newunits = variables[var].get("units", None)
                if newunits:
                    data[source].attrs.update({"units": newunits})
                    unit = newunits

                # Override source units
                src_units = variables[var].get("src_units", None)
                if src_units:
                    data[source].attrs.update({"units": src_units})

                # adjust units
                if unit:
                    if unit.count('{'):
                        unit = fixes["defaults"]["units"][unit.replace('{', '').replace('}', '')]
                    self.logger.info("%s: %s --> %s", var, data[source].units, unit)
                    factor, offset = self.convert_units(data[source].units, unit, var)
                    if (factor != 1.0) or (offset != 0):
                        data[source].attrs.update({"target_units": unit})
                        data[source].attrs.update({"factor": factor})
                        data[source].attrs.update({"offset": offset})
                        self.logger.info("Fixing %s to %s. Unit fix: factor=%f, offset=%f", source, var, factor, offset)

        # Only now rename everything
        data = data.rename(fixd)

        for var in variables:
            # Decumulate if required
            if variables[var].get("decumulate", None):
                varname = varlist[var]
                if varname in data.variables:
                    keep_first = variables[var].get("keep_first", True)
                    data[varname] = self.simple_decumulate(data[varname],
                                                           jump=jump,
                                                           keep_first=keep_first)
                    log_history(data[varname], "variable decumulated by AQUA fixer")

        if apply_unit_fix:
            for var in data.variables:
                self.apply_unit_fix(data[var])

        dellist = [x for x in fix.get("delete", []) if x in data.variables]
        if dellist:
            data = data.drop_vars(dellist)

        # Fix coordinates according to a given data model
        src_datamodel = fix.get("data_model", src_datamodel)
        if src_datamodel and src_datamodel is not False:
            data = self.change_coord_datamodel(data, src_datamodel, self.dst_datamodel)
            log_history(data, "coordinates adjusted by AQUA fixer")

        return data

    def change_coord_datamodel(self, data, src_datamodel, dst_datamodel):
        """
        Wrapper around cfgrib.cf2cdm to perform coordinate conversions

        Arguments:
            data (xr.DataSet):      input dataset to process
            src_datamodel (str):    input datamodel (e.g. "cf")
            dst_datamodel (str):    output datamodel (e.g. "cds")

        Returns:
            The processed input dataset
        """
        fn = os.path.join(self.configdir, 'data_models', f'{src_datamodel}2{dst_datamodel}.json')
        self.logger.info("Data model: %s", fn)
        with open(fn, 'r', encoding="utf8") as f:
            dm = json.load(f)

        if "IFSMagician" in data.attrs.get("history", ""):  # Special fix for gribscan levels
            if "level" in data.coords:
                data.level.attrs["units"]="hPa"
                data.level.attrs["standard_name"]="air_pressure"
                data.level.attrs["long_name"]="pressure"

        # this is needed since cf2cdm issues a (useless) UserWarning
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning)
            data = cf2cdm.translate_coords(data, dm)
            # Hack needed because cfgrib.cf2cdm mixes up coordinates with dims
            if "forecast_reference_time" in data.dims:
                data = data.swap_dims({"forecast_reference_time": "time"})
        return data

    def convert_units(self, src, dst, var="input var"):
        """
        Converts source to destination units using metpy.

        Arguments:
            src (str):  source units
            dst (str):  destination units
            var (str):  variable name (optional, used only for diagnostic output)

        Returns:
            factor, offset (float): a factor and an offset to convert the input data (None if not needed).
        """

        src = normalize_units(src)
        dst = normalize_units(dst)
        factor = units(src).to_base_units() / units(dst).to_base_units()

        if factor.units == units('dimensionless'):
            offset = (0 * units(src)).to(units(dst)) - (0 * units(dst))
        else:
            if factor.units == "meter ** 3 / kilogram":
                # Density of water was missing
                factor = factor * 1000 * units("kg m-3")
                self.logger.info("%s: corrected multiplying by density of water 1000 kg m-3", var)
            elif factor.units == "meter ** 3 * second / kilogram":
                # Density of water and accumulation time were missing
                factor = factor * 1000 * units("kg m-3") / (self.deltat * units("s"))
                self.logger.info("%s: corrected multiplying by density of water 1000 kg m-3", var)
                self.logger.info("%s: corrected dividing by accumulation time %s s", var, self.deltat)
            elif factor.units == "second":
                # Accumulation time was missing
                factor = factor / (self.deltat * units("s"))
                self.logger.info("%s: corrected dividing by accumulation time %s s", var, self.deltat)
            elif factor.units == "kilogram / meter ** 3":
                # Density of water was missing
                factor = factor / (1000 * units("kg m-3"))
                self.logger.info("%s: corrected dividing by density of water 1000 kg m-3", var)
            else:
                self.logger.info("%s: incommensurate units converting %s to %s --> %s", var, src, dst, factor.units)
            offset = 0 * units(dst)

        if offset.magnitude != 0:
            factor = 1
            offset = offset.magnitude
        else:
            offset = 0
            factor = factor.magnitude
        return factor, offset

    def apply_unit_fix(self, data):
        """
        Applies unit fixes stored in variable attributes (target_units, factor and offset)

        Arguments:
            data (xr.DataArray):  input DataArray
        """
        target_units = data.attrs.get("target_units", None)
        if target_units:
            d = {"src_units": data.attrs["units"], "units_fixed": 1}
            data.attrs.update(d)
            data.attrs["units"] = normalize_units(target_units)
            factor = data.attrs.get("factor", 1)
            offset = data.attrs.get("offset", 0)
            if factor != 1:
                data *= factor
            if offset != 0:
                data += offset
            log_history(data, "units changed by AQUA fixer")

# The following are not methods of the class - to be moved to a separate source file ultmately
# (let's wait for a general refactor)


def normalize_units(src):
    """Get rid of crazy grib units"""
    if src == '1':
        return 'dimensionless'
    else:
        return str(src).replace("of", "").replace("water", "").replace("equivalent", "")


def _check_catalog_source(cat, model, exp, source, name="dictionary"):
    """
    Check the entries of a nested dictionary based on the model/exp/source structure
    and return an updated source.
    The name argument can be used for a proper printing.

    Returns:
    an updated source id (str).
    If source is not specified "default" is chosen or, if missing, the first source
    """

    if model not in cat:
        raise KeyError(f"Model {model} not found in {name}.")
    if exp not in cat[model]:
        raise KeyError(f"Experiment {exp} not found in {name} for model {model}.")

    if source:
        if source not in cat[model][exp]:
            if "default" not in cat[model][exp]:
                raise KeyError(f"Source {source} of experiment {exp} "
                               f"not found in {name} for model {model}.")
            else:
                source = "default"
    else:
        source = list(cat[model][exp].keys())[0]  # take first source if none provided

    return source
