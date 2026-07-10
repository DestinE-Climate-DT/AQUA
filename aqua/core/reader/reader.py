"""The main AQUA Reader class"""

# import intake_esm
import xarray as xr
from metpy.units import units
from smmregrid import GridInspector

# This is needed to initialize the gsv driver
from aqua.core.backend import BackendFactory
from aqua.core.configurer import ConfigPath
from aqua.core.data_model import DataModel, counter_reverse_coordinate

# set default data model
from aqua.core.default import DEFAULT_ENGINE, DEFAULT_NPROC
from aqua.core.exceptions import NoRegridError
from aqua.core.fixer import Fixer
from aqua.core.fldstat import FldStat
from aqua.core.histogram import histogram
from aqua.core.logger import log_configure, log_history
from aqua.core.regridder import Regridder
from aqua.core.timstat import TimStat
from aqua.core.util import load_multi_yaml, set_attrs

from .trender import Trender

# set default options for xarray
xr.set_options(keep_attrs=True)


class Reader:
    """General reader for climate data."""

    instance = None  # Used to store the latest instance of the class

    def __init__(
        self,
        model=None,
        exp=None,
        source=None,
        catalog=None,
        path=None,
        fix=True,
        datamodel=None,
        convention=None,
        regrid=None,
        regrid_method=None,
        areas=True,
        startdate=None,
        enddate=None,
        rebuild=False,
        loglevel="WARNING",
        nproc=DEFAULT_NPROC,
        chunks=None,
        preproc=None,
        engine=DEFAULT_ENGINE,
        **kwargs,
    ):
        """
        TODO: adapt types and docstring
        Initializes the Reader class, which uses the catalog
        `config/config.yaml` to identify the required data.

        Args:
            model (str): Model ID. Mandatory
            exp (str): Experiment ID. Mandatory.
            source (str): Source ID. Mandatory
            catalog (str, optional): Catalog where to search for the triplet.  Default to None will allow for autosearch in
                                     the installed catalogs.
            datamodel (str, optional): Data model to apply for coordinate transformations (e.g., 'aqua'). Defaults to 'aqua'.
            regrid (str, optional): Perform regridding to grid `regrid`, as defined in `config/regrid.yaml`. Defaults to None.
            regrid_method (str, optional): CDO Regridding regridding method. Read from grid configuration.
                                           If not specified anywhere, using "ycon".
            fix (bool, optional): Activate data fixing
            areas (bool, optional): Compute pixel areas if needed. Defaults to True.
            startdate (str, optional): The starting date for reading/streaming the data (e.g. '2020-02-25'). Defaults to None.
            enddate (str, optional): The final date for reading/streaming the data (e.g. '2020-03-25'). Defaults to None.
            rebuild (bool, optional): Force rebuilding of area and weight files. Defaults to False.
            loglevel (str, optional): Level of logging according to logging module.
                                      Defaults to log_level_default of loglevel().
            nproc (int, optional): Number of processes to use for weights generation. Defaults to 4.
            chunks (str or dict, optional): chunking to be used for data access.
                                            Defaults to None (using default from catalog, recommended).
                                            If it is a string time chunking is assumed.
                                            If it is a dictionary the keys 'time' and 'vertical' are looked for.
                                            Time chunking can be one of S (step), 10M, 15M, 30M, h, 1h, 3h, 6h, D, 5D, W, M, Y.
                                            Vertical chunking is expressed as the number of vertical levels to be used.
            preproc (function, optional): a function to be applied to the dataset when retrieved. Defaults to None.
            convention (str, optional): convention to be used for reading data. Defaults to 'eccodes'.
                                        (Only one supported so far)
            engine (str, optional): Engine to be used for GSV retrieval: 'polytope' or 'fdb'. Defaults to 'fdb'.

        Keyword Args:
            zoom (int, optional): HEALPix grid zoom level (e.g. zoom=10 is h1024). Allows for multiple gridname definitions.
            realization (int, optional): The ensemble realization number.
            **kwargs: Additional arbitrary keyword arguments to be passed as additional parameters to the intake catalog entry.

        Returns:
            Reader: A `Reader` class object.
        """

        Reader.instance = self  # record the latest instance of the class (used for accessor)

        # define the internal logger
        self.loglevel = loglevel
        self.logger = log_configure(log_level=self.loglevel, log_name="Reader")

        # intake arguments
        self.exp = exp
        self.model = model
        self.source = source

        # xarray native argument
        self.path = path

        # these infos are used by the regridder to correct define areas/weights name
        # TODO: find an alterantive approah when path is used, and possibly rename to avoid confusion
        reader_kwargs = {"model": model, "exp": exp, "source": source}
        self.kwargs = kwargs

        # regridding
        self.regrid_method = regrid_method
        self.nproc = nproc

        # various option
        self.time_correction = False  # extra flag for correction data with cumulation time on monthly timescale
        self.chunks = chunks

        # Preprocessing function
        self.preproc = preproc

        # init the areas and dimensions
        self.grid_area = None
        self.src_grid_area = None
        self.tgt_grid_area = None
        self.src_space_coord = None
        self.tgt_space_coord = None
        self.vert_coord = None

        # time options
        self.startdate = startdate
        self.enddate = enddate

        # fixer
        self.fix = fix

        self.sample_data = None  # used to avoid multiple calls of retrieve_plain

        # define configuration file and paths
        # TODO: revisit configpath to allow xarray backend. define a behaviour without catalog.
        configurer = ConfigPath(catalog=catalog, loglevel=loglevel)
        self.fixer_folder, self.grids_folder = configurer.get_reader_filenames()

        # extend the unit registry
        units_extra_definition()

        # we use the backend factory to select the appropriate backend based on the provided arguments
        backend_factory = BackendFactory(
            model=self.model,
            exp=self.exp,
            source=self.source,
            path=self.path,
            configurer=configurer,
            catalog=catalog,
            loglevel=self.loglevel,
        )
        # configure the intake catalog for the backend
        backend_factory.select_backend()
        self.metadata = backend_factory.metadata
        self.catalog = backend_factory.catalog
        self.machine_paths = backend_factory.machine_paths

        # return the metadata for fixer, src_grid, convention and datamodel
        self.fixer_name, self.src_grid_name, self.convention, self.datamodel_name = backend_factory.get_metadata(
            convention=convention, fixer_name=None, src_grid_name=None, datamodel_name=datamodel
        )

        # case to disable automatic fix
        if self.fixer_name is False:
            self.logger.warning("A False flag is specified in fixer_name metadata, disabling fix!")
            self.fix = False

        # Initialize variable fixer
        if self.fix:
            self.fixer = Fixer(
                fixer_name=self.fixer_name,
                convention=self.convention,
                metadata=self.metadata,
                loglevel=self.loglevel,
            )
        else:
            self.fixer = None

        # if data model is not passed to Reader, try to get it from the catalog source metadata
        if self.datamodel_name is False:
            self.logger.warning("A False flag is specified in data_model metadata, disabling data model!")
            self.datamodel = None
        else:
            self.datamodel = DataModel(name=self.datamodel_name, loglevel=self.loglevel)

        # create the backend: this is the interface that access the data
        self.backend = backend_factory.create_backend(
            fixer=self.fixer if self.fix else None,
            datamodel=self.datamodel,
            chunks=self.chunks,
            engine=engine,
            databridge=None,
            loglevel=self.loglevel,
            **kwargs,
        )

        # define tgt grid names
        self.tgt_grid_name = regrid

        # init the regridder and the areas
        self.regridder = None
        areas, regrid = self._configure_regridder(
            self.machine_paths, regrid=regrid, areas=areas, rebuild=rebuild, reader_kwargs=reader_kwargs
        )

        # init the fldstat modules. if areas are not available, will issue a warning
        cell_area = self.src_grid_area.cell_area if areas else None
        self.src_fldstat = FldStat(
            cell_area, grid_name=self.src_grid_name, horizontal_dims=self.src_space_coord, loglevel=self.loglevel
        )
        self.tgt_fldstat = None
        if regrid:
            if not areas:
                self.logger.warning(
                    "Regridding requires info on areas. As areas can usually be generated with smmregrid, "
                    "setting areas to 'True'"
                )
                areas = True
            self.tgt_fldstat = FldStat(
                self.tgt_grid_area.cell_area,
                grid_name=self.tgt_grid_name,
                horizontal_dims=self.tgt_space_coord,
                loglevel=self.loglevel,
            )

        self.trender = Trender(loglevel=self.loglevel)

    def _configure_regridder(self, machine_paths, regrid=False, areas=False, rebuild=False, reader_kwargs=None):
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
            if self.src_grid_name is False:
                self.logger.warning("Grid metadata is False, regrid and areas disabled")
                return False, False

            # create the configuration dictionary
            cfg_regrid = load_multi_yaml(
                folder_path=self.grids_folder, definitions=machine_paths["paths"], loglevel=self.loglevel
            )
            cfg_regrid = {**machine_paths, **cfg_regrid}

            if self.src_grid_name is None:
                self.logger.info("Grid metadata is not defined. Trying to access the real data")
                data = self._retrieve_plain()
                self.regridder = Regridder(cfg_regrid, data=data, loglevel=self.loglevel)
            else:
                self.logger.info("Grid metadata is %s", self.src_grid_name)
                self.regridder = Regridder(cfg_regrid, src_grid_name=self.src_grid_name, loglevel=self.loglevel)

                if self.regridder.error:
                    self.logger.info("Regridder() cannot init with the provided grid metadata: trying with data")
                    data = self._retrieve_plain()
                    self.regridder = Regridder(cfg_regrid, src_grid_name=self.src_grid_name, data=data, loglevel=self.loglevel)

            # export src space coord and vertical coord
            self.src_space_coord = self.regridder.src_horizontal_dims
            self.vert_coord = self.regridder.src_mask_dim

            # TODO: it is likely there are other cases where we need to disable regrid.
            if not self.regridder.cdo:
                return False, False

        if areas:
            # generate source areas and expose them in the reader
            self.src_grid_area = self.regridder.areas(rebuild=rebuild, reader_kwargs=reader_kwargs)
            # apply optional fixes to areas
            if self.fix:
                self.src_grid_area = self.fixer.fixerdatamodel.apply(self.src_grid_area)
            # Apply data model transformation to areas
            if self.datamodel:
                self.src_grid_area = self.datamodel.apply(self.src_grid_area)

        # configure regridder and generate weights
        if regrid:
            # generate weights and init the SMMregridder
            weights = self.regridder.weights(
                rebuild=rebuild,
                tgt_grid_name=self.tgt_grid_name,
                regrid_method=self.regrid_method,
                reader_kwargs=reader_kwargs,
                initialize=False,
            )
            if self.fix:
                weights = self._fix_datamodel_weights(weights, mode="fixer")
            if self.datamodel:
                weights = self._fix_datamodel_weights(weights, mode="datamodel")
            self.regridder.initialize(weights)

        # generate destination areas, expose them and the associated space coordinates
        if areas and regrid:
            self.tgt_grid_area = self.regridder.areas(tgt_grid_name=self.tgt_grid_name, rebuild=rebuild)
            # apply optional fixes to areas
            if self.fix:
                self.tgt_grid_area = self.fixer.fixerdatamodel.apply(self.tgt_grid_area)
            # Apply data model transformation to target areas
            if self.datamodel:
                self.tgt_grid_area = self.datamodel.apply(self.tgt_grid_area, flip_coords=False)
            # expose target horizontal dimensions
            self.tgt_space_coord = self.regridder.tgt_horizontal_dims

        # activate time statistics
        self.timemodule = TimStat(loglevel=self.loglevel)

        return areas, regrid

    def _fix_datamodel_weights(self, weights, mode="datamodel"):
        """
        Mask coordinate of the weights need to be adjusted according to the data model or fix applied.
        Arguments:
                weights (dict): The weights dictionary from smmregrid
                mode (str): "datamodel" or "fixer" to apply the respective data model or fixer datamodel
        """
        new_weights = {}
        for item, value in weights.items():
            self.logger.debug("Applying %s to weights item %s", mode, item)
            if mode == "datamodel":
                fixed = self.datamodel.apply(value, flip_coords=False)
            elif mode == "fixer":
                fixed = self.fixer.fixerdatamodel.apply(value)
            else:
                raise ValueError(f"Mode {mode} not recognized for weights fixing")

            # Check if fixed object has coordinates before accessing
            coords = list(fixed.coords) if hasattr(fixed, "coords") and fixed.coords else []
            if not coords:
                self.logger.debug("No coordinates found in weights item %s after applying %s", item, mode)
                new_weights[item] = fixed  # Use original item name as fallback
            else:
                new_weights[coords[0]] = fixed
        return new_weights

    # TODO: sample is not used, so no sampling is done for retrieve_plain and all the vars are loaded.
    # also chunking can be specified to reduce the amount of data.
    def retrieve(self, var=None, startdate=None, enddate=None, level=None, level_coord=None):
        """
        Perform a data retrieve.

        Arguments:
            var (str, list): the variable(s) to retrieve. Defaults to None. If None, all variables are retrieved.
            level (list, float, int): Levels to be read, overriding default in catalog source.
            level_coord (str): The name of the vertical coordinate. Defaults to None.
            startdate (str): The starting date for reading/streaming the data (e.g. '2020-02-25'). Defaults to None.
            enddate (str): The final date for reading/streaming the data (e.g. '2020-03-25'). Defaults to None.

        Returns:
            A xarray.Dataset containing the required data.
        """

        # fallback to reader-init stardate and enddate
        if not startdate:
            startdate = self.startdate
        if not enddate:
            enddate = self.enddate

        data = self.backend.retrieve(var=var, level=level, level_coord=level_coord, startdate=startdate, enddate=enddate)

        # This links the dataset accessor to this instance of the Reader class
        if isinstance(data, xr.Dataset):
            data.aqua.set_default(self)

        # Preprocessing function
        if self.preproc:
            data = self.preproc(data)

        return data

    def select_area(self, data, lon=None, lat=None, **kwargs):
        """
        Select a specific area from the dataset based on longitude and latitude ranges.

        Args:
            lon (list, optional): Longitude limits for the area selection.
            lat (list, optional): Latitude limits for the area selection.
            **kwargs: Additional keyword arguments to pass to the selection function. (See AreaSelection)
        """
        # We're keeping the fldstat call separate, however at the current stage there is
        # no difference in behavior between the src and tgt fldstat calls.
        if self._check_if_regridded(data) and self.tgt_fldstat:
            return self.tgt_fldstat.select_area(data, lon=lon, lat=lat, **kwargs)
        return self.src_fldstat.select_area(data, lon=lon, lat=lat, **kwargs)

    def set_default(self):
        """Sets this reader as the default for the accessor."""

        Reader.instance = self  # Refresh the latest reader instance used

    def regrid(self, data):
        """Call the regridder function returning container or iterator"""

        if self.regridder is None:
            raise NoRegridError("regrid has not been initialized in the Reader, cannot perform any regrid.")

        data = counter_reverse_coordinate(data)

        out = self.regridder.regrid(data)

        # set regridded attribute to 1 for all vars
        out = set_attrs(
            out, {"AQUA_regridded": 1, "AQUA_source_grid": self.src_grid_name, "AQUA_target_grid": self.tgt_grid_name}
        )
        return out

    # def trend(self, data, dim='time', degree=1, skipna=False):
    #     """
    #     Estimate the trend of an xarray object using polynomial fitting.

    #     Args:
    #         data (DataArray or Dataset): The input data.
    #         dim (str): Dimension to apply trend along. Defaults to 'time'.
    #         degree (int): Degree of the polynomial. Defaults to 1.
    #         skipna (bool): Whether to skip NaNs. Defaults to False.

    #     Returns:
    #         DataArray or Dataset: The trend component.
    #     """
    #     final = self.trender.trend(data, dim=dim, degree=degree, skipna=skipna)
    #     final.aqua.set_default(self)
    #     return final

    def detrend(self, data, dim="time", degree=1, skipna=False):
        """
        Remove the trend from an xarray object using polynomial fitting.

        Args:
            data (DataArray or Dataset): The input data.
            dim (str): Dimension to apply detrend along. Defaults to 'time'.
            degree (int): Degree of the polynomial. Defaults to 1.
            skipna (bool): Whether to skip NaNs. Defaults to False.

        Returns:
            DataArray or Dataset: The detrended data.
        """
        final = self.trender.detrend(data, dim=dim, degree=degree, skipna=skipna)
        final.aqua.set_default(self)
        return final

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

    def vertinterp(self, data, levels=None, vert_coord="plev", units=None, method="linear"):
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
            raise KeyError("Levels for interpolation must be specified")

        # error if vert_coord is not there
        if vert_coord not in data.coords:
            raise KeyError(f"The vert_coord={vert_coord} is not in the data!")

        # if you not specified the units, guessing from the data
        if units is None:
            if hasattr(data[vert_coord], "units"):
                self.logger.warning("Units of vert_coord=%s has not defined, reading from the data", vert_coord)
                units = data[vert_coord].units
            else:
                raise ValueError("Original dataset has not unit on the vertical axis, failing!")

        if isinstance(data, xr.DataArray):
            final = self._vertinterp(data=data, levels=levels, units=units, vert_coord=vert_coord, method=method)

        elif isinstance(data, xr.Dataset):
            selected_vars = [da for da in data.data_vars if vert_coord in data[da].coords]
            final = data[selected_vars].map(
                self._vertinterp, keep_attrs=True, levels=levels, units=units, vert_coord=vert_coord, method=method
            )
        else:
            raise ValueError("This is not an xarray object!")

        final = log_history(
            final,
            f"Interpolated from original levels {data[vert_coord].values} "
            f"{data[vert_coord].units} to level {levels} using {method} method.",
        )

        final.aqua.set_default(self)  # This links the dataset accessor to this instance of the Reader class

        return final

    def _vertinterp(self, data, levels=None, units="Pa", vert_coord="plev", method="linear"):

        # verify units are good
        if data[vert_coord].units != units:
            self.logger.warning("Converting vert_coord units to interpolate from %s to %s", data[vert_coord].units, units)
            data = data.metpy.convert_coordinate_units(vert_coord, units)

        # very simple interpolation
        final = data.interp({vert_coord: levels}, method=method)

        return final

    # def reader_esm(self, esmcat, var):
    #     """
    #     Read intake-esm entry. Returns a dataset.

    #     Args:
    #         esmcat (intake_esm.core.esm_datastore): The intake-esm catalog datastore to read from.
    #         var (str or list): Variable(s) to retrieve. If None, uses the query from catalog metadata.

    #     Returns:
    #         xarray.Dataset: The dataset retrieved from the intake-esm catalog.
    #     """
    #     xarray_open_kwargs = esmcat.metadata.get(
    #         "xarray_open_kwargs", esmcat.metadata.get("cdf_kwargs", {"chunks": {"time": 1}})
    #     )
    #     query = esmcat.metadata["query"]
    #     if var:
    #         query_var = esmcat.metadata.get("query_var", "short_name")
    #         # Convert to list if not already
    #         query[query_var] = var.split() if isinstance(var, str) else var
    #     subcat = esmcat.search(**query)
    #     data = subcat.to_dataset_dict(
    #         xarray_open_kwargs=xarray_open_kwargs,
    #         # zarr_kwargs=dict(consolidated=True),
    #         # decode_times=True,
    #         # use_cftime=True)
    #         progressbar=False,
    #     )
    #     return list(data.values())[0]

    def _retrieve_plain(self):
        """
        Retrieve data without any additional processing.
        Making use of GridInspector, provide a sample data which has minimum
        size by subselecting along variables and time dimensions.
        Uses Reader's startdate/enddate if set.

        Args:
            *args: arguments to be passed to retrieve
            **kwargs: keyword arguments to be passed to retrieve

        Returns:
            A xarray.Dataset containing the required miminal sample data.
        """
        if self.sample_data is not None:
            self.logger.debug("Sample data already availabe, avoid _retrieve_plain()")
            return self.sample_data

        self.sample_data = self.backend.retrieve_plain(startdate=self.startdate)
        return self.sample_data

    def _grid_inspector(self, data):
        """
        Use smmregrid GridInspector to get minimal sample data

        Args:
            data (xarray.Dataset): input data

        Returns:
            A xarray.Dataset containing the required miminal sample data.
        """

        # get gridtypes from smrregird
        gridinspect = GridInspector(data, loglevel=self.loglevel)
        gridtypes = gridinspect.get_gridtype()

        # get info on time dimensions and variables
        minimal_variables = gridinspect.get_gridtype_attr(gridtypes, "variables")
        minimal_time = gridinspect.get_gridtype_attr(gridtypes, "time_dims")

        if minimal_variables:
            self.logger.debug("Variables found: %s", minimal_variables)
            data = data[minimal_variables]
        if minimal_time:
            self.logger.debug("Time dimensions found: %s", minimal_time)
            data = data.isel({t: 0 for t in minimal_time})
        return data

    def fldstat(
        self, data, stat, lon_limits=None, lat_limits=None, dims=None, region=None, region_sel=None, mask_kwargs={}, **kwargs
    ):
        """
        Field statistic wrapper which is calling the fldstat module from FldStat class.
        This method is exposing and providing field functions as Reader class
        methods through the wrapper accessors.

        Args:
            data (xr.DataArray or xarray.Dataset):  the input data
            stat (str):  the statistical function to be applied
            lon_limits (list, optional):  the longitude limits of the subset
            lat_limits (list, optional):  the latitude limits of the subset
            dims (list, optional):  the dimensions to average over
            region (regionmask.Regions, optional): A regionmask Regions object defining a class regions.
            region_sel (str, int or list, optional): The region(s) to select by name or number from the region object.
            mask_kwargs (dict, optional): Additional keyword arguments passed to region.mask().
            **kwargs: additional arguments passed to fldstat
        """
        # Handle regridding logic - use appropriate fldstat module
        if self._check_if_regridded(data) and self.tgt_fldstat is not None:
            data = self.tgt_fldstat.fldstat(
                data,
                stat=stat,
                lon_limits=lon_limits,
                lat_limits=lat_limits,
                region=region,
                region_sel=region_sel,
                mask_kwargs=mask_kwargs,
                dims=dims,
                **kwargs,
            )
        else:
            data = self.src_fldstat.fldstat(
                data,
                stat=stat,
                lon_limits=lon_limits,
                lat_limits=lat_limits,
                region=region,
                region_sel=region_sel,
                mask_kwargs=mask_kwargs,
                dims=dims,
                **kwargs,
            )

        data.aqua.set_default(self)
        return data

    # Field stats wrapper. If regridded, uses the target grid areas.
    def fldmean(self, data, **kwargs):
        """
        Field mean wrapper which is calling the fldstat module.
        """
        return self.fldstat(data, stat="mean", **kwargs)

    def fldmax(self, data, **kwargs):
        """
        Field max wrapper which is calling the fldstat module.
        """
        return self.fldstat(data, stat="max", **kwargs)

    def fldmin(self, data, **kwargs):
        """
        Field min wrapper which is calling the fldstat module.
        """
        return self.fldstat(data, stat="min", **kwargs)

    def fldstd(self, data, **kwargs):
        """
        Field standard deviation wrapper which is calling the fldstat module.
        """
        return self.fldstat(data, stat="std", **kwargs)

    def fldsum(self, data, **kwargs):
        """
        Field sum wrapper which is calling the fldstat module.
        """
        return self.fldstat(data, stat="sum", **kwargs)

    def fldintg(self, data, **kwargs):
        """
        Field integral wrapper which is calling the fldstat module.
        """
        return self.fldstat(data, stat="integral", **kwargs)

    def fldarea(self, data, **kwargs):
        """
        Field area wrapper which is calling the fldstat module.
        """
        return self.fldstat(data, stat="areasum", **kwargs)

    def timstat(self, data, stat, freq=None, exclude_incomplete=False, time_bounds=False, center_time=False, **kwargs):
        """
        Time statistic wrapper which is calling the timstat module from TimStat class.
        This method is exposing and providing time functions as Reader class
        methods through the wrapper accessors.

        Args:
            data (xr.DataArray or xarray.Dataset):  the input data
            stat (str):  the statistical function to be applied
            freq (str):  the frequency of the time average
            exclude_incomplete (bool):  exclude incomplete time averages
            time_bounds (bool):  produce time bounds after averaging
            center_time (bool):  center time for averaging
            kwargs:  additional arguments to be passed to the statistical function
        """
        data = self.timemodule.timstat(
            data,
            stat=stat,
            freq=freq,
            exclude_incomplete=exclude_incomplete,
            time_bounds=time_bounds,
            center_time=center_time,
            **kwargs,
        )
        data.aqua.set_default(self)  # accessor linking
        return data

    def timmean(self, data, **kwargs):
        """
        Time mean wrapper which is calling the timstat module.
        """
        return self.timstat(data, stat="mean", **kwargs)

    def timmax(self, data, **kwargs):
        """
        Time max wrapper which is calling the timstat module.
        """
        return self.timstat(data, stat="max", **kwargs)

    def timmin(self, data, **kwargs):
        """
        Time min wrapper which is calling the timstat module.
        """
        return self.timstat(data, stat="min", **kwargs)

    def timstd(self, data, **kwargs):
        """
        Time standard deviation wrapper which is calling the timstat module.
        """
        return self.timstat(data, stat="std", **kwargs)

    def timsum(self, data, **kwargs):
        """
        Time sum wrapper which is calling the timstat module.
        """
        return self.timstat(data, stat="sum", **kwargs)

    def timfirst(self, data, **kwargs):
        """
        Time first wrapper which is calling the timstat module.
        """
        return self.timstat(data, stat="first", **kwargs)

    def timlast(self, data, **kwargs):
        """
        Time last wrapper which is calling the timstat module.
        """
        return self.timstat(data, stat="last", **kwargs)

    def timhist(self, data, **kwargs):
        """
        Wrapper for the histogram function, with added timstat functionality.
        It accepts arguments of timstat to resample in time before computing the histogram.
        """
        return self.timstat(data, stat=histogram, **kwargs)

    def histogram(self, data, **kwargs):
        """
        Wrapper for the histogram function
        """
        return histogram(data, **kwargs)


def units_extra_definition():
    """Add units to the pint registry"""

    # special units definition
    # needed to work with metpy 1.4.0 see
    # https://github.com/Unidata/MetPy/issues/2884
    units._on_redefinition = "ignore"
    units.define("fraction = [] = Fraction = frac")
    units.define("psu = 1e-3 frac")
    units.define("PSU = 1e-3 frac")
    units.define("Sv = 1e+6 m^3/s")
    units.define("North = degrees_north = degreesN = degN")
    units.define("East = degrees_east = degreesE = degE")
