"""New Regrid class independent from the Reader"""
import os
import re
import shutil
from tempfile import TemporaryDirectory
import xarray as xr
from smmregrid import CdoGenerate, GridInspector
from smmregrid import Regridder as SMMRegridder
from smmregrid.util import check_gridfile
from aqua.core.logger import log_configure
from aqua.core.util import to_list
from .griddicthandler import GridDictHandler
from .regridder_util import check_existing_file, validate_reader_kwargs


# parameters which will affect the weights and areas name
DEFAULT_WEIGHTS_AREAS_PARAMETERS = ['zoom']

# default CDO regrid method
DEFAULT_GRID_METHOD = 'ycon'

# default dimension for the weights and areas
DEFAULT_DIMENSION = '2d'
DEFAULT_DIMENSION_MASK = '2dm'  # masked grid

# please notice: check_gridfile is a function from smmregrid.util
# to check and if a grid is a cdo grid,
# a file on the disk or xarray dataset. Possible inclusion of CDOgrid object is considered
# but should be likely developed on the smmregrid side.


class Regridder():
    """AQUA Regridder class"""

    def __init__(self, cfg_grid_dict: dict = None,
                 src_grid_name: str = None,
                 data: xr.Dataset = None,
                 cdo: str = None,
                 loglevel: str = "WARNING"):
        """
        The (new) Regridder class. Can be initialized with a data (xr.Dataset/DataArray) or a src_grid_name
        It provides methods to generate areas and weights, and to regrid a dataset.

        Args:
            cfg_grid_dict (dict): The dictionary containing the full AQUA grid configuration.
            src_grid_name (str, optional): The name of the source grid in the AQUA convention.
            data (xarray.Dataset, optional): The dataset to be regridded if src_grid_name is not provided.
            cdo (str, optional): The path to the CDO executable. If None, guess it from the system.
            loglevel (str): The logging level.

        Attributes:
            loglevel (str): The logging level.
            logger (logging.Logger): The logger.
            cfg_grid_dict (dict): The full AQUA grid dictionary.
            src_grid_name (str): The source grid name.
            handler (GridDictHandler): The grid dictionary handler.
            src_grid_dict (dict): The normalized source grid dictionary.
            src_horizontal_dims (str): The source horizontal dimensions.
            src_vertical_dim (str): The source vertical dimension.
            tgt_horizontal_dims (str): The target horizontal dimensions.
            error (str): The error message to be used by the Reader.
            cdo (str): The CDO path.
            smmregridder (dict): The SMMregrid regridder object for each vertical coordinate.
            src_grid_area (xarray.Dataset): The source grid area.
            tgt_grid_area (xarray.Dataset): The target grid area.
            masked_attrs (dict): The masked attributes.
            masked_vars (list): The masked variables.
            extra_dims (dict): The extra dimensions (from cfg_grid_dict) to be sent to smmregrid.
        """

        if src_grid_name is None and data is None:
            raise ValueError("Either src_grid_name or data must be provided.")

        self.loglevel = loglevel
        self.logger = log_configure(log_level=loglevel, log_name='Regridder')

        # define basic attributes:
        self.cfg_grid_dict = cfg_grid_dict if cfg_grid_dict else {}  # full grid dictionary
        self.src_grid_name = src_grid_name  # source grid name

        # we want all the grid dictionary to be real dictionaries
        self.handler = GridDictHandler(cfg_grid_dict,
                                       default_dimension=DEFAULT_DIMENSION,
                                       loglevel=loglevel)
        self.src_grid_dict = self.handler.normalize_grid_dict(self.src_grid_name)
        self.src_grid_path = self.src_grid_dict.get('path')

        self.logger.debug("Normalized grid dictionary: %s", self.src_grid_dict)
        self.logger.debug("Normalized grid path: %s", self.src_grid_path)

        # this not used but can be shipped back to the reader
        self.src_horizontal_dims = self.src_grid_dict.get('space_coord', None)
        self.src_vertical_dim = list(self.src_grid_path.keys())
        self.tgt_horizontal_dims = None
        self.error = None

        self.logger.debug("Horizontal dimensions: %s", self.src_horizontal_dims)
        self.logger.debug("Vertical dimensions: %s", self.src_vertical_dim)

        # store dimension to be send to smmregrid if needed
        self.extra_dims = {
            'vertical': to_list(self.src_vertical_dim),
            'horizontal': to_list(self.src_horizontal_dims)
        }

        # check data to extract information for CDO
        if data is not None:
            self._get_info_from_data(data)

        if not self.src_horizontal_dims:
            self.error = "Horizontal dimensions not found in the grid path. Please provide a dataset. "
            self.logger.warning(self.error)
            return

        # check if the grid path has been defined
        if not self.src_grid_path:
            self.error = "Source grid path not found. Please provide a dataset."
            self.logger.warning(self.error)
            return

        # check if CDO is available
        self.cdo = self._set_cdo(cdo=cdo)

        # SMMregridders dictionary for each vertical coordinate
        self.smmregridder = {}

        # source and target areas
        self.src_grid_area = None
        self.tgt_grid_area = None

        # configure the masked fields
        self.masked_attrs, self.masked_vars = self.configure_masked_fields(self.src_grid_dict)

        self.logger.info("Grid name: %s", self.src_grid_name)
        self.logger.debug("Grid dictionary: %s", self.src_grid_dict)

    def _set_cdo(self, cdo=None):
        """
        Check information on CDO to set the correct version.

        Args:
            cdo (str, optional): The path to the CDO executable. If None, guess it from the system.

        Returns:
            str: The path to the CDO executable.

        Raises:
            FileNotFoundError: If CDO is not found in the system path.
        """
        if cdo:
            # TODO: add a subprocess call to add if cdo is available
            self.logger.debug("Going to use CDO in: %s", cdo)
            return cdo

        cdo = shutil.which("cdo")
        if cdo:
            self.logger.debug("Found CDO path: %s", cdo)
            return cdo

        raise FileNotFoundError(
                "CDO not found in path: Weight and area generation will fail.")

    def _get_info_from_data(self, data):
        """
        Extract information from the dataset to be used in the regridding process
        """

        gridinspector = GridInspector(data, loglevel=self.loglevel, extra_dims=self.extra_dims)
        gridtypes = gridinspector.get_gridtype()

        # if we have not them from the dictionary, get it from the file
        if not self.src_horizontal_dims:
            # however we have only one 2d grid at the time in AQUA
            self.src_horizontal_dims = gridtypes[0].horizontal_dims
        self.logger.debug("Horizontal dimensions guessed from data: %s", self.src_horizontal_dims)

        # This should be not necessary since vertical coordinate is always provided
        # if we have not them from the dictionary, get it from the file
        # if not self.src_vertical_dim:
        #    # get all vertical grid available
        #    self.src_vertical_dim = [getattr(gridtype, "vertical_dim") for gridtype in gridtypes]
        # self.logger.debug("Vertical dimensions guessed from data: %s", self.src_vertical_dim)

        # if the path is missing, use the data to init smmregrid
        if not self.src_grid_path:
            vdim = self.src_vertical_dim if self.src_vertical_dim else DEFAULT_DIMENSION
            self.logger.info("Using provided dataset as a grid path for %s", vdim)
            self.src_grid_dict = {"path": {vdim: data}}
            self.src_grid_path = self.src_grid_dict.get('path')

    def areas(self, tgt_grid_name=None, rebuild=False, reader_kwargs=None):
        """
        Load or generate regridding areas for the source or target grid.

        Args:
            tgt_grid_name (str, optional): Name of the target grid.
                                           If None, the self.src_grid_name is used.
            rebuild (bool, optional): If True, forces regeneration of the area.
            reader_kwargs (dict, optional): Additional parameters for the reader.

        Returns:
            xr.Dataset: The computed grid area.
        """

        # normalize dictionaries for target grid
        if tgt_grid_name:
            grid_dict = self.handler.normalize_grid_dict(tgt_grid_name)
        else:
            grid_dict = self.src_grid_dict

        # generate the area
        grid_area = self._load_area(
            grid_name=tgt_grid_name,
            grid_dict=grid_dict,
            reader_kwargs=reader_kwargs,
            rebuild=rebuild
        )

        # assign the area to the correct attribute
        if tgt_grid_name:
            self.tgt_grid_area = grid_area
            # Extra: infer target grid horizontal dimensions
            self.tgt_horizontal_dims = GridInspector(
                self.tgt_grid_area, loglevel=self.loglevel
            ).get_gridtype()[0].horizontal_dims

        else:
            self.src_grid_area = grid_area

        return grid_area
    
    def _safe_to_netcdf(self, data, filename):
        """Save to netcdf safely using a temporary file.
        
        Args:
            data (xr.Dataset or xr.DataArray): Data to save.
            filename (str): Destination file path.
        """
        # Ensure parent directory exists
        dest_dir = os.path.dirname(os.path.abspath(filename))
        if dest_dir:  # Handle edge case where filename has no directory component
            os.makedirs(dest_dir, exist_ok=True)
        else:
            dest_dir = '.'  # Use current directory
        
        # Create temp file in same directory as destination (same filesystem)
        with TemporaryDirectory(dir=dest_dir) as tmpdirname:
            tmp_file = os.path.join(tmpdirname, "temp.nc")
            data.to_netcdf(tmp_file)
            os.replace(tmp_file, filename)

    def _load_area(self, grid_name, grid_dict, reader_kwargs, rebuild=False):
        """
        Load or generate the grid area.

        Args:
            grid_name (str): The grid name. If None, roll back to src_grid_name.
            grid_dict (dict): The normalized grid dictionary.
            reader_kwargs (dict): Additional reader parameters.
            target (bool): Whether this is for the target grid (default: False).
            rebuild (bool): If True, forces regeneration of the area.

        Returns:
            xr.Dataset: The computed grid area.
        """
        area_filename = self._area_filename(grid_name if grid_name else None, reader_kwargs)
        area_type = "target" if grid_name else "source"

        # if file exists, load it
        if not rebuild and check_existing_file(area_filename):
            self.logger.info("Loading existing %s area from %s.", area_type, area_filename)
            return xr.open_dataset(area_filename)

        # generate and save the area
        grid_area = self._generate_area(grid_name, grid_dict, area_filename, area_type)
        self._safe_to_netcdf(grid_area, area_filename)
        self.logger.info("Saved %s area to %s.", area_type, area_filename)

        return grid_area

    def _generate_area(self, grid_name, grid_dict, area_filename, area_type):
        """
        Loads cell areas if available; otherwise, generates the area.

        Args:
            grid_name (str): The grid name: if None, the source grid is used.
            grid_dict (dict): The normalized grid dictionary.
            area_filename (str): The precomputed area filename.
            area_type (str): The area type (i.e. source or target)
        """

        # if they have been provided, read from the AQUA dict
        cellareas, cellareas_var = grid_dict.get('cellareas'), grid_dict.get('cellareas_var')
        if cellareas and cellareas_var:
            self.logger.info("Using cellareas from variable %s in file %s",
                             cellareas_var, cellareas)
            if not os.path.exists(cellareas):
                raise FileNotFoundError(f"Grid based cell area  file {cellareas} not found.")
            return xr.open_mfdataset(cellareas)[cellareas_var].rename("cell_area").squeeze().to_dataset()

        # clean if necessary
        if os.path.exists(area_filename):
            self.logger.info("%s areas file %s exists. Regenerating.", area_type, area_filename)

        self.logger.info("Generating %s area for %s", area_type, grid_name)

        source_grid = self._get_grid_path(grid_dict.get('path')) if area_type == "source" else None
        target_grid = self._get_grid_path(grid_dict.get('path')) if area_type == "target" else None

        return CdoGenerate(
            source_grid=source_grid,
            target_grid=target_grid,
            cdo_extra=grid_dict.get('cdo_extra'),
            cdo_options=grid_dict.get('cdo_options'),
            cdo=self.cdo,
            loglevel=self.loglevel
        ).areas(target=bool(grid_name))

    def weights(self, tgt_grid_name, regrid_method=None, nproc=1,
                rebuild=False, reader_kwargs=None):
        """
        Load or generate regridding weights calling smmregrid

        Args:
            tgt_grid_name (str): The destination grid name.
            regrid_method (str): The regrid method.
            nproc (int): The number of processors to use.
            rebuild (bool): If True, rebuild the weights.
            reader_kwargs (dict): The reader kwargs for filename definition,
                                  including info on model, exp, source, etc.
        """

        # define regrid method
        default_regrid_method = self.src_grid_dict.get('regrid_method', DEFAULT_GRID_METHOD)
        regrid_method = regrid_method if regrid_method else default_regrid_method
        if regrid_method is not DEFAULT_GRID_METHOD:
            self.logger.info("Regrid method: %s", regrid_method)

        # normalize the tgt grid dictionary and path
        tgt_grid_dict = self.handler.normalize_grid_dict(tgt_grid_name)

        # get the cdo options from the configuration
        cdo_extra = self.src_grid_dict.get('cdo_extra', None)
        cdo_options = self.src_grid_dict.get('cdo_options', None)

        # loop over the vertical coordinates: DEFAULT_DIMENSION, DEFAULT_DIMENSION_MASK, or any other
        for vertical_dim in self.src_grid_path:

            # define the vertical coordinate in the smmregrid world
            smm_vertical_dim = None if vertical_dim in [
                DEFAULT_DIMENSION, DEFAULT_DIMENSION_MASK] else vertical_dim

            weights_filename = self._weights_filename(tgt_grid_name, regrid_method,
                                                      vertical_dim, reader_kwargs)

            # check if weights already exist, if not, generate them
            if rebuild or not check_existing_file(weights_filename):

                if os.path.exists(weights_filename):
                    self.logger.info(
                        "Weights file %s exists. Regenerating.", weights_filename)
                else:
                    self.logger.info(
                        "Generating weights for %s grid: %s", tgt_grid_name, vertical_dim)

                if smm_vertical_dim:
                    self.logger.warning("Mask-changing vertical dimension identified, weights generation might take a few!")

                # smmregrid call
                # TODO: here or better in smmregird, we could use GridInspect to get the grid info
                # and reduce the dimensionality of the input data.
                generator = CdoGenerate(source_grid=self.src_grid_path[vertical_dim],
                                        target_grid=self._get_grid_path(tgt_grid_dict.get('path')),
                                        cdo_extra=cdo_extra,
                                        cdo_options=cdo_options,
                                        cdo=self.cdo,
                                        loglevel=self.loglevel)

                # generate and save the weights
                weights = generator.weights(method=regrid_method,
                                            vertical_dim=smm_vertical_dim,
                                            nproc=nproc)
                self._safe_to_netcdf(weights, weights_filename)

            else:
                self.logger.info(
                    "Loading existing weights from %s.", weights_filename)
                
            # load the weights
            weights = xr.open_dataset(weights_filename)

            # initialize the regridder
            self.smmregridder[vertical_dim] = SMMRegridder(
                weights=weights,
                horizontal_dims=self.src_horizontal_dims,
                vertical_dim=smm_vertical_dim,
                loglevel=self.loglevel
            )

    def _area_filename(self, tgt_grid_name, reader_kwargs):
        """"
        Generate the area filename.

        Args:
            tgt_grid_name (str): The destination grid name.
            reader_kwargs (dict): The reader kwargs, including info on model, exp, source, etc.
        """

        area_dict = self.cfg_grid_dict.get('areas')

        if not area_dict:
            self.logger.warning(
                "Areas block not found in the configuration file, using fallback naming scheme.")
            if tgt_grid_name:
                return f"cell_area_{tgt_grid_name}.nc"
            return f"cell_area_{self.src_grid_name}.nc"

        # destination grid name is provided, use grid template
        if tgt_grid_name:
            filename = area_dict["template_grid"].format(grid=tgt_grid_name)
            self.logger.debug(
                "Using grid-based template for target grid. Filename: %s", filename)
        # source grid name is provided, check if it is data
        else:
            if check_gridfile(self._get_grid_path(self.src_grid_path)) != 'xarray':
                filename = area_dict["template_grid"].format(
                    grid=self.src_grid_name)
                self.logger.debug(
                    "Using grid-based template for source grid. Filename: %s", filename)
            else:
                reader_kwargs = validate_reader_kwargs(reader_kwargs)
                filename = area_dict["template_default"].format(
                    model=reader_kwargs["model"],
                    exp=reader_kwargs["exp"],
                    source=reader_kwargs["source"])
                self.logger.debug(
                    "Using source-based template for source grid. Filename: %s", filename)

        filename = self._insert_kwargs(filename, reader_kwargs)
        filename = self._filename_prepend_path(filename, kind="areas")
        return filename

    def _weights_filename(self, tgt_grid_name, regrid_method, vertical_dim, reader_kwargs):
        """
        Generate the weights filename.

        Args:
            tgt_grid_name (str): The destination grid name.
            regrid_method (str): The regrid method.
            vertical_dim (str): The vertical dimension.
            reader_kwargs (dict): The reader kwargs, including info on model, exp, source, etc.
        """

        levname = vertical_dim if vertical_dim in [
            DEFAULT_DIMENSION, DEFAULT_DIMENSION_MASK] else f"3d-{vertical_dim}"

        weights_dict = self.cfg_grid_dict.get('weights')

        if not weights_dict:
            self.logger.warning(
                "Weights block not found in the configuration file, using fallback naming scheme.")
            return f"weights_{tgt_grid_name}_{regrid_method}_l{levname}.nc"

        # destination grid name is provided, use grid template
        if check_gridfile(self.src_grid_path[vertical_dim]) != 'xarray':
            filename = weights_dict["template_grid"].format(
                sourcegrid=self.src_grid_name,
                method=regrid_method,
                targetgrid=tgt_grid_name,
                level=levname)
            self.logger.debug(
                "Using grid-based template for target grid. Filename: %s", filename)
        else:
            reader_kwargs = validate_reader_kwargs(reader_kwargs)
            filename = weights_dict["template_default"].format(
                model=reader_kwargs["model"],
                exp=reader_kwargs["exp"],
                source=reader_kwargs["source"],
                method=regrid_method,
                targetgrid=tgt_grid_name,
                level=levname)
            self.logger.debug(
                "Using source-based template for target grid. Filename: %s", filename)

        filename = self._insert_kwargs(filename, reader_kwargs)
        filename = self._filename_prepend_path(filename, kind="weights")
        return filename

    def _filename_prepend_path(self, filename, kind="weights"):
        """
        Prepend the path to the filename with some fall back option
        """
        if not self.cfg_grid_dict.get("paths"):
            self.logger.warning(
                "Paths block not found in the configuration file, using present directory.")
        else:
            if not self.cfg_grid_dict["paths"].get(kind):
                self.logger.warning(
                    "%s block not found in the paths block, using present directory.", kind)
            else:
                # if path does not exist, create it
                if not os.path.exists(self.cfg_grid_dict["paths"][kind]):
                    self.logger.warning(
                        "%s path in %s does not exist: creating!", kind, self.cfg_grid_dict["paths"][kind])
                    os.makedirs(self.cfg_grid_dict["paths"][kind], exist_ok=True)
                filename = os.path.join(
                    self.cfg_grid_dict["paths"][kind], filename)
        return filename

    def _expand_dims(self, data, vertical_dims):
        """
        Expand the dimensions of the dataset or dataarray to include the vertical dimensions
        """

        if not list(set(data.dims) & set(vertical_dims)):
            for vertical_dim in vertical_dims:
                if vertical_dim in data.coords:
                    self.logger.debug(
                        "Expanding dimensions to include %s", vertical_dim)
                    data = data.expand_dims(dim=vertical_dim, axis=0)
        return data

    def _group_shared_dims(self, data):
        """
        Groups variables in a dataset that share the same AQUA vertical dimension.
        Built on GridInspector and GridType classes from smmregrid.
        It is a sort of overkill of what smmregrid do internally.

        Args:
            data (xarray.Dataset): The dataset to be regridded.

        Return:
            shared_vars (dict): A dictionary of variables that share the same vertical dimensions.
        """

        shared_vars = {}
        # TODO: masked vars based on attributes are still missing
        if self.masked_vars:
            shared_vars[DEFAULT_DIMENSION_MASK] = self.masked_vars
            self.logger.debug("Variables for coordinate %s: %s",
                              DEFAULT_DIMENSION_MASK, shared_vars[DEFAULT_DIMENSION_MASK])
        # Get the masked variables safely
        masked_vars = shared_vars.get(DEFAULT_DIMENSION_MASK, [])

        # scan the grid
        gridtypes = GridInspector(data, extra_dims=self.extra_dims, loglevel=self.loglevel).get_gridtype()

        self.logger.debug("Gridtypes found: %s", len(gridtypes))
        for gridtype in gridtypes:
            variables = list(gridtype.variables.keys())

            if gridtype.vertical_dim:
                self.logger.debug("Variables for dimension %s: %s",
                                  gridtype.vertical_dim, variables)
                shared_vars[gridtype.vertical_dim] = [var for var in variables if var not in masked_vars]
            else:
                shared_vars[DEFAULT_DIMENSION] = [var for var in variables if var not in masked_vars]
                self.logger.debug("Variables for dimensions %s: %s",
                                  DEFAULT_DIMENSION, shared_vars[DEFAULT_DIMENSION])

        return shared_vars

    def regrid(self, data):
        """
        Actual regridding core function. Regrid the dataset or dataarray using common gridtypes
        Firstly, expand the dimensions of the dataset to include the vertical dimensions if necessary.
        Then, group variables that share the same dimensions.
        Finally, apply regridding on the different vertical coordinates, including 2d and 2dm.

        Args:
            data (xarray.Dataset, xarray.DataArray): The dataset to be regridded.
        """

        # expand the dimensions of the dataset to include the vertical dimensions
        if isinstance(data, xr.Dataset):
            data = data.map(self._expand_dims, vertical_dims=list(self.src_vertical_dim))
        elif isinstance(data, xr.DataArray):
            data = self._expand_dims(data, vertical_dims=list(self.src_vertical_dim))
        else:
            raise ValueError("Data must be an xarray Dataset or DataArray.")

        # get which variables share the same dimensions
        shared_vars = self._group_shared_dims(data)

        # compact regridding on all dataset with map
        if isinstance(data, xr.Dataset):
            data = data.map(self._apply_regrid, shared_vars=shared_vars)
        elif isinstance(data, xr.DataArray):
            data = self._apply_regrid(data, shared_vars)

        return data

    def _apply_regrid(self, data, shared_vars):
        """
        Core regridding function.
        Apply regridding on the different vertical coordinates, including 2d and 2dm
        """

        for vertical, variables in shared_vars.items():
            if data.name in variables:
                if not self.smmregridder.get(vertical):
                    self.logger.error("Regridder for vertical coordinate %s not found.", vertical)
                    self.logger.error("Cannot regrid variable %s", data.name)
                    continue
                # TODO: if smmregridder is not found, we can call the weights method to generate on the fly
                return self.smmregridder[vertical].regrid(data)
        return data

    @staticmethod
    def _insert_kwargs(filename, reader_kwargs):
        """
        Insert the DEFAULT_WEIGHTS_AREAS_PARAMETERS in the filename template.
        """
        # add the kwargs naming in the template file
        if isinstance(reader_kwargs, dict):
            for parameter in DEFAULT_WEIGHTS_AREAS_PARAMETERS:
                if parameter in reader_kwargs:
                    filename = re.sub(
                        r'\.nc', '_' + parameter +
                        str(reader_kwargs[parameter]) + r'\g<0>',
                        filename)

        return filename

    @staticmethod
    def configure_masked_fields(src_grid_dict):
        """
        if the grids has the 'masked' option, this can be based on
        generic attribute or alternatively of a series of specific variables using the 'vars' key

        Args:
            source_grid (dict): Dictionary containing the grid information

        Returns:
            masked_attr (dict): Dict with name and proprierty of the attribute to be used for masking
            masked_vars (list): List of variables to mask
        """
        masked_info = src_grid_dict.get("masked")
        if masked_info is None:
            return None, None

        masked_vars = masked_info.get("vars")
        masked_attrs = {k: v for k, v in masked_info.items() if k !=
                        "vars"} or None

        return masked_attrs, masked_vars

    @staticmethod
    def _get_grid_path(grid_path):
        """
        Get the grid path from the grid dictionary.
        This looks for `DEFAULT_DIMENSION` key,
        otherwise takes the first available value in the dict.
        """
        return grid_path.get(DEFAULT_DIMENSION, next(iter(grid_path.values()), None))
