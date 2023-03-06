import intake
import intake_esm
import xarray as xr
import os
from metpy.units import units
import smmregrid as rg
from aqua.util import load_yaml, get_catalog_file
import sys
import subprocess
import tempfile

class Reader():
    """General reader for NextGEMS data (on Levante for now)"""

    def __init__(self, model="ICON", exp="tco2559-ng5", source=None, freq=None,
                 regrid=None, method="ycon", zoom=None, configdir=None,
                 level=None, areas=True, var=None, vars=None):
        """
        The Reader constructor.
        It uses the catalog `config/config.yaml` to identify the required data.
        
        Arguments:
            model (str):      model ID
            exp (str):        experiment ID
            source (str):     source ID
            regrid (str):     perform regridding to grid `regrid`, as defined in `config/regrid.yaml` (None)
            method (str):     regridding method (ycon)
            zoom (int):       healpix zoom level
            configdir (str)   folder where the config/catalog files are located (config)
            level (int):      level to extract if input data are 3D (starting from 0)
            areas (bool):     compute pixel areas if needed (True)
            var (str, list):  variable(s) which we will extract. "vars" is a synonym (None)
        
        Returns:
            A `Reader` class object.
        """

        if vars:
            self.var = vars
        else:
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
        extra = []

        self.grid_area = None
        self.src_grid_area = None
        self.dst_grid_area = None

        self.configdir, catalog_file = get_catalog_file(configdir=configdir)
        self.cat = intake.open_catalog(catalog_file)

        cfg_regrid = load_yaml(os.path.join(self.configdir,"regrid.yaml"))

        if source:
            self.source = source
        else:
            self.source = list(self.cat[model][exp].keys())[0]  # take first source if none provided
        
        source_grid = cfg_regrid["source_grids"][self.model][self.exp].get(self.source, None)
        if not source_grid:
            source_grid = cfg_regrid["source_grids"][self.model][self.exp]["default"]

        self.src_space_coord = source_grid.get("space_coord", None)
        self.space_coord = self.src_space_coord
        self.dst_space_coord = ["lon", "lat"]

        if regrid:
            self.vertcoord = source_grid.get("vertcoord", None) # Some more checks needed
            if level is not None:
                if not self.vertcoord:
                    raise KeyError("You should specify a vertcoord key in regrid.yaml for this source to use levels.")
                extra = f"-sellevidx,{level+1} "

            if (level is None) and self.vertcoord:
                raise RuntimeError("This is a masked 3d source: you should specify a specific level.")

            self.weightsfile =os.path.join(
                cfg_regrid["weights"]["path"],
                cfg_regrid["weights"]["template"].format(model=model,
                                                    exp=exp, 
                                                    method=method, 
                                                    target=regrid,
                                                    source=self.source,
                                                    level=("2d" if level is None else level)))
            if os.path.exists(self.weightsfile):
                self.weights = xr.open_mfdataset(self.weightsfile)
            else:
                sgridpath = source_grid["path"]
                if zoom:
                    sgridpath = sgridpath.format(zoom=(9-zoom))            
                print("Weights file not found:", self.weightsfile)
                print("Attempting to generate it ...")
                print("Source grid: ", sgridpath)

                # hack to  pass a correct list of all options
                src_extra = source_grid.get("extra", [])
                if src_extra:
                    if not isinstance(src_extra, list):
                        src_extra = [src_extra]
                if extra:
                    extra = [extra] 
                extra = extra + src_extra
                weights = rg.cdo_generate_weights(source_grid=sgridpath,
                                                      target_grid=cfg_regrid["target_grids"][regrid], 
                                                      method='ycon', 
                                                      gridpath=cfg_regrid["paths"]["grids"],
                                                      icongridpath=cfg_regrid["paths"]["icon"],
                                                      extra=extra)
                weights.to_netcdf(self.weightsfile)
                # For some weird reason it is better to reopen this from file ... to be investigated. It was failing for FESOM on orig. grid
                self.weights = xr.open_mfdataset(self.weightsfile)
                print("Success!")

            self.regridder = rg.Regridder(weights=self.weights)

            #if areas:
                # All needed areas have already been computed by the regridding procedure
                #planet_radius = 6371000  # same default as cdo
                #r2 = planet_radius * planet_radius
                #self.src_grid_area = self.weights.src_grid_area * r2
                #self.dst_grid_area = self.weights.dst_grid_area * r2
                #self.src_grid_area.attrs['units'] = 'm2'
                #self.dst_grid_area.attrs['units'] = 'm2'
                #self.src_grid_area.attrs['standard_name'] = 'area'
                #self.dst_grid_area.attrs['standard_name'] = 'area'
                #self.src_grid_area.attrs['long_name'] = 'area of grid cell'
                #self.dst_grid_area.attrs['long_name'] = 'area of grid cell'
                #self.grid_area = self.src_grid_area
        
        if areas:
            self.src_areafile =os.path.join(
                cfg_regrid["areas"]["path"],
                cfg_regrid["areas"]["src_template"].format(model=model, exp=exp, source=self.source))
            if os.path.exists(self.src_areafile):
                self.src_grid_area = xr.open_mfdataset(self.src_areafile).cell_area
            else:
                sgridpath = source_grid["path"]
                if zoom:
                    sgridpath = sgridpath.format(zoom=(9-zoom)) 
                print("Source areas file not found:", self.src_areafile)
                print("Attempting to generate it ...")
                print("Source grid: ", sgridpath)
                src_extra = source_grid.get("extra", [])
                grid_area = self.cdo_generate_areas(source=sgridpath,
                                                    gridpath=cfg_regrid["paths"]["grids"],
                                                    icongridpath=cfg_regrid["paths"]["icon"],
                                                    extra=src_extra)
                # Make sure that the new DataArray uses the expected spatial dimensions
                grid_area = self._rename_dims(grid_area, self.src_space_coord)
                data = self.retrieve(fix=False)
                grid_area = grid_area.assign_coords({coord: data.coords[coord] for coord in self.src_space_coord})
                
                self.src_grid_area = grid_area                           
                self.src_grid_area.to_netcdf(self.src_areafile)
                # ... aaand we reopen it because something is still not ok with our treatment of temp files
                self.src_grid_area = xr.open_mfdataset(self.src_areafile).cell_area
                print("Success!")

            if regrid:
                self.dst_areafile =os.path.join(
                    cfg_regrid["areas"]["path"],
                    cfg_regrid["areas"]["dst_template"].format(grid=self.targetgrid))
                if os.path.exists(self.dst_areafile):
                    self.dst_grid_area = xr.open_mfdataset(self.dst_areafile).cell_area
                else:
                    print("Destination areas file not found:", self.dst_areafile)
                    print("Attempting to generate it ...")
                    grid = cfg_regrid["target_grids"][regrid]
                    dst_extra = f"-const,1,{grid}"
                    grid_area = self.cdo_generate_areas(source=dst_extra)

                    data = self.retrieve(fix=False)
                    data = self.regridder.regrid(data.isel(time=0))
                    grid_area = grid_area.assign_coords({coord: data.coords[coord] for coord in self.dst_space_coord})

                    self.dst_grid_area = grid_area                           
                    self.dst_grid_area.to_netcdf(self.dst_areafile)
                    print("Success!")

            self.grid_area = self.src_grid_area

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

            areas = xr.open_dataset(area_file.name, engine="netcdf4")
            areas.cell_area.attrs['units'] = 'm2'  
            areas.cell_area.attrs['standard_name'] = 'area'
            areas.cell_area.attrs['long_name'] = 'area of grid cell'
            return areas.cell_area

        except subprocess.CalledProcessError as e:
            # Print the CDO error message
            print(e.output.decode(), file=sys.stderr)
            raise

        finally:
            # Clean up the temporary files
            if not isinstance(source, str):
                source_grid_file.close()
            area_file.close()


    def retrieve(self, regrid=False, timmean=False, fix=True, apply_unit_fix=True, var=None, vars=None):
        """
        Perform a data retrieve.
        
        Arguments:
            regrid (bool):          if to regrid the retrieved data (False)
            timmean (bool)          if to perform timmean of the retrieved data (False)
            fix (bool):             if to perform a fix (var name, units, coord name adjustments) (True)
            apply_unit_fix (bool):  if to already adjust units by multiplying by a factor or adding
                                    an offset (this can also be done later with the `fix_units` method) (True)
            var (str, list):  variable(s) which we will extract. "vars" is a synonym (None)
        Returns:
            A xarray.Dataset containing the required data.
        """

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
            cdf_kwargs = esmcat.metadata.get('cdf_kwargs', {"chunks": {"time":1}})
            query = esmcat.metadata['query']
            if var:
                query_var = esmcat.metadata.get('query_var', 'short_name')
                # Convert to list if not already
                query[query_var] = var.split() if isinstance(var, str) else var
            subcat = esmcat.search(**query)
            data = subcat.to_dataset_dict(cdf_kwargs=cdf_kwargs,
                                         zarr_kwargs=dict(consolidated=True),
                                              #decode_times=True,
                                              #use_cftime=True)
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

        # sequence which should be more efficient: averaging - regridding - fixing
        if self.freq and timmean:
            data = self.timmean(data)
        if self.targetgrid and regrid:
            data = self.regridder.regrid(data)  # XXX check attrs
            self.grid_area = self.dst_grid_area 
        if fix:
            data = self.fixer(data, apply_unit_fix=apply_unit_fix)
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

        out.attrs["regridded"]=1
        # set these two to the target grid (but they are actually not used so far)
        self.grid_area = self.dst_grid_area
        self.space_coord = ["lon", "lat"]
        return out
    
    def timmean(self, data, freq = None):
        """
        Perform daily and monthly averaging

        Arguments:
            data (xr.Dataset):  the input xarray.Dataset
        Returns:
            A xarray.Dataset containing the regridded data.
        """

        # translate frequency in pandas-style time
        if self.freq == 'mon':
            resample_freq = '1M'
        elif self.freq == 'day':
            resample_freq = '1D'
        else:
            resample_freq = self.freq
        
        try: 
            # resample, and assign the correct time
            out = data.resample(time=resample_freq).mean()
            proper_time = data.time.resample(time=resample_freq).mean()
            out['time'] = proper_time.values
        except: 
            sys.exit('Cant find a frequency to resample, aborting!')
   
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
        

    def _get_spatial_sample(self, da, space_coord):
        """
        Selects a single spatial sample along the dimensions specified in `space_coord`.

        Arguments:
            da (xarray.DataArray): Input data array to select the spatial sample from.
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
        Renames the dimensions of a DataArray so that any dimension which is already in `dim_list` keeps its name, 
        and the others are renamed to whichever other dimension name is in `dim_list`. 
        If `da` has only one dimension with a name which is different from that in `dim_list`, it is renamed to that new name. 
        If it has two coordinate names (e.g. "lon" and "lat") which appear also in `dim_list`, these are not touched.

        Parameters
        ----------
        da : xarray.DataArray
            The input DataArray to rename.
        dim_list : list of str
            The list of dimension names to use. 
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
        i=0
        da_out = da
        for dim in extra_dims:
            if dim not in shared_dims:
                da_out = da.rename({dim: new_dims[i]})
                i+=1
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
                                    These can be applied also later with the method `fix_units`. (false)

        Returns:
            A xarray.Dataset containing the fixed data and target units, factors and offsets in variable attributes.
        """

        fixes = load_yaml(os.path.join(self.configdir, "fixes.yaml"))
        model=self.model

        fix = fixes["models"].get(model, None)
        if not fix:
            print("No fixes defined for model ", model)
            return data

        self.deltat = fix.get("deltat", 1.0)
        fixd = {}
        allvars = data.variables
        fixvars = fix.get("vars", None)
        if fixvars:
            for var in fixvars:
                shortname = fix["vars"][var]["short_name"]
                if var in allvars: 
                    fixd.update({f"{var}": f"{shortname}"})
                    src_units = fix["vars"][var].get("src_units", None)
                    if src_units:
                        data[var].attrs.update({"units": src_units})
                    units = fix["vars"][var]["units"]
                    if units.count('{'):
                        units = fixes["defaults"]["units"][units.replace('{','').replace('}','')]                
                    data[var].attrs.update({"target_units": units})
                    factor, offset = self.convert_units(data[var].units, units, var)
                    data[var].attrs.update({"factor": factor})
                    data[var].attrs.update({"offset": offset})
                    if apply_unit_fix:
                        self.apply_unit_fix(data[var])

        allcoords = data.coords
        fixcoord = fix.get("coords", None)
        if fixcoord:
            for coord in fixcoord:
                    newcoord = fix["coords"][coord]["name"]
                    newcoord = fixes["defaults"]["coords"][newcoord.replace('{','').replace('}','')] 
                    if coord in allcoords: 
                        fixd.update({f"{coord}": f"{newcoord}"})

        return data.rename(fixd)


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
        factor = units(src).to_base_units() / units(dst).to_base_units()

        if factor.units == units('dimensionless'):
            offset = (0 * units(src)).to(units(dst)) - (0 * units(dst))
        else:
            if factor.units == "meter ** 3 / kilogram":
                # Density of water was missing
                factor = factor * 1000 * units("kg m-3")
                print(f"{var}: corrected multiplying by density of water 1000 kg m-3")
            if factor.units == "meter ** 3 * second / kilogram":
                # Density of water and accumulation time were missing
                factor = factor * 1000 * units("kg m-3") / (self.deltat * units("s"))
                print(f"{var}: corrected multiplying by density of water 1000 kg m-3")
                print(f"{var}: corrected dividing by accumulation time {self.deltat} s")
            else:
                print(f"{var}: incommensurate units converting {src} to {dst} --> {factor.units}")
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
            data.attrs["units"] = target_units
            factor = data.attrs.get("factor", 1)
            offset = data.attrs.get("offset", 0)
            if factor != 1:
                data *= factor
            if offset != 0:
                data += offset


        