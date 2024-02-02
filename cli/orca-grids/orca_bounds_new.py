#!/usr/bin/env python3

import abc

import numpy as np
import xarray as xr
import traceback
import argparse
import sys


class OrcaMesh(metaclass=abc.ABCMeta):
    """ An ORCA-specific mesh class. """

    # vertical axis dimension name
    #VDIM = 'level'
    
    # bounds dimension name
    BNDS_DIM = 'bnds'
    VBNDS_DIM = 'vbnds'
    
    # dimension for unstructured grid
    UNSTRUCT_DIM = 'cell'

    FILLVAL = -1.e20

    def __init__(self, args):
        self.stagg = (args.stagg).lower()
        self.VDIM = (args.zname).lower()
        self.level = args.level
        self.ds_xesmf = self._geom_to_xesmf(args.meshmask)
        self.ds_xesmf = self._set_mesh_attrs()

    @staticmethod
    def _get_level_bnds(depths_ctn, vbnds_dim):
        """
        Get the bounds of the depth levels.
        WARNING: this is not perfect as it does **not** account for:
        1) partial cells at the bottom (on each column, each deepest cell is 'cut'
           thinner to better represent topography');
        2) vertical-varying layers, if used in the NEMO runs (at each time step and on each column, 
           ssh variations are distributed throughout the full column.
        """
        
        zdim = list(depths_ctn.sizes)[0]
        nz = depths_ctn.sizes[zdim]
        
        ctns_arr = depths_ctn.values
        bnds_tmp = np.ndarray(shape=[nz+1], dtype=float)
        
        bnds_tmp[0] = 0.
        for k in range(0, nz):
            bnds_tmp[k+1] = 2 * ctns_arr[k] - bnds_tmp[k]
        
        return xr.DataArray(data=np.transpose([bnds_tmp[:-1],
                                               bnds_tmp[1:]]),
                            dims=[zdim, vbnds_dim])
        

    def _set_mesh_attrs(self):
        """ Set attributes on mesh dataset. """

        ds_out = self.ds_xesmf.copy()

        ds_out['lat'].attrs = {'standard_name': 'latitude',
                               'units': 'degrees North',
                               'bounds': 'lat_b'}

        ds_out['lon'].attrs = {'standard_name': 'longitude',
                               'units': 'degrees East',
                               'bounds': 'lon_b'}

        for var in ds_out.variables:
            ds_out[var].encoding = {'_FillValue': None}

        ds_out.attrs['node_type'] = self.stagg.upper()
            
        return ds_out
        
    def _geom_to_xesmf(self, meshfile):
        """ Return grid/mask dataset understandable by xESMF. """

        get_vars = ["glam"+self.stagg,
                    "gphi"+self.stagg,
                    "e1"+self.stagg,
                    "e2"+self.stagg,
                    self.stagg+"mask"]

        if self.level:
            get_vars += ['gdept_1d']
            
        ds_mesh = xr.open_dataset(meshfile, drop_variables=['time_counter'])\
                    .squeeze()

        ds_bounds = self._get_bounds_coords(ds_mesh, self.stagg)

        ds_mesh = ds_mesh[get_vars]
        ds_mesh = xr.merge((ds_mesh, ds_bounds))
        
        if 'nav_lev' in ds_mesh.variables:
            ds_mesh = ds_mesh.drop_vars(['nav_lev'])

        ds_mesh = ds_mesh.rename({'glam'+self.stagg: 'lon',
                                  'gphi'+self.stagg: 'lat'}).set_coords(['lat', 'lon'])

        ds_mesh = ds_mesh.rename({self.stagg+'mask': 'mask'})
        ds_mesh['mask'].attrs = {'standard_name': 'mask',
                                 'flag_values': '0; 1',
                                 'flag_meanings': 'dry; wet'}
        ds_mesh['mask'].encoding = {'_FillValue': None,
                                    'dtype': 'int32'}
        if not self.level:
            ds_mesh['mask'] = ds_mesh['mask'].max(dim='nav_lev')
        
        ds_mesh['cell_area'] = (ds_mesh['e1'+self.stagg] * ds_mesh['e2'+self.stagg])
        ds_mesh['cell_area'].attrs = {'standard_name': 'cell_area',
                                      'units': 'm2'}
        ds_mesh['cell_area'].encoding = {'_FillValue': -1.e20,
                                         'dtype': 'int64'}
                        
        ds_mesh = ds_mesh.drop_vars(['e1'+self.stagg, 'e2'+self.stagg])

        if self.level:
            ds_mesh = ds_mesh.rename_dims({'nav_lev': self.VDIM})
            da_zaxis = ds_mesh['gdept_1d']
            ds_mesh = ds_mesh.drop_vars(['gdept_1d'])\
                             .assign_coords({self.VDIM: da_zaxis})
            ds_mesh[self.VDIM].attrs = {'standard_name': 'depth',
                                        'positive': 'down',
                                        'units': 'm',
                                        'bounds': self.VDIM+'_'+self.VBNDS_DIM}
            ds_mesh[self.VDIM].encoding = {'dtype': 'float64',
                                           '_FillValue': None}
            ds_mesh[self.VDIM+'_'+self.VBNDS_DIM] = self._get_level_bnds(ds_mesh[self.VDIM],
                                                                         self.VBNDS_DIM)
                
            ds_mesh['cell_area'] = ds_mesh['cell_area']\
                .where((ds_mesh['mask'] > 0.5).any(dim=self.VDIM))
        else:
            ds_mesh['cell_area'] = ds_mesh['cell_area']\
                .where(ds_mesh['mask'] > 0.5)
        
        return ds_mesh

    def _get_corner_dict(self):
        """ Get an info dictionary about the relative arranging (center, vertex, symmetry for the edges) of the desired gridpoint."""
        nodetype = self.stagg
        assert nodetype in ('t', 'u', 'v', 'f'), "nodetype {} unknown".format(nodetype)

        crn_infos = {'ctn': nodetype}
        if nodetype == 't':
            crn_infos['crn'] = 'f'
            crn_infos['pivot_x'] = 'v'
            crn_infos['pivot_y'] = 'u'
            crn_infos['fwd_x'] = True
            crn_infos['fwd_y'] = True
        elif nodetype == 'u':
            crn_infos['crn'] = 'v'
            crn_infos['pivot_x'] = 'f'
            crn_infos['pivot_y'] = 't'
            crn_infos['fwd_x'] = False
            crn_infos['fwd_y'] = True
        elif nodetype == 'v':
            crn_infos['crn'] = 'u'
            crn_infos['pivot_x'] = 't'
            crn_infos['pivot_y'] = 'f'
            crn_infos['fwd_x'] = True
            crn_infos['fwd_y'] = False
        elif nodetype == 'f':
            crn_infos['crn'] = 't'
            crn_infos['pivot_x'] = 'u'
            crn_infos['pivot_y'] = 'v'
            crn_infos['fwd_x'] = False
            crn_infos['fwd_y'] = False

        return crn_infos

    def _get_bounds_coords(self, ds_mesh, grid_ctn):
        """ Get the geogrpahical coordinates of the cell vertexes from the grid_ctn dictionary."""

        ds_all_coords = self._get_all_coords(ds_mesh)
        crn_info = self._get_corner_dict()

        nx = ds_mesh.sizes['x']
        ny = ds_mesh.sizes['y']

        ifwd_x = int(crn_info['fwd_x'])
        ifwd_y = int(crn_info['fwd_y'])

        slice_main_x = slice(ifwd_x, nx + ifwd_x)
        slice_main_y = slice(ifwd_y, ny + ifwd_y)

        idx_single_x = -1 + ifwd_x
        idx_single_y = -1 + ifwd_y

        ds_out = xr.Dataset()

        for coord in ['lon', 'lat']:
            tmp_bnds = xr.DataArray(data=np.ndarray(shape=[ny + 1, nx + 1],
                                                    dtype=float), dims=['y_b', 'x_b'])
            
            tmp_bnds.loc[dict(x_b=slice_main_x,
                              y_b=slice_main_y)] = \
                                  ds_all_coords[crn_info['crn']][coord].data

            tmp_bnds.loc[dict(x_b=idx_single_x, y_b=slice_main_y)] = \
                2 * ds_all_coords[crn_info['pivot_x']][coord].isel(x=idx_single_x).data \
                - ds_all_coords[crn_info['crn']][coord].isel(x=idx_single_x).data
            
            tmp_bnds.loc[dict(y_b=idx_single_y, x_b=slice_main_x)] = \
                2 * ds_all_coords[crn_info['pivot_y']][coord].isel(y=idx_single_y).data \
                - ds_all_coords[crn_info['crn']][coord].isel(y=idx_single_y).data

            tmp_bnds.loc[dict(x_b=idx_single_x, y_b=idx_single_y)] = \
                2 * ds_all_coords[crn_info['ctn']][coord].isel(x=idx_single_x, y=idx_single_y).data \
                - ds_all_coords[crn_info['crn']][coord].isel(x=idx_single_x, y=idx_single_y).data

            ds_out[coord+'_b'] = tmp_bnds
            
        return ds_out

    def get_ds_cf(self):
        """ Convert the bounds from xESMF (y+1, x+1) convention into CF convention (y,x,bnds). """
        
        ds_out = self.ds_xesmf.drop_vars(['lon_b', 'lat_b'])
        ny, nx = ds_out['lat'].sizes['y'], ds_out['lat'].sizes['x']

        for coord in ['lat', 'lon']:
            bnds_xesmf = self.ds_xesmf[coord+'_b']
            bnds_cf = xr.zeros_like(ds_out[coord])\
                        .expand_dims({self.BNDS_DIM: 4}, axis=-1)\
                        .copy()
            bnds_cf.attrs = {}
            bnds_cf.loc[{self.BNDS_DIM: 0}] = bnds_xesmf.isel({'x_b': slice(0, nx),
                                                               'y_b': slice(0, ny)}).data
            bnds_cf.loc[{self.BNDS_DIM: 1}] = bnds_xesmf.isel({'x_b': slice(1, nx+1),
                                                               'y_b': slice(0, ny)}).data
            bnds_cf.loc[{self.BNDS_DIM: 2}] = bnds_xesmf.isel({'x_b': slice(1, nx+1),
                                                               'y_b': slice(1, ny+1)}).data
            bnds_cf.loc[{self.BNDS_DIM: 3}] = bnds_xesmf.isel({'x_b': slice(0, nx),
                                                               'y_b': slice(1, ny+1)}).data

            ds_out[coord+'_'+self.BNDS_DIM] = bnds_cf
            ds_out[coord].attrs['bounds'] = coord+'_'+self.BNDS_DIM
            ds_out[coord+'_'+self.BNDS_DIM].encoding['coordinates'] = None
            ds_out[coord+'_'+self.BNDS_DIM].encoding['_FillValue'] = None

        ds_out['mask'].encoding['coordinates'] = None
        ds_out['dummy'] = xr.ones_like(ds_out['mask'], dtype=float)\
                            .where(ds_out['mask'] > 0.5)
        ds_out['dummy'].encoding = {'dtype': 'float32',
                                    '_FillValue': self.FILLVAL}
        ds_out['dummy'].attrs = {'standard_name': 'dummy_variable'}
            
        return ds_out
    
    def reorder_vars(self, dset):
        """ Reorder variable order in netCDF file. """

        vvars = []
        if self.level:
            vvars += [self.VDIM, self.VDIM+'_'+self.VBNDS_DIM]
        vvars += ['lat', 'lat_bnds',
                  'lon', 'lon_bnds',
                  'cell_area',
                  'mask',
                  'dummy']
        
        return dset[vvars]        
    
    @staticmethod
    def _get_all_coords(ds_mesh):
        """ Get u/u/v/f node coordinates. """

        grids = ['t', 'u', 'v', 'f']
        all_coords = {}
        for grid in grids:
            all_coords[grid] = {'lon': ds_mesh['glam'+grid],
                                'lat': ds_mesh['gphi'+grid]}

        return all_coords

    def reshape_unstructured(self, original):
        """Reshape your array to be a unstructured grid instead of a curvilinear grid"""

        new = original.stack({self.UNSTRUCT_DIM: ('y', 'x')}).drop_vars(['x','y', self.UNSTRUCT_DIM])
        new['lon_' + self.BNDS_DIM] = new['lon_' + self.BNDS_DIM].transpose(self.UNSTRUCT_DIM, self.BNDS_DIM) 
        new['lat_'+ self.BNDS_DIM] = new['lat_' + self.BNDS_DIM].transpose(self.UNSTRUCT_DIM, self.BNDS_DIM)
        new['dummy'].attrs['grid_type'] = 'unstructured'
        new['mask'].attrs['grid_type'] = 'unstructured'
        
        return new
 

def get_args():
    parser = argparse.ArgumentParser(
        description="Generates a lat/lon file for the ORCA grid, with bounds.")
    parser.add_argument(
        "meshmask", type=str, help="path to input meshmask file")
    parser.add_argument(
        "--stagg", type=str,  default='T', help="type of ORCA points concerned (T, U, V or F)")
    parser.add_argument('--xesmf', action=argparse.BooleanOptionalAction,
                        help="generate xesmf-type of file, with bounds stored as (y+1, x+1) array, instead of CF-compliant (y,x,4) bounds")
    parser.add_argument('--unstructured', action=argparse.BooleanOptionalAction,
                        help="produce unstructured grid (instead of curvilinear) to be used with NEMO GRIB files")
    parser.add_argument('--level', action=argparse.BooleanOptionalAction,
                        help="include vertical axis")
    parser.add_argument(
        "--zname", type=str,  default='level', help="vertical axis name")

    parser.add_argument(
        "outfile", type=str,  help="path to output file")

    return parser.parse_args()

    
def main(args):

    orca = OrcaMesh(args)
    
    if args.xesmf:
        ds_out = orca.ds_xesmf
    else:
        ds_out = orca.get_ds_cf()

    if args.unstructured:
        ds_out = orca.reshape_unstructured(ds_out)

    ds_out = orca.reorder_vars(ds_out)
    
    #print(ds_out)
    ds_out.to_netcdf(args.outfile)

if __name__ == "__main__":
    try:
        main(get_args())
    except Exception as err:
        traceback.print_tb(err.__traceback__)
        print(err.__class__.__name__)
        print(err)
        sys.exit(1)
