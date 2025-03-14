# """Regridder mixin for the Reader class"""

# import types

# class RegridMixin():
#     """Regridding mixin for the Reader class"""

   
    # def _weights_generation_time(self, original_grid_size=None, new_grid_size=None, nproc=None, vert_coord_size=None):
    #     """
    #     Helper function to estimate the time required for generating regridding weights.

    #     Args:
    #         original_grid_size (int, optional): Size of the original grid. Defaults to None.
    #         new_grid_size (int, optional): Size of the new grid. Defaults to None.
    #         nproc (int, optional): Number of processors to be used in the computation. Defaults to None.
    #         vert_coord_size (int, optional): Size of the vertical coordinate. Defaults to None.

    #     Returns:
    #         None: This function does not return a value but logs the estimated time for weight generation.
    #     """
    #     if None in [original_grid_size, new_grid_size, nproc, vert_coord_size]:
    #         self.logger.error("Missing required parameter for weight generation time estimation.")
    #         return

    #     warning_threshold = 59  # seconds

    #     # Log comparison of grid sizes
    #     self.logger.debug(f"Grid size comparison - Original: {original_grid_size}, New: {new_grid_size}.")

    #     # Assumptions for Instructions Per Second (IPS)
    #     IPS_original = 0.00013 / max(vert_coord_size, 1)
    #     IPS_new = 0.000043 / max(vert_coord_size / (nproc + 1), 1)

    #     clock_speed = 3.5 * 10**9  # Hz

    #     # Operations Per Second (OPS)
    #     OPS_original = clock_speed * IPS_original
    #     OPS_new = clock_speed * IPS_new

    #     expected_time_original = original_grid_size / OPS_original
    #     expected_time_new = new_grid_size / OPS_new
    #     expected_time = expected_time_original + expected_time_new

    #     self.logger.debug(f"The total expected processing time is {expected_time} seconds.")

    #     if expected_time > warning_threshold:
    #         hours, remainder = divmod(int(expected_time), 3600)
    #         minutes = round((int(expected_time) % 3600) / 60)
    #         formatted_time = f'{hours} hours, {minutes} minutes'
    #         self.logger.warning(f'Time to generate the weights will take approximately {formatted_time}.')

    # def _guess_coords(self, space_coord, vert_coord, default_horizontal_dims, default_vertical_dims):
    #     """
    #     Given a set of default space and vertical dimensions, 
    #     find the one present in the data and return them

    #     Args:
    #         space_coord (str or list): horizontal dimension already defined. If None, autosearch enabled.
    #         vert_coord (str or list): vertical dimension already defined. If None, autosearch enabled.
    #         default_horizontal_dims (list): default dimensions for the horizontal search
    #         default_vertical_dims (list): default dimensions for the vertical search 

    #     Return
    #         space_coord and vert_coord from the data source
    #     """

    #     if space_coord is None:

    #         data = self._retrieve_plain(startdate=None)
    #         space_coord = [x for x in data.dims if x in default_horizontal_dims]
    #         if not space_coord:
    #             self.logger.debug('Default dims that are screened are %s', default_horizontal_dims)
    #             raise KeyError('Cannot identify any space_coord, you will will need to define it regrid.yaml')
    #         self.logger.info('Space_coords deduced from the source are %s', space_coord)

    #     if vert_coord is None:
     
    #         # this is done to load only if necessary
    #         data = self._retrieve_plain(startdate=None)
    #         vert_coord = [x for x in data.dims if x in default_vertical_dims]
    #         if not vert_coord:
    #             self.logger.debug('Default dims that are screened are %s', default_vertical_dims)
    #             self.logger.debug('Assuming this is a 2d file, i.e. vert_coord=2d')
    #             # If not specified we assume that this is only a 2D case
    #             vert_coord = '2d'
            
    #         self.logger.info('vert_coord deduced from the source are %s', vert_coord)

    #     return space_coord, vert_coord
