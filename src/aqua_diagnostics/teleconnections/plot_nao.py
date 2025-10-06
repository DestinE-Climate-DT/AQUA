import xarray as xr
import matplotlib.pyplot as plt
from cartopy.crs import NorthPolarStereo
from aqua.logger import log_configure
from aqua.graphics import indexes_plot, plot_single_map, plot_single_map_diff
from aqua.util import apply_circular_window
from .base import PlotBaseMixin, _homogeneize_maps


class PlotNAO(PlotBaseMixin):

    def __init__(self, indexes=None, ref_indexes=None, outputdir: str = './', rebuild: bool = True,
                 loglevel: str = 'WARNING'):
        """
        Plot the NAO products.

        Args:
            indexes (list): List of indexes to plot.
            ref_indexes (list): List of reference indexes to plot.
            outputdir (str): Directory to save the plots. Default is './'.
            rebuild (bool): If True, rebuild the plots. Default is True.
            loglevel (str): Log level for the logger. Default is 'WARNING'.
        """
        super().__init__(indexes=indexes, ref_indexes=ref_indexes, diagnostic='nao',
                         outputdir=outputdir, rebuild=rebuild, loglevel=loglevel)
        self.logger = log_configure(log_name='PlotNAO', log_level=loglevel)

    def plot_index(self, thresh: float = 0.):

        # Join the indexes in a single list
        indexes = self.indexes + self.ref_indexes

        labels = super().set_labels()

        fig, axs = indexes_plot(indexes=indexes, thresh=thresh, suptitle='NAO index',
                                ylabel='NAO index', labels=labels, loglevel=self.loglevel)

        return fig, axs

    def set_index_description(self):
        return super().set_index_description(index_name='NAO')

    def plot_maps(self, maps=None, ref_maps=None, statistic: str = None, vmin: float = None, vmax: float = None,
                  vmin_diff: float = None, vmax_diff: float = None, **kwargs):
        """
        Plot the maps for the NAO products.

        Args:
            maps (list): List of maps to plot.
            ref_maps (list): List of reference maps to plot.
            statistic (str): Statistic to plot. Default is None.
            vmin (float): Minimum value for the color value. Default is None.
            vmax (float): Maximum value for the color value. Default is None.
            vmin_diff (float): Minimum value for the color value for the difference. Default is None.
            vmax_diff (float): Maximum value for the color value for the difference. Default is None.
            **kwargs: Additional arguments for the plotting function.

        Returns:
            fig: Figure object.
        """
        map_to_check = maps if isinstance(maps, xr.DataArray) else maps[0]
        var = map_to_check.shortName if hasattr(map_to_check, 'shortName') else map_to_check.long_name
        self.logger.debug(f'Plotting {var} maps')

        if statistic == 'correlation' and vmin is None and vmax is None:
            vmin = -1.
            vmax = 1.
            vmin_diff = -0.5
            vmax_diff = 0.5
        elif statistic == 'regression' and vmin is None and vmax is None and var == 'msl':
            vmin = -4.0
            vmax = 4.0
            vmin_diff = -5.0
            vmax_diff = 5.0

        maps, ref_maps = _homogeneize_maps(maps=maps, ref_maps=ref_maps, var=var)

        # Plot details
        proj = NorthPolarStereo(central_longitude=-20.0)
        extent = [-180, 180, 10, 90]

        fig = plt.figure(figsize=(11, 8.5))
        ax = fig.add_subplot(111, projection=proj)

        ax = apply_circular_window(ax, extent=extent)

        # Case 1: no reference maps
        if maps is not None and ref_maps is None:

            # Case 1a: single map
            if isinstance(maps, xr.DataArray):
                title = f"NAO {maps.AQUA_model} {maps.AQUA_exp} {statistic} map ({var})"
                if hasattr(maps, 'AQUA_season'):
                    title += f" ({maps.AQUA_season})"
                fig, ax = plot_single_map(data=maps, fig=fig, ax=ax,
                                          vmin=vmin, vmax=vmax, title=title,
                                          return_fig=True, loglevel=self.loglevel, **kwargs)

            # Case 1b: multiple maps
            elif isinstance(maps, list):
                self.logger.warning('Multiple maps are not implemented yet.')

        # # Case 2: reference maps (maps and ref_maps are not None)
        if ref_maps is not None:

            # Case 2a: both maps and ref_maps are only one (we consider only both lists of one or both xarrays)
            if isinstance(maps, xr.DataArray) and isinstance(ref_maps, xr.DataArray):
                title = f"NAO {maps.AQUA_model} {maps.AQUA_exp} {statistic} map ({var}) compared to {ref_maps.AQUA_model} {ref_maps.AQUA_exp}"
                if hasattr(maps, 'AQUA_season'):
                    title += f" ({maps.AQUA_season})"
                fig, _ = plot_single_map_diff(data=maps, data_ref=ref_maps,
                                               fig=fig, ax=ax,
                                               vmin_contour=vmin if vmin is not None else None,
                                               vmax_contour=vmax if vmax is not None else None,
                                               vmin_fill=vmin_diff if vmin_diff is not None else None,
                                               vmax_fill=vmax_diff if vmax_diff is not None else None,
                                               sym=True if vmax_diff is None and vmin_diff is None else False,
                                               sym_contour=True if vmax is None and vmin is None else False,
                                               title=title, return_fig=True, loglevel=self.loglevel, **kwargs)

            # Case 2b: maps are list and ref_maps is only one
            if isinstance(maps, list) and isinstance(ref_maps, xr.DataArray):
                self.logger.error('maps is a list and ref_maps is a single map. This case is not implemented yet.')
                return None

            # Case 2c: maps is only one and ref_maps is list
            if isinstance(maps, xr.DataArray) and isinstance(ref_maps, list):
                self.logger.error('maps is a single map and ref_maps is a list. This case is not implemented yet.')
                return None

            # Case 2d: maps and ref_maps are lists
            if isinstance(maps, list) and isinstance(ref_maps, list):
                self.logger.error('Both maps and ref_maps are lists. This case is not implemented yet.')
                return None

        return fig

    def set_map_description(self, maps=None, ref_maps=None, statistic: str = None):
        """
        Set the description for the maps.

        Args:
            maps (list): List of maps to plot.
            ref_maps (list): List of reference maps to plot.
            statistic (str): Statistic to plot. Default is None.

        Returns:
            str: Description of the maps.
        """
        return super().set_map_description(maps=maps, ref_maps=ref_maps, statistic=statistic, telecname='NAO')
