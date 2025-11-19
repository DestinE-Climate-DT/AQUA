import matplotlib.pyplot as plt
import xarray as xr
from aqua.logger import log_configure
from aqua.graphics import indexes_plot
from aqua.graphics import plot_maps, plot_single_map
from aqua.graphics import plot_maps_diff, plot_single_map_diff
from .base import PlotBaseMixin, _homogeneize_maps


class PlotENSO(PlotBaseMixin):

    def __init__(self, indexes=None, ref_indexes=None, outputdir: str = './', rebuild: bool = True,
                 loglevel: str = 'WARNING'):
        """
        Plot the ENSO products.

        Args:
            indexes (list): List of indexes to plot.
            ref_indexes (list): List of reference indexes to plot.
            outputdir (str): Directory to save the plots. Default is './'.
            rebuild (bool): If True, rebuild the plots. Default is True.
            loglevel (str): Log level for the logger. Default is 'WARNING'.
        """
        super().__init__(indexes=indexes, ref_indexes=ref_indexes, diagnostic='enso',
                         outputdir=outputdir, rebuild=rebuild, loglevel=loglevel)
        self.logger = log_configure(log_name='PlotENSO', log_level=loglevel)

    def plot_index(self, thresh: float = 0.5):

        # Join the indexes in a single list
        indexes = self.indexes + self.ref_indexes

        labels = super().set_labels()

        fig, axs = indexes_plot(indexes=indexes, thresh=thresh, suptitle='ENSO3.4 index',
                                ylabel='ENSO3.4 index', labels=labels, loglevel=self.loglevel)

        if isinstance(axs, plt.Axes):
            axs = [axs]

        # Indexes customization for ENSO only
        for i in range(len(axs)):
            ax = axs[i]

            ylim = 2.3
            ax.set_ylim(min(ax.get_ylim()[0], -ylim), max(ax.get_ylim()[1], ylim))

            textoffset = 0.08

            ax.text(
                x=ax.get_xlim()[1],
                y=2. + textoffset,
                s='El Niño',
                color='red',
                fontsize=10,
                ha='right',
                va='center'
            )

            ax.text(
                x=ax.get_xlim()[1],
                y=-2. + textoffset,
                s='La Niña',
                color='blue',
                fontsize=10,
                ha='right',
                va='center'
            )

        return fig, axs

    def set_index_description(self):
        return super().set_index_description(index_name='ENSO3.4')

    def plot_maps(self, maps=None, ref_maps=None, statistic: str = None, vmin: float = None, vmax: float = None,
                  vmin_diff: float = None, vmax_diff: float = None, **kwargs):
        """
        Plot the maps for the ENSO products.

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
        if statistic == 'correlation' and vmin is None and vmax is None:
            vmin = -1.
            vmax = 1.
            vmin_diff = -0.5
            vmax_diff = 0.5
        elif statistic == 'regression' and vmin is None and vmax is None and var == 'tos':
            vmin = -1.0
            vmax = 1.0
            vmin_diff = -1.0
            vmax_diff = 1.0

        maps, ref_maps = _homogeneize_maps(maps=maps, ref_maps=ref_maps, var=var)

        # Case 1: no reference maps
        if maps is not None and ref_maps is None:

            # Case 1a: single map
            if isinstance(maps, xr.DataArray):
                title = f"ENSO {maps.AQUA_model} {maps.AQUA_exp} {statistic} map ({var})"
                if hasattr(maps, 'AQUA_season'):
                    title += f" ({maps.AQUA_season})"
                fig, _ = plot_single_map(data=maps, vmin=vmin, vmax=vmax, title=title,
                                         return_fig=True, loglevel=self.loglevel, **kwargs)
                return fig

            # Case 1b: multiple maps
            elif isinstance(maps, list):
                titles = []
                for map in maps:
                    title = f"ENSO {map.AQUA_model} {map.AQUA_exp} {statistic} map ({var})"
                    if hasattr(map, 'AQUA_season'):
                        title += f" ({map.AQUA_season})"
                    titles.append(title)
                fig = plot_maps(maps=maps, vmin=vmin, vmax=vmax, titles=titles,
                                return_fig=True, loglevel=self.loglevel, **kwargs)
                return fig

        # # Case 2: reference maps (maps and ref_maps are not None)
        if ref_maps is not None:

            # Case 2a: both maps and ref_maps are only one (we consider only both lists of one or both xarrays)
            if isinstance(maps, xr.DataArray) and isinstance(ref_maps, xr.DataArray):
                title = f"ENSO {maps.AQUA_model} {maps.AQUA_exp} {statistic} map ({var}) compared to {ref_maps.AQUA_model} {ref_maps.AQUA_exp}"
                if hasattr(maps, 'AQUA_season'):
                    title += f" ({maps.AQUA_season})"
                fig, _ = plot_single_map_diff(data=maps, data_ref=ref_maps,
                                              vmin_contour=vmin if vmin is not None else None,
                                              vmax_contour=vmax if vmax is not None else None,
                                              vmin_fill=vmin_diff if vmin_diff is not None else None,
                                              vmax_fill=vmax_diff if vmax_diff is not None else None,
                                              sym=True if vmax_diff is None and vmin_diff is None else False,
                                              sym_contour=True if vmax is None and vmin is None else False,
                                              title=title, return_fig=True, loglevel=self.loglevel, **kwargs)
                return fig

            # Case 2b: maps are list and ref_maps is only one
            if isinstance(maps, list) and isinstance(ref_maps, xr.DataArray):
                titles = []
                for map in maps:
                    title = f"{map.AQUA_model} {map.AQUA_exp}"
                    titles.append(title)
                title = f"ENSO {statistic} map ({var}) compared to {ref_maps.AQUA_model} {ref_maps.AQUA_exp}"
                if hasattr(ref_maps, 'AQUA_season'):
                    title += f" ({ref_maps.AQUA_season})"

                # plot_maps_diff wants a list of reference maps of the same length as maps
                maps_ref = [ref_maps] * len(maps)
                fig = plot_maps_diff(maps=maps, maps_ref=maps_ref,
                                     vmin_contour=vmin if vmin is not None else None,
                                     vmax_contour=vmax if vmax is not None else None,
                                     vmin_fill=vmin_diff if vmin_diff is not None else None,
                                     vmax_fill=vmax_diff if vmax_diff is not None else None,
                                     sym=True if vmax_diff is None and vmin_diff is None else False,
                                     sym_contour=True if vmax is None and vmin is None else False,
                                     titles=titles, title=title, return_fig=True,
                                     loglevel=self.loglevel, **kwargs)
                return fig

            # Case 2c: maps is only one and ref_maps is list
            if isinstance(maps, xr.DataArray) and isinstance(ref_maps, list):
                self.logger.critical('maps is a single map, ref_maps is a list.')
                titles = []
                for map in ref_maps:
                    title = f"Compared to {map.AQUA_model} {map.AQUA_exp}"
                    titles.append(title)
                title = f"ENSO {statistic} map ({var}) of {maps.AQUA_model} {maps.AQUA_exp}"
                if hasattr(maps, 'AQUA_season'):
                    title += f" ({maps.AQUA_season})"

                # plot_maps_diff wants a list of reference maps of the same length as maps
                maps = [maps] * len(ref_maps)
                fig = plot_maps_diff(maps=maps, maps_ref=ref_maps,
                                     vmin_contour=vmin if vmin is not None else None,
                                     vmax_contour=vmax if vmax is not None else None,
                                     vmin_fill=vmin_diff if vmin_diff is not None else None,
                                     vmax_fill=vmax_diff if vmax_diff is not None else None,
                                     sym=True if vmax_diff is None and vmin_diff is None else False,
                                     sym_contour=True if vmax is None and vmin is None else False,
                                     titles=titles, title=title, return_fig=True,
                                     loglevel=self.loglevel, **kwargs)
                self.logger.critical(f'Fig: {fig}')
                return fig

            # Case 2d: maps and ref_maps are lists
            if isinstance(maps, list) and isinstance(ref_maps, list):
                self.logger.error('Both maps and ref_maps are lists. This case is not implemented yet.')
                return None

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
        return super().set_map_description(maps=maps, ref_maps=ref_maps, statistic=statistic, telecname='ENSO3.4')
