from aqua.logger import log_configure
from aqua.diagnostics.core import OutputSaver
from aqua.graphics import plot_single_map, plot_single_map_diff, plot_maps, plot_vertical_profile_diff
from aqua.util import get_projection, get_realizations, time_to_string
from .util import handle_pressure_level

class PlotGlobalBiases: 
    def __init__(self, 
                 diagnostic='globalbiases',
                 save_pdf=True, save_png=True, 
                 dpi=300, outputdir='./',
                 cmap='RdBu_r',
                 loglevel='WARNING'):
        """
        Initialize the PlotGlobalBiases class.

        Args:
            diagnostic (str): Name of the diagnostic.
            save_pdf (bool): Whether to save the figure as PDF.
            save_png (bool): Whether to save the figure as PNG.
            dpi (int): Resolution of saved figures.
            outputdir (str): Output directory for saved plots.
            cmap (str): Colormap to use for the plots.
            loglevel (str): Logging level.
        """
        self.diagnostic = diagnostic
        self.save_pdf = save_pdf
        self.save_png = save_png
        self.dpi = dpi
        self.outputdir = outputdir
        self.cmap = cmap
        self.loglevel = loglevel

        self.logger = log_configure(log_level=loglevel, log_name='Global Biases')

    def _save_figure(self, fig, diagnostic_product, 
                     data, description, var, data_ref=None, 
                     plev=None, **kwargs):
        """
        Handles the saving of a figure using OutputSaver.

        Args:
            fig (matplotlib.Figure): The figure to save.
            data (xarray.Dataset): Dataset.
            data_ref (xarray.Dataset, optional): Reference dataset.
            diagnostic_product (str): Name of the diagnostic product.
            description (str): Description of the figure.
            var (str): Variable name.
            plev (float, optional): Pressure level.

        Keyword Args:
            **kwargs: Additional keyword arguments to be passed to the OutputSaver.
        """
        outputsaver = OutputSaver(
            diagnostic=self.diagnostic,
            catalog=data.AQUA_catalog,
            model=data.AQUA_model,
            exp=data.AQUA_exp,
            model_ref=data_ref.AQUA_model if data_ref else None,
            exp_ref=data_ref.AQUA_exp if data_ref else None,
            outputdir=self.outputdir,
            loglevel=self.loglevel,
            **kwargs
        )

        metadata = {"Description": description}
        extra_keys = {}

        if var is not None:
            extra_keys.update({'var': var})
        if plev is not None:
            extra_keys.update({'plev': plev})

        outputsaver.save_figure(fig, diagnostic_product,
                                extra_keys=extra_keys, metadata=metadata,
                                save_pdf=self.save_pdf, save_png=self.save_png,
                                dpi=self.dpi)

    def plot_climatology(self, data, var, plev=None, proj='robinson', proj_params={}, vmin=None, vmax=None, cbar_label=None):
        """
        Plots the climatology map for a given variable and time range.

        Args:
            data (xarray.Dataset): Climatology dataset to plot.
            var (str): Variable name.
            plev (float, optional): Pressure level to plot (if applicable).
            proj (string, optional): Desired projection for the map.
            proj_params (dict, optional): Additional arguments for the projection (e.g., {'central_longitude': 0}).
            vmin (float, optional): Minimum color scale value.
            vmax (float, optional): Maximum color scale value.
            cbar_label (str, optional): Label for the colorbar.

        Returns:
            tuple: Matplotlib figure and axis objects.
        """
        self.logger.info('Plotting climatology.')
        
        data = handle_pressure_level(data, var, plev, loglevel=self.loglevel)
        if data is None:
            return None

        realization = get_realizations(data)
        proj = get_projection(proj, **proj_params)
        
        title = (f"Climatology of {data[var].attrs.get('long_name', var)} \n for {data.AQUA_model} {data.AQUA_exp}" 
                + (f" at {int(plev / 100)} hPa" if plev else ""))

        fig, ax = plot_single_map(
            data[var],
            return_fig=True,
            title=title,
            title_size=16,
            vmin=vmin,
            vmax=vmax,
            proj=proj,
            loglevel=self.loglevel,
            cbar_label=cbar_label,
            cmap=self.cmap
        )
        ax.set_xlabel("Longitude")
        ax.set_ylabel("Latitude")

        description = (
            f"Spatial map of the climatology for {data[var].attrs.get('long_name', var)}"
            f"{' at ' + str(int(plev / 100)) + ' hPa' if plev else ''}"
            f" from {time_to_string(data.startdate, format='%Y-%m')} to {time_to_string(data.enddate, format='%Y-%m')} for the {data.AQUA_model} model, experiment {data.AQUA_exp}."
        )

        self._save_figure(fig=fig, diagnostic_product='annual_climatology',
                          data=data, description=description, var=var, plev=plev, realization=realization)


    def plot_bias(self, data, data_ref, var, plev=None, proj='robinson', proj_params={}, vmin=None, vmax=None, cbar_label=None):
        """
        Plots the bias map between two datasets.

        Args:
            data (xarray.Dataset): Primary dataset.
            data_ref (xarray.Dataset): Reference dataset.
            var (str): Variable name.
            plev (float, optional): Pressure level.
            proj (str, optional): Desired projection for the map.
            proj_params (dict, optional): Additional arguments for the projection.
            vmin (float, optional): Minimum colorbar value.
            vmax (float, optional): Maximum colorbar value.
            cbar_label (str, optional): Label for the colorbar.
        """
        self.logger.info('Plotting global biases.')

        data = handle_pressure_level(data, var, plev, loglevel=self.loglevel)
        data_ref = handle_pressure_level(data_ref, var, plev, loglevel=self.loglevel)

        realization = get_realizations(data)

        sym = vmin is None or vmax is None

        proj = get_projection(proj, **proj_params)

        title = (f"Global difference of {data[var].attrs.get('long_name', var)} \n for {data.AQUA_model} {data.AQUA_exp}"
                 f" relative to {data_ref.AQUA_model} climatology"
                 + (f" at {int(plev / 100)} hPa" if plev else ""))

        fig, ax = plot_single_map_diff(
            data=data[var], 
            data_ref=data_ref[var],
            return_fig=True,
            contour=True, 
            title=title,
            title_size=16,
            sym=sym,
            proj=proj,
            vmin_fill=vmin, 
            vmax_fill=vmax,
            cbar_label=cbar_label,
            cmap=self.cmap,
            loglevel=self.loglevel
        )
        ax.set_xlabel("Longitude")
        ax.set_ylabel("Latitude")
        

        description = (
            f"Climatology of {data[var].attrs.get('long_name', var)}"
            f"{' at ' + str(int(plev / 100)) + ' hPa' if plev else ''} "
            f"{data.AQUA_model} {data.AQUA_exp} (from {time_to_string(data.startdate, format='%Y-%m')} to {time_to_string(data.enddate, format='%Y-%m')}, contours) "
            f"and differences against {data_ref.AQUA_model} (from {time_to_string(data_ref.startdate, format='%Y-%m')} to {time_to_string(data_ref.enddate, format='%Y-%m')}, shading)."
        )

        self._save_figure(fig=fig, diagnostic_product='bias', data=data, data_ref=data_ref,
                          description=description, var=var, plev=plev, realization=realization)


    def plot_seasonal_bias(self, data, data_ref, var, plev=None, proj='robinson', proj_params={}, vmin=None, vmax=None, cbar_label=None):
        """
        Plots seasonal biases for each season (DJF, MAM, JJA, SON).

        Args:
            data (xarray.Dataset): Primary dataset.
            data_ref (xarray.Dataset): Reference dataset.
            var (str): Variable name.
            plev (float, optional): Pressure level.
            proj (str, optional): Desired projection for the map.
            proj_params (dict, optional): Additional arguments for the projection.
            vmin (float, optional): Minimum colorbar value.
            vmax (float, optional): Maximum colorbar value.
            cbar_label (str, optional): Label for the colorbar.

        Returns:
            matplotlib.figure.Figure: The resulting figure.
        """
        self.logger.info('Plotting seasonal biases.')

        data = handle_pressure_level(data, var, plev, loglevel=self.loglevel)
        data_ref = handle_pressure_level(data_ref, var, plev, loglevel=self.loglevel)

        realization = get_realizations(data)

        season_list = ['DJF', 'MAM', 'JJA', 'SON']
        sym = vmin is None or vmax is None

        title = (f"Seasonal difference of {data[var].attrs.get('long_name', var)} \n for {data.AQUA_model} {data.AQUA_exp} "
                 f"relative to {data_ref.AQUA_model} climatology"
                 + (f" at {int(plev / 100)} hPa" if plev else ""))

        plot_kwargs = {
            'maps': [data[var].sel(season=season) - data_ref[var].sel(season=season) for season in season_list],
            'proj': get_projection(proj, **proj_params),
            'return_fig': True,
            'title': title,
            'title_size': 16,
            'titles': season_list,
            'titles_size': 14,
            'figsize':(10, 8),
            'contour': True,
            'sym': sym,
            'cbar_label': cbar_label,
            'cmap': self.cmap,
            'loglevel': self.loglevel
        }

        if vmin is not None:
            plot_kwargs['vmin'] = vmin
        if vmax is not None:
            plot_kwargs['vmax'] = vmax

        fig = plot_maps(**plot_kwargs)


        description = (
            f"Seasonal climatology of {data[var].attrs.get('long_name', var)}"
            f"{' at ' + str(int(plev / 100)) + ' hPa' if plev else ''} "
            f"{data.AQUA_model} {data.AQUA_exp} (from {time_to_string(data.startdate, format="%Y-%m")} to {time_to_string(data.enddate, format="%Y-%m")}, contours) "
            f"and differences against {data_ref.AQUA_model} (from {time_to_string(data_ref.startdate, format="%Y-%m")} to {time_to_string(data_ref.enddate, format="%Y-%m")}, shading)."
        )

        self._save_figure(fig=fig, diagnostic_product='seasonal_bias', data=data, data_ref=data_ref,
                          description=description, var=var, plev=plev, realization=realization)


    def plot_vertical_bias(self, data, data_ref, var, plev_min=None, plev_max=None, 
                            vmin=None, vmax=None, vmin_contour=None, vmax_contour=None, nlevels=18):
        """
        Calculates and plots the vertical bias between two datasets.

        Args:
            data (xarray.Dataset): Dataset to analyze.
            data_ref (xarray.Dataset): Reference dataset for comparison.
            var (str): Variable name to analyze.
            plev_min (float, optional): Minimum pressure level.
            plev_max (float, optional): Maximum pressure level.
            vmin (float, optional): Minimum colorbar value.
            vmax (float, optional): Maximum colorbar value.
            vmin_contour (float, optional): Minimum contour value.
            vmax_contour (float, optional): Maximum contour value.
            nlevels (int, optional): Number of contour levels for the plot.
        """
        self.logger.info('Plotting vertical biases for variable: %s', var)

        realization = get_realizations(data)

        title = (
            f"Vertical difference of {data[var].attrs.get('long_name', var)} for \n {data.AQUA_model} {data.AQUA_exp}"
            f"relative to {data_ref.AQUA_model} climatology\n"
        )

        description = (
            f"Vertical cross-section of {data[var].attrs.get('long_name', var)}"
            f"{data.AQUA_model} {data.AQUA_exp} (from {time_to_string(data.startdate, format='%Y-%m')} to {time_to_string(data.enddate, format='%Y-%m')}, contours) "
            f"and differences against {data_ref.AQUA_model} (from {time_to_string(data_ref.startdate, format='%Y-%m')} to {time_to_string(data_ref.enddate, format='%Y-%m')}, shading)."
        )

        fig, ax = plot_vertical_profile_diff(
            data=data[var].mean(dim='lon'),
            data_ref=data_ref[var].mean(dim='lon'),
            var=var,
            lev_min=plev_min,
            lev_max=plev_max,
            vmin=vmin,
            vmax=vmax,
            vmin_contour=vmin_contour,
            vmax_contour=vmax_contour,
            logscale=True,
            add_contour=True, 
            cmap=self.cmap,
            nlevels=nlevels,
            title=title,
            title_size=16,
            return_fig=True,
            loglevel=self.loglevel
        )

        self._save_figure(fig=fig, diagnostic_product='vertical_bias', data=data, data_ref=data_ref,
                          description=description, var=var, realization=realization)

        self.logger.info("Vertical bias plot completed successfully.")
