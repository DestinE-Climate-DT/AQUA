import healpy as hp
import numpy as np
import xarray as xr
from aqua import Regridder
from aqua.fldstat import AreaSelection
from aqua.graphics import plot_single_map
from aqua.util import get_projection, healpix_resample

# import matplotlib.pyplot as plt
# from aqua.exceptions import NoDataError, NoObservationError, NotEnoughDataError

from .base import PlotBaseMixin

xr.set_options(keep_attrs=True)


class sshVariabilityPlot(PlotBaseMixin):
    """
    Plot sshVariability and the difference of sshVariability
    """

    def __init__(
        self,
        diagnostic_name="sshVariability",
        outputdir="./",
        loglevel="WARNING",
    ):
        """
        Initialize the sshVariability.

        Args:
            diagnostic_name (str): sshVariability
            outputdir (str): output directory
            loglevel (str): Default WARNING
        """

        self.loglevel = loglevel
        self.outputdir = outputdir

        super().__init__(diagnostic_name=diagnostic_name, loglevel=loglevel)

    def plot(
        self,
        var=None,
        dataset_std=None,
        catalog=None,
        model=None,
        exp=None,
        startdate=None,
        enddate=None,
        plot_options={},
        figsize: tuple = (11, 8.5),
        ax_pos: tuple = (1, 1, 1),
        vmin=None,
        vmax=None,
        gridlines=True,
        proj="robinson",
        proj_params={},
        save_png=True,
        save_pdf=True,
        dpi=600,
        region=None,
        lon_limits=None,
        lat_limits=None,
        # Retrieve the masking flags and boundary latitudes from the configuration, Specific to ICON
        mask_options={},
        mask_northern_boundary=True,
        mask_southern_boundary=True,
        northern_boundary_latitude=70,
        southern_boundary_latitude=-62,
        diagnostic_product="sshVariability",
        rebuild: bool = True,
        description=None,
        tgt_grid_name="r1440x721",
        regrid_method="ycon",
    ):
        """
        Visualize the SSH variability.

        Plot the variability of sea surface height (SSH) from an input dataset.

        This function visualizes SSH variability using configurable spatial, temporal,
        and plotting options. It supports contou, regional selection, custom projections,
        masking, and output saving in multiple formats.

        Args:
            var (str, optional): Variable name for SSH, e.g., ``'zos'``.
            dataset_std (xarray.Dataset, optional): Dataset containing the SSH field to be plotted.
            catalog (str, optional): Catalog name. Used in plot titles. (Mandatory for labeling)
            model (str, optional): Model or dataset name. Used in plot titles. (Mandatory for labeling)
            exp (str, optional): Experiment identifier. Used in plot titles. (Mandatory for labeling)
            startdate (str, optional): Start date label to include in the plot title.
            enddate (str, optional): End date label to include in the plot title.
            regrid (str or dict, optional): Regridding option or parameters for spatial interpolation.
            plot_options (dict, optional): Additional keyword arguments for customizing the plot (e.g., colormap, linewidth).
            vmin (float, optional): Minimum value for color scaling. If ``None``, determined automatically.
            vmax (float, optional): Maximum value for color scaling. If ``None``, determined automatically.
            proj (str, optional): Map projection type. Default is ``'robinson'``.
            proj_params (dict, optional): Additional keyword arguments passed to the projection.
            save_png (bool, optional): If ``True``, save plot as PNG. Default is ``True``.
            save_pdf (bool, optional): If ``True``, save plot as PDF. Default is ``True``.
            dpi (int, optional): Resolution (dots per inch) for saved figures. Default is ``300``.
            region (str, optional): Region identifier. If provided, overrides lat/lon limits.
            lon_limits (list[float], optional): Longitude limits [min, max] for the plot.
            lat_limits (list[float], optional): Latitude limits [min, max] for the plot.
            mask_options (dict, optional): Options for masking grid cells (specific to ICON).
            mask_northern_boundary (bool, optional): If ``True``, mask latitudes north of ``northern_boundary_latitude``.
            mask_southern_boundary (bool, optional): If ``True``, mask latitudes south of ``southern_boundary_latitude``.
            northern_boundary_latitude (float, optional): Latitude above which data will be masked. Default is ``70``.
            southern_boundary_latitude (float, optional): Latitude below which data will be masked. Default is ``-62``.
            diagnostic_product (str, optional): Diagnostic type, e.g., ``'sshVariability'``. Default is ``'sshVariability'``.
            rebuild (bool, optional): If ``True``, rebuild the data from the original files. Default is ``True``.
            description (str, optional): Additional description to include in the plot or metadata.
            tgt_grid_name='r1440x720',
            regrid_method='ycon',

        Returns:
            matplotlib.figure.Figure: The generated plot figure object.

        Raises:
            ValueError: If required arguments (e.g., ``catalog``, ``model``, ``exp``) are missing.
            TypeError: If inputs are of invalid type (e.g., dataset not an xarray.Dataset).
        """

        # TODO:
        # Regridding the input dataset_std for plotting

        if dataset_std is None:
            self.logger.error("Please provide the data to the plot function")
            raise RuntimeError(f"No model data found")

        if isinstance(dataset_std, xr.Dataset):
            dataset_std = dataset_std[var]
        else:
            dataset_std = dataset_std
        # This is important to provide the start and end dates. These dates will be used in the title of the plot
        if startdate is None or enddate is None:
            self.logger.error("Please specify the time period of the data")

        self.logger.info(f"Plotting SSH Variability for {model} and {exp}, from {startdate} to {enddate}.")
        long_name = dataset_std.attrs.get("long_name", var)
        units = dataset_std.attrs.get("units", var)
        title = f"SSH Variability of {long_name} for {model} {exp} ({startdate} to {enddate}) "

        description = f"SSH Variability of {long_name} for {model} {exp} ({startdate} to {enddate}) "

        # Check if the dataset is in HEALPix format
        npix = dataset_std.size  # Number of cells in the data
        nside = hp.npix2nside(npix) if hp.isnpixok(npix) else None

        if nside is not None:
            self.logger.info(f"Input data is in HEALPix format with nside={nside}.")
            dataset_std = healpix_resample(dataset_std)
            self.logger.debug("resampling HEALPix dataset_std")

        if tgt_grid_name is not None:
            self.logger.info(
                f"Regridding model data and reference data using target grid name {tgt_grid_name} and regrid method {regrid_method}"
            )
            regrid_data = Regridder(data=dataset_std, loglevel=self.loglevel)
            regrid_data.weights(tgt_grid_name=tgt_grid_name, regrid_method=regrid_method)
            dataset_std = regrid_data.regrid(dataset_std)

        if region is not None:
            title = title + f"{region} "
            if lon_limits is None or lat_limits is None:
                self.logger.error(f"For the {region}, please specify the lon_limits and lat_limits.")
            description = description + f"for {region} "
            dataset_std = self.subregion_selection(
                data=dataset_std,
                model=model,
                exp=exp,
                mask_northern_boundary=mask_northern_boundary,
                northern_boundary_latitude=northern_boundary_latitude,
                mask_southern_boundary=mask_southern_boundary,
                southern_boundary_latitude=southern_boundary_latitude,
                lon_lim=lon_limits,
                lat_lim=lat_limits,
                region_name=region,
            )

        if vmin is None or (isinstance(vmin, (float, int)) and np.isnan(vmin)):
            vmin = float(dataset_std.min(skipna=True))
        if vmax is None or (isinstance(vmax, (float, int)) and np.isnan(vmax)):
            vmax = float(dataset_std.max(skipna=True))

        proj = get_projection(proj, **proj_params)
        # fig = plt.figure(figsize=figsize)
        # ax = fig.add_subplot(ax_pos[0], ax_pos[1], ax_pos[2], projection=proj)
        if vmin == vmax:
            self.logger.info("STD is Zero everywhere")
            fig, ax = plot_single_map(
                # ax,
                dataset_std,
                contour=False,
                return_fig=True,
                title=title,
                proj=proj,
                #cyclic_lon=False,
                add_land=True,
                #transform_first=True,
                gridlines=gridlines,
                loglevel=self.loglevel,
                **plot_options,
            )
        else:
            fig, ax = plot_single_map(
                # ax,
                dataset_std,
                contour=False,
                return_fig=True,
                title=title,
                vmin=vmin,
                vmax=vmax,
                proj=proj,
                #cyclic_lon=False,
                add_land=True,
                #transform_first=True,
                gridlines=gridlines,
                loglevel=self.loglevel,
                **plot_options,
            )
        ax.set_xlabel("Longitude")
        ax.set_ylabel("Latitude")

        # Saving plots
        if save_png:
            self.save_plot(
                var=var,
                fig=fig,
                description=description,
                rebuild=rebuild,
                outputdir=self.outputdir,
                format="png",
                catalog=catalog,
                model=model,
                exp=exp,
                startdate=startdate,
                enddate=enddate,
                long_name=long_name,
                units=units,
                region=region,
                dpi=dpi,
            )
        if save_pdf:
            self.save_plot(
                var=var,
                fig=fig,
                description=description,
                rebuild=rebuild,
                outputdir=self.outputdir,
                format="pdf",
                catalog=catalog,
                model=model,
                exp=exp,
                startdate=startdate,
                enddate=enddate,
                long_name=long_name,
                units=units,
                region=region,
                dpi=dpi,
            )

        return fig, ax

    def plot_diff(
        self,
        var=None,
        dataset_std=None,
        catalog=None,
        model=None,
        exp=None,
        startdate=None,
        enddate=None,
        dataset_std_ref=None,
        catalog_ref=None,
        model_ref=None,
        exp_ref=None,
        startdate_ref=None,
        enddate_ref=None,
        figsize: tuple = (11, 8.5),
        ax_pos: tuple = (1, 1, 1),
        plot_options={},
        vmin_diff=None,
        vmax_diff=None,
        gridlines=True,
        proj="robinson",
        proj_params={},
        save_png=True,
        save_pdf=True,
        dpi=600,
        region=None,
        lon_limits=None,
        lat_limits=None,
        # Retrieve the masking flags and boundary latitudes from the configuration, Specific to ICON
        mask_options={},
        mask_northern_boundary=True,
        mask_southern_boundary=True,
        northern_boundary_latitude=70,
        southern_boundary_latitude=-62,
        diagnostic_product="sshVariability_Difference",
        description=None,
        rebuild: bool = True,
        tgt_grid_name="r1440x721",
        regrid_method="ycon",
    ):
        """
        Visualize the difference in sea surface height (SSH) variability between a model and a reference dataset.

        This function generates a map of SSH variability differences using Cartopy projections,
        supporting custom contour, masking, regional selection, and configurable plotting options.
        The plot can be saved as PNG or PDF.

        Args:
            var (str, optional): Variable name to plot (e.g., 'zos').
            dataset_std (xarray.Dataset, optional): Dataset of the model to be plotted.
            catalog (str, optional): Catalog name for the model dataset (used in plot title).
            model (str, optional): Model name of the dataset (used in plot title).
            exp (str, optional): Experiment name of the dataset (used in plot title).
            startdate (str, optional): Start date of the dataset for the plot title.
            enddate (str, optional): End date of the dataset for the plot title.
            dataset_std_ref (xarray.Dataset, optional): Reference dataset for comparison.
            catalog_ref (str, optional): Catalog name for the reference dataset.
            model_ref (str, optional): Model name of the reference dataset.
            exp_ref (str, optional): Experiment name of the reference dataset.
            startdate_ref (str, optional): Start date of the reference dataset.
            enddate_ref (str, optional): End date of the reference dataset.
            regrid (str or dict, optional): Regridding method or parameters.
            plot_options (dict, optional): Additional keyword arguments for plotting (e.g., colormap, alpha).
            vmin_diff (float, optional): Minimum value for color scaling. If None, determined automatically.
            vmax_diff (float, optional): Maximum value for color scaling. If None, determined automatically.
            proj (str, optional): Map projection. Default is 'robinson'.
            proj_params (dict, optional): Additional keyword arguments for the projection.
            save_png (bool, optional): Save plot as PNG. Default is True.
            save_pdf (bool, optional): Save plot as PDF. Default is True.
            dpi (int, optional): Resolution of the saved figure. Default is 300.
            region (str, optional): Region identifier for the plot.
            lon_limits (list[float], optional): Longitude limits [min, max] for the plot.
            lat_limits (list[float], optional): Latitude limits [min, max] for the plot.
            mask_options (dict, optional): Options for masking (specific to ICON grids).
            mask_northern_boundary (bool, optional): Mask latitudes above northern_boundary_latitude. Default is True.
            mask_southern_boundary (bool, optional): Mask latitudes below southern_boundary_latitude. Default is True.
            northern_boundary_latitude (float, optional): Latitude above which data is masked. Default is 70.
            southern_boundary_latitude (float, optional): Latitude below which data is masked. Default is -62.
            diagnostic_product (str, optional): Diagnostic product identifier. Default is 'sshVariability_Difference'.
            description (str, optional): Additional description for the plot metadata or title.
            rebuild (bool, optional): If ``True``, rebuild the data from the original files. Default is ``True``.
            tgt_grid_name='r1440x720',
            regrid_method='ycon',
        Returns:
            matplotlib.figure.Figure: The generated figure object.

        Raises:
            ValueError: If required dataset or catalog/model/exp information is missing.
            TypeError: If input datasets are not xarray.Datasets.
        """
        # TODO:
        # Test if the sshVariability is computed in healpix/native grid then compte the difference will be an issue.
        # Therefore perform regridding via Regridding class.

        if dataset_std is None and dataset_std_ref is None:
            self.logger.error("Please provide the data to the plot function")

        if isinstance(dataset_std, xr.Dataset):
            dataset_std = dataset_std[var]
        else:
            dataset_std = dataset_std

        if isinstance(dataset_std_ref, xr.Dataset):
            dataset_std_ref = dataset_std_ref[var]
        else:
            dataset_std_ref = dataset_std_ref

        # Check if the dataset is in HEALPix format
        npix = dataset_std.size  # Number of cells in the data
        nside = hp.npix2nside(npix) if hp.isnpixok(npix) else None

        if nside is not None:
            self.logger.info(f"Input data is in HEALPix format with nside={nside}.")
            dataset_std = healpix_resample(dataset_std)
            self.logger.debug("resampling HEALPix dataset_std")

        # Check if the data is in HEALPix format
        npix_ref = dataset_std_ref.size  # Number of cells in the data
        nside_ref = hp.npix2nside(npix_ref) if hp.isnpixok(npix_ref) else None

        if nside_ref is not None:
            self.logger.info(f"Reference data is in HEALPix format with nside={nside_ref}.")
            dataset_std_ref = healpix_resample(dataset_std_ref)
            self.logger.debug("resampling HEALPix dataset_ref_std")

        if tgt_grid_name is not None:

            self.logger.info(
                f"Regridding model data and reference data using target grid name {tgt_grid_name} and regrid method {regrid_method}"
            )
            regrid_data = Regridder(data=dataset_std, loglevel=self.loglevel)
            regrid_data.weights(tgt_grid_name=tgt_grid_name, regrid_method=regrid_method)
            dataset_std = regrid_data.regrid(dataset_std)

            regrid_data_ref = Regridder(data=dataset_std_ref, loglevel=self.loglevel)
            regrid_data_ref.weights(tgt_grid_name=tgt_grid_name, regrid_method=regrid_method)
            dataset_std_ref = regrid_data_ref.regrid(dataset_std_ref)

        if startdate is None or enddate is None:
            self.logger.error("Please specify the time period of the data")
        if startdate_ref is None or enddate_ref is None:
            self.logger.error("Please specify the time period of the reference data")

        long_name = dataset_std.attrs.get("long_name", var)
        units = dataset_std.attrs.get("units", var)
        title = f"The difference of the SSH Variability of {long_name} for {model} {exp} ({startdate}-{enddate}) and, reference {catalog_ref} {model_ref} and {exp_ref} ({startdate_ref}-{enddate_ref}) "

        description = f"The difference of the SSH Variability of {long_name} for {model} {exp} ({startdate}-{enddate}) and, reference {catalog_ref} {model_ref} and {exp_ref} ({startdate_ref}-{enddate_ref}) "

        if region:
            title = title + f"{region} "

            if lon_limits is None or lat_limits is None:
                self.logger.error(f"For the {region}, please specify the lon_limits and lat_limits.")
            description = description + f"for {region} "
            dataset_std = self.subregion_selection(
                data=dataset_std,
                model=model,
                exp=exp,
                mask_northern_boundary=mask_northern_boundary,
                northern_boundary_latitude=northern_boundary_latitude,
                mask_southern_boundary=mask_southern_boundary,
                southern_boundary_latitude=southern_boundary_latitude,
                lon_lim=lon_limits,
                lat_lim=lat_limits,
                region_name=region,
            )
            dataset_std_ref = self.subregion_selection(
                data=dataset_std_ref, model=model_ref, exp=exp_ref, lon_lim=lon_limits, lat_lim=lat_limits, region_name=region
            )

        if isinstance(dataset_std_ref, xr.DataArray) is False or isinstance(dataset_std, xr.DataArray) is False:
            raise ValueError("Both data and data_ref must be an xarray.DataArray")

        diff_map = (dataset_std - dataset_std_ref).persist()

        if np.array_equal(np.nan_to_num(dataset_std.values), np.nan_to_num(dataset_std_ref.values)):
            self.logger.warning("The values are exactly the same (ignoring NaNs), no difference to plot")

        proj = get_projection(proj, **proj_params)
        # fig = plt.figure(figsize=figsize)
        # ax = fig.add_subplot(ax_pos[0], ax_pos[1], ax_pos[2], projection=proj)

        if vmin_diff is None or (isinstance(vmin_diff, (float, int)) and np.isnan(vmin_diff)):
            vmin_diff = float(diff_map.min(skipna=True))
        if vmax_diff is None or (isinstance(vmax_diff, (float, int)) and np.isnan(vmax_diff)):
            vmax_diff = float(diff_map.max(skipna=True))

        if vmin_diff == vmax_diff:
            # TODO: discuss what should do here in this case.
            self.logger.info("STD is Zero everywhere")
            fig, ax = plot_single_map(
                # ax,
                diff_map,
                contour=False,
                return_fig=True,
                title=title,
                #cyclic_lon=False,
                add_land=True,
                #transform_first=True,
                proj=proj,
                gridlines=gridlines,
                loglevel=self.loglevel,
                # cbar_label=cbar_label
            )
        else:
            fig, ax = plot_single_map(
                # ax,
                diff_map,
                contour=False,
                return_fig=True,
                title=title,
                vmin=vmin_diff,
                vmax=vmax_diff,
                #cyclic_lon=False,
                add_land=True,
                #transform_first=True,
                proj=proj,
                gridlines=gridlines,
                loglevel=self.loglevel,
                # cbar_label=cbar_label
            )

        ax.set_xlabel("Longitude")
        ax.set_ylabel("Latitude")

        # Saving plots
        if save_png:
            self.save_diff_plot(
                var=var,
                fig=fig,
                description=description,
                rebuild=rebuild,
                outputdir=self.outputdir,
                format="png",
                catalog=catalog,
                model=model,
                exp=exp,
                startdate=startdate,
                enddate=enddate,
                catalog_ref=catalog_ref,
                model_ref=model_ref,
                exp_ref=exp_ref,
                startdate_ref=startdate_ref,
                enddate_ref=enddate_ref,
                long_name=long_name,
                units=units,
                region=region,
                dpi=dpi,
            )
        if save_pdf:
            self.save_diff_plot(
                var=var,
                fig=fig,
                description=description,
                rebuild=rebuild,
                outputdir=self.outputdir,
                format="pdf",
                catalog=catalog,
                model=model,
                exp=exp,
                startdate=startdate,
                enddate=enddate,
                catalog_ref=catalog_ref,
                model_ref=model_ref,
                exp_ref=exp_ref,
                startdate_ref=startdate_ref,
                enddate_ref=enddate_ref,
                long_name=long_name,
                units=units,
                region=region,
                dpi=dpi,
            )
        return fig, ax

    def subregion_selection(
        self,
        data=None,
        model=None,
        exp=None,
        mask_northern_boundary=None,
        northern_boundary_latitude=None,
        mask_southern_boundary=None,
        southern_boundary_latitude=None,
        lon_lim=None,
        lat_lim=None,
        region_name=None,
    ):
        """
        Selecting sub-region based on lon-lat
        """
        self.logger.info(f"Selecting the sub-region plots: {region_name}.")
        # Apply masking if necessary
        if "ICON" in model and mask_northern_boundary and northern_boundary_latitude:
            data = data.where(data.lat < northern_boundary_latitude)
        if "ICON" in model and mask_southern_boundary and southern_boundary_latitude:
            data = data.where(data.lat > southern_boundary_latitude)

        area_sel = AreaSelection(loglevel=self.loglevel)

        return area_sel.select_area(data, lon=lon_lim, lat=lat_lim, drop=True)
