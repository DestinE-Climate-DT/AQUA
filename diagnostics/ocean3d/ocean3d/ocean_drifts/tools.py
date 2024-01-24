"""
Global Ocean module
"""

import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import xarray as xr
import numpy as np
from scipy.stats import t as statt
from ocean3d import weighted_area_mean
from ocean3d import area_selection
from ocean3d import weighted_zonal_mean
from ocean3d import logger
from ocean3d import dir_creation
from ocean3d import custom_region
from ocean3d import write_data



def data_process_by_type(**kwargs):
    """
    Selects the type of timeseries and colormap based on the given parameters.

    Args:
        data (DataArray): Input data containing temperature (ocpt) and salinity (so).
        anomaly (bool, optional): Specifies whether to compute anomalies. Defaults to False.
        standardise (bool, optional): Specifies whether to standardize the data. Defaults to False.
        anomaly_ref (str, optional): Reference for the anomaly computation. Valid options: "t0", "tmean". Defaults to None.

    Returns:
        process_data (Dataset): Processed data based on the selected preprocessing approach.
        type (str): Type of preprocessing approach applied
        cmap (str): Colormap to be used for the plot.
    """
    data = kwargs["data"]
    anomaly = kwargs["anomaly"]
    anomaly_ref = kwargs["anomaly_ref"]
    standardise = kwargs["standardise"]
    
    
    process_data = xr.Dataset()

    
    if anomaly:
        anomaly_ref = anomaly_ref.lower().replace(" ", "").replace("_", "")
        if not standardise:
            if anomaly_ref in ['tmean', "meantime", "timemean"]:
                cmap = "PuOr"
                for var in list(data.data_vars.keys()):
                    process_data[var] = data[var] - data[var].mean(dim='time')
                type = "anomaly wrt temporal mean"
            elif anomaly_ref in ['t0', "intialtime", "firsttime"]:
                cmap = "PuOr"
                for var in list(data.data_vars.keys()):
                    process_data[var] = data[var] - data[var].isel(time=0)
                type = "anomaly wrt initial time"
            else:
                raise ValueError(
                    "Select proper value of anomaly_ref: t0 or tmean, when anomaly = True ")
            logger.info(f"Data processed for {type}")
        if standardise:
            if anomaly_ref in ['t0', "intialtime", "firsttime"]:
                cmap = "PuOr"
                for var in list(data.data_vars.keys()):
                    var_data = data[var] - data[var].isel(time=0)
                    var_data.attrs['units'] = 'Stand. Units'
                    # Calculate the standard anomaly by dividing the anomaly by its standard deviation along the time dimension
                    process_data[var] = var_data / var_data.std(dim="time")
                type = "Std. anomaly wrt initial time"
            elif anomaly_ref in ['tmean', "meantime", "timemean"]:
                cmap = "PuOr"
                for var in list(data.data_vars.keys()):
                    var_data = data[var] - data[var].mean(dim='time')
                    var_data.attrs['units'] = 'Stand. Units'
                    # Calculate the standard anomaly by dividing the anomaly by its standard deviation along the time dimension
                    process_data[var] = var_data / var_data.std(dim="time")
                type = "Std. anomaly wrt temporal mean"
            else:
                raise ValueError(
                    "Select proper value of type: t0 or tmean, when anomaly = True ")
            logger.info(
                f"Data processed for {type}")

    else:
        cmap = 'jet'
        logger.info("Data processed for Full values as anomaly = False")
        type = "Full values"

        process_data = data
    # logger.info(f"Data processed for {type}")
    return process_data, type, cmap
    
    
def zonal_mean_trend_plot(o3d_request):
    """
    Plots spatial trends at different vertical levels for two variables.

    Args:
        data (xarray.Dataset):
            Input dataset containing a single 3D field with trends as a function of depth, latitude, and longitude.

        region (str, optional):
            Region name.

        latS (float, optional):
            Southern latitude bound for the region.

        latN (float, optional):
            Northern latitude bound for the region.

        lonW (float, optional):
            Western longitude bound for the region.

        lonE (float, optional):
            Eastern longitude bound for the region.

        output (bool, optional):
            Flag indicating whether to save the output figure and data. Defaults to True.

        output_dir (str, optional):
            Output directory path. Defaults to "output".

    Returns:
        None
    """
    data = o3d_request.get('data')
    model = o3d_request.get('model')
    exp = o3d_request.get('exp')
    source = o3d_request.get('source')
    region = o3d_request.get('region', None)
    latS = o3d_request.get('latS', None)
    latN = o3d_request.get('latN', None)
    lonW = o3d_request.get('lonW', None)
    lonE = o3d_request.get('lonE', None)
    output = o3d_request.get('output')
    output_dir = o3d_request.get('output_dir')
    
    TS_3dtrend_data = TS_3dtrend(data)
    TS_3dtrend_data.attrs = data.attrs
    data = TS_3dtrend_data

    data = weighted_zonal_mean(data, region, latS, latN, lonW, lonE)

    fig, (axs) = plt.subplots(nrows=1, ncols=2, figsize=(14, 5))

    data.ocpt.plot.contourf(levels=20, ax=axs[0])
    axs[0].set_ylim((5500, 0))

    region_title = custom_region(region=region, latS=latS, latN=latN,
                                 lonW=lonW, lonE=lonE)

    fig.suptitle(
        f"Zonally-averaged long-term trends in the {region_title}", fontsize=20)

    plt.subplots_adjust(top=0.85)

    axs[0].set_title("Temperature", fontsize=14)
    axs[0].set_ylabel("Depth (in m)", fontsize=9)
    axs[0].set_xlabel("Latitude (in deg North)", fontsize=9)
    axs[0].set_facecolor('grey')

    data.so.plot.contourf(levels=20, ax=axs[1])
    axs[1].set_ylim((5500, 0))
    axs[1].set_title("Salinity", fontsize=14)
    axs[1].set_ylabel("Depth (in m)", fontsize=12)
    axs[1].set_xlabel("Latitude (in deg North)", fontsize=12)
    axs[1].set_facecolor('grey')

    if output:
        output_path, fig_dir, data_dir, filename = dir_creation(data,
            region, latS, latN, lonW, lonE, output_dir, plot_name="zonal_mean_trend")
        filename = f"{model}_{exp}_{source}_{filename}"
        write_data(f'{data_dir}/{filename}.nc',data)
        plt.savefig(f"{fig_dir}/{filename}.pdf")
        logger.info(
            "Figure and data used for this plot are saved here: %s", output_path)

    # plt.show()

    return


def reg_mean(data, region=None, latS=None, latN=None, lonW=None, lonE=None):
    """
    Computes the weighted box mean for some predefined or customized region

    Parameters:
        data (xarray.Dataset): Input data.

        region (str, optional): Predefined region name. If provided, latitude and longitude bounds will be fetched from predefined regions.

        latS (float, optional): Southern latitude bound. Required if region is not provided or None.

        latN (float, optional): Northern latitude bound. Required if region is not provided or None.

        lonW (float, optional): Western longitude bound. Required if region is not provided or None.

        lonE (float, optional): Eastern longitude bound. Required if region is not provided or None.

    Returns:
        xarray.Dataset: Standardised anomaly with respect to the initial time step of the input data.
    """
    data = weighted_area_mean(data, region, latS, latN, lonW, lonE)

    return data


# def data_process_by_type(data, anomaly: bool = False,
#                          standardise: bool = False, anomaly_ref: str = None):
#     """
#     Selects the type of timeseries and colormap based on the given parameters.

#     Args:
#         data (DataArray): Input data containing temperature (ocpt) and salinity (so).
#         anomaly (bool, optional): Specifies whether to compute anomalies. Defaults to False.
#         standardise (bool, optional): Specifies whether to standardize the data. Defaults to False.
#         anomaly_ref (str, optional): Reference for the anomaly computation. Valid options: "t0", "tmean". Defaults to None.

#     Returns:
#         process_data (Dataset): Processed data based on the selected preprocessing approach.
#         type (str): Type of preprocessing approach applied
#         cmap (str): Colormap to be used for the plot.
#     """

#     process_data = xr.Dataset()

#     if anomaly:
#         anomaly_ref = anomaly_ref.lower().replace(" ", "").replace("_", "")
#         if not standardise:
#             if anomaly_ref in ['tmean', "meantime", "timemean"]:
#                 cmap = "PuOr"
#                 for var in list(data.data_vars.keys()):
#                     process_data[var] = data[var] - data[var].mean(dim='time')
#                 type = "anomaly wrt temporal mean"
#             elif anomaly_ref in ['t0', "intialtime", "firsttime"]:
#                 cmap = "PuOr"
#                 for var in list(data.data_vars.keys()):
#                     process_data[var] = data[var] - data[var].isel(time=0)
#                 type = "anomaly wrt initial time"
#             else:
#                 raise ValueError(
#                     "Select proper value of anomaly_ref: t0 or tmean, when anomaly = True ")
#             logger.info(f"Data processed for {type}")
#         if standardise:
#             if anomaly_ref in ['t0', "intialtime", "firsttime"]:
#                 cmap = "PuOr"
#                 for var in list(data.data_vars.keys()):
#                     var_data = data[var] - data[var].isel(time=0)
#                     var_data.attrs['units'] = 'Stand. Units'
#                     # Calculate the standard anomaly by dividing the anomaly by its standard deviation along the time dimension
#                     process_data[var] = var_data / var_data.std(dim="time")
#                 type = "Std. anomaly wrt initial time"
#             elif anomaly_ref in ['tmean', "meantime", "timemean"]:
#                 cmap = "PuOr"
#                 for var in list(data.data_vars.keys()):
#                     var_data = data[var] - data[var].mean(dim='time')
#                     var_data.attrs['units'] = 'Stand. Units'
#                     # Calculate the standard anomaly by dividing the anomaly by its standard deviation along the time dimension
#                     process_data[var] = var_data / var_data.std(dim="time")
#                 type = "Std. anomaly wrt temporal mmean"
#             else:
#                 raise ValueError(
#                     "Select proper value of type: t0 or tmean, when anomaly = True ")
#             logger.info(
#                 f"Data processed for {type}")

#     else:
#         cmap = 'jet'
#         logger.info("Data processed for Full values as anomaly = False")
#         type = "Full values"

#         process_data = data
#     # logger.info(f"Data processed for {type}")
#     return process_data, type, cmap



# # def hovmoller_lev_time_plot(o3d_request, anomaly: bool = False,
#                             standardise: bool = False, anomaly_ref=None
#                             ):
#     """
#     Create a Hovmoller plot of temperature and salinity full values.

#     Parameters:
#         data (DataArray): Input data containing temperature (ocpt) and salinity (so).
#         region (str): Region represented in the plot.
#         anomaly (bool): To decide whether to compute anomalies. Default is False.
#         standardise (bool): To decide whether to standardise the anomalies. Default is False.
#         anomaly_ref (str): Reference for computing the anomaly (t0=first time step, tmean=whole temporal period; only valid if anomaly is True)
#         latS (float): Southern latitude boundary of the region. Default is None. (Optional)
#         latN (float): Northern latitude boundary of the region. Default is None. (Optional)
#         lonW (float): Western longitude boundary of the region. Default is None. (Optional)
#         lonE (float): Eastern longitude boundary of the region. Default is None. (Optional)
#         output (bool): Indicates whether to save the plot and data. Default is False. (Optional)
#         output_dir (str): Directory to save the output files. Default is None. (Optional)

#     Returns:
#         None
#     """
#     data = o3d_request.get('data')
#     model = o3d_request.get('model')
#     exp = o3d_request.get('exp')
#     source = o3d_request.get('source')
#     region = o3d_request.get('region', None)
#     latS = o3d_request.get('latS', None)
#     latN = o3d_request.get('latN', None)
#     lonW = o3d_request.get('lonW', None)
#     lonE = o3d_request.get('lonE', None)
#     output = o3d_request.get('output')
#     output_dir = o3d_request.get('output_dir')
    
    
#     data = weighted_area_mean(data, region, latS, latN, lonW, lonE)
#     # Reads the type of timeseries to plot
#     data, type, cmap = data_process_by_type(
#         data, anomaly, standardise, anomaly_ref)

#     logger.info("Hovmoller plotting in process")
#     # Create subplots for temperature and salinity plots
#     fig, (axs) = plt.subplots(nrows=1, ncols=2, figsize=(14, 5))
#     region_title = custom_region(region=region, latS=latS, latN=latN, lonW=lonW, lonE=lonE)
#     fig.suptitle(f"Spatially averaged {region_title} T,S {type}", fontsize=22)

#     if output:
#         output_path, fig_dir, data_dir, filename = dir_creation(data,
#             region, latS, latN, lonW, lonE, output_dir, plot_name=f'hovmoller_plot_{type.replace(" ","_")}')


#     # plt.pcolor(X, Y, Z, vmin=vmin, vmax=vmax, norm=norm)

#     # To center the colorscale around zero when we plot temperature anomalies
#     ocptmin = round(np.nanmin(data.ocpt.values), 2)
#     ocptmax = round(np.nanmax(data.ocpt.values), 2)

#     if ocptmin < 0:
#         if abs(ocptmin) < ocptmax:
#             ocptmin = ocptmax*-1
#         else:
#             ocptmax = ocptmin*-1

#         ocptlevs = np.linspace(ocptmin, ocptmax, 21)

#     else:
#         ocptlevs = 20

#     # And we do the same for salinity
#     somin = round(np.nanmin(data.so.values), 3)
#     somax = round(np.nanmax(data.so.values), 3)

#     if somin < 0:
#         if abs(somin) < somax:
#             somin = somax*-1
#         else:
#             somax = somin*-1

#         solevs = np.linspace(somin, somax, 21)

#     else:
#         solevs = 20

#     cs1 = axs[0].contourf(data.time, data.lev, data.ocpt.transpose(),
#                           levels=ocptlevs, cmap=cmap, extend='both')
#     cbar_ax = fig.add_axes([0.13, 0.1, 0.35, 0.05])
#     fig.colorbar(cs1, cax=cbar_ax, orientation='horizontal', label=f'Potential temperature in {data.ocpt.attrs["units"]}')

#     cs2 = axs[1].contourf(data.time, data.lev, data.so.transpose(),
#                           levels=solevs, cmap=cmap, extend='both')
#     cbar_ax = fig.add_axes([0.54, 0.1, 0.35, 0.05])
#     fig.colorbar(cs2, cax=cbar_ax, orientation='horizontal', label=f'Salinity in {data.so.attrs["units"]}')

#     if output:
#         data.to_netcdf(f'{data_dir}/{filename}.nc')
#         # obs_clim.to_netcdf(f'{data_dir}/{filename}_Rho.nc')

#     axs[0].invert_yaxis()
#     axs[1].invert_yaxis()

#     axs[0].set_ylim((max(data.lev).data, 0))
#     axs[1].set_ylim((max(data.lev).data, 0))

#     axs[0].set_title("Temperature", fontsize=15)
#     axs[0].set_ylabel("Depth (in m)", fontsize=12)
#     axs[0].set_xlabel("Time (in years)", fontsize=12)
#     axs[0].set_xticklabels(axs[0].get_xticklabels(), rotation=30)
#     axs[1].set_xticklabels(axs[0].get_xticklabels(), rotation=30)
#     max_num_ticks = 5  # Adjust this value to control the number of ticks
#     from matplotlib.ticker import MaxNLocator
#     locator = MaxNLocator(integer=True, prune='both', nbins=max_num_ticks)
#     axs[0].xaxis.set_major_locator(locator)
#     axs[1].xaxis.set_major_locator(locator)

#     axs[1].set_title("Salinity", fontsize=15)
#     axs[1].set_xlabel("Time (in years)", fontsize=12)
#     axs[1].set_yticklabels([])

#     plt.subplots_adjust(bottom=0.3, top=0.85, wspace=0.1)

#     if output:
#         plt.savefig(f"{fig_dir}/{filename}.pdf")
#         logger.info(
#             "Figure and data used for this plot are saved here: %s", output_path)

#     return




# def hovmoller_lev_time_plot(o3d_request, anomaly: bool = False,
#                             standardise: bool = False, anomaly_ref=None
#                             ):
#     """
#     Create a Hovmoller plot of temperature and salinity full values.

#     Parameters:
#         data (DataArray): Input data containing temperature (ocpt) and salinity (so).
#         region (str): Region represented in the plot.
#         anomaly (bool): To decide whether to compute anomalies. Default is False.
#         standardise (bool): To decide whether to standardise the anomalies. Default is False.
#         anomaly_ref (str): Reference for computing the anomaly (t0=first time step, tmean=whole temporal period; only valid if anomaly is True)
#         latS (float): Southern latitude boundary of the region. Default is None. (Optional)
#         latN (float): Northern latitude boundary of the region. Default is None. (Optional)
#         lonW (float): Western longitude boundary of the region. Default is None. (Optional)
#         lonE (float): Eastern longitude boundary of the region. Default is None. (Optional)
#         output (bool): Indicates whether to save the plot and data. Default is False. (Optional)
#         output_dir (str): Directory to save the output files. Default is None. (Optional)

#     Returns:
#         None
#     """
#     data = o3d_request.get('data')
#     model = o3d_request.get('model')
#     exp = o3d_request.get('exp')
#     source = o3d_request.get('source')
#     region = o3d_request.get('region', None)
#     latS = o3d_request.get('latS', None)
#     latN = o3d_request.get('latN', None)
#     lonW = o3d_request.get('lonW', None)
#     lonE = o3d_request.get('lonE', None)
#     output = o3d_request.get('output')
#     output_dir = o3d_request.get('output_dir')
    
    
#     data = weighted_area_mean(data, region, latS, latN, lonW, lonE)
#     # Reads the type of timeseries to plot
#     data, type, cmap = data_for_hovmoller_lev_time_plot(data)

#     logger.info("Hovmoller plotting in process")
#     # Create subplots for temperature and salinity plots
#     fig, (axs) = plt.subplots(nrows=1, ncols=2, figsize=(14, 5))
#     region_title = custom_region(region=region, latS=latS, latN=latN, lonW=lonW, lonE=lonE)
#     fig.suptitle(f"Spatially averaged {region_title} T,S {type}", fontsize=22)

#     if output:
#         output_path, fig_dir, data_dir, filename = dir_creation(data,
#             region, latS, latN, lonW, lonE, output_dir, plot_name=f'hovmoller_plot_{type.replace(" ","_")}')


#     # plt.pcolor(X, Y, Z, vmin=vmin, vmax=vmax, norm=norm)

#     # To center the colorscale around zero when we plot temperature anomalies
#     ocptmin = round(np.nanmin(data.ocpt.values), 2)
#     ocptmax = round(np.nanmax(data.ocpt.values), 2)

#     if ocptmin < 0:
#         if abs(ocptmin) < ocptmax:
#             ocptmin = ocptmax*-1
#         else:
#             ocptmax = ocptmin*-1

#         ocptlevs = np.linspace(ocptmin, ocptmax, 21)

#     else:
#         ocptlevs = 20

#     # And we do the same for salinity
#     somin = round(np.nanmin(data.so.values), 3)
#     somax = round(np.nanmax(data.so.values), 3)

#     if somin < 0:
#         if abs(somin) < somax:
#             somin = somax*-1
#         else:
#             somax = somin*-1

#         solevs = np.linspace(somin, somax, 21)

#     else:
#         solevs = 20

#     cs1 = axs[0].contourf(data.time, data.lev, data.ocpt.transpose(),
#                           levels=ocptlevs, cmap=cmap, extend='both')
#     cbar_ax = fig.add_axes([0.13, 0.1, 0.35, 0.05])
#     fig.colorbar(cs1, cax=cbar_ax, orientation='horizontal', label=f'Potential temperature in {data.ocpt.attrs["units"]}')

#     cs2 = axs[1].contourf(data.time, data.lev, data.so.transpose(),
#                           levels=solevs, cmap=cmap, extend='both')
#     cbar_ax = fig.add_axes([0.54, 0.1, 0.35, 0.05])
#     fig.colorbar(cs2, cax=cbar_ax, orientation='horizontal', label=f'Salinity in {data.so.attrs["units"]}')

#     if output:
#         data.to_netcdf(f'{data_dir}/{filename}.nc')
#         # obs_clim.to_netcdf(f'{data_dir}/{filename}_Rho.nc')

#     axs[0].invert_yaxis()
#     axs[1].invert_yaxis()

#     axs[0].set_ylim((max(data.lev).data, 0))
#     axs[1].set_ylim((max(data.lev).data, 0))

#     axs[0].set_title("Temperature", fontsize=15)
#     axs[0].set_ylabel("Depth (in m)", fontsize=12)
#     axs[0].set_xlabel("Time (in years)", fontsize=12)
#     axs[0].set_xticklabels(axs[0].get_xticklabels(), rotation=30)
#     axs[1].set_xticklabels(axs[0].get_xticklabels(), rotation=30)
#     max_num_ticks = 5  # Adjust this value to control the number of ticks
#     from matplotlib.ticker import MaxNLocator
#     locator = MaxNLocator(integer=True, prune='both', nbins=max_num_ticks)
#     axs[0].xaxis.set_major_locator(locator)
#     axs[1].xaxis.set_major_locator(locator)

#     axs[1].set_title("Salinity", fontsize=15)
#     axs[1].set_xlabel("Time (in years)", fontsize=12)
#     axs[1].set_yticklabels([])

#     plt.subplots_adjust(bottom=0.3, top=0.85, wspace=0.1)

#     if output:
#         plt.savefig(f"{fig_dir}/{filename}.pdf")
#         logger.info(
#             "Figure and data used for this plot are saved here: %s", output_path)

#     return


def time_series_multilevs(o3d_request, anomaly: bool = False,
                          standardise: bool = False, anomaly_ref=None,
                          customise_level=False, levels=None):
    """
    Create time series plots of global temperature and salinity at selected levels.

    Parameters:
        data (DataArray): Input data containing temperature (ocpt) and salinity (so).
        region (str): Region name. (Optional)
        anomaly (bool): To decide whether to compute anomalies. Default is False.
        standardise (bool): To decide whether to standardise the anomalies. Default is False.
        anomaly_ref (str): Reference for computing the anomaly (t0=first time step, tmean=whole temporal period; only valid if anomaly is True)
        customise_level (bool): Whether to use custom levels or predefined levels.
        levels (list): List of levels to plot. Ignored if customise_level is False.
        latS (float): Southern latitude bound. (Optional)
        latN (float): Northern latitude bound. (Optional)
        lonW (float): Western longitude bound. (Optional)
        lonE (float): Eastern longitude bound. (Optional)
        output (bool): Whether to save the plot and data. Default is True.
        output_dir (str): Directory to save the plot and data. (Optional)

    Returns:
        None
    """
    data = o3d_request.get('data')
    model = o3d_request.get('model')
    exp = o3d_request.get('exp')
    source = o3d_request.get('source')
    region = o3d_request.get('region', None)
    latS = o3d_request.get('latS', None)
    latN = o3d_request.get('latN', None)
    lonW = o3d_request.get('lonW', None)
    lonE = o3d_request.get('lonE', None)
    output = o3d_request.get('output')
    output_dir = o3d_request.get('output_dir')
    
    
    
    data = weighted_area_mean(data, region, latS, latN, lonW, lonE)
    data, type, cmap = data_process_by_type(
                        data=data, anomaly=anomaly, standardise=standardise, anomaly_ref=anomaly_ref)

    logger.info("Time series plot is in process")

    # Create subplots for temperature and salinity time series plots
    fig, axs = plt.subplots(nrows=1, ncols=2, figsize=(16, 5))
    region_title = custom_region(region=region, latS=latS, latN=latN, lonW=lonW, lonE=lonE)

    fig.suptitle(f"Spatially averaged {region_title} T,S {type}", fontsize=20)

    # Define the levels at which to plot the time series
    if customise_level:
        if levels is None:
            raise ValueError(
                "Custom levels are selected, but levels are not provided.")
    else:
        levels = [0, 100, 500, 1000, 2000, 3000, 4000, 5000]

    # Iterate over the levels and plot the time series for each level
    for level in levels:
        if level != 0:
            # Select the data at the specified level
            data_level = data.sel(lev=slice(None, level)).isel(lev=-1)
        else:
            # Select the data at the surface level (0)
            data_level = data.isel(lev=0)
        # Plot the temperature time series
        data_level.ocpt.plot.line(
            ax=axs[0], label=f"{round(int(data_level.lev.data), -2)}")

        # Plot the salinity time series
        data_level.so.plot.line(
            ax=axs[1], label=f"{round(int(data_level.lev.data), -2)}")
    if output:
        output_path, fig_dir, data_dir, filename = dir_creation(data,
            region, latS, latN, lonW, lonE, output_dir, plot_name=f'time_series_{type.replace(" ","_").replace(".","")}')
        filename = f"{model}_{exp}_{source}_{filename}"


    tunits = data_level.ocpt.attrs['units']
    sunits = data_level.so.attrs['units']
    axs[0].set_title("Temperature", fontsize=15)
    axs[0].set_ylabel(f"Potential Temperature (in {tunits})", fontsize=12)
    axs[0].set_xlabel("Time", fontsize=12)
    axs[1].set_title("Salinity", fontsize=15)
    axs[1].set_ylabel(f"Salinity (in {sunits})", fontsize=12)
    axs[1].set_xlabel("Time (in years)", fontsize=12)
    axs[1].legend(loc='right')
    # axs[1].set_yticklabels([])
    plt.subplots_adjust(bottom=0.3, top=0.85, wspace=0.2)

    if output:
        write_data(f'{data_dir}/{filename}.nc', data)
        plt.savefig(f"{fig_dir}/{filename}.pdf")
        logger.info(
            " Figure and data used in the plot, saved here : %s", output_path)
    plt.show()
    return


def linregress_3D(y_array):
    """
    Computes the linear regression against the temporal dimension of a 3D array formatted in time, latitude, and longitude.

    The function is a very efficient way to compute trends in 3D arrays.

    Parameters:
        y_array (np.ndarray): 3D array with time, latitude, and longitude as dimensions.

    Returns:
        n (np.ndarray): Number of non-nan values over each (latitude, longitude) grid box.
        slope (np.ndarray): Slope of the linear regression line over each (latitude, longitude) grid box.
        intercept (np.ndarray): Intercept of the linear regression line over each (latitude, longitude) grid box.
        p_val (np.ndarray): p-value of the linear regression over each (latitude, longitude) grid box.
        r_square (np.ndarray): R-squared value of the linear regression over each (latitude, longitude) grid box.
        rmse (np.ndarray): Root mean squared error (RMSE) of the linear regression over each (latitude, longitude) grid box.
    """

    x_array = np.empty(y_array.shape)
    for i in range(y_array.shape[0]):
        # This would be fine if time series is not too long. Or we can use i+yr (e.g. 2019).
        x_array[i, :, :] = i+1
    x_array[np.isnan(y_array)] = np.nan
    # Compute the number of non-nan over each (lon,lat) grid box.
    n = np.sum(~np.isnan(x_array), axis=0)
    # Compute mean and standard deviation of time series of x_array and y_array over each (lon,lat) grid box.
    x_mean = np.nanmean(x_array, axis=0)
    y_mean = np.nanmean(y_array, axis=0)
    x_std = np.nanstd(x_array, axis=0)
    y_std = np.nanstd(y_array, axis=0)
    # Compute co-variance between time series of x_array and y_array over each (lon,lat) grid box.
    cov = np.nansum((x_array-x_mean)*(y_array-y_mean), axis=0)/n
    # Compute correlation coefficients between time series of x_array and y_array over each (lon,lat) grid box.
    cor = cov/(x_std*y_std)
    # Compute slope between time series of x_array and y_array over each (lon,lat) grid box.
    slope = cov/(x_std**2)
    # Compute intercept between time series of x_array and y_array over each (lon,lat) grid box.
    intercept = y_mean-x_mean*slope
    # Compute tstats, stderr, and p_val between time series of x_array and y_array over each (lon,lat) grid box.
    tstats = cor*np.sqrt(n-2)/np.sqrt(1-cor**2)
    # stderr = slope/tstats
    p_val = statt.sf(tstats, n-2)*2
    # Compute r_square and rmse between time series of x_array and y_array over each (lon,lat) grid box.
    # r_square also equals to cor**2 in 1-variable lineare regression analysis, which can be used for checking.
    r_square = np.nansum((slope*x_array+intercept-y_mean)
                         ** 2, axis=0)/np.nansum((y_array-y_mean)**2, axis=0)
    rmse = np.sqrt(np.nansum((y_array-slope*x_array-intercept)**2, axis=0)/n)
    # Do further filteration if needed (e.g. We stipulate at least 3 data records are needed to do regression analysis) and return values
    n = n*1.0  # convert n from integer to float to enable later use of np.nan
    n[n < 3] = np.nan
    slope[np.isnan(n)] = np.nan
    intercept[np.isnan(n)] = np.nan
    p_val[np.isnan(n)] = np.nan
    r_square[np.isnan(n)] = np.nan
    rmse[np.isnan(n)] = np.nan
    return n, slope, intercept, p_val, r_square, rmse


def lintrend_2D(y_array):
    """
    Simplified version of linregress_3D that computes the trends in a 3D array formated in time, latitude and longitude coordinates

    It outputs the trends in xarray format

    Parameters
    ----------
    data : y_array.Dataset

    Dataset containing a single 3D field with time, latitude and longitude as coordinates


    Returns
    -------
    n,slope,intercept,p_val,r_square,rmse

    """

    x_array = np.empty(y_array.shape)
    for i in range(y_array.shape[0]):
        # This would be fine if time series is not too long. Or we can use i+yr (e.g. 2019).
        x_array[i, :, :] = i+1
    x_array[np.isnan(y_array)] = np.nan
    # Compute the number of non-nan over each (lon,lat) grid box.
    n = np.sum(~np.isnan(x_array), axis=0)
    # Compute mean and standard deviation of time series of x_array and y_array over each (lon,lat) grid box.
    x_mean = np.nanmean(x_array, axis=0)
    y_mean = np.nanmean(y_array, axis=0)
    x_std = np.nanstd(x_array, axis=0)
    # y_std = np.nanstd(y_array, axis=0)
    # Compute co-variance between time series of x_array and y_array over each (lon,lat) grid box.
    cov = np.nansum((x_array-x_mean)*(y_array-y_mean), axis=0)/n
    # Compute slope between time series of x_array and y_array over each (lon,lat) grid box.
    trend = cov/(x_std**2)

    # Do further filteration if needed (e.g. We stipulate at least 3 data records are needed to do regression analysis) and return values
    n = n*1.0  # convert n from integer to float to enable later use of np.nan
    n[n < 3] = np.nan
    trend[np.isnan(n)] = np.nan

    # trend=xr.DataArray(trend,coords={"lat": y_array.lat,"lon": y_array.lon},name=str(y_array.name),dims=["lat","lon"])
    trend = xr.DataArray(trend, coords={
                         "lat": y_array.lat, "lon": y_array.lon}, name=f"{y_array.name} trends", dims=["lat", "lon"])
    trend.attrs['units'] = f"{y_array.units}/year"

    # data.ocpt.attrs['units'] = 'Standardised Units'
    return trend


def lintrend_3D(y_array):
    """
    Simplified version of linregress_3D that computes the trends in a 4D array formated in time, depth, latitude and longitude coordinates

    It outputs the trends in xarray format

    Parameters
    ----------
    data : y_array.Dataset

    Dataset containing a single 4D field with time, depth, latitude and longitude as coordinates


    Returns
    -------
    n,slope,intercept,p_val,r_square,rmse

    """

    x_array = np.empty(y_array.shape)
    for i in range(y_array.shape[0]):
        # This would be fine if time series is not too long. Or we can use i+yr (e.g. 2019).
        x_array[i, :, :, :] = i+1
    x_array[np.isnan(y_array)] = np.nan
    # Compute the number of non-nan over each (lon,lat) grid box.
    n = np.sum(~np.isnan(x_array), axis=0)
    # Compute mean and standard deviation of time series of x_array and y_array over each (lon,lat) grid box.
    x_mean = np.nanmean(x_array, axis=0)
    y_mean = np.nanmean(y_array, axis=0)
    x_std = np.nanstd(x_array, axis=0)
    # y_std = np.nanstd(y_array, axis=0)
    # Compute co-variance between time series of x_array and y_array over each (lon,lat) grid box.
    cov = np.nansum((x_array-x_mean)*(y_array-y_mean), axis=0)/n
    # Compute slope between time series of x_array and y_array over each (lon,lat) grid box.
    trend = cov/(x_std**2)

    # Do further filteration if needed (e.g. We stipulate at least 3 data records are needed to do regression analysis) and return values
    n = n*1.0  # convert n from integer to float to enable later use of np.nan
    n[n < 3] = np.nan
    trend[np.isnan(n)] = np.nan

    # trend=xr.DataArray(trend,coords={"lat": y_array.lat,"lon": y_array.lon},name=str(y_array.name),dims=["lat","lon"])
    trend = xr.DataArray(trend, coords={"lev": y_array.lev, "lat": y_array.lat,
                         "lon": y_array.lon}, name=f"{y_array.name} trends", dims=["lev", "lat", "lon"])
    trend.attrs['units'] = f"{y_array.units}/year"

    # data.ocpt.attrs['units'] = 'Standardised Units'
    return trend


def TS_3dtrend(data):
    """
    Computes the trend values for temperature and salinity variables in a 3D dataset.

    Parameters:
        data (xarray.Dataset): Input dataset containing temperature (ocpt) and salinity (so) variables.

    Returns:
        xarray.Dataset: Dataset with trend values for temperature and salinity variables.
    """
    TS_3dtrend_data = xr.Dataset()

    so = lintrend_3D(data.so)
    ocpt = lintrend_3D(data.ocpt)

    TS_3dtrend_data = TS_3dtrend_data.merge({"ocpt": ocpt, "so": so})

    logger.info("Trend value calculated")
    return TS_3dtrend_data


def multilevel_t_s_trend_plot(o3d_request, customise_level=False, levels=None):
    """
    Generates a plot showing trends at different depths for temperature and salinity variables.

    Parameters:
        data (xarray.Dataset): Input data containing temperature (ocpt) and salinity (so) variables.
        region (str): Region name. (Optional)
        customise_level (bool): Whether to use custom levels or predefined levels.
        levels (list): List of levels to plot. Ignored if customise_level is False.
        latS (float): Southern latitude bound. (Optional)
        latN (float): Northern latitude bound. (Optional)
        lonW (float): Western longitude bound. (Optional)
        lonE (float): Eastern longitude bound. (Optional)
        output (bool): Whether to save the plot and data. Default is True.
        output_dir (str): Output directory to save the plot and data. (Optional)

    Returns:
        None
    """
    data = o3d_request.get('data')
    model = o3d_request.get('model')
    exp = o3d_request.get('exp')
    source = o3d_request.get('source')
    region = o3d_request.get('region', None)
    latS = o3d_request.get('latS', None)
    latN = o3d_request.get('latN', None)
    lonW = o3d_request.get('lonW', None)
    lonE = o3d_request.get('lonE', None)
    output = o3d_request.get('output')
    output_dir = o3d_request.get('output_dir')
    
    data = area_selection(data, region, latS, latN, lonW, lonE)
    TS_trend_data = TS_3dtrend(data)
    TS_trend_data.attrs = data.attrs
    data = TS_trend_data
    # Define the levels for plotting
    if customise_level:
        if levels is None:
            raise ValueError(
                "Custom levels are selected, but levels are not provided.")
    else:
        levels = [10, 100, 500, 1000, 3000, 5000]

    # To fix the dimensions so that all subpanels are well visible
    dim1 = 16
    dim2 = 5*len(levels)

    fig, axs = plt.subplots(nrows=len(levels), ncols=2, figsize=(dim1, dim2))

    fig.subplots_adjust(hspace=0.18, wspace=0.15, top=0.95)
    for levs in range(len(levels)):

        data["ocpt"].interp(lev=levels[levs]).plot.contourf(
            ax=axs[levs, 0], levels=18)
        data["so"].interp(lev=levels[levs]).plot.contourf(
            ax=axs[levs, 1], levels=18)

        axs[levs, 0].set_ylabel("Latitude (in deg North)", fontsize=9)
        axs[levs, 0].set_facecolor('grey')
        # axs[levs, 0].set_aspect('equal', adjustable='box')

        axs[levs, 1].set_yticklabels([])

        if levs == (len(levels)-1):
            axs[levs, 0].set_xlabel("Longitude (in de East)", fontsize=12)
            axs[levs, 0].set_ylabel("Latitude (in deg North)", fontsize=12)

        if levs != (len(levels)-1):
            axs[levs, 0].set_xticklabels([])
            axs[levs, 1].set_xticklabels([])
        axs[levs, 1].set_facecolor('grey')
        # axs[levs, 1].set_aspect('equal', adjustable='box')
    region_title = custom_region(region=region, latS=latS, latN=latN, lonW=lonW, lonE=lonE)

    plt.suptitle(
        f'Linear Trends of T,S at different depths in the {region_title}', fontsize=24)
    axs[0, 0].set_title("Temperature", fontsize=18)
    axs[0, 1].set_title("Salinity", fontsize=18)
    if output:
        output_path, fig_dir, data_dir, filename = dir_creation(data,
            region, latS, latN, lonW, lonE, output_dir, plot_name="multilevel_t_s_trend")
        filename = f"{model}_{exp}_{source}_{filename}"
        write_data(f'{data_dir}/{filename}.nc',data.interp(lev=levels[levs]))
        plt.savefig(f"{fig_dir}/{filename}.pdf")
        logger.info(
            "Figure and data used for this plot are saved here: %s", output_path)

    #plt.show()

    return
