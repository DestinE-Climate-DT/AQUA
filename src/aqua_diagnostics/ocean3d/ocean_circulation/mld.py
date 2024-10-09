from .ocean_circulation import *

def data_for_plot_spatial_mld_clim(data, region=None, time=None,
                                   lat_s: float = None, lat_n: float = None,
                                   lon_w: float = None, lon_e: float = None, loglevel= "WARNING"):
    """
    Extracts and prepares data for plotting spatial mean mixed layer depth (MLD) climatology.
    Parameters:
    - data (pandas.DataFrame): Input data containing relevant variables.
    - region (str or None): Optional region to subset the data (e.g., 'North Atlantic').
    - time (str or None): Optional time period to select from the data (e.g., '2010-2020').
    - lat_s (float or None): Southernmost latitude of the region (default: None).
    - lat_n (float or None): Northernmost latitude of the region (default: None).
    - lon_w (float or None): Westernmost longitude of the region (default: None).
    - lon_e (float or None): Easternmost longitude of the region (default: None).
    - Returns:
        xarray.Dataset: Processed data suitable for plotting spatial MLD climatology.
    """
    logger = log_configure(loglevel, 'data_for_plot_spatial_mld_clim')

    data = area_selection(data, region, lat_s, lat_n, lon_w, lon_e)
    data = convert_variables(data)
    data = compute_mld_cont(data[["rho"]])
    data, time = data_time_selection(data, time)
    return data.mean("time"), time


def plot_spatial_mld_clim(o3d_request,
                          overlap=True, loglevel= "WARNING"):
    """
    Plots the climatology of mixed layer depth in the NH as computed with de Boyer Montegut (2004)'s criteria in
    an observational dataset and a model dataset, allowing the user to select the month the climatology is computed
    (the recommended one is march (month=3) that is when the NH MLD peaks)
    Parameters
    ----------
        datamod (xarray.Dataset): Model Dataset containing 2D fields of density (rho).
        dataobs (xarray.Dataset): Observational dataset containing 2D fields of density (rho)
        month (integer): Number of the month on which to compute the climatologies
        overlap (boolean):  To indicate if OBS and Model are cropped to overlap time period
    Returns
    -------
    None
    """
    logger = log_configure(loglevel, 'plot_spatial_mld_clim')
    mod_data = o3d_request.get('data')
    obs_data = o3d_request.get('obs_data')
    model = o3d_request.get('model')
    exp = o3d_request.get('exp')
    source = o3d_request.get('source')
    time = o3d_request.get('time')
    region = o3d_request.get('region', None)
    lat_s = o3d_request.get('lat_s', None)
    lat_n = o3d_request.get('lat_n', None)
    lon_w = o3d_request.get('lon_w', None)
    lon_e = o3d_request.get('lon_e', None)
    output = o3d_request.get('output')
    output_dir = o3d_request.get('output_dir')


    if overlap:
        obs_data = crop_obs_overlap_time(mod_data, obs_data)
        mod_data = crop_obs_overlap_time(obs_data, mod_data)

    mod_clim, time = data_for_plot_spatial_mld_clim(mod_data, region, time,
                                                    lat_s, lat_n, lon_w, lon_e)  # To select the month and compute its climatology
    obs_clim, time = data_for_plot_spatial_mld_clim(obs_data, region, time,
                                                    lat_s, lat_n, lon_w, lon_e)  # To select the month and compute its climatology
    # obs_data=crop_obs_overlap_time(mod_data, obs_data)

    # We identify the first year used in the climatology
    myr1 = mod_data.time.dt.year[0].values
    # We identify the last year used in the climatology
    myr2 = mod_data.time.dt.year[-1].values

    # We identify the first year used in the climatology
    oyr1 = obs_data.time.dt.year[0].values
    # We identify the last year used in the climatology
    oyr2 = obs_data.time.dt.year[-1].values

    mod_clim = mod_clim["rho"]
    obs_clim = obs_clim["rho"]


    logger.info("Spatial MLD plot is in process")
    fig, axs = plt.subplots(nrows=1, ncols=2, figsize=(20, 6.5))

    region_title = custom_region(region=region, lat_s=lat_s, lat_n=lat_n, lon_w=lon_w, lon_e=lon_e)
    title = f'Climatology of {time.upper()} mixed layer depth in {region_title}'
    fig.suptitle(title, fontsize=20)
    fig.set_figwidth(18)

    clev1 = 0.0
    # We round up to next hundreth
    # clev2 = max(np.max(mod_clim), np.max(obs_clim))
    clev2 = np.max(obs_clim)

    # print(clev2)
    if clev2 < 200:
        inc = 10
        clev2 = round(int(clev2), -1)
    elif clev2 > 1500:
        inc = 100
        clev2 = round(int(clev2),-2)
    else:
        inc = 50
        clev2 = round(int(clev2), -2)

    nclev = int(clev2/inc)+1
    clev2 = float(clev2)

    cs1 = axs[0].contourf(mod_clim.lon, mod_clim.lat, mod_clim,
                          levels=np.linspace(clev1, clev2, nclev), cmap='jet')
    fig.colorbar(cs1, location="bottom", label='Mixed layer depth (in m)')

    cs1 = axs[1].contourf(obs_clim.lon, obs_clim.lat, obs_clim,
                          levels=np.linspace(clev1, clev2, nclev), cmap='jet')

    fig.colorbar(cs1, location="bottom", label='Mixed layer depth (in m)')


    axs[0].set_title(f"Model climatology {myr1}-{myr2}", fontsize=18)
    axs[0].set_ylabel("Latitude", fontsize=14)
    axs[0].set_xlabel("Longitude", fontsize=14)
    axs[0].set_facecolor('grey')

    axs[1].set_title(f"EN4 OBS climatology {oyr1}-{oyr2}", fontsize=18)
    # axs[1].set_ylabel("Latitude", fontsize=12)
    axs[1].set_xlabel("Longitude", fontsize=14)
    axs[1].set_yticklabels([])
    axs[1].set_facecolor('grey')

    plt.subplots_adjust(top=0.85, wspace=0.1)

    if output:
        filename = file_naming(region, lat_s, lat_n, lon_w, lon_e, plot_name=f"{model}-{exp}-{source}_spatial_MLD_{time}")
        write_data(output_dir,f"{filename}_mod_clim", mod_clim)
        write_data(output_dir,f"{filename}_obs_clim", obs_clim)
        export_fig(output_dir, filename , "pdf", metadata_value = title, loglevel= loglevel)

    plt.show()
    return