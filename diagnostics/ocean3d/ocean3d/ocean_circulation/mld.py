from .ocean_circulation import *

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
    loglevel = o3d_request.get('loglevel')
    logger = log_configure(loglevel, 'plot_spatial_mld')


    if overlap:
        if obs_data:
            obs_data = crop_obs_overlap_time(mod_data, obs_data)
            mod_data = crop_obs_overlap_time(obs_data, mod_data)
            logger.debug("cropped the overlapped time of the model and obs")
    logger.debug("Processing Model")
    mod_clim, time = prepare_data_for_stratification_plot(mod_data, region, time,
                                                    lat_s, lat_n, lon_w, lon_e,
                                                    areamean= False, timemean= True,
                                                    compute_mld= True, loglevel= loglevel)  # To select the month and compute its climatology
    
    if obs_data:
        logger.debug("Processing Observation for MLD")
        obs_clim, time = prepare_data_for_stratification_plot(obs_data, region, time,
                                                        lat_s, lat_n, lon_w, lon_e, areamean= False,
                                                        timemean= True, compute_mld= True, loglevel= loglevel )  # To select the month and compute its climatology
        # obs_clim = obs_clim.chunk({"lev": 23, "lat": 13, "lon": 17}).compute()
        # obs_clim = obs_clim["mld"]
        logger.debug("Processing done for Observation")

    myr1 = mod_clim.attrs["start_year"].astype('datetime64[Y]').astype(str)
    myr2 = mod_clim.attrs["end_year"].astype('datetime64[Y]').astype(str)

    if obs_data:
        oyr1 = obs_clim.attrs["start_year"].astype('datetime64[Y]').astype(str)
        oyr2 = obs_clim.attrs["end_year"].astype('datetime64[Y]').astype(str)

    logger.debug("Loading MLD into memory before plotting")
    logger.debug("Loading Model")
    mod_clim = mod_clim["mld"].compute()
    logger.debug("Loaded Model")
    
    if obs_data:
        logger.debug("Loading Observation")
        obs_clim = obs_clim["mld"].compute()
        logger.debug("Loaded Observation")
    logger.debug("Data loaded into memory")
    
    
    logger.debug("Processing done for Model")

    # mod_clim = mod_clim["rho"]
    # if obs_data:
    #     obs_clim = obs_clim["rho"]


    logger.debug("Spatial MLD plot is in process")
    fig, axs = plt.subplots(nrows=1, ncols=2, figsize=(20, 6.5))

    region_title = custom_region(region=region, lat_s=lat_s, lat_n=lat_n, lon_w=lon_w, lon_e=lon_e)
    title = f'Climatology of {time.upper()} mixed layer depth in {region_title}'
    fig.suptitle(title, fontsize=20)
    fig.set_figwidth(18)

    clev1 = 0.0
    # We round up to next hundreth
    # clev2 = max(np.max(mod_clim), np.max(obs_clim))
    if obs_data:
        clev2 = obs_clim.max()
        
    else: 
        clev2 = mod_clim.max()

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
    if obs_data:
        cs1 = axs[1].contourf(obs_clim.lon, obs_clim.lat, obs_clim,
                            levels=np.linspace(clev1, clev2, nclev), cmap='jet')

        fig.colorbar(cs1, location="bottom", label='Mixed layer depth (in m)')


    axs[0].set_title(f"Model climatology {myr1}-{myr2}", fontsize=18)
    axs[0].set_ylabel("Latitude", fontsize=14)
    axs[0].set_xlabel("Longitude", fontsize=14)
    axs[0].set_facecolor('grey')
    if obs_data:

        axs[1].set_title(f"EN4 OBS climatology {oyr1}-{oyr2}", fontsize=18)
        # axs[1].set_ylabel("Latitude", fontsize=12)
        axs[1].set_xlabel("Longitude", fontsize=14)
        axs[1].set_yticklabels([])
        axs[1].set_facecolor('grey')
    else:
        axs[1].set_title(f"Observation data not available", fontsize=18)
        

    plt.subplots_adjust(top=0.85, wspace=0.1)

    if output:
        filename = file_naming(region, lat_s, lat_n, lon_w, lon_e, plot_name=f"{model}-{exp}-{source}_spatial_MLD_{time}")
        write_data(output_dir,f"{filename}_mod_clim", mod_clim)
        if obs_data:
            write_data(output_dir,f"{filename}_obs_clim", obs_clim)
        export_fig(output_dir, filename , "pdf", metadata_value = title, loglevel= loglevel)

    plt.show()
    return