from ocean3d.ocean_circulation.mld import data_for_plot_spatial_mld_clim
from ocean3d.ocean_circulation.ocean_circulation import *


def mld_multi_model(o3d_request,
                          overlap=True,
                          loglevel= "WARNING"):
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
    data_dict = o3d_request.get('data_dict')
    # mod_data = o3d_request.get('data')
    # obs_data = o3d_request.get('obs_data')
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

    clim_data_dict = {}
    for i, data_name in enumerate(data_dict):
        data = data_dict[data_name]
        clim_data_name = f"{data_name}_clim"
        data_clim, time = data_for_plot_spatial_mld_clim(data, region, time, lat_s, lat_n, lon_w, lon_e)  # To select the month and compute its climatology
        clim_data_dict[clim_data_name] = data_clim.load()
       
    logger.info("Spatial MLD plot is in process")
    fig, axs = plt.subplots(nrows=3, ncols=2, figsize=(23, 16))
    axs = axs.flatten()
    
    region_title = custom_region(region=region, lat_s=lat_s, lat_n=lat_n, lon_w=lon_w, lon_e=lon_e)
    title = f'Climatology of {time.upper()} mixed layer depth in {region_title}'
    fig.suptitle(title, fontsize=25)
    fig.set_figwidth(18)
    # plt.subplots_adjust(top=1, wspace=0.1)

    max_values_list = []
    for data in clim_data_dict.values():
        max_values_list.append(np.max(data["rho"]))

    clev2 = max(max_values_list)
    clev1 = 0.0


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
    
    # clev1 = 0
    # clev2 = 1500
    # nclev = 200
    levels = np.linspace(clev1, clev2, nclev)




    # levels = np.append(levels, [np.inf])    

    for num, data_name in enumerate(data_dict):
        data_clim = clim_data_dict[f"{data_name}_clim"]["rho"]
        data = data_dict[data_name]
        start_year = data.time.dt.year[0].values
        end_year = data.time.dt.year[-1].values
        
        cs1 = axs[num].contourf(data_clim.lon, data_clim.lat, data_clim,
                            levels = levels, cmap='jet')
        
        axs[num].set_title(f"{data_name} climatology {start_year}-{end_year}", fontsize=18)
        axs[num].set_facecolor('grey')
        if num not in [len(data_dict)-1, len(data_dict)-2]:
            axs[num].set_xticklabels([])
        if num % 2 != 0:
            axs[num].set_yticklabels([])
    cb = fig.colorbar(cs1, ax=axs, location="bottom", pad=0.05, aspect=40, label='Mixed layer depth (in m)')
        
        
    # cs1 = axs[0,0].contourf(mod_data_clim.lon, mod_data_clim.lat, mod_data_clim,
    #                     levels=np.linspace(clev1, clev2, nclev), cmap='jet')
    # fig.colorbar(cs1, location="bottom", label='Mixed layer depth (in m)')

    # cs1 = axs[0,1].contourf(obs_data_clim.lon, obs_data_clim.lat, obs_data_clim,
    #                     levels=np.linspace(clev1, clev2, nclev), cmap='jet')

    # fig.colorbar(cs1, location="bottom", label='Mixed layer depth (in m)')


    # axs[0,0].set_ylabel("Latitude", fontsize=14)
    # axs[0,0].set_xlabel("Longitude", fontsize=14)

    # axs[0,1].set_title(f"EN4 OBS climatology {oyr1}-{oyr2}", fontsize=18)
    # # axs[1].set_ylabel("Latitude", fontsize=12)
    # axs[0,1].set_xlabel("Longitude", fontsize=14)
    # axs[0,1].set_yticklabels([])
    # axs[0,1].set_facecolor('grey')






    # if output:
    #     filename = file_naming(region, lat_s, lat_n, lon_w, lon_e, plot_name=f"{model}-{exp}-{source}_spatial_MLD_{time}")
    #     write_data(output_dir,f"{filename}_mod_clim", mod_data_clim)
    #     write_data(output_dir,f"{filename}_obs_clim", obs_data_clim)
    #     export_fig(output_dir, filename , "pdf", metadata_value = title, loglevel= loglevel)

    plt.show()
    return