"""
Ocean Circulation module
"""

import warnings
import numpy as np
import xarray as xr
import matplotlib.pyplot as plt
from ocean3d import weighted_area_mean
from ocean3d import area_selection
from ocean3d import data_time_selection
from ocean3d import load_obs_data
from ocean3d import crop_obs_overlap_time
from ocean3d import compare_arrays
from ocean3d import dir_creation
from ocean3d import custom_region
from ocean3d import write_data
from aqua.logger import log_configure

def convert_so(avg_so, loglevel= "WARNING"):
    """
    Convert practical salinity to absolute.

    Parameters
    ----------
    avg_so: dask.array.core.Array
        Masked array containing the practical salinity values (psu or 0.001).

    Returns
    -------
    absso: dask.array.core.Array
        Masked array containing the absolute salinity values (g/kg).

    Note
    ----
    This function use an approximation from TEOS-10 equations and could
    lead to different values in particular in the Baltic Seas.
    http://www.teos-10.org/pubs/gsw/pdf/SA_from_SP.pdf

    """
    logger = log_configure(loglevel, 'convert_so')
    return avg_so / 0.99530670233846


def convert_avg_thetao(absso, avg_thetao, loglevel= "WARNING"):
    """
    convert potential temperature to conservative temperature

    Parameters
    ----------
    absso: dask.array.core.Array
        Masked array containing the absolute salinity values.
    avg_thetao: dask.array.core.Array
        Masked array containing the potential temperature values (degC).

    Returns
    -------
    bigavg_thetao: dask.array.core.Array
        Masked array containing the conservative temperature values (degC).

    Note
    ----
    http://www.teos-10.org/pubs/gsw/html/gsw_CT_from_pt.html

    """
    logger = log_configure(loglevel, 'convert_avg_thetao')
    x = np.sqrt(0.0248826675584615*absso)
    y = avg_thetao*0.025e0
    enthalpy = 61.01362420681071e0 + y*(168776.46138048015e0 +
                                        y*(-2735.2785605119625e0 + y*(2574.2164453821433e0 +
                                                                      y*(-1536.6644434977543e0 + y*(545.7340497931629e0 +
                                                                                                    (-50.91091728474331e0 - 18.30489878927802e0*y) *
                                                                                                    y))))) + x**2*(268.5520265845071e0 + y*(-12019.028203559312e0 +
                                                                                                                                            y*(3734.858026725145e0 + y*(-2046.7671145057618e0 +
                                                                                                                                                                        y*(465.28655623826234e0 + (-0.6370820302376359e0 -
                                                                                                                                                                                                   10.650848542359153e0*y)*y)))) +
                                                                                                                   x*(937.2099110620707e0 + y*(588.1802812170108e0 +
                                                                                                                                               y*(248.39476522971285e0 + (-3.871557904936333e0 -
                                                                                                                                                                          2.6268019854268356e0*y)*y)) +
                                                                                                                      x*(-1687.914374187449e0 + x*(246.9598888781377e0 +
                                                                                                                                                   x*(123.59576582457964e0 - 48.5891069025409e0*x)) +
                                                                                                                         y*(936.3206544460336e0 +
                                                                                                                            y*(-942.7827304544439e0 + y*(369.4389437509002e0 +
                                                                                                                                                         (-33.83664947895248e0 - 9.987880382780322e0*y)*y))))))

    return enthalpy/3991.86795711963


def compute_rho(absso, bigavg_thetao, ref_pressure, loglevel= "WARNING"):
    """
    Computes the potential density in-situ.

    Parameters
    ----------
    absso: dask.array.core.Array
        Masked array containing the absolute salinity values (g/kg).
    bigavg_thetao: dask.array.core.Array
        Masked array containing the conservative temperature values (degC).
    ref_pressure: float
        Reference pressure (dbar).

    Returns
    -------
    rho: dask.array.core.Array
        Masked array containing the potential density in-situ values (kg m-3).

    Note
    ----
    https://github.com/fabien-roquet/polyTEOS/blob/36b9aef6cd2755823b5d3a7349cfe64a6823a73e/polyTEOS10.py#L57

    """
    logger = log_configure(loglevel, 'compute_rho')
    # reduced variables
    SAu = 40.*35.16504/35.
    CTu = 40.
    Zu = 1e4
    deltaS = 32.
    ss = np.sqrt((absso+deltaS)/SAu)
    tt = bigavg_thetao / CTu
    pp = ref_pressure / Zu

    # vertical reference profile of density
    R00 = 4.6494977072e+01
    R01 = -5.2099962525e+00
    R02 = 2.2601900708e-01
    R03 = 6.4326772569e-02
    R04 = 1.5616995503e-02
    R05 = -1.7243708991e-03
    r0 = (((((R05*pp + R04)*pp + R03)*pp + R02)*pp + R01)*pp + R00)*pp

    # density anomaly
    R000 = 8.0189615746e+02
    R100 = 8.6672408165e+02
    R200 = -1.7864682637e+03
    R300 = 2.0375295546e+03
    R400 = -1.2849161071e+03
    R500 = 4.3227585684e+02
    R600 = -6.0579916612e+01
    R010 = 2.6010145068e+01
    R110 = -6.5281885265e+01
    R210 = 8.1770425108e+01
    R310 = -5.6888046321e+01
    R410 = 1.7681814114e+01
    R510 = -1.9193502195e+00
    R020 = -3.7074170417e+01
    R120 = 6.1548258127e+01
    R220 = -6.0362551501e+01
    R320 = 2.9130021253e+01
    R420 = -5.4723692739e+00
    R030 = 2.1661789529e+01
    R130 = -3.3449108469e+01
    R230 = 1.9717078466e+01
    R330 = -3.1742946532e+00
    R040 = -8.3627885467e+00
    R140 = 1.1311538584e+01
    R240 = -5.3563304045e+00
    R050 = 5.4048723791e-01
    R150 = 4.8169980163e-01
    R060 = -1.9083568888e-01
    R001 = 1.9681925209e+01
    R101 = -4.2549998214e+01
    R201 = 5.0774768218e+01
    R301 = -3.0938076334e+01
    R401 = 6.6051753097e+00
    R011 = -1.3336301113e+01
    R111 = -4.4870114575e+00
    R211 = 5.0042598061e+00
    R311 = -6.5399043664e-01
    R021 = 6.7080479603e+0
    R121 = 3.5063081279e+00
    R221 = -1.8795372996e+00
    R031 = -2.4649669534e+00
    R131 = -5.5077101279e-01
    R041 = 5.5927935970e-01
    R002 = 2.0660924175e+00
    R102 = -4.9527603989e+00
    R202 = 2.5019633244e+00
    R012 = 2.0564311499e+00
    R112 = -2.1311365518e-01
    R022 = -1.2419983026e+00
    R003 = -2.3342758797e-02
    R103 = -1.8507636718e-02
    R013 = 3.7969820455e-01

    rz3 = R013*tt + R103*ss + R003
    rz2 = (R022*tt+R112*ss+R012)*tt+(R202*ss+R102)*ss+R002
    rz1 = (((R041*tt+R131*ss+R031)*tt +
            (R221*ss+R121)*ss+R021)*tt +
           ((R311*ss+R211)*ss+R111)*ss+R011)*tt + \
        (((R401*ss+R301)*ss+R201)*ss+R101)*ss+R001
    rz0 = (((((R060*tt+R150*ss+R050)*tt +
              (R240*ss+R140)*ss+R040)*tt +
             ((R330*ss+R230)*ss+R130)*ss+R030)*tt +
            (((R420*ss+R320)*ss+R220)*ss+R120)*ss+R020)*tt +
           ((((R510*ss+R410)*ss+R310)*ss+R210)*ss+R110)*ss+R010)*tt + \
        (((((R600*ss+R500)*ss+R400)*ss+R300)*ss+R200)*ss+R100)*ss+R000
    r = ((rz3*pp + rz2)*pp + rz1)*pp + rz0

    # in-situ density
    return r + r0


def convert_variables(data, loglevel= "WARNING"):
    """
    Convert variables in the given dataset to absolute salinity,
    conservative temperature, and potential density.

    Parameters
    ----------
    data : xarray.Dataset
        Dataset containing the variables to be converted.

    Returns
    -------
    converted_data : xarray.Dataset
        Dataset containing the converted variables: absolute salinity (avg_so),
        conservative temperature (avg_thetao),
        and potential density (rho) at reference pressure 0 dbar.

    """
    logger = log_configure(loglevel, 'convert_variables')
    converted_data = xr.Dataset()

    # Convert practical salinity to absolute salinity
    absso = convert_so(data.avg_so)
    logger.debug("Practical salinity converted to absolute salinity")
    # Convert potential temperature to conservative temperature
    avg_thetao = convert_avg_thetao(absso, data.avg_thetao)
    logger.debug("Potential temperature converted to conservative temperature")
    # Compute potential density in-situ at reference pressure 0 dbar
    rho = compute_rho(absso, avg_thetao, 0)
    logger.debug(
        "Calculated potential density in-situ at reference pressure 0 dbar ")
    # Merge the converted variables into a new dataset
    converted_data = converted_data.merge(
        {"avg_thetao": avg_thetao, "avg_so": absso, "rho": rho})

    return converted_data


def prepare_data_for_stratification_plot(data, region=None, time=None, lat_s: float = None, lat_n: float = None, lon_w: float = None,
                                         lon_e: float = None, loglevel= "WARNING"):
    """
    Prepare data for plotting stratification profiles.

    Parameters:
        data (xarray.Dataset): Input data containing temperature (avg_thetao) and salinity (avg_so).
        region (str, optional): Region name.
        time (str or list, optional): Time period to select. Can be a single date or a list of start and end dates.
        lat_s (float, optional): Southern latitude bound. Required if region is not provided or None.
        lat_n (float, optional): Northern latitude bound. Required if region is not provided or None.
        lon_w (float, optional): Western longitude bound. Required if region is not provided or None.
        lon_e (float, optional): Eastern longitude bound. Required if region is not provided or None.

    Returns:
        xarray.Dataset: Prepared data for plotting stratification profiles.
    """
    logger = log_configure(loglevel, 'prepare_data_for_stratification_plot')
    data = weighted_area_mean(data, region, lat_s, lat_n, lon_w, lon_e)
    data = convert_variables(data)
    data_rho = data["rho"] - 1000
    data["rho"] = data_rho
    data, time = data_time_selection(data, time)
    return data, time


def plot_stratification(o3d_request,time=None, loglevel= "WARNING"):
    """
    Create a stratification plot showing the mean state temperature, salinity, and density profiles.

    Parameters:
        mod_data (xarray.Dataset): Model data.
        region (str): Region name.
        time (str): Time selection for data (e.g., 'yearly', '3M', 'Jan', 'Feb', etc.).
        lat_s (float): Southern latitude bound.
        lat_n (float): Northern latitude bound.
        lon_w (float): Western longitude bound.
        lon_e (float): Eastern longitude bound.
        output (bool): Flag indicating whether to save the output.
        output_dir (str): Directory path for saving the output.

    Returns:
        None
    """
    logger = log_configure(loglevel, 'plot_stratification')
    mod_data = o3d_request.get('data')
    model = o3d_request.get('model')
    exp = o3d_request.get('exp')
    source = o3d_request.get('source')
    obs_data = o3d_request.get('obs_data')
    region = o3d_request.get('region', None)
    lat_s = o3d_request.get('lat_s', None)
    lat_n = o3d_request.get('lat_n', None)
    lon_w = o3d_request.get('lon_w', None)
    lon_e = o3d_request.get('lon_e', None)
    output = o3d_request.get('output')
    output_dir = o3d_request.get('output_dir')
    
    obs_data = crop_obs_overlap_time(mod_data, obs_data)

    obs_data, time = prepare_data_for_stratification_plot(
        obs_data, region, time, lat_s, lat_n, lon_w, lon_e)
    mod_data, time = prepare_data_for_stratification_plot(
        mod_data, region, time, lat_s, lat_n, lon_w, lon_e)
    mod_data_list, obs_data = compare_arrays(mod_data, obs_data)

    mod_data_list = list(
        filter(lambda value: value is not None, mod_data_list))

    fig, axs = plt.subplots(nrows=1, ncols=3, figsize=(14, 8))
    logger.info("Stratification plot is in process")

    if output:
        output_path, fig_dir, data_dir, filename = dir_creation(mod_data,
             region, lat_s, lat_n, lon_w, lon_e, output_dir, plot_name=f"stratification_{time}_clim")
        filename = f"{model}_{exp}_{source}_{filename}"
        
    legend_list = []
    if time in ["Yearly"]:
        start_year = mod_data_list[0].time[0].data
        end_year = mod_data_list[0].time[-1].data
        logger.debug(end_year)
    else:
        start_year = mod_data_list[0].time[0].dt.year.data
        end_year = mod_data_list[0].time[-1].dt.year.data

    for i, var in zip(range(len(axs)), ["avg_thetao", "avg_so", "rho"]):
        axs[i].set_ylim((4500, 0))
        data_1 = mod_data_list[0][var].mean("time")

        axs[i].plot(data_1, data_1.lev, 'g-', linewidth=2.0)
        legend_info = f"Model {start_year}-{end_year}"
        legend_list.append(legend_info)
        if output:
            write_data(f'{data_dir}/{filename}_{legend_info.replace(" ","_")}.nc',data_1)

        if len(mod_data_list) > 1:
            data_2 = mod_data_list[1][var].mean("time")
            axs[i].plot(data_2, data_2.lev, 'b-', linewidth=2.0)
            legend_info = f"Model {start_year}-{end_year}"
            legend_list.append(legend_info)
            if output:
                write_data(f'{data_dir}/{filename}_{legend_info.replace(" ","_")}.nc',data_2)

        if obs_data is not None:
            data_3 = obs_data[var].mean("time")
            axs[i].plot(data_3, data_3.lev, 'r-', linewidth=2.0)
            legend_info = f"Obs {start_year}-{end_year}"
            legend_list.append(legend_info)
            if output:
                write_data(f'{data_dir}/{filename}_{legend_info.replace(" ","_")}.nc',data_3)

    region_title = custom_region(region=region, lat_s=lat_s, lat_n=lat_n, lon_w=lon_w, lon_e=lon_e)

    fig.suptitle(
        f"Climatological {time.upper()} T, S and rho0 stratification in {region_title}", fontsize=20)
    axs[0].set_title("Temperature Profile", fontsize=16)
    axs[0].set_ylabel("Depth (m)", fontsize=15)
    axs[0].set_xlabel("Temperature (°C)", fontsize=12)

    axs[1].set_title("Salinity Profile", fontsize=16)
    axs[1].set_xlabel("Salinity (psu)", fontsize=12)
    # axs[1].set_ylabel("", fontsize=0)
    axs[1].set_yticklabels([])

    axs[2].set_title("Rho (ref 0) Profile", fontsize=16)
    axs[2].set_xlabel("Density Anomaly (kg/m³)", fontsize=12)
    # axs[2].set_ylabel("", fontsize=0)
    axs[2].set_yticklabels([])

    axs[0].legend(legend_list, loc='best')

    if output:
        plt.savefig(f"{fig_dir}/{filename}.pdf")
        logger.info(
            "Figure and data used in the plot, saved here : %s", output_path)
    return

def plot_stratification_parallel(o3d_request, loglevel= "WARNING"):
    logger = log_configure(loglevel, 'plot_stratification_parallel')
    import concurrent.futures
    import threading
    lock = threading.Lock()
    
    logger.info("Starting plot_stratification in parallel")
    time_list = [time for time in range(13, 18)]
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(plot_stratification, [o3d_request] * len(time_list), time_list)
    logger.info(" plot_stratification completed")
     
    return

def compute_mld_cont(rho, loglevel= "WARNING"):
    """To compute the mixed layer depth from density fields in continous levels
    using the same criteria as in de Boyer and Montegut (2004). The continuous distribution of MLD
    values is achieved by performing an interpolation between the first level that exceeds the
    threshold and the one immediately after to linearly estimate where the 0.03 value would be reached

    Warning!!: It does not provide reasonable estimates in some instances in which the upper level
    has higher densities than the lower one. This function is therefore not recommended until this
    issue is addressed and corrected

    Parameters
    ----------
    rho : xarray.DataArray for sigma0, dims must be time, space, depth (must be in metres)
    Returns
    -------
    mld: xarray.DataArray, dims of time, space
    """
    logger = log_configure(loglevel, 'compute_mld_cont')
    # Here we identify the first level to represent the surfac
    surf_dens = rho.isel(lev=slice(0, 1)).mean("lev")

    # We compute the density anomaly between surface and whole field
    dens_ano = rho-surf_dens

    # We define now a sigma difference wrt to the threshold of 0.03 kg/m3 in de Boyer Montegut (2004)
    dens_diff = dens_ano-0.03

    # Now we only keep the levels for which the threshold has not been surpassed
    # The threshold in de Boyer Montegut (2004)
    dens_diff2 = dens_diff.where(dens_diff < 0)

    # And keep the deepest one
    cutoff_lev1 = dens_diff2.lev.where(dens_diff2 > -9999).max(["lev"])
    # And the following one (for which the threshold will have been overcome)
    cutoff_lev2 = dens_diff2.lev.where(
        dens_diff2.lev > cutoff_lev1).min(["lev"])

    # Here we identify the last level of the ocean
    depth = rho.lev.where(rho > -9999).max(["lev"])

    ddif = cutoff_lev2-cutoff_lev1
    # The MLD is established by linearly interpolating to the level on which the difference wrt to the reference is zero
    rdif1 = dens_diff.where(dens_diff.lev == cutoff_lev1).max(
        ["lev"])  # rho diff in first lev

    rdif2 = dens_diff.where(dens_diff.lev == cutoff_lev2).max(
        ["lev"])  # rho diff in second lev
    mld = cutoff_lev1+((ddif)*(rdif1))/(rdif1-rdif2)
    # The MLD is set as the maximum depth if the threshold is not exceeded before
    mld = np.fmin(mld, depth)
    # mld=mld.rename("mld")

    return mld


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
    data = compute_mld_cont(data)
    data, time = data_time_selection(data, time)
    return data.mean("time"), time


def plot_spatial_mld_clim(o3d_request, time=None,
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
 
    if output:
        output_path, fig_dir, data_dir, filename = dir_creation(mod_data,
             region, lat_s, lat_n, lon_w, lon_e, output_dir, plot_name=f"spatial_MLD_{time}")

    logger.info("Spatial MLD plot is in process")
    fig, axs = plt.subplots(nrows=1, ncols=2, figsize=(20, 6.5))

    region_title = custom_region(region=region, lat_s=lat_s, lat_n=lat_n, lon_w=lon_w, lon_e=lon_e)

    fig.suptitle(
        f'Climatology of {time.upper()} mixed layer depth in {region_title}', fontsize=20)
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

    filename = f"{model}_{exp}_{source}_{filename}"

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
        plt.savefig(f"{fig_dir}/{filename}.pdf")
        logger.info(
            "Figure and data used for this plot are saved here: %s", output_path)
        mod_clim.to_netcdf(f'{data_dir}/{filename}_Rho.nc')
        obs_clim.to_netcdf(f'{data_dir}/{filename}_Rho.nc')

    plt.show()
    return

def plot_spatial_mld_clim_parallel(o3d_request, loglevel= "WARNING"):
    logger = log_configure(loglevel, 'plot_spatial_mld_clim_parallel')
    import concurrent.futures
    import threading
    logger.info("Starting plot_spatial_mld_clim in parallel")
    time_list = [time for time in range(13, 18)]
    for time in time_list:
        plot_spatial_mld_clim(o3d_request, time= time)
        
    # with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
    #     executor.map(plot_spatial_mld_clim, [o3d_request] * len(time_list), time_list)
    # logger.info(" plot_spatial_mld_clim completed")
     
    return



# def plot_spatial_mld_clim_parallel(o3d_request, max_workers=None, loglevel= "WARNING"):
#    logger = log_configure(loglevel, 'plot_spatial_mld_clim_parallel')
#     import concurrent.futures
#     logger.info("Starting plot_spatial_mld_clim in parallel")
#     time_list = [time for time in range(13, 18)]

#     with concurrent.futures.ProcessPoolExecutor(max_workers=1) as executor:
#         # Submit the tasks
#         futures = [executor.submit(plot_spatial_mld_clim, o3d_request, time) for time in time_list]

#         # Wait for all tasks to complete
#         concurrent.futures.wait(futures)

#     logger.info("plot_spatial_mld_clim completed")
#     return