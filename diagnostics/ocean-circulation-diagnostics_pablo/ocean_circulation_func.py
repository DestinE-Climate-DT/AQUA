import datetime
import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings("ignore")



def weighted_area_mean(data, latN: float, latS: float, lonW: float, lonE: float):
    """
    Compute the weighted area mean of data within the specified latitude and longitude bounds.

    Parameters:
        data (xarray.Dataset): Input data.
        latN (float): Northern latitude bound.
        latS (float): Southern latitude bound.
        lonW (float): Western longitude bound.
        lonE (float): Eastern longitude bound.

    Returns:
        xarray.Dataset: Weighted area mean of the input data.
    """
    data = data.sel(lat=slice(latN, latS), lon=slice(lonW, lonE))
    weighted_data = data.weighted(np.cos(np.deg2rad(data.lat)))
    wgted_mean = weighted_data.mean(("lat", "lon"))
    return wgted_mean



def mean_value_plot(data, title):
    # Calculate weighted area mean
    data = fn.weighted_area_mean(data, -90, 90, 0, 360)

    # Create subplots
    fig, (ax1, ax2) = plt.subplots(nrows=1, ncols=2, figsize=(14, 5))

    # Set the title
    fig.suptitle(title, fontsize=16)

    # Define the levels for plotting
    levels = [0, 100, 500, 1000, 2000, 3000, 4000, 5000]

    # Plot data for each level
    for level in levels:
        if level != 0:
            data_level = data.sel(lev=slice(None, level)).isel(lev=-1)
        else:
            data_level = data.isel(lev=0)

        # Plot temperature
        data_level.ocpt.plot.line(ax=ax1)

        # Plot salinity
        data_level.so.plot.line(ax=ax2)

    # Set properties for the temperature subplot
    ax1.set_title("Temperature", fontsize=14)
    ax1.set_ylabel("Standardized Units (at the respective level)", fontsize=12)
    ax1.set_xlabel("Time (in years)", fontsize=12)
    ax1.legend(["0", "100", "500", "1000", "2000", "3000", "4000", "5000"], loc="best")

    # Set properties for the salinity subplot
    ax2.set_title("Salinity", fontsize=14)
    ax2.set_ylabel("Standardized Units (at the respective level)", fontsize=12)
    ax2.set_xlabel("Time (in years)", fontsize=12)
    ax2.legend(["0", "100", "500", "1000", "2000", "3000", "4000", "5000"], loc="best")

    # Adjust the layout and display the plot
    plt.tight_layout()
    plt.show()

    # Return the last value of data_level
    return data_level




def std_anom_wrt_initial(data, latN: float, latS: float, lonW: float, lonE: float):
    """
    Calculate the standard anomaly of input data relative to the initial time step.

    Args:
        data (DataArray): Input data to be processed.
        latN (float): North latitude.
        latS (float): South latitude.
        lonW (float): West longitude.
        lonE (float): East longitude.

    Returns:
        DataArray: Standard anomaly of the input data.
    """
    # Create an empty dataset to store the results
    std_anomaly = xr.Dataset()

    # Compute the weighted area mean over the specified latitude and longitude range
    wgted_mean = weighted_area_mean(data, latN, latS, lonW, lonE)

    # Calculate the anomaly from the initial time step for each variable
    for var in list(data.data_vars.keys()):
        anomaly_from_initial = wgted_mean[var] - wgted_mean[var][0,]
        
        # Calculate the standard anomaly by dividing the anomaly by its standard deviation along the time dimension
        std_anomaly[var] = anomaly_from_initial / anomaly_from_initial.std("time")

    return std_anomaly


def std_anom_wrt_time_mean(data, latN: float, latS: float, lonW: float, lonE: float):
    """
    Calculate the standard anomaly of input data relative to the time mean.

    Args:
        data (DataArray): Input data to be processed.
        latN (float): North latitude.
        latS (float): South latitude.
        lonW (float): West longitude.
        lonE (float): East longitude.

    Returns:
        DataArray: Standard anomaly of the input data.
    """
    # Create an empty dataset to store the results
    std_anomaly = xr.Dataset()

    # Compute the weighted area mean over the specified latitude and longitude range
    wgted_mean = weighted_area_mean(data, latN, latS, lonW, lonE)

    # Calculate the anomaly from the time mean for each variable
    for var in list(data.data_vars.keys()):
        anomaly_from_time_mean = wgted_mean[var] - wgted_mean[var].mean("time")
        
        # Calculate the standard anomaly by dividing the anomaly by its standard deviation along the time dimension
        std_anomaly[var] = anomaly_from_time_mean / anomaly_from_time_mean.std("time")

    return std_anomaly



def ocpt_so_anom_plot(data, title):
    """
    Create a Hovmoller plot of temperature and salinity anomalies.

    Args:
        data (DataArray): Input data containing ocean potential temperature (ocpt) and practical salinity (so).
        title (str): Title of the plot.

    Returns:
        None
    """
    # Create subplots for temperature and salinity plots
    fig, (ax1, ax2) = plt.subplots(nrows=1, ncols=2, figsize=(14, 5))
    fig.suptitle(title, fontsize=16)

    # Extract temperature data and plot the contour filled plot
    tgt = data.ocpt.transpose()
    tgt.plot.contourf(levels=12, ax=ax1)

    # Add contour lines with black color and set the line width
    tgt.plot.contour(colors="black", levels=12, linewidths=0.5, ax=ax1)

    # Set the title, y-axis label, and x-axis label for the temperature plot
    ax1.set_title("Temperature", fontsize=14)
    ax1.set_ylim((5500, 0))
    ax1.set_ylabel("Depth (in m)", fontsize=12)
    ax1.set_xlabel("Time (in years)", fontsize=12)

    # Extract salinity data and plot the contour filled plot
    sgt = data.so.transpose()
    sgt.plot.contourf(levels=12, ax=ax2)

    # Add contour lines with black color and set the line width
    sgt.plot.contour(colors="black", levels=12, linewidths=0.5, ax=ax2)

    # Set the title, y-axis label, and x-axis label for the salinity plot
    ax2.set_title("Salinity", fontsize=14)
    ax2.set_ylim((5500, 0))
    ax2.set_ylabel("Depth (in m)", fontsize=12)
    ax2.set_xlabel("Time (in years)", fontsize=12)

    # Return the plot
    return



def time_series(data, title):
    """
    Create time series plots of global temperature and salinity standardised anomalies at selected levels.

    Args:
        data (DataArray): Input data containing ocean potential temperature (ocpt) and practical salinity (so).
        title (str): Title of the plots.

    Returns:
        None
    """
    # Create subplots for temperature and salinity time series plots
    fig, (ax1, ax2) = plt.subplots(nrows=1, ncols=2, figsize=(14, 5))

    fig.suptitle(title, fontsize=16)

    # Define the levels at which to plot the time series
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
        data_level.ocpt.plot.line(ax=ax1)

        # Plot the salinity time series
        data_level.so.plot.line(ax=ax2)

    # Set the title, y-axis label, and x-axis label for the temperature plot
    ax1.set_title("Temperature", fontsize=14)
    ax1.set_ylabel("Standardised Units (at the respective level)", fontsize=12)
    ax1.set_xlabel("Time (in years)", fontsize=12)
    ax1.legend(["0", "100", "500", "1000", "2000", "3000", "4000", "5000"], loc='best')

    # Set the title, y-axis label, and x-axis label for the salinity plot
    ax2.set_title("Salinity", fontsize=14)
    ax2.set_ylabel("Standardised Units (at the respective level)", fontsize=12)
    ax2.set_xlabel("Time (in years)", fontsize=12)
    plt.legend(["0", "100", "500", "1000", "2000", "3000", "4000", "5000"], loc='best')

    # Return the plot
    return



def convert_so(so):
    """
    Convert practical salinity to absolute.

    Parameters
    ----------
    so: dask.array.core.Array
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
    return so / 0.99530670233846


def convert_ocpt(absso, ocpt):
    """
    convert potential temperature to conservative temperature
    
    Parameters
    ----------
    absso: dask.array.core.Array
        Masked array containing the absolute salinity values.
    ocpt: dask.array.core.Array
        Masked array containing the potential temperature values (degC).

    Returns
    -------
    occt: dask.array.core.Array
        Masked array containing the conservative temperature values (degC).

    Note
    ----
    http://www.teos-10.org/pubs/gsw/html/gsw_CT_from_pt.html

    """
    x = np.sqrt(0.0248826675584615*absso)
    y = ocpt*0.025e0
    enthalpy = 61.01362420681071e0 + y*(168776.46138048015e0 +
        y*(-2735.2785605119625e0 + y*(2574.2164453821433e0 +
        y*(-1536.6644434977543e0 + y*(545.7340497931629e0 +
        (-50.91091728474331e0 - 18.30489878927802e0*y)*
        y))))) + x**2*(268.5520265845071e0 + y*(-12019.028203559312e0 +
        y*(3734.858026725145e0 + y*(-2046.7671145057618e0 +
        y*(465.28655623826234e0 + (-0.6370820302376359e0 -
        10.650848542359153e0*y)*y)))) +
        x*(937.2099110620707e0 + y*(588.1802812170108e0+
        y*(248.39476522971285e0 + (-3.871557904936333e0-
        2.6268019854268356e0*y)*y)) +
        x*(-1687.914374187449e0 + x*(246.9598888781377e0 +
        x*(123.59576582457964e0 - 48.5891069025409e0*x)) +
        y*(936.3206544460336e0 +
        y*(-942.7827304544439e0 + y*(369.4389437509002e0 +
        (-33.83664947895248e0 - 9.987880382780322e0*y)*y))))))

    return enthalpy/3991.86795711963


def compute_rho(absso, occt, ref_pressure):
    """
    Computes the potential density in-situ.

    Parameters
    ----------
    absso: dask.array.core.Array
        Masked array containing the absolute salinity values (g/kg).
    occt: dask.array.core.Array
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
    # reduced variables
    SAu = 40.*35.16504/35.
    CTu = 40.
    Zu = 1e4
    deltaS = 32.
    ss = np.sqrt((absso+deltaS)/SAu)
    tt = occt / CTu
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


    
def convert_variables(data):
    """
    Convert variables in the given dataset to absolute salinity, conservative temperature, and potential density.

    Parameters
    ----------
    data : xarray.Dataset
        Dataset containing the variables to be converted.

    Returns
    -------
    converted_data : xarray.Dataset
        Dataset containing the converted variables: absolute salinity (absso), conservative temperature (ocpt),
        and potential density (rho) at reference pressure 0 dbar.

    """
    converted_data = xr.Dataset()

    # Convert practical salinity to absolute salinity
    absso = convert_so(data.so)

    # Convert potential temperature to conservative temperature
    occt = convert_ocpt(absso, data.ocpt)

    # Compute potential density in-situ at reference pressure 0 dbar
    rho = compute_rho(absso, occt, 0)

    # Merge the density variable with so and ocpt into a new dataset
    converted_data = converted_data.merge({"so": data.so, "ocpt": data.ocpt, "rho": rho})

    return converted_data




def plot_strat_2halves(datamod, dataobs, area_name):
    """
    Plot the mean state annual temperature, salinity, and density stratification splitting the temporal window in 2 halves 
    to identified potential changes in stratification

    Parameters
    ----------
    datamod : xarray.Dataset
        Model dataset containing inputs of potential temperature (ocpt), practical salinity (so), and density (rho).
    dataobs : xarray.Dataset
        Obs dataset containing inputs of potential temperature (ocpt), practical salinity (so), and density (rho)
    area_name : str
        Name of the area for the plot title.

    Returns
    -------
    None

    """
    date_len = len(data.time)
    if date_len != 1:
        if date_len % 2 == 0:
            data_1 = data.isel(time=slice(0, int(date_len/2)))
            data_2 = data.isel(time=slice(int(date_len/2), date_len))
        else:
            data_1 = data.isel(time=slice(0, int((date_len-1)/2)))
            data_2 = data.isel(time=slice(int((date_len-1)/2), date_len))

    fig, (ax1, ax2, ax3) = plt.subplots(nrows=1, ncols=3, figsize=(14, 8))
    fig.suptitle(f"Mean state annual T, S, rho0 stratification in {area_name}", fontsize=16)

    ax1.set_ylim((4500, 0))
    ax1.plot(data_1.ocpt.mean("time"), data.lev, 'g-', linewidth=2.0)
    ax1.plot(data_2.ocpt.mean("time"), data.lev, 'b-', linewidth=2.0)
    ax1.set_title("Temperature Profile", fontsize=14)
    ax1.set_ylabel("Depth (m)", fontsize=12)
    ax1.set_xlabel("Temperature (°C)", fontsize=12)
    ax1.legend([f"EXP first half {data_1.time[0].dt.year.data}-{data_1.time[-1].dt.year.data}",
                f"EXP last half {data_2.time[0].dt.year.data}-{data_2.time[-1].dt.year.data}",
                "EN4 1950-1980", "EN4 1990-2020"], loc='best')

    ax2.set_ylim((4500, 0))
    ax2.plot(data_1.so.mean("time"), data.lev, 'g-', linewidth=2.0)
    ax2.plot(data_2.so.mean("time"), data.lev, 'b-', linewidth=2.0)
    ax2.set_title("Salinity Profile", fontsize=14)
    ax2.set_xlabel("Salinity (psu)", fontsize=12)

    ax3.set_ylim((4500, 0))
    ax3.plot(data_1.rho.mean("time")-1000, data.lev, 'g-', linewidth=2.0)
    ax3.plot(data_2.rho.mean("time")-1000, data.lev, 'b-', linewidth=2.0)
    ax3.set_title("Rho (ref 0) Profile", fontsize=14)
    ax3.set_xlabel("Density Anomaly (kg/m³)", fontsize=12)

    plt.show()


def compute_mld_monthly(rho):
    """To compute the mixed layer depth from monthly density fields in discrete levels
    Parameters
    ----------
    rho : xarray.DataArray for sigma0, dims must be time, space, depth (must be in metres)
    Returns
    -------
    mld: xarray.DataArray, dims of time, space
    
      This function developed by Dhruv Balweda, Andrew Pauling, Sarah Ragen, Lettie Roach
      
    """
    mld=rho
    
    # Here we identify the last level before 10m
    slevs=rho.lev
    ilev0=0
    slevs
    for ilev in range(len(slevs)):   
     tlev = slevs[ilev]
     if tlev<= 10: slev10=ilev

    #  We take the last level before 10m  as our sigma0 surface reference

    surf_ref = rho[:,slev10]

    # We compute the density difference between surface and whole field
    dens_diff = rho-surf_ref
        
    
    # keep density differences exceeding threshold, discard other values
    dens_diff = dens_diff.where(dens_diff > 0.03)   ### The threshold to exit the MLD is 0.03 kg/m3

    # We determine the level at which the threshold is exceeded by the minimum margin
    cutoff_lev=dens_diff.lev.where(dens_diff==dens_diff.min(["lev"])).max(["lev"])        
    mld=cutoff_lev.rename("mld")

    
    # compute water depth
    # note: pressure.lev, cthetao.lev, and abs_salinity.lev are identical
#    test = sigma0.isel(time=0) + sigma0.lev
#    bottom_depth = (
#        pressure.lev.where(test == test.max(dim="lev"))
#        .max(dim="lev")
#        .rename("bottom_depth")
#    )  # units 'meters'

    # set MLD to water depth where MLD is NaN
#    mld = mld.where(~np.isnan(mld), bottom_depth)

    return mld


def compute_mld_cont_monthly(rho):
    """To compute the mixed layer depth from monthly density fields in continuous levels

    Parameters
    ----------
    rho : xarray.DataArray for sigma0, dims must be time, space, depth (must be in metres)
    Returns
    -------
    mld: xarray.DataArray, dims of time, space
    
      This function developed by Dhruv Balweda, Andrew Pauling, Sarah Ragen, Lettie Roach
      
    """
    mld=rho
    
    # Here we identify the last level before 10m
    slevs=rho.lev
    ilev0=0
    slevs
    for ilev in range(len(slevs)):   
     tlev = slevs[ilev]
     if tlev<= 10: slev10=ilev

    #  We take the density at 10m as the mean of the upper and lower level around 10 m

    surf_ref = (rho[:,slev10]+rho[:,slev10+1])/2
    #print(surf_ref.values)

    # We compute the density difference between surface and whole field
    dens_diff = rho-surf_ref
        
    
    # keep density differences exceeding threshold, discard other values
    dens_diff = dens_diff.where(dens_diff > 0.03)   ### The threshold to exit the MLD is 0.03 kg/m3
    
    # We determine the level at which the threshold is exceeded by the minimum margin
 #   print(dens_diff.values)
 #   cutoff1=dens_diff.lev.where(dens_diff==dens_diff.min(["lev"]))
 #   print(cutoff1.values)
  
    cutoff_lev=dens_diff.lev.where(dens_diff==dens_diff.min(["lev"])).max(["lev"])
 #   print(cutoff_lev.values) 
    mld=cutoff_lev.rename("mld")

    
    # compute water depth
    # note: pressure.lev, cthetao.lev, and abs_salinity.lev are identical
#    test = sigma0.isel(time=0) + sigma0.lev
#    bottom_depth = (
#        pressure.lev.where(test == test.max(dim="lev"))
#        .max(dim="lev")
#        .rename("bottom_depth")
#    )  # units 'meters'

    # set MLD to water depth where MLD is NaN
#    mld = mld.where(~np.isnan(mld), bottom_depth)

    return mld


