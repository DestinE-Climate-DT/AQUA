import datetime
import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings("ignore")



def wgt_area_mean(data, latN: float, latS: float,
                  lonW: float, lonE: float):

    data=data.sel(lat=slice(latN,latS), lon=slice(lonW,lonE))
    t3gw=data.weighted(np.cos(np.deg2rad(data.lat)))
    wgted_mean=t3gw.mean(("lat","lon"))
    return wgted_mean

def mean_value_plot(data,title):
    data=fn.wgt_area_mean(data,-90,90,0,360)
    fig, (ax1, ax2) = plt.subplots(nrows=1,ncols=2,
                        figsize=(14,5))

    fig.suptitle(title, fontsize=16)
    levels=[0,100,500,1000,2000,3000,4000,5000]
    for level in levels:
        if level!=0:        
            data_level=data.sel(lev=slice(None, level)).isel(lev=-1) 
        else:
            data_level=data.isel(lev=0)
        data_level.thetao.plot.line(ax=ax1)
        data_level.so.plot.line(ax=ax2)
    ax1.set_title("Temperature", fontsize=14)
    ax1.set_ylabel("Standardised Units (at the respective level)",fontsize=12)
    ax1.set_xlabel("Time (in years)",fontsize=12)
    ax1.legend(["0","100","500","1000","2000","3000","4000","5000"], loc='best')

    ax2.set_title("Salinity", fontsize=14)
    ax2.set_ylabel("Standardised Units (at the respective level)",fontsize=12)
    ax2.set_xlabel("Time (in years)",fontsize=12)
    # ax1.set_legend(["0","100","500","1000","2000","3000","4000","5000"], loc='best')
    plt.legend(["0","100","500","1000","2000","3000","4000","5000"], loc='best')
    # plt.savefig('Timeseries-Global-standardized-anomalies.png',transparent=True)
    return data_level


def std_anom_wrt_initial(data, latN: float, latS: float,
                  lonW: float, lonE: float,):
    std_anomaly = xr.Dataset()
    wgted_mean=wgt_area_mean(data, latN, latS, lonW, lonE)
    for var in list(data.data_vars.keys()):
        anomaly_from_initial=wgted_mean[var]-wgted_mean[var][0,]
        std_anomaly[var]= anomaly_from_initial/anomaly_from_initial.std("time")

    return std_anomaly
def std_anom_wrt_time_mean(data, latN: float, latS: float,
                  lonW: float, lonE: float,):
    std_anomaly = xr.Dataset()
    wgted_mean=wgt_area_mean(data, latN, latS, lonW, lonE)
    for var in list(data.data_vars.keys()):
        anomaly_from_initial=wgted_mean[var]-wgted_mean[var].mean("time")
        std_anomaly[var]= anomaly_from_initial/anomaly_from_initial.std("time")

    return std_anomaly

def thetao_so_anom_plot(data, title):
    # And we perform the corresponding hovmoller plot
    fig, (ax1, ax2) = plt.subplots(nrows=1,ncols=2,
                            figsize=(14,5))
    fig.suptitle(title, fontsize=16)

    tgt=data.thetao.transpose()

    tgt.plot.contourf(levels=12,ax=ax1)
    tgt.plot.contour(colors="black",levels=12,linewidths=0.5,ax=ax1)
    ax1.set_title("Temperature", fontsize=14)
    ax1.set_ylim((5500,0))
    ax1.set_ylabel("Depth (in m)",fontsize=12)
    ax1.set_xlabel("Time (in years)",fontsize=12)

    sgt=data.so.transpose()
    sgt.plot.contourf(levels=12, ax=ax2)
    sgt.plot.contour(colors="black",levels=12,linewidths=0.5, ax=ax2)
    ax2.set_title("Salinity", fontsize=14)
    ax2.set_ylim((5500,0))
    ax2.set_ylabel("Depth (in m)",fontsize=12)
    ax2.set_xlabel("Time (in years)",fontsize=12)

    # plt.savefig('Hovmoller-Global-standardized-anomalies.png',transparent=True)
    return

def time_series(data, title):
    # We now produce timeseries for GLOBAL temperature and salinity standardised anomalies at selected levels
    fig, (ax1, ax2) = plt.subplots(nrows=1,ncols=2,
                            figsize=(14,5))

    fig.suptitle(title, fontsize=16)
    
    levels=[0,100,500,1000,2000,3000,4000,5000]
    for level in levels:
        if level!=0:        
            data_level=data.sel(lev=slice(None, level)).isel(lev=-1) 
        else:
            data_level=data.isel(lev=0)
        data_level.thetao.plot.line(ax=ax1)
        data_level.so.plot.line(ax=ax2)
        
    ax1.set_title("Temperature", fontsize=14)
    ax1.set_ylabel("Standardised Units (at the respective level)",fontsize=12)
    ax1.set_xlabel("Time (in years)",fontsize=12)
    ax1.legend(["0","100","500","1000","2000","3000","4000","5000"], loc='best')

    ax2.set_title("Salinity", fontsize=14)
    ax2.set_ylabel("Standardised Units (at the respective level)",fontsize=12)
    ax2.set_xlabel("Time (in years)",fontsize=12)
    # ax1.set_legend(["0","100","500","1000","2000","3000","4000","5000"], loc='best')
    plt.legend(["0","100","500","1000","2000","3000","4000","5000"], loc='best')
    # plt.savefig('Timeseries-Global-standardized-anomalies.png',transparent=True)
    return





# Here we compute the density values at 0 level. For that we use the Equation of State that requires absolute salinity and conservative temperature as inputs
# First we introduce a function to convert practical salinity to absolute salinity
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

# Here we introduce a function to convert potential temperature to conservative temperature
def convert_thetao(absso, thetao):
    """
    Parameters
    ----------
    absso: dask.array.core.Array
        Masked array containing the absolute salinity values.
    thetao: dask.array.core.Array
        Masked array containing the potential temperature values (degC).

    Returns
    -------
    bigthetao: dask.array.core.Array
        Masked array containing the conservative temperature values (degC).

    Note
    ----
    http://www.teos-10.org/pubs/gsw/html/gsw_CT_from_pt.html

    """
    x = np.sqrt(0.0248826675584615*absso)
    y = thetao*0.025e0
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

    # bigthetao = enthalpy/Specific heat
    return enthalpy/3991.86795711963

# Finally we compute the potential in-situ density
# First we define the associated function
def compute_rho(absso, bigthetao, ref_pressure):
    """
    Computes the potential density in-situ.

    Parameters
    ----------
    absso: dask.array.core.Array
        Masked array containing the absolute salinity values (g/kg).
    bigthetao: dask.array.core.Array
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
    tt = bigthetao / CTu
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
    new_data=xr.Dataset()
    so=convert_so(data.so)
    thetao=convert_thetao(so, data.thetao)
    rho=compute_rho(so, thetao, 0)
    new_data=new_data.merge({"so": so, "thetao": thetao, "rho": rho})
    return new_data
    
    
def plot_temporal_split(data, area_name):
    date_len = len(data.time)
    if date_len !=1:
        if (date_len % 2) ==0:
            data_1= data.isel(time=slice(0, int(date_len/2)))
            data_2= data.isel(time=slice(int(date_len/2),date_len))                
        if (date_len % 2) !=0:
            data_1= data.isel(time=slice(0, int((date_len-1)/2)))
            data_2= data.isel(time=slice(int((date_len-1)/2),date_len))                  

    fig, (ax1, ax2, ax3) = plt.subplots(nrows=1,ncols=3,figsize=(14,8))
    fig.suptitle(f'Mean state annual T, S, rho0 stratification in the {area_name}', fontsize=16)

    ax1.set_ylim((4500,0))

    ax1.plot(data_1.thetao.mean("time"), data.lev,'g-',linewidth=2.0)
    ax1.plot(data_2.thetao.mean("time"), data.lev,'b-',linewidth=2.0)


    # ax1.plot(obsT1_ls_w_av.thetao,obsT1_ls_w_av.thetao.lev,'k:')
    # ax1.plot(obsT2_ls_w_av.thetao,obsT2_ls_w_av.thetao.lev,'k--')

    ax1.set_title("Temperature Profile", fontsize=14)
    ax1.set_ylabel("Depth (in m)",fontsize=12)
    ax1.set_xlabel("Temperature (in degC)",fontsize=12)
    ax1.legend([f"EXP first half {data_1.time[0].dt.year.data}- {data_1.time[-1].dt.year.data}",
                f"EXP last half {data_2.time[0].dt.year.data}- {data_2.time[-1].dt.year.data}","EN4 1950-1980","EN4 1990-2020"], loc='best')


    # Now we plot salinity
    ax2.set_ylim((4500,0))

    ax2.plot(data_1.so.mean("time"), data.lev,'g-',linewidth=2.0)
    ax2.plot(data_2.so.mean("time"), data.lev,'b-',linewidth=2.0)


    # ax2.plot(obsS1_ls_w_av.so,obsS1_ls_w_av.so.lev,'k:')
    # ax2.plot(obsS2_ls_w_av.so,obsS2_ls_w_av.so.lev,'k--')

    ax2.set_title("Salinity Profile", fontsize=14)
    ax2.set_xlabel("Salinity (in psu)",fontsize=12)
    #ax1.legend(["EXP","EN4 1950-1980","EN4 1990-2020"], loc='best')
    #ax2.legend(["EXP","EN4 1950-1980","EN4 1990-2020"], loc='best')

    ax3.set_ylim((4500,0))

    ax3.plot(data_1.rho.mean("time")-1000, data.lev,'g-',linewidth=2.0)
    ax3.plot(data_2.rho.mean("time")-1000, data.lev,'b-',linewidth=2.0)


    # ax3.plot(obsarho01_ls_w_av-1000,obsarho01_ls_w_av.lev,'k:')
    # ax3.plot(obsarho02_ls_w_av-1000,obsarho02_ls_w_av.lev,'k--')

    ax3.set_title("Rho (ref 0) Profile", fontsize=14)
    ax3.set_xlabel("Density anomaly(in kg/m3)",fontsize=12)
    #ax1.legend(["EXP","EN4 1950-1980","EN4 1990-2020"], loc='best')
    #ax2.legend(["EXP","EN4 1950-1980","EN4 1990-2020"], loc='best')
    
    return 


    



