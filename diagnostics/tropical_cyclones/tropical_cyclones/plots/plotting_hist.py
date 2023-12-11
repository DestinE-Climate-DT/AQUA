import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from tropical_cyclones.tools.tempest_utils import getTrajectories

    
def plot_hist_cat(trajfile1, trajfile2, trajfile3, ibtracs_file=None):
    # tempest settings
    nVars = 10
    headerStr = 'start'
    isUnstruc = 0
    
    # set the bins values for wind
    bins = [ 40, 80, 120, 160, 200, 240, 280]

    # Extract trajectories from tempest file and assign to arrays
    nstorms, ntimes, traj_data1 = getTrajectories(trajfile1, nVars, headerStr, isUnstruc)
    nstorms, ntimes, traj_data2 = getTrajectories(trajfile2, nVars, headerStr, isUnstruc)
    nstorms, ntimes, traj_data3 = getTrajectories(trajfile3, nVars, headerStr, isUnstruc)

    xwind1 = traj_data1[5, :, :] * 3.6  # maximum wind speed near node - convert to km/h
    xwind2 = traj_data2[5, :, :] * 3.6  # maximum wind speed near node - convert to km/h
    xwind3 = traj_data3[5, :, :] * 3.6  
    
    if ibtracs_file is not None:
        # Read the IBTrACS dataset
        ibtracs_data = pd.read_csv(ibtracs_file)

        # Filter the 'USA_WIND' column to include only the values without 'kts'
        wind_intensities_num = ibtracs_data.loc[~ibtracs_data['USA_WIND'].str.contains('kts'), 'USA_WIND']

        # Convert the filtered values to numeric
        xwind4 = pd.to_numeric(wind_intensities_num, errors='coerce')
        
        xwind4 = xwind4 * 1.852  # convert from kts to km/h
        

        hist4, _ = np.histogram(xwind4, bins=bins)
    else:
        hist4 = np.zeros(len(bins) - 1)

    hist1, _ = np.histogram(xwind1, bins=bins)
    hist2, _ = np.histogram(xwind2, bins=bins)
    hist3, _ = np.histogram(xwind3, bins=bins)

    width = 0.6 * np.diff(bins)
    center = bins[:-1] + np.diff(bins) / 2 - width / 6

    total_traj1 = np.sum(hist1)
    total_traj2 = np.sum(hist2)
    total_traj3 = np.sum(hist3)
    total_traj4 = np.sum(hist4)

    # now prepare the figure
    
    fig, ax = plt.subplots(figsize=(8, 3))
    if ibtracs_file is not None:
        ax.bar(center - width/2, hist4 / total_traj4, align='center', width=width/4, label='IBTrACS', alpha=0.5)
        ax.bar(center - width/6, hist3 / total_traj3, align='center', width=width/4, label='ERA5', alpha=0.5, color='black')
        ax.bar(center + width/6, hist1 / total_traj1, align='center', width=width/4, label='tco1279-orca025-cycle3')
        ax.bar(center + width/2, hist2 / total_traj2, align='center', width=width/4, label='tco2559-ng5-cycle3', alpha=0.5)

    else:
        ax.bar(center - width/6, hist3 / total_traj3, align='center', width=width/4, label='ERA5', alpha=0.5, color='black')
        ax.bar(center + width/6, hist1 / total_traj1, align='center', width=width/4, label='tco1279-orca025-cycle3')
        ax.bar(center + width/2, hist2 / total_traj2, align='center', width=width/4, label='tco2559-ng5-cycle3', alpha=0.5)

    ax.set_xticks(bins)
    ax.legend()

    # Set title, x&y axes labels
    plt.title("TCs intensities")
    plt.xlabel("Max wind speed (km/h)")
    plt.ylabel("Frequency (Normalized)")
    
    # save figure
    plt.savefig('/home/b/b382216/work/tc/PDF/hist_wind_all_fullres.pdf', bbox_inches="tight")
    
    #show
    plt.show()


def plot_press_wind(trajfile1, trajfile2, trajfile3, ibtracs_file, dot_dim):
    # tempest settings
    nVars = 10
    headerStr = 'start'
    isUnstruc = 0

    # Extract trajectories from tempest file and assign to arrays
    nstorms, ntimes, traj_data1 = getTrajectories(trajfile1, nVars, headerStr, isUnstruc)
    nstorms, ntimes, traj_data2 = getTrajectories(trajfile2, nVars, headerStr, isUnstruc)
    nstorms, ntimes, traj_data3 = getTrajectories(trajfile3, nVars, headerStr, isUnstruc)

    xwind1 = traj_data1[5, :, :] * 3.6  # maximum wind speed near node - convert to km/h
    xwind2 = traj_data2[5, :, :] * 3.6  # maximum wind speed near node - convert to km/h
    xwind3 = traj_data3[5, :, :] * 3.6  # maximum wind speed near node - convert to km/h
    
    xpres1 = traj_data1[4, :, :] / 100.0  # SLP at node in hPa
    xpres2 = traj_data2[4, :, :] / 100.0  # SLP at node in hPa
    xpres3 = traj_data3[4, :, :] / 100.0  # SLP at node in hPa
    
    #now deal with ibtracks file if present
    
    if ibtracs_file is not None:
    
        # Read the IBTrACS dataset
        ibtracs_data = pd.read_csv(ibtracs_file)


        # Filter the 'USA_PRES' column to include only the values without 'mb'
        press_num = ibtracs_data.loc[~ibtracs_data['USA_PRES'].str.contains('mb'), 'USA_PRES']

        # Convert the filtered values to numeric
        xpres4 = pd.to_numeric(press_num, errors='coerce') #already in mb/hPa
        
        # Filter the 'USA_WIND' column to include only the values without 'kts'
        wind_intensities_num = ibtracs_data.loc[~ibtracs_data['USA_WIND'].str.contains('kts'), 'USA_WIND']

        # Convert the filtered values to numeric
        xwind4 = pd.to_numeric(wind_intensities_num, errors='coerce')
        
        xwind4=xwind4*1.852 #convert from kts to km/h

        fig, ax = plt.subplots(figsize=(8, 6))
        ax.scatter(xwind4, xpres4, s=dot_dim, alpha=0.2, marker="x", label='IBTrACS')  # IBTrACS data
        ax.scatter(xwind3, xpres3, s=dot_dim, alpha=0.2, marker="x", label='ERA5', color="black")  # ERA5 data
        ax.scatter(xwind1, xpres1, s=dot_dim, alpha=0.2, marker="x", label='tco1279-orca025-cycle3')
        ax.scatter(xwind2, xpres2, s=dot_dim, alpha=0.2, marker="x", label='tco2559-ng5-cycle3')
        
    else:
        fig, ax = plt.subplots(figsize=(8, 6))
        ax.scatter(xwind3, xpres3, s=dot_dim, alpha=0.4, marker="x", label='ERA5', color="black")  # ERA5 data
        ax.scatter(xwind1, xpres1, s=dot_dim, alpha=0.6, marker="x", label='tco1279-orca025-cycle3')
        ax.scatter(xwind2, xpres2, s=dot_dim, alpha=0.6, marker="x", label='tco2559-ng5-cycle3')
    
    ax.set_xlabel('Max Wind Speed (km/h)')
    ax.set_ylabel('Min slp (hPa)')
    ax.set_title('Max Wind Speed vs min slp')
    ax.legend()

    # save figure
    plt.savefig('/home/b/b382216/work/tc/PDFslp_vs_wind_all_fullres.pdf', format='pdf')
    plt.show()

