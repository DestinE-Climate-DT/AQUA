import os
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import cartopy.crs as ccrs
from cartopy.mpl.gridliner import LONGITUDE_FORMATTER, LATITUDE_FORMATTER
import cartopy.feature as cfeature

from .tempest_utils import getTrajectories


def multi_plot(tracks_nc_file, title=None, units=None):
    """
    Create a plot of multiple cyclone zoom-ins.

    Args:
        tracks_nc_file (xarray.Dataset): Dataset containing the selected variable.
        title (str, optional): Title of the figure.
        units (str, optional): Units of the selected variable.

    Returns:
        None
    """

    delta = 10  # further extension of the are domain for plotting

    # Create 10 subplots of a selected variable
    _, axs = plt.subplots(nrows=3, ncols=3, figsize=(
        16, 8), subplot_kw={'projection': ccrs.PlateCarree()})
    axs = axs.flatten()

    # Loop over subplots and plot different time slices in each one
    for i, ax in enumerate(axs):
        # Get the non-NaN indices for the selected variable
        non_nan_indices = np.where(~np.isnan(tracks_nc_file.isel(time=i)))
        # Get the longitude and latitude coordinates for the non-NaN values
        lon = tracks_nc_file.lon.values[non_nan_indices[1]]

        lon_min = int(np.min(lon))-delta
        lon_max = int(np.max(lon))+delta
        lat = tracks_nc_file.lat.values[non_nan_indices[0]]
        lat_min = int(np.min(lat))-delta
        lat_max = int(np.max(lat))+delta
        # Plot the non-NaN values using scatter plot
        tracks_nc_file.isel(time=i).plot.pcolormesh(
            ax=ax, transform=ccrs.PlateCarree(), add_colorbar=False)
        ax.set_extent([lon_min, lon_max, lat_min, lat_max], ccrs.PlateCarree())
        ax.coastlines()
        if title is None:
            ax.set_title(tracks_nc_file.name + " " +
                         f'{str(tracks_nc_file.time[i].values)[:13]}')
        elif title:
            ax.set_title(
                title + " " + f'{str(tracks_nc_file.time[i].values)[:13]}')
    # Add a colorbar
    if units is None and 'units' in tracks_nc_file.attrs:
        plt.colorbar(ax.collections[0], ax=axs, shrink=0.4, pad=0.1,
                     location='bottom', label=tracks_nc_file.attrs['units'])
    elif units:
        plt.colorbar(ax.collections[0], ax=axs, shrink=0.4,
                     pad=0.1, location='bottom', label=units)

    plt.show()


def plot_trajectories(trajfile, plotdir):
    """
    Plot the trajectories of the selected cyclone.

    Args:
        trajfile (str): Path to the trajectory file.
        plotdir (str): Path to the directory where the plot will be saved.

    Returns:
        None
    """

    # tempest settings
    nVars = 10
    headerStr = 'start'
    isUnstruc = 0

    # Extract trajectories from tempest file and assign to arrays
    # USER_MODIFY

    nstorms, _, traj_data = getTrajectories(
        trajfile, nVars, headerStr, isUnstruc)
    xlon = traj_data[2, :, :]
    xlat = traj_data[3, :, :]
    # xpres  = traj_data[4,:,:]/100.
    # xwind  = traj_data[5,:,:]
    # xyear  = traj_data[7,:,:]
    # xmonth = traj_data[8,:,:]

    # Initialize axes
    ax = plt.axes(projection=ccrs.PlateCarree())
    ax.set_extent([-180, 180, -50, 50], crs=None)

    # Set title and subtitle
    plt.title("TCs tracks")

    # Set land feature and change color to 'lightgrey'
    # See link for extensive list of colors:
    # https://matplotlib.org/3.1.0/gallery/color/named_colors.html
    ax.add_feature(cfeature.LAND, color='lightgrey')
    gl = ax.gridlines(crs=ccrs.PlateCarree(), draw_labels=True,
                      linewidth=0.5, color='k', alpha=0.5, linestyle='--')
    gl.xlabels_top = False
    gl.ylabels_left = False
    # gl.xlines = False
    gl.xlocator = mticker.FixedLocator([-180, -90, 0, 90, 180])
    gl.xformatter = LONGITUDE_FORMATTER
    gl.yformatter = LATITUDE_FORMATTER
    gl.ylabel_style = {'size': 12, 'color': 'black'}
    gl.xlabel_style = {'size': 12, 'color': 'black'}

    for i in range(nstorms):
        plt.scatter(x=xlon[i], y=xlat[i],
                    color="black",
                    s=30,
                    linewidths=0.5,
                    marker=".",
                    alpha=0.8,
                    transform=ccrs.PlateCarree())  # Important
    os.makedirs(plotdir, exist_ok=True)
    plt.savefig(plotdir + "tracks.png", bbox_inches='tight', dpi=350)
    plt.show()
