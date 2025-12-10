"""Module to handle gridlines in Cartopy maps"""
import numpy as np
import cartopy.crs as ccrs

def draw_manual_gridlines(ax, lon_interval=30, lat_interval=30, 
                          lon_range=(-180, 180), lat_range=(-90, 90),
                          linestyle='--', color='gray', linewidth=1,
                          alpha=0.5, zorder=50):
    """
    Draw manual gridlines over a Cartopy map using ax.plot, with full zorder control.

    Args:
        ax (GeoAxes): The Cartopy axis to draw on.
        lon_interval (int): Interval for longitude lines (degrees).
        lat_interval (int): Interval for latitude lines (degrees).
        lon_range (tuple): Min/max longitudes to span.
        lat_range (tuple): Min/max latitudes to span.
        linestyle (str): Line style for gridlines.
        color (str): Color of the gridlines.
        linewidth (float): Width of the lines.
        alpha (float): Opacity.
        zorder (int): Z-order for rendering.
    """
    # Meridians (vertical lines)
    lons = np.arange(lon_range[0], lon_range[1] + lon_interval, lon_interval)
    lats = np.arange(lat_range[0], lat_range[1] + 1, 1)
    for lon in lons:
        ax.plot([lon] * len(lats), lats, transform=ccrs.PlateCarree(),
                linestyle=linestyle, color=color, linewidth=linewidth,
                alpha=alpha, zorder=zorder)

    # Parallels (horizontal lines)
    lats = np.arange(lat_range[0], lat_range[1] + lat_interval, lat_interval)
    lons = np.arange(lon_range[0], lon_range[1] + 1, 1)
    for lat in lats:
        ax.plot(lons, [lat] * len(lons), transform=ccrs.PlateCarree(),
                linestyle=linestyle, color=color, linewidth=linewidth,
                alpha=alpha, zorder=zorder)
