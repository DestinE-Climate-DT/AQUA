import xarray as xr
import matplotlib.pyplot as plt

from .styles import ConfigStyle
from aqua.logger import log_configure
from .lat_lon_profiles import plot_lat_lon_profiles

def get_seasonal_and_annual_means(da):
    """Restituisce [DJF, MAM, JJA, SON, annual_mean] per un DataArray mensile."""
    seasons = {
        "DJF": [11, 0, 1],
        "MAM": [2, 3, 4],
        "JJA": [5, 6, 7],
        "SON": [8, 9, 10]
    }
    season_means = []
    for months in seasons.values():
        season_means.append(da.isel(time=months).mean(dim='time'))
    annual_mean = da.mean(dim='time')
    return season_means + [annual_mean]

def plot_lines(maps, 
               plot_type: str = 'seasonal',
               style: str = None,
               loglevel='WARNING',
               data_labels: list = None,
               **kwargs):

    logger = log_configure(loglevel, 'plot_lines')
    ConfigStyle(style=style, loglevel=loglevel)

    # Se maps è un singolo DataArray, calcola le medie stagionali e annuali
    if isinstance(maps, xr.DataArray):
        da = maps
        seasonal_and_annual = get_seasonal_and_annual_means(da)
        maps = [[seasonal_and_annual[i]] for i in range(4)] + [[seasonal_and_annual[4]]]
        if data_labels is None:
            data_labels = [da.attrs.get("long_name", "Data")]
        plot_type = 'seasonal'

    # Se maps è una lista di DataArray, calcola le medie stagionali e annuali per ciascuno
    elif isinstance(maps, list) and all(isinstance(m, xr.DataArray) for m in maps):
        seasonal_and_annual = [get_seasonal_and_annual_means(m) for m in maps]
        # Ricostruisci la struttura: lista di liste per stagione/annuale
        maps = [
            [seasonal_and_annual[j][i] for j in range(len(maps))]  # per ogni stagione/annuale
            for i in range(5)
        ]
        if data_labels is None:
            data_labels = [
                m.attrs.get("long_name", f"Data {i+1}") for i, m in enumerate(maps[0])
            ]
        plot_type = 'seasonal'

    # Ora gestiamo il caso "classico" (lista di liste già stagionali)
    if plot_type == 'seasonal':
        fig = plt.figure(figsize=(14, 10), constrained_layout=True)
        gs = fig.add_gridspec(3, 2)
        axs = [
            fig.add_subplot(gs[0, 0]),  # DJF
            fig.add_subplot(gs[0, 1]),  # MAM
            fig.add_subplot(gs[1, 0]),  # JJA
            fig.add_subplot(gs[1, 1]),  # SON
            fig.add_subplot(gs[2, :])   # Annuale (grande)
        ]
        season_names = ["DJF", "MAM", "JJA", "SON"]
        for i, ax in enumerate(axs[:4]):
            for j, data in enumerate(maps[i]):
                plot_lat_lon_profiles(mean_type='zonal',
                                      monthly_data=data, 
                                      fig=fig, ax=ax
                                      )
            ax.set_title(season_names[i])
            if len(maps[i]) > 1:
                ax.legend(fontsize='small')
            ax.grid(True, linestyle='--', alpha=0.7)
        # Annuale
        for j, data in enumerate(maps[4]):
            plot_lat_lon_profiles(mean_type='zonal',
                                  monthly_data=data, 
                                  fig=fig, ax=axs[4]
                                  )
        axs[4].set_title("Annual Mean")
        if len(maps[4]) > 1:
            axs[4].legend(fontsize='small')
        axs[4].grid(True, linestyle='--', alpha=0.7)
        return fig, axs
    else:
        raise NotImplementedError("only 'seasonal' plot type is implemented.")