import xarray as xr
import matplotlib.pyplot as plt

from aqua.logger import log_configure
from .styles import ConfigStyle
from .lat_lon_profiles import plot_lat_lon_profiles

def plot_seasonal_lat_lon_profiles(maps,
                                   ref_maps=None,
                                   std_maps=None,
                                   ref_std_maps=None,
                                   style: str = None,
                                   loglevel='WARNING',
                                   data_labels: list = None,
                                   title: str = None,
                                   **kwargs):
    """
   Plot seasonal lat-lon profiles in a 2x2 subplot layout for the four meteorological seasons.

    This function creates exactly 4 subplots arranged in a 2x2 grid, each showing lat-lon 
    profiles for a specific season. The seasons are hardcoded and must be provided in the 
    exact order: [DJF, MAM, JJA, SON].

    Args:
        maps (list): List of exactly 4 elements, one for each season.
                    Must be in order: [DJF, MAM, JJA, SON].
                    Each element can be either:
                    - A single xarray DataArray (for single model)
                    - A list of xarray DataArrays (for multiple models)
                    
                    Examples:
                    Single model: [djf_data, mam_data, jja_data, son_data]
                    Multiple models: [[model1_djf, model2_djf], [model1_mam, model2_mam], ...]
                    
                    DJF = December-January-February (Winter)
                    MAM = March-April-May (Spring) 
                    JJA = June-July-August (Summer)
                    SON = September-October-November (Autumn)
        ref_maps (list, optional): Reference data for each season, same structure as maps.
        std_maps (list, optional): Standard deviation data for each season, same structure as maps.
        ref_std_maps (list, optional): Reference standard deviation data for each season.
        style (str, optional): Style configuration for the plot.
        loglevel (str): Logging level.
        data_labels (list, optional): Labels for the data series.
        title (str, optional): Overall title for the 2x2 subplot figure.
        **kwargs: Additional keyword arguments.

    Returns:
        fig, axs: Matplotlib figure and axes objects (2x2 subplot layout).
        
    Raises:
        ValueError: If maps is not a list of exactly 4 elements.
    """
    logger = log_configure(loglevel, 'plot_lines')
    ConfigStyle(style=style, loglevel=loglevel)
    
    # Validate input data structure - now only 4 seasons
    if not isinstance(maps, list) or len(maps) != 4:
        raise ValueError("maps must be a list of 4 elements: [DJF, MAM, JJA, SON]")
    
    # Validate std_maps if provided
    if std_maps is not None:
        computed_std_maps = []
        for s in std_maps:
            if s is not None and hasattr(s, 'compute'):
                computed_std_maps.append(s.compute())
            else:
                computed_std_maps.append(s)
        std_maps = computed_std_maps
    else:
        std_maps = [None] * 4

    # Validate ref_std_maps if provided
    if ref_std_maps is not None:
        computed_ref_std_maps = []
        for s in ref_std_maps:
            if s is not None and hasattr(s, 'compute'):
                computed_ref_std_maps.append(s.compute())
            else:
                computed_ref_std_maps.append(s)
        ref_std_maps = computed_ref_std_maps
    else:
        ref_std_maps = [None]

    fig, axs = plt.subplots(2, 2, figsize=(12, 8), constrained_layout=True)
    axs = axs.flatten()
    season_names = ["DJF", "MAM", "JJA", "SON"]

    # Plot the 4 seasonal subplots
    for i, ax in enumerate(axs):
        season_data = maps[i]
        ref_data = ref_maps[i] if ref_maps is not None and i < len(ref_maps) else None
        std_data = std_maps[i] if std_maps is not None and i < len(std_maps) else None
        ref_std_data = ref_std_maps[i] if ref_std_maps is not None and i < len(ref_std_maps) else None
        
        if ref_data is not None:
            ref_label = f"{ref_data.attrs.get('AQUA_model', 'Reference')} {ref_data.attrs.get('AQUA_exp', 'Data')}"
            
            if isinstance(season_data, list):
                # Handle multiple model data
                model_labels = []
                for j, data in enumerate(season_data):
                    if data_labels and j < len(data_labels):
                        label = data_labels[j]
                    else:
                        model = data.attrs.get('AQUA_model', f'Model {j+1}')
                        exp = data.attrs.get('AQUA_exp', '')
                        label = f"{model} {exp}".strip()
                    model_labels.append(label)
                
                _, _ = plot_lat_lon_profiles(data=season_data,
                                    ref_data=ref_data,
                                    std_data=std_data,
                                    ref_std_data=ref_std_data,
                                    data_labels=model_labels,
                                    ref_label=ref_label,
                                    fig=fig, ax=ax,
                                    loglevel=loglevel)
            else:
                # Single model data
                model_label = data_labels[0] if data_labels and len(data_labels) > 0 else f"{season_data.attrs.get('AQUA_model', 'Model')} {season_data.attrs.get('AQUA_exp', 'Exp')}"
                
                _, _ = plot_lat_lon_profiles(data=[season_data],
                                    ref_data=ref_data,
                                    std_data=std_data,
                                    ref_std_data=ref_std_data,
                                    data_labels=[model_label],
                                    ref_label=ref_label,
                                    fig=fig, ax=ax,
                                    loglevel=loglevel)
        else:
            # No reference data, just plot model data
            _, _ = plot_lat_lon_profiles(data=season_data,
                                std_data=std_data,
                                ref_std_data=ref_std_data,
                                data_labels=data_labels,
                                fig=fig, ax=ax,
                                loglevel=loglevel)
        
        ax.set_title(season_names[i])
        ax.grid(True, linestyle='--', alpha=0.7)
        
        # Show legend only on the first subplot
        if i == 0:
            ax.legend(fontsize='small', loc='upper right')
        elif hasattr(ax, 'legend_') and ax.legend_:
            ax.legend_.remove()
    
    # Add overall title if provided
    if title:
        fig.suptitle(title, fontsize=14, fontweight='bold', y=0.98)
    
    return fig, axs
    