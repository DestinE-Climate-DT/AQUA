import xarray as xr
import matplotlib.pyplot as plt

from .styles import ConfigStyle
from .lat_lon_profiles import plot_lat_lon_profiles
            
def plot_seasonal_lat_lon_profiles(seasonal_data,
                                   ref_data=None,
                                   ref_std_data=None,
                                   style: str = None,
                                   loglevel='WARNING',
                                   data_labels: list = None,
                                   title: str = None,
                                   ref_label: str = None
                                   ):
    """
    Plot seasonal lat-lon profiles in a 2x2 subplot layout for the four meteorological seasons.

    This function creates exactly 4 subplots arranged in a 2x2 grid, each showing lat-lon 
    profiles for a specific season. The seasons are hardcoded and must be provided in the 
    exact order: [DJF, MAM, JJA, SON].

    Args:
        seasonal_data (list): List of exactly 4 elements, one for each season.
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
        ref_data (list, optional): Reference data for each season, same structure as seasonal_data.
        ref_std_data (list, optional): Reference standard deviation data for each season.
        style (str, optional): Style configuration for the plot.
        loglevel (str): Logging level.
        data_labels (list, optional): List of data_labels for each subplot. If provided, must have 4 elements.
        title (str, optional): Overall title for the 2x2 subplot figure.
        ref_label (str, optional): Label for the reference data in the legend.

    Returns:
        fig, axs: Matplotlib figure and axes objects (2x2 subplot layout).
        
    Raises:
        ValueError: If seasonal_data is not a list of exactly 4 elements.
    """
    ConfigStyle(style=style, loglevel=loglevel)
    
    # Validate seasonal_data structure
    if not isinstance(seasonal_data, list) or len(seasonal_data) != 4:
        raise ValueError("seasonal_data must be a list of 4 elements: [DJF, MAM, JJA, SON]")
    
    # Validate and prepare data labels
    for i, season_data in enumerate(seasonal_data):
        if isinstance(season_data, list):
            if not all(isinstance(d, xr.DataArray) for d in season_data):
                raise ValueError(f"Season {i} contains non-DataArray elements")
        elif not isinstance(season_data, xr.DataArray):
            raise ValueError(f"Season {i} must be DataArray or list of DataArrays")

    # Validate ref_std_data if provided
    if ref_std_data is not None:
        computed_ref_std_data = []
        for s in ref_std_data:
            if s is not None and hasattr(s, 'compute'):
                computed_ref_std_data.append(s.compute())
            else:
                computed_ref_std_data.append(s)
        ref_std_data = computed_ref_std_data
    else:
        ref_std_data = [None]

    fig, axs = plt.subplots(2, 2, figsize=(12, 8), constrained_layout=True)
    axs = axs.flatten()
    season_names = ["DJF", "MAM", "JJA", "SON"]

    # Plot the 4 seasonal subplots
    for i, ax in enumerate(axs):
        season_data = seasonal_data[i]
        season_title = season_names[i]
        season_ref_data = ref_data[i] if ref_data is not None and i < len(ref_data) else None
        season_ref_std_data = ref_std_data[i] if ref_std_data is not None and i < len(ref_std_data) else None
        
            
        _, _ = plot_lat_lon_profiles(data=season_data,
                                     ref_data=season_ref_data,
                                     ref_std_data=season_ref_std_data,
                                     ref_label=ref_label,
                                     data_labels=data_labels,
                                     fig=fig, ax=ax,
                                     loglevel=loglevel)

        ax.set_title(season_title)
        ax.grid(True, linestyle='--', alpha=0.7)
        
        # Show legend only on the first subplot
        if i == 0:
            ax.legend(fontsize='small', loc='upper right')
        elif hasattr(ax, 'legend_') and ax.legend_:
            ax.legend_.remove()
    
    # Add overall title if provided
    if title:
        fig.suptitle(title, fontsize=14, fontweight='bold', y=0.98)

    fig.set_layout_engine('tight')

    return fig, axs
    