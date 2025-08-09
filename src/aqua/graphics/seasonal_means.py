import xarray as xr
import matplotlib.pyplot as plt

from aqua.logger import log_configure
from .styles import ConfigStyle
from .lat_lon_profiles import plot_lat_lon_profiles

def plot_seasonal_data(maps,
                      ref_maps=None,
                      std_maps=None,
                      ref_std_maps=None,
                      plot_type: str = 'seasonal',
                      style: str = None,
                      loglevel='WARNING',
                      data_labels: list = None,
                      title: str = None,
                      **kwargs):
    
    logger = log_configure(loglevel, 'plot_lines')
    ConfigStyle(style=style, loglevel=loglevel)
    
    # Validate input data structure - now only 4 seasons
    if not isinstance(maps, list) or len(maps) != 4:
        raise ValueError("maps must be a list of 4 elements: [DJF, MAM, JJA, SON]")
    
    # Create the seasonal plot layout (only 4 subplots)
    if plot_type == 'seasonal':
        fig, axs = plt.subplots(2, 2, figsize=(12, 8), constrained_layout=True)
        axs = axs.flatten()  # Make it easier to iterate
        season_names = ["DJF", "MAM", "JJA", "SON"]
    else:
        raise NotImplementedError("only 'seasonal' plot type is implemented.")

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
                
                plot_lat_lon_profiles(data=season_data,
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
                
                plot_lat_lon_profiles(data=[season_data],
                                    ref_data=ref_data,
                                    std_data=std_data,
                                    ref_std_data=ref_std_data,
                                    data_labels=[model_label],
                                    ref_label=ref_label,
                                    fig=fig, ax=ax,
                                    loglevel=loglevel)
        else:
            # No reference data, just plot model data
            plot_lat_lon_profiles(data=season_data,
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
    