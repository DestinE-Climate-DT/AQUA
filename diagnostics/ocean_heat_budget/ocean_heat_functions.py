"""Functions for global time series diagnostics.
"""

import numpy as np
import xarray as xr
import matplotlib.pyplot as plt

def compute_net_surface_fluxes(dataset_atm, dataset_ocn):
    mslhf = dataset_atm['mslhf']
    msnlwrf = dataset_atm['msnlwrf']
    msnswrf = dataset_atm['msnswrf']
    msshf = dataset_atm['msshf']
    avg_tos = dataset_ocn['avg_tos']
    mask=xr.where(np.isnan(avg_tos[0,:,:]), 0, 1)

    net_fluxes = mslhf + msnlwrf + msnswrf + msshf
    net_fluxes = net_fluxes * mask
    return net_fluxes, mask

def plot_time_series(var1, var2, title_args, var1_label, var2_label):
    fig, ax1 = plt.subplots()

    color1 = 'tab:red'
    ax1.set_xlabel('Time')
    ax1.set_ylabel(var1_label, color=color1)
    ax1.plot(var1.time.values, var1, color=color1, label=var1_label)
    ax1.tick_params(axis='y', labelcolor=color1)

    color2 = 'tab:blue'
    ax2 = ax1.twinx()
    ax2.set_ylabel(var2_label, color=color2)
    ax2.plot(var1.time.values[:-1], var2, color=color2, label=var2_label)
    ax2.tick_params(axis='y', labelcolor=color2)

    title = f"Model: {title_args['model']}, Exp: {title_args['exp']}, Source: {title_args['source']}"
    fig.suptitle(title)  # Add the title
    fig.legend(loc='best')
    plt.show()