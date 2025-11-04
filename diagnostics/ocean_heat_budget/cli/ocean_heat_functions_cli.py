"""Functions for global time series diagnostics.
"""

import numpy as np
import xarray as xr
import matplotlib.pyplot as plt

def compute_net_surface_fluxes(dataset_atm, dataset_oc):

    wflh=3.34e5 # water fusion latent heat in J/kg

    mslhf = dataset_atm['mslhf']
    msnlwrf = dataset_atm['msnlwrf']
    msnswrf = dataset_atm['msnswrf']
    msshf = dataset_atm['msshf']
    snow_prec = dataset_atm['msr'] 
    avg_tos = dataset_oc['avg_tos']
    mask=xr.where(np.isnan(avg_tos[0,:]), 0, 1)

    # net fluxes are the sum of the 4 fluxes minus the snow precipitation times
    # the water latent heat of fusion (minus means the flux is from the ocean to atmosphere)
    net_fluxes = mslhf + msnlwrf + msnswrf + msshf - snow_prec*wflh
    net_fluxes = net_fluxes * mask
    return net_fluxes, mask

def plot_time_series(var1, var2, title_args, var1_label, var2_label, outdir):
    fig, ax1 = plt.subplots()

    color1 = 'tab:red'
    ax1.set_xlabel('Time')
    ax1.set_ylabel(var1_label, color=color1)
    ax1.plot(var1.time.values[:-1], var1[:-1], color=color1, label=var1_label)
    ax1.tick_params(axis='y', labelcolor=color1)

    color2 = 'tab:blue'
    ax2 = ax1.twinx()
    ax2.set_ylabel(var2_label, color=color2)
    ax2.plot(var2.time.values, var2, color=color2, label=var2_label)
    ax2.tick_params(axis='y', labelcolor=color2)

    # Set the same y limits for both axes
    ax1.set_ylim([min(var1.min(), var2.min()), max(var1.max(), var2.max())])
    ax2.set_ylim([min(var1.min(), var2.min()), max(var1.max(), var2.max())])

    title = f"Model: {title_args['model']}, Exp: {title_args['exp']}, Source: {title_args['source']}"
    fig.suptitle(title)  # Add the title
    ax1.legend(loc='upper left')  # Add legend to ax1
    ax2.legend(loc='upper right')  # Add legend to ax2
    plt.savefig(outdir + f"/ocean_heat_budget_timeseries_{title_args['model']}_{title_args['exp']}_{title_args['source']}.pdf")
    plt.show()

def plot_difference(var1, var2, title_args, var1_label, var2_label, outdir):
    diff = var1[:-1] - var2

    avg_diff=np.mean(diff)
    fig, ax = plt.subplots()
    ax.plot(var1.time.values[:-1], diff)
    ax.axhline(y=avg_diff, color='r', linestyle='--', label='diff mean')
    ax.set_xlabel('Time')
    ax.set_ylabel(f"{var1_label} - {var2_label}")

    title = f"Model: {title_args['model']}, Exp: {title_args['exp']}, Sources: {title_args['source']}"
    subtitle = f"Difference between {var1_label} and {var2_label}"
    fig.suptitle(title, fontsize=10)  # Add the title with reduced fontsize
    ax.set_title(subtitle, fontsize=8)  # Add the subtitle 
    ax.legend()
    plt.savefig(outdir + f"/ocean_heat_budget_difference_{title_args['model']}_{title_args['exp']}_{title_args['source']}.pdf")
    plt.show()