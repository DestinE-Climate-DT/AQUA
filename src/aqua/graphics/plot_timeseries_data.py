import xarray as xr
import matplotlib.pyplot as plt

def plot_monthly_data(ax, monthly_data, data_labels, logger):
    if isinstance(monthly_data, xr.DataArray):
        monthly_data = [monthly_data]
    for i in range(len(monthly_data)):
        try:
            mon_data = monthly_data[i]
            if data_labels:
                label = data_labels[i]
                label += ' monthly'
            else:
                label = None
            mon_data.plot(ax=ax, label=label, lw=1.5)
        except Exception as e:
            logger.debug(f"Error plotting monthly data: {e}")

def plot_annual_data(ax, annual_data, data_labels, logger):
    for i in range(len(annual_data)):
        try:
            ann_data = annual_data[i]
            if data_labels:
                label = data_labels[i]
                label += ' annual'
            else:
                label = None
            ann_data.plot(ax=ax, label=label, color='#1898e0', linestyle='--', lw=1.5)
        except Exception as e:
            logger.debug(f"Error plotting annual data: {e}")