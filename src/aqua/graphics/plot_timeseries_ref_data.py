import xarray as xr
import matplotlib.pyplot as plt

def plot_ref_monthly_data(ax, ref_monthly_data, std_monthly_data, ref_label, logger):
    try:
        if ref_label:
            ref_label_mon = ref_label + ' monthly'
        else:
            ref_label_mon = None
        ref_monthly_data.plot(ax=ax, label=ref_label_mon, color='black', lw=0.6)
        if std_monthly_data is not None:
            std_monthly_data.compute()
            ax.fill_between(ref_monthly_data.time,
                            ref_monthly_data - 2.*std_monthly_data.sel(month=ref_monthly_data["time.month"]),
                            ref_monthly_data + 2.*std_monthly_data.sel(month=ref_monthly_data["time.month"]),
                            facecolor='grey', alpha=0.25)
            ax.set(xlim=(ref_monthly_data.time[0], ref_monthly_data.time[-1]))
    except Exception as e:
        logger.debug(f"Error plotting monthly std data: {e}")

def plot_ref_annual_data(ax, ref_annual_data, std_annual_data, ref_label, logger):
        try:
            if ref_label:
                ref_label_ann = ref_label + ' annual'
            else:
                ref_label_ann = None
            ref_annual_data.plot(ax=ax, label=ref_label_ann, color='black', linestyle='--', lw=0.6)
            if std_annual_data is not None:
                std_annual_data.compute()
                ax.fill_between(ref_annual_data.time,
                                ref_annual_data - 2.*std_annual_data,
                                ref_annual_data + 2.*std_annual_data,
                                facecolor='black', alpha=0.2)
        except Exception as e:
            logger.debug(f"Error plotting annual std data: {e}")

