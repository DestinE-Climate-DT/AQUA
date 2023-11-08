import os
import xarray as xr
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

import calendar
from cdo import Cdo
import cartopy.crs as ccrs
from matplotlib.legend_handler import HandlerTuple
from aqua import Reader
import matplotlib.gridspec as gridspec
from aqua.util import create_folder, add_cyclic_lon
from aqua.logger import log_configure

cdo = Cdo(tempdir='./tmp/cdo-py')
tempdir='./tmp/cdo-py'
if not os.path.exists(tempdir):
    os.makedirs(tempdir)

loglevel: str = 'WARNING'
logger = log_configure(log_level=loglevel, log_name='Radiation')

def process_ceres_data(exp=None, source=None):
    """
    Function to extract CERES data for further analyis + create global means

    Args:
        exp (str):   Input experiment to be selected from the catalogue.
        source (str): Input source to be selected from the catalogue.

    Returns:
        dict: A dictionary containing the following information:
            - "model": "CERES".
            - "exp": Experiment name.
            - "source": Source name.
            - "gm": Global means of CERES data.
            - "clim_gm": Global means of climatology data.
            - "anom_gm": Global means of monthly anomalies data.
            - "clim": Climatology data.
            - "anom": Monthly anomalies data.
    """

    # reader_ceres_toa
    reader = Reader(model='CERES', exp=exp, source=source, regrid='r100')
    data = reader.retrieve()
    data['tnr'] = data['mtntrf'] + data['mtnsrf']
    ceres = reader.regrid(data[['tnr', 'mtntrf', 'mtnsrf']])

    starting_year = str(ceres["time.year"][0].values) if len(ceres.sel(time=str(ceres["time.year"][0].values)).time) == 12 \
                    else str(ceres["time.year"][0].values + 1)
    final_year = str(ceres["time.year"][-1].values) if len(ceres.sel(time=str(ceres["time.year"][-1].values)).time) == 12 \
                 else str(ceres["time.year"][-1].values -1)
    # limit to years that are complete
    complete = ceres.sel(time=slice(starting_year, final_year))

    # time averages over each month and get the monthly anomaly
    clim = complete.groupby('time.month').mean('time')
    monthly_anomalies = complete.groupby('time.month') - clim

    clim = clim.rename({'month': 'time'})
    # global mean
    clim_gm = reader.fldmean(clim)
    ceres_gm = reader.fldmean(ceres)
    anom_gm = reader.fldmean(monthly_anomalies)

    dictionary = {
        "model": "CERES".lower(),
        "exp": exp.lower(),
        "source": source.lower(),
        "data": complete,
        "gm": ceres_gm,
        "clim_gm": clim_gm,
        "anom_gm": anom_gm,
        "clim": clim,
        "anom": monthly_anomalies

    }
    return dictionary

def process_model_data(model=None, exp=None, source=None):
    """
    Function to extract Model output data for further analysis and create global means.

    Args:
        model (str):   Input model to be selected from the catalogue.
        exp (str):     Input experiment to be selected from the catalogue.
        source (str):  Input source to be selected from the catalogue.

    Returns:
        dict: A dictionary containing the following information:
            - "model": Model name.
            - "exp": Experiment name.
            - "source": Source name.
            - "gm": Global means of model output data.
            - "data": Model output data.
    """

    reader = Reader(model=model, exp=exp, source=source,
                    regrid='r100')
    data = reader.retrieve(var=['2t','mtntrf', 'mtnsrf'])
    data['tnr'] = data['mtntrf'] + data['mtnsrf']
    gm = reader.fldmean(data)

    dictionary = {
        "model": model.lower(),
        "exp": exp.lower(),
        "source": source.lower(),
        "data": data,
        "gm": gm
    }

    return dictionary

def gregory_plot(obs_data=None, models=None, obs_time_range=None, model_labels=None, obs_labels=None,  outputdir=None, outputfig=None, 
                 fontsize=14, markersize=3):
    """
    Create a Gregory Plot with various models and an observational dataset (e.g., ERA5).
    
    Args:
        obs_data (dict): Xarray Dataset containing the observational data.
        models (list): A list of models that should be plotted.
        obs_time_range (tuple, optional): A tuple containing the start and end dates of the time range for the observational data. 
            Format: ('YYYY-MM-DD', 'YYYY-MM-DD')
        model_labels (list, optional): Labels for the models. If not provided, default labels are used.
        obs_labels (str, optional): Desired label for the observational data.
        outputdir (str, optional): The directory to save the data files. 
        outputfig (str, optional): The directory to save the plot as a PDF.
        fontsize (int, optional): Font size for the plot labels and title.
        markersize (int, optional): Size of the markers in the plot.

    Returns:
        A Gregory Plot displaying the data for each model and the observational data.
    """
    # Create the plot and axes
    fig, ax = plt.subplots()
    fig.set_facecolor('white')
    # Colors for the plots
    colors = ["orange", "gray", "dodgerblue", "yellow", "indigo", "violet"]
    
    # Plot the data for each model
    handles = []
    labels = []

    models = models if isinstance(models, list) else [models]


    # Plot the data for observation
    if obs_time_range is None:
        dummy_model_gm = models[0]["gm"]
        starting_year = str(dummy_model_gm["time.year"][0].values) if len(dummy_model_gm.sel(time=str(dummy_model_gm["time.year"][0].values)).time) == 12 \
                        else str(dummy_model_gm["time.year"][0].values + 1)
        final_year = str(dummy_model_gm["time.year"][-1].values) if len(dummy_model_gm.sel(time=str(dummy_model_gm["time.year"][-1].values)).time) == 12 \
                    else str(dummy_model_gm["time.year"][-1].values -1)
        # limit to years that are complete
        obs_data_gm = obs_data["gm"].sel(time=slice(starting_year, final_year))
    else:
        obs_data_gm = obs_data["gm"].sel(time=slice(*obs_time_range))
    obs_2t_resampled = obs_data_gm["2t"].resample(time="M").mean()
    obs_tnr_resampled = obs_data_gm["tnr"].resample(time="M").mean()

    if obs_labels is None:
        obs_labels = obs_data["model"]+' '+obs_data["exp"]+' '+obs_data["source"] +', ('+starting_year+'-'+final_year+')'
    # Plot the data
    line = ax.plot(
        obs_2t_resampled - 273.15, obs_tnr_resampled,
        marker="o", color="mediumseagreen", linestyle="-", markersize=markersize, label=obs_labels)
    handles.append(line[0])  # Append the line object itself
    labels.append(obs_labels)

    for i, model in enumerate(models):
        model_name = model["model"]+' '+model["exp"]+' '+model["source"] if model_labels is None else model_labels[i]
        model_color = colors[i % len(colors)]  # Rotate colors for each model
        model_2t = model["gm"]["2t"].resample(time="M").mean() - 273.15
        model_tnr = model["gm"]["tnr"].resample(time="M").mean()
        
        line, = ax.plot(model_2t, model_tnr, color=model_color,
            linestyle="-", marker="o", markersize=5, label=model_name)
        handles.append(line)  # Append the line object itself
        labels.append(model_name)

        ax.plot(model_2t[0], model_tnr[0],
            marker="*", color="black", linestyle="-", markersize=markersize*5)
        ax.plot(model_2t[-1], model_tnr[-1],
            marker="X", color="tab:red", linestyle="-", markersize=markersize*5)
    # Set labels and title
    ax.set_xlabel("2m temperature [$^{\circ} C$]", fontsize=fontsize-2)
    ax.set_ylabel("Net radiation TOA [Wm$^{-2}$]", fontsize=fontsize-2)
    ax.set_title("Gregory Plot", fontsize=fontsize)
    ax.legend(handles, labels + ["Start", "End"], handler_map={tuple: HandlerTuple(ndivide=None)})
    # ax.text(0.5, -0.15, "Black stars indicate the first value of the dataseries\nRed X indicate the last value of the dataseries.",
    #         transform=ax.transAxes, fontsize=fontsize-6, verticalalignment='top', horizontalalignment='center')
    ax.tick_params(axis="both", which="major", labelsize=10)
    
    ax.grid(True, linestyle="--", linewidth=0.5)

    if outputdir is not None:
        create_folder(folder=str(outputdir), loglevel='WARNING')
        for model in models:
            start_date = str(model["data"]["time.year"][0].values) +'-'+str(model["data"]["time.month"][0].values)+'-'+str(model["data"]["time.day"][0].values)
            end_date = str(model["data"]["time.year"][-1].values) +'-'+str(model["data"]["time.month"][-1].values)+'-'+str(model["data"]["time.day"][-1].values)
            model_name = model["model"]+'_'+model["exp"]+'_'+model["source"] if model_labels is None else model_labels[i]
            model_data_resampled = model["gm"].resample(time="M").mean()
            model_data_resampled.to_netcdf(f"{outputdir}/gregory_plot_{model_name}_{start_date}_{end_date}.nc")
            logger.info(f"Data has been saved to {outputdir}.")

    # Save the data for each model to separate netCDF files
    if outputfig is not None:
        create_folder(folder=str(outputfig), loglevel='WARNING')
        for model in models:
            start_date = str(model["data"]["time.year"][0].values) +'-'+str(model["data"]["time.month"][0].values)+'-'+str(model["data"]["time.day"][0].values)
            end_date = str(model["data"]["time.year"][-1].values) +'-'+str(model["data"]["time.month"][-1].values)+'-'+str(model["data"]["time.day"][-1].values)
            model_name = model["model"]+'_'+model["exp"]+'_'+model["source"] if model_labels is None else model_labels[i]
            filename = f"{outputfig}/gregory_plot_{model_name}_{start_date}_{end_date}.pdf"
            plt.savefig(filename, dpi=300, format='pdf', bbox_inches="tight")
            logger.info(f"Plot has been saved to {outputfig}.")
    else:
        plt.show()
    

def barplot_model_data(datasets=None, model_names=None, outputdir=None, outputfig=None, year=None, fontsize=14):
    """
    Create a grouped bar plot with various models and CERES data. Variables 'mtntrf' and 'mtnsrf' are plotted to show imbalances.
    The default mean for CERES data is calculated over the entire time range.

    Args:
        datasets (list of xarray.DataSets): A list of xarray.DataSets to be plotted (e.g., global means).
        model_names (list of str): Your desired naming for the plotting, corresponding to the datasets.
        outputdir (str, optional): Directory where the output data will be saved. Default is None.
        outputfig (str, optional): Directory where the output figure will be saved. Default is None.
        year (int, optional): The year for which the plot is generated. Default is None.
        fontsize (int, optional): Font size for labels and legends in the plot. Default is 14.

    Returns:
        A bar plot showing the global mean radiation variables ('mtntrf' and 'mtnsrf') for different models and CERES data.
    """
    # Set a seaborn color palette
    sns.set_palette("pastel")
    global_mean = {'Variables': ["mtntrf", "mtnsrf"]}

    model_names = [dataset["model"] + ' ' + dataset["exp"]+ ' ' + dataset["source"] for dataset in datasets] if model_names is None else model_names
    for i in range(0, len(datasets)):
        if "clim_gm" in datasets[i]:
            global_mean.update({model_names[i]: [-datasets[i]["clim_gm"]["mtntrf"].mean(
                ).values.item(), datasets[i]["clim_gm"]["mtnsrf"].mean().values.item()]})
        else:
            global_mean.update({model_names[i]: [-datasets[i]["gm"]["mtntrf"].mean(
                ).values.item(), datasets[i]["gm"]["mtnsrf"].mean().values.item()]})
    global_mean = pd.DataFrame(global_mean)
    # Create a grouped bar plot
    ax = global_mean.plot(x='Variables', kind='bar', figsize=(8, 6))
    # Show the plot
    plt.legend(title='Datasets', fontsize=fontsize-2)
    plt.xlabel('Variables', fontsize=fontsize)
    plt.ylabel('Global mean ($W/m^2$)', fontsize=fontsize)
    plt.ylim(236, 250)
    plt.xticks(rotation=0, fontsize=fontsize-2)
    plt.yticks(fontsize=fontsize-2)
    if year is not None:
        plt.title(
            f"Global Mean TOA radiation for different models ({year}) (all CERES years from 2001 to 2021)", fontsize=fontsize+2)
    else:
        plt.title('Global Mean TOA radiation for different models',
                  fontsize=fontsize+2)  # (mean over all model times)')

    if outputdir is not None:
        create_folder(folder=str(outputdir), loglevel='WARNING')
        # Save the data to a NetCDF file
        output_data = xr.Dataset(global_mean)
        filename = f"{outputdir}/barplot_mtntrf_mtnsrf_{'_'.join(model_names).replace(' ', '_').lower()}.nc"
        output_data.to_netcdf(filename)
        logger.info(f"Data has been saved to {outputdir}.")

    if outputfig is not None:
        create_folder(folder=str(outputfig), loglevel='WARNING')
        
        filename = f"{outputfig}/barplot_mtntrf_mtnsrf_{'_'.join(model_names).replace(' ', '_').lower()}.pdf"
        plt.savefig(filename, dpi=300, format='pdf', bbox_inches="tight")
        logger.info(f"Plot has been saved to {outputfig}.")
    else:
        plt.show()

    

def plot_model_comparison_timeseries(models=None, linelabels=None, ceres=None, outputdir=None, outputfig=None, ylim = 6.5):
                        
    """
    Create time series bias plot with various models and CERES, including the individual CERES years to show variabilities.
    Variables ttr, tsr, and tnr are plotted to show imbalances. Default mean for CERES data is the whole time range.

    Args:
        models (list of xarray.DataSets): A list of xarray.DataSets of the respective models.
        linelabels (list of str): Your desired naming for the plotting (this will also be used in the filename).
        ceres (xarray.DataSet): The CERES data to be compared with the models.
        outputdir (str, optional): Directory where the output data will be saved. Default is None.
        outputfig (str, optional): Directory where the output figure will be saved. Default is None.
        ylim (float, optional): The limit for the y-axis in the plot. Default is 6.5.

    Returns:
        A plot to show the model biases for the whole time range.
    """
        
    fig, axes = plt.subplots(3, 1, figsize=(12, 8))
    # Set the Seaborn style (you can choose other styles if needed)
    sns.set_style("darkgrid")
    # Choose a Seaborn color palette (you can select a different one if needed)
    color_palette = sns.color_palette("Set1")  # Change "Set1" to your preferred palette
    # Get a list of colors from the palette
    linecolors = color_palette.as_hex()
    #linecolors = plt.cm.get_cmap('tab10').colors

    if models is None:
        raise ValueError("models cannot be None")
    elif isinstance(models, list):
        pass
    else:
        models = [models]
    if linelabels is None or isinstance(linelabels, list):
        pass
    else:
        linelabels = [linelabels]

    dummy_model_gm = models[0]["gm"]
    starting_year = int(dummy_model_gm["time.year"][0].values) if len(dummy_model_gm.sel(time=str(dummy_model_gm["time.year"][0].values)).time) == 12 \
                    else int(dummy_model_gm["time.year"][0].values) + 1
    final_year = int(dummy_model_gm["time.year"][-1].values) if len(dummy_model_gm.sel(time=str(dummy_model_gm["time.year"][-1].values)).time) == 12 \
                 else int(dummy_model_gm["time.year"][-1].values) -1
    years = range(starting_year, final_year+1)

    xlim = [pd.to_datetime(str(dummy_model_gm["time.year"][0].values) +'-'+str(dummy_model_gm["time.month"][0].values)+'-'+str(dummy_model_gm["time.day"][0].values)), \
        pd.to_datetime(str(dummy_model_gm["time.year"][-1].values) +'-'+str(dummy_model_gm["time.month"][-1].values)+'-'+str(dummy_model_gm["time.day"][-1].values))]
    
    if linelabels is None:
        linelabels = []
        for model in models:
            linelabels.append(model["model"]+' '+model["exp"]+' '+model["source"])

    for i, model in enumerate(models):
        ttr_diff = []  # Initialize an empty list to store the data for each year
        tsr_diff = []
        tnr_diff = []
        # Iterate through the years
        for year in years:
            ttr_diff.append(model["gm"].mtntrf.sel(time=str(year))- ceres['clim_gm'].mtntrf.values)
            tsr_diff.append(model["gm"].mtnsrf.sel(time=str(year))- ceres['clim_gm'].mtnsrf.values)
            tnr_diff.append(model["gm"].tnr.sel(time=str(year)) - ceres['clim_gm'].tnr.values)
        # Concatenate the data along the 'time' dimension
        ttr_diff = xr.concat(ttr_diff, dim='time')
        tsr_diff = xr.concat(tsr_diff, dim='time')
        tnr_diff = xr.concat(tnr_diff, dim='time')
        # Plot the data for the current model
        ttr_diff.plot(ax=axes[0], color=linecolors[i], label=linelabels[i], x='time')
        tsr_diff.plot(ax=axes[1], color=linecolors[i], label=linelabels[i], x='time')
        ttr_diff.plot(ax=axes[2], color=linecolors[i], label=linelabels[i], x='time')
    
    samples_tmp= []
    for year in range(int(ceres["data"]["time.year"][0].values), int(ceres["data"]["time.year"][-1].values)-1):
            # select year and assign (fake) time coordinates so that the differencing works
            samples_tmp.append(ceres["gm"].sel(time=str(year)).assign_coords(time=ceres["clim_gm"].time)- ceres["clim_gm"])
    TOA_ceres_diff_samples_gm = xr.concat(samples_tmp, dim='ensemble')
    shading_data_list = []
    for year in years:
        new_data = TOA_ceres_diff_samples_gm.assign_coords(time=dummy_model_gm.sel(time=str(year)).time)
        shading_data_list.append(new_data)
        shading_data = xr.concat(shading_data_list, dim='time')
        long_time = np.append(shading_data['time'], shading_data['time'][::-1])

    axes[0].fill(long_time, np.append(shading_data['mtntrf'].min(dim='ensemble'), shading_data['mtntrf'].max(dim='ensemble')[::-1]), 
                 color='lightgrey', alpha=0.6, label='ceres individual years', zorder=0)
    axes[0].set_title('LW', fontsize=16)
    axes[0].set_xticklabels([])
    axes[0].set_xlabel('')
    axes[0].legend(loc="upper left", frameon=False, fontsize='medium', ncol=3)

    axes[1].fill(long_time, np.append(shading_data['mtnsrf'].min(dim='ensemble'), shading_data['mtnsrf'].max(dim='ensemble')[::-1]),
                 color='lightgrey', alpha=0.6, label='ceres individual years', zorder=0)
    axes[1].set_title('SW', fontsize=16)
    axes[1].set_xticklabels([])
    axes[1].set_xlabel('')

    axes[2].fill(long_time, np.append(shading_data['tnr'].min(dim='ensemble'), shading_data['tnr'].max(dim='ensemble')[::-1]), 
                 color='lightgrey', alpha=0.6, label='ceres individual years', zorder=0)
    axes[2].set_title('net', fontsize=16)

    for i in range(3):
        axes[i].set_ylabel('$W/m^2$')
        axes[i].set_xlim(xlim)
        axes[i].plot(xlim , [0, 0], color='black', linestyle=':')
        axes[i].set_ylim([-ylim, ylim])

    #plt.suptitle('Global mean TOA radiation bias relative to CERES climatology - nextGEMS Cycle 3', fontsize=18)
    plt.suptitle('Global mean TOA radiation bias relative to CERES climatology', fontsize=18)
    
    plt.tight_layout()

    if outputdir is not None:
        create_folder(folder=str(outputdir), loglevel='WARNING')
        # Save the data for each model to separate netCDF files
        for i, model in enumerate(models):
            model_name = linelabels[i].replace(' ', '_').lower()
            start_date = str(model["data"]["time.year"][0].values) +'-'+str(model["data"]["time.month"][0].values)+'-'+str(model["data"]["time.day"][0].values)
            end_date = str(model["data"]["time.year"][-1].values) +'-'+str(model["data"]["time.month"][-1].values)+'-'+str(model["data"]["time.day"][-1].values)
            model["gm"].to_netcdf(f"{outputdir}timeseries_{model_name}_{start_date}_{end_date}.nc")
        logger.info(f"Data has been saved to {outputdir}.")

    if outputfig is not None:
        create_folder(folder=str(outputfig), loglevel='WARNING')
        all_labels = '_'.join(linelabels).replace(' ', '_').lower()
        filename = f"{outputfig}/timeseries_{all_labels}.pdf"
        plt.savefig(filename, dpi=300, format='pdf', bbox_inches="tight")
        logger.info(f"Plot has been saved to {outputfig}.")
    else:
        plt.show()
    
def plot_bias(data=None, iax=None, title=None, plotlevels=None, lower=None, upper=None, index=None):
    """
    Plot the bias of the data on a map. Bias is calculated as the difference between the data and a reference, with stippling applied to highlight data points falling within specified lower and upper thresholds.

    Args:
        data (xarray.DataArray): The model data to plot.
        iax (matplotlib.axes.Axes): The axes object to plot on.
        title (str): The title of the plot.
        plotlevels (int or list): The contour plot levels.
        lower (xarray.DataArray): Lower threshold for stippling.
        upper (xarray.DataArray): Upper threshold for stippling.
        index (int): The index of the data.

    Returns:
        A contour plot representing the bias of the data on the specified axes.
    """

    plot = data.plot(ax=iax,
                     transform=ccrs.PlateCarree(),
                     colors='RdBu_r',
                     linewidths=0.3,
                     levels=plotlevels,
                     add_colorbar=False,
                    )
    stipple_data = data.where(np.logical_and(data > lower.isel(month=index), data < upper.isel(month=index))) / data.where(np.logical_and(data > lower.isel(month=index), data < upper.isel(month=index)))
    #plot2 = stipple_data.plot.contourf(ax=iax, levels=[-10, 0, 10], hatches=["", "...."], add_colorbar=False, alpha=0, transform=ccrs.PlateCarree())

    iax.set_title(title, fontsize=small_fonts)
    # iax.set_title(data.label+' ('+str(len(data.ensemble))+')',fontsize=small_fonts)

    return plot


def plot_maps(model=None, var=None, year=None, model_label=None,  ceres=None, outputdir=None, outputfig=None):
    """
    Plot monthly bias maps of the specified variable and model using a Robinson projection.
    The bias is calculated as the difference between TOA model data and TOA CERES climatology.
    
    Args:
        model (xarray.DataArray): The TOA model data.
        var (str): The variable to plot ('tnr', 'mtnsrf', or 'mtntrf').
        year (int, optional): The year to plot. Defaults to None.
        model_label (str, optional): Desired label for the model (used as the filename to save the figure). Defaults to None.
        ceres (xarray.DataArray): The TOA CERES data to be compared with the model.
        outputdir (str, optional): Directory where the output data will be saved. Defaults to None.
        outputfig (str, optional): Directory where the output figure will be saved. Defaults to None.

    Returns:
        Monthly bias plots of the chosen model, variable, and year.


    """
    samples_tmp= []
    for _year in range(int(ceres["data"]["time.year"][0].values), int(ceres["data"]["time.year"][-1].values)-1):
            # select year and assign (fake) time coordinates so that the differencing works
            samples_tmp.append(ceres["data"].sel(time=str(_year)).assign_coords(time=ceres["clim"].time)- ceres["clim"])
    TOA_ceres_diff_samples = xr.concat(samples_tmp, dim='ensemble')
    TOA_ceres_diff_samples = TOA_ceres_diff_samples.assign_coords(ensemble=range(len(samples_tmp)))
    range_min = TOA_ceres_diff_samples.groupby('time').min(dim='ensemble') #time.month
    range_max = TOA_ceres_diff_samples.groupby('time').max(dim='ensemble')
    if year is None:
        year = int(model["data"]["time.year"][0].values)

    label_dict = {'tnr': 'net', 'mtnsrf': 'SW', 'mtntrf': 'LW'}
    label = label_dict.get(var, None)

    model_label = model["model"]+'_'+model["exp"]+'_'+model["source"] if model_label is None else model_label

    # what to use for significance testing
    # everything between these values will be considered not significant and hatched in the plot
    lower = range_min[var].rename({'time': 'month'})
    upper = range_max[var].rename({'time': 'month'})

    fig, ax = plt.subplots(4, 3, figsize=(22, 10), subplot_kw={'projection': ccrs.Robinson(central_longitude=180, globe=None)})
    global_mean_bias = model["gm"][var].mean().values-ceres["clim_gm"][var].mean().values
    plotlevels = np.arange(-50+global_mean_bias, 51+global_mean_bias, 10)
    global small_fonts
    small_fonts = 12
    axes = ax.flatten()

    data = model["gm"].sel(time=str(year))
    for index in range(len(data.time)):  # some experiments have less than 12 months, draw fewer panels for those
            plot = plot_bias(data [var].isel(time=index)-ceres["clim"][var].isel(time=index),
                            iax=axes[index], title=calendar.month_name[index+1], plotlevels=plotlevels,
                            lower=lower, upper=upper, index=index)

    for axi in axes:
        axi.coastlines(color='black', linewidth=0.5)

    # common colorbar
    fig.subplots_adjust(right=0.95)
    cbar_ax = fig.add_axes([0.96, 0.3, 0.02, 0.4])  # [left, bottom, width, height]
    cbar = fig.colorbar(plot, cax=cbar_ax)
    cbar.ax.tick_params(labelsize=small_fonts)
    cbar.set_label('$W m^{-2}$', labelpad=-32, y=-.08, rotation=0)

    plt.suptitle(label+' TOA bias ' + model_label + ' ' + str(year) + '\nrel. to CERES climatology (2001-2021)', fontsize=small_fonts*2)
    plt.tight_layout()

    if outputdir is not None:
        create_folder(folder=str(outputdir), loglevel='WARNING')
        # Save the data to a netCDF file
        data = model["data"][var].sel(time=str(year)) - ceres["clim"][var].isel(time=0)
        filename = f"{outputdir}toa_bias_maps_{var}_{year}_{model_label}.nc"
        data.to_netcdf(filename)
        logger.info(f"Data has been saved to {outputdir}.")

    if outputfig is not None:
        create_folder(folder=str(outputfig), loglevel='WARNING')
        filename = f"{outputfig}toa_bias_maps_{label}_{year}_{model_label}.pdf"
        plt.savefig(filename, dpi=300, format='pdf', bbox_inches="tight")
        logger.info(f"Plot has been saved to {outputfig}.") 
    else:
        plt.show()

    


def plot_mean_bias(model=None, var=None, model_label=None, ceres=None, start_year=None, end_year=None, outputdir=None, outputfig=None):
    """
    Plot the mean bias of the data over the specified time range and relative to CERES climatology.

    Args:
        model (xarray.Dataset): The model TOA radiation data.
        var (str): The variable to plot (e.g., 'mtnsrf', 'mtntrf', 'tnr').
        model_label (str): The label for the model.
        ceres (float): The CERES TOA radiation climatology.
        start_year (str): The start year of the time range for the model data.
        end_year (str): The end year of the time range for the model data.
        outputdir (str, optional): Directory where the output data will be saved. Defaults to None.
        outputfig (str, optional): Directory where the output figure will be saved. Defaults to None.

    Returns:
        None. Displays the plot of the mean bias.
    """
    # Calculate the mean bias over the specified time range
    if start_year is None or end_year is None:

        start_year = str(model["data"]["time.year"][0].values) if len(model["data"].sel(time=str(model["data"]["time.year"][0].values)).time) == 12 \
                    else str(model["data"]["time.year"][0].values + 1)
        end_year = str(model["data"]["time.year"][-1].values) if len(model["data"].sel(time=str(model["data"]["time.year"][-1].values)).time) == 12 \
                 else str(model["data"]["time.year"][-1].values -1)
    
    mean_bias = (model["data"][var].sel(time=slice(str(start_year), str(end_year))).mean(dim='time') - ceres["clim"][var]).mean(dim='time')
    # Convert masked values to NaN
    mean_bias = mean_bias.where(~mean_bias.isnull(), np.nan)

    # Add cyclic longitude
    mean_bias = add_cyclic_lon(mean_bias)

    model_label = model["model"]+'_'+model["exp"]+'_'+model["source"] if model_label is None else model_label
    
    # Generate the figure
    fig, ax = plt.subplots(subplot_kw={'projection': ccrs.PlateCarree()},
                            figsize=(11, 8.5))

    contour_plot = mean_bias.plot.contourf(ax=ax, transform=ccrs.PlateCarree(), cmap='RdBu_r', levels=20,
                            add_colorbar=False, add_labels=False, extend='both')
    fig.subplots_adjust(bottom=0.25, top=0.9, left=0.05, right=0.95,
                         wspace=0.1, hspace=0.5)
    cbar_ax = fig.add_axes([0.2, 0.15, 0.6, 0.02])
    try:
        fig.colorbar(contour_plot, cax=cbar_ax, orientation='horizontal',
                    label=mean_bias.long_name+' ['+mean_bias.units+']')
    except AttributeError:
        fig.colorbar(contour_plot, cax=cbar_ax, orientation='horizontal')

    ax.coastlines(color='black', linewidth=0.5)
    ax.gridlines(linewidth=0.5)
    ax.set_title(f'{var.upper()} bias of the {model_label.replace("_", " ")} climatology ({start_year} to {end_year})\n relative to the CERES climatology (2001-2021)', fontsize=14)
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    ax.set_xticks(np.arange(-180, 181, 30), crs=ccrs.PlateCarree())
    ax.set_yticks(np.arange(-90, 91, 30), crs=ccrs.PlateCarree())
    ax.tick_params(axis='both', which='both', labelsize=10)
    ax.xaxis.set_ticklabels(['-180°', '-150°', '-120°', '-90°', '-60°', '-30°', '0°', '30°', '60°', '90°', '120°', '150°', '180°'])
    ax.yaxis.set_ticklabels(['-90°', '-60°', '-30°', '0°', '30°', '60°', '90°'])

    if outputdir is not None:
        create_folder(folder=str(outputdir), loglevel='WARNING')
        # Save the data to a netCDF file
        ceres_model_name = ceres["model"]+'_'+ceres["exp"]+'_'+ceres["source"]
        filename = f"{outputdir}toa_mean_biases_{var}_{model_label}_{start_year}_{end_year}_{ceres_model_name}.nc"
        mean_bias.to_netcdf(filename)
        logger.info(f"Data has been saved to {outputdir}.")

    if outputfig is not None:
        create_folder(folder=str(outputfig), loglevel='WARNING')
        ceres_model_name = ceres["model"]+'_'+ceres["exp"]+'_'+ceres["source"]
        filename = f"{outputfig}toa_mean_biases_{var}_{model_label}_{start_year}_{end_year}_{ceres_model_name}.pdf"
        plt.savefig(filename, dpi=300, format='pdf', bbox_inches="tight")
        logger.info(f"Plot has been saved to {outputfig}.")
    else:
        plt.show()

    

