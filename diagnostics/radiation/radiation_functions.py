import xarray as xr
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import cartopy.crs as ccrs
from matplotlib.legend_handler import HandlerTuple
from aqua import Reader
from aqua.util import create_folder, add_cyclic_lon
from aqua.logger import log_configure


def process_ceres_data(exp=None, source=None, fix=True, loglevel='WARNING'):
    """
    Function to extract CERES data for further analyis + create global means

    Args:
        exp (str):   Input experiment to be selected from the catalogue.
        source (str): Input source to be selected from the catalogue.
        fix (bool):  If True, apply the fix to the CERES data. Default is True.
        loglevel (str): The log level for the logger. Default is 'WARNING'.

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
    logger = log_configure(log_level=loglevel, log_name='Process CERES Data')

    # reader_ceres_toa
    if fix is None or fix is False:
        reader = Reader(model='CERES', exp=exp, source=source, regrid='r100', fix=False, loglevel=loglevel)
    else:
        reader = Reader(model='CERES', exp=exp, source=source, regrid='r100', fix=True, loglevel=loglevel)
    data = reader.retrieve()

    data['tnr'] = data['mtnlwrf'] + data['mtnswrf']
    ceres = reader.regrid(data[['tnr', 'mtnlwrf', 'mtnswrf']])

    starting_year = str(ceres["time.year"][0].values) if len(ceres.sel(time=str(ceres["time.year"][0].values)).time) == 12 \
        else str(ceres["time.year"][0].values + 1)
    final_year = str(ceres["time.year"][-1].values) if len(ceres.sel(time=str(ceres["time.year"][-1].values)).time) == 12 \
        else str(ceres["time.year"][-1].values - 1)
    logger.debug(f"Starting year: {starting_year}, Final year: {final_year}")

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


def process_model_data(model=None, exp=None, source=None, fix=True, loglevel='WARNING'):
    """
    Function to extract Model output data for further analysis and create global means.

    Args:
        model (str):   Input model to be selected from the catalogue.
        exp (str):     Input experiment to be selected from the catalogue.
        source (str):  Input source to be selected from the catalogue.
        fix (bool):    If True, apply the fix to the model data. Default is False.
        loglevel (str): The log level for the logger. Default is 'WARNING'.

    Returns:
        dict: A dictionary containing the following information:
            - "model": Model name.
            - "exp": Experiment name.
            - "source": Source name.
            - "gm": Global means of model output data.
            - "data": Model output data.
    """
    if fix is None or fix is False:
        reader = Reader(model=model, exp=exp, source=source,
                        regrid='r100', fix=False, loglevel=loglevel)
    else:
        reader = Reader(model=model, exp=exp, source=source,
                        regrid='r100', fix=True, loglevel=loglevel)

    data = reader.retrieve(var=['2t', 'mtnlwrf', 'mtnswrf'])
    data['tnr'] = data['mtnlwrf'] + data['mtnswrf']
    gm = reader.fldmean(data)

    dictionary = {
        "model": model.lower(),
        "exp": exp.lower(),
        "source": source.lower(),
        "data": data,
        "gm": gm
    }

    return dictionary


def gregory_plot(obs_data=None, models=None, obs_time_range=None, model_labels=None, obs_labels=None,  outputdir=None,
                 outputfig=None, fontsize=14, markersize=3, loglevel='WARNING'):
    """
    Create a Gregory Plot with various models and an observational dataset (e.g., ERA5).

    Args:
        obs_data (dict): Xarray Dataset containing the observational data.
        models (list): A list of models that should be plotted.
        obs_time_range (tuple, optional): A tuple containing the start and end dates
                                          of the time range for the observational data.
                                          Format: ('YYYY-MM-DD', 'YYYY-MM-DD')
        model_labels (list, optional): Labels for the models. If not provided, default labels are used.
        obs_labels (str, optional): Desired label for the observational data.
        outputdir (str, optional): The directory to save the data files.
        outputfig (str, optional): The directory to save the plot as a PDF.
        fontsize (int, optional): Font size for the plot labels and title.
        markersize (int, optional): Size of the markers in the plot.
        loglevel (str, optional): The log level for the logger. Default is 'WARNING'.

    Returns:
        A Gregory Plot displaying the data for each model and the observational data.
    """
    logger = log_configure(log_level=loglevel, log_name='Gregory Plot')

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
        starting_year = str(dummy_model_gm["time.year"][0].values) if \
            len(dummy_model_gm.sel(time=str(dummy_model_gm["time.year"][0].values)).time) == 12 \
            else str(dummy_model_gm["time.year"][0].values + 1)
        final_year = str(dummy_model_gm["time.year"][-1].values) \
            if len(dummy_model_gm.sel(time=str(dummy_model_gm["time.year"][-1].values)).time) == 12 \
            else str(dummy_model_gm["time.year"][-1].values - 1)
        # limit to years that are complete
        obs_data_gm = obs_data["gm"].sel(time=slice(starting_year, final_year))
    else:
        obs_data_gm = obs_data["gm"].sel(time=slice(*obs_time_range))
    obs_2t_resampled = obs_data_gm["2t"].resample(time="M").mean()
    obs_tnr_resampled = obs_data_gm["tnr"].resample(time="M").mean()

    if obs_labels is None:
        if obs_time_range is None:
            obs_labels = obs_data["model"]+' '+obs_data["exp"]+' '+obs_data["source"] + ', ('+starting_year+'-'+final_year+')'
        else:
            obs_labels = obs_data["model"]+' '+obs_data["exp"]+' '+obs_data["source"] + f', ({obs_time_range[0]}\
                  - {obs_time_range[1]})'
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
    ax.text(0.5, -0.15,
            "Black stars indicate the first value of the dataseries\nRed X indicate the last value of the dataseries.",
            transform=ax.transAxes, fontsize=fontsize-6, verticalalignment='top', horizontalalignment='center')
    ax.tick_params(axis="both", which="major", labelsize=10)

    ax.grid(True, linestyle="--", linewidth=0.5)

    if outputdir is not None:
        create_folder(folder=str(outputdir), loglevel='WARNING')
        for model in models:
            start_date = str(model["data"]["time.year"][0].values) + '-' + str(model["data"]["time.month"][0].values) +\
                '-' + str(model["data"]["time.day"][0].values)
            end_date = str(model["data"]["time.year"][-1].values) + '-' + str(model["data"]["time.month"][-1].values) +\
                '-' + str(model["data"]["time.day"][-1].values)
            model_name = model["model"]+'_'+model["exp"]+'_'+model["source"] if model_labels is None else model_labels[i]
            model_data_resampled = model["gm"].resample(time="M").mean()
            model_data_resampled.to_netcdf(f"{outputdir}/gregory_plot_{model_name}_{start_date}_{end_date}.nc")
            logger.info(f"Data has been saved to {outputdir}.")

    # Save the data for each model to separate netCDF files
    if outputfig is not None:
        create_folder(folder=str(outputfig), loglevel='WARNING')
        for model in models:
            start_date = str(model["data"]["time.year"][0].values) + '-' + str(model["data"]["time.month"][0].values) +\
                '-' + str(model["data"]["time.day"][0].values)
            end_date = str(model["data"]["time.year"][-1].values) + '-' + str(model["data"]["time.month"][-1].values) +\
                '-' + str(model["data"]["time.day"][-1].values)
            model_name = model["model"]+'_'+model["exp"]+'_'+model["source"] if model_labels is None else model_labels[i]
            filename = f"{outputfig}/gregory_plot_{model_name}_{start_date}_{end_date}.pdf"
            plt.savefig(filename, dpi=300, format='pdf', bbox_inches="tight")
            logger.info(f"Plot has been saved to {outputfig}.")
    else:
        plt.show()


def boxplot_model_data(datasets=None, model_names=None, outputdir=None, outputfig=None, year=None,
                       fontsize=14, loglevel='WARNING'):
    """
    Create a boxplot with various models and CERES data. Variables 'mtnlwrf' and 'mtnswrf' are plotted to show imbalances.
    The default mean for CERES data is calculated over the entire time range.

    Args:
        datasets (list of xarray.DataSets): A list of xarray.DataSets to be plotted (e.g., global means).
        model_names (list of str): Your desired naming for the plotting, corresponding to the datasets.
        outputdir (str, optional): Directory where the output data will be saved. Default is None.
        outputfig (str, optional): Directory where the output figure will be saved. Default is None.
        year (int, optional): The year for which the plot is generated.
                              Default is None (calculation for the entire time range).
        fontsize (int, optional): Font size for labels and legends in the plot. Default is 14.
        loglevel (str, optional): The log level for the logger. Default is 'WARNING'.

    Returns:
        A boxplot showing the uncertainty of global mean radiation variables ('mtnlwrf' and 'mtnswrf')
        for different models and CERES data.
    """
    logger = log_configure(log_level=loglevel, log_name='Boxplot Model Data')

    # Set a seaborn color palette
    sns.set_palette("pastel")

    # Initialize a dictionary to store data for the boxplot
    boxplot_data = {'Variables': [], 'Values': [], 'Datasets': []}

    model_names = [dataset["model"] + ' ' + dataset["exp"] + ' ' + dataset["source"] for dataset in datasets]\
        if model_names is None else model_names

    for i in range(0, len(datasets)):
        # Extract values for 'mtnlwrf' and 'mtnswrf' from each dataset
        if year is not None:
            # Select data for the specified year if 'gm' key exists
            if 'gm' in datasets[i]:
                dataset_year = datasets[i]['gm'].sel(time=str(year))
                mtnlwrf_values = -dataset_year["mtnlwrf"].values.flatten()
                mtnswrf_values = dataset_year["mtnswrf"].values.flatten()
            else:
                # Handle the case where 'gm' key is not present in the dictionary
                mtnlwrf_values = []
                mtnswrf_values = []
        else:
            # Use the entire dataset if 'gm' key exists
            if 'gm' in datasets[i]:
                mtnlwrf_values = -datasets[i]['gm']["mtnlwrf"].values.flatten()
                mtnswrf_values = datasets[i]['gm']["mtnswrf"].values.flatten()
            else:
                mtnlwrf_values = []
                mtnswrf_values = []

        # Update the boxplot_data dictionary
        boxplot_data['Variables'].extend(['mtnlwrf'] * len(mtnlwrf_values))
        boxplot_data['Variables'].extend(['mtnswrf'] * len(mtnswrf_values))
        boxplot_data['Values'].extend(mtnlwrf_values)
        boxplot_data['Values'].extend(mtnswrf_values)
        boxplot_data['Datasets'].extend([model_names[i]] * (len(mtnlwrf_values) + len(mtnswrf_values)))

    # Create a DataFrame from the boxplot_data dictionary
    boxplot_df = pd.DataFrame(boxplot_data)

    # Create a boxplot
    ax = sns.boxplot(x='Variables', y='Values', hue='Datasets', data=boxplot_df)

    # Add a legend outside the plot to the right side
    ax.legend(loc='center left', bbox_to_anchor=(1, 0.5), title='Datasets', fontsize=fontsize-2)

    # Set labels and title
    plt.xlabel('Variables', fontsize=fontsize)
    plt.ylabel('Global mean ($W/m^2$)', fontsize=fontsize)
    # plt.ylim(230, 255)
    plt.xticks(rotation=0, fontsize=fontsize-2)
    plt.yticks(fontsize=fontsize-2)

    if year is not None:
        plt.title(
            f"Global Mean TOA radiation for different models ({year})", fontsize=fontsize+2)
    else:
        plt.title("Global Mean TOA radiation for different models", fontsize=fontsize+2)

    if outputdir is not None:
        create_folder(folder=str(outputdir), loglevel='WARNING')
        # Save the data to a NetCDF file
        output_data = xr.Dataset(boxplot_data)
        filename = f"{outputdir}/boxplot_mtnlwrf_mtnswrf_{'_'.join(model_names).replace(' ', '_').lower()}.nc"
        output_data.to_netcdf(filename)
        logger.info(f"Data has been saved to {outputdir}.")

    if outputfig is not None:
        create_folder(folder=str(outputfig), loglevel='WARNING')

        filename = f"{outputfig}/boxplot_mtnlwrf_mtnswrf_{'_'.join(model_names).replace(' ', '_').lower()}.pdf"
        plt.savefig(filename, dpi=300, format='pdf', bbox_inches="tight")
        logger.info(f"Plot has been saved to {outputfig}.")
    else:
        plt.show()


def plot_model_comparison_timeseries(models=None, linelabels=None, ceres=None,
                                     outputdir=None, outputfig=None, ylim=6.5, loglevel='WARNING'):
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
        loglevel (str, optional): The log level for the logger. Default is 'WARNING'.

    Returns:
        A plot to show the model biases for the whole time range.
    """
    logger = log_configure(log_level=loglevel, log_name='Plot Model Comparison Timeseries')

    fig, axes = plt.subplots(3, 1, figsize=(12, 8))
    # Set the Seaborn style (you can choose other styles if needed)
    sns.set_style("darkgrid")
    # Choose a Seaborn color palette (you can select a different one if needed)
    color_palette = sns.color_palette("Set1")  # Change "Set1" to your preferred palette
    # Get a list of colors from the palette
    linecolors = color_palette.as_hex()
    # linecolors = plt.cm.get_cmap('tab10').colors

    if models is None:
        raise ValueError("models cannot be None")
    elif isinstance(models, list):
        pass
    else:  # if models is not a list, convert it to a list
        models = [models]
    if linelabels is None or isinstance(linelabels, list):
        pass
    else:  # if linelabels is not a list, convert it to a list
        linelabels = [linelabels]

    logger.debug(f"Analyzing models: {[model['model']+' '+model['exp']+' '+model['source'] for model in models]}")

    dummy_model_gm = models[0]["gm"]
    starting_year = int(dummy_model_gm["time.year"][0].values)\
        if len(dummy_model_gm.sel(time=str(dummy_model_gm["time.year"][0].values)).time) == 12 \
        else int(dummy_model_gm["time.year"][0].values) + 1
    final_year = int(dummy_model_gm["time.year"][-1].values)\
        if len(dummy_model_gm.sel(time=str(dummy_model_gm["time.year"][-1].values)).time) == 12 \
        else int(dummy_model_gm["time.year"][-1].values) - 1
    years = range(starting_year, final_year+1)
    logger.debug(f"Starting year: {starting_year}, Final year: {final_year}")

    xlim = [pd.to_datetime(str(dummy_model_gm["time.year"][0].values) + '-' + str(dummy_model_gm["time.month"][0].values)
                           + '-' + str(dummy_model_gm["time.day"][0].values)),
            pd.to_datetime(str(dummy_model_gm["time.year"][-1].values) + '-' +
                           str(dummy_model_gm["time.month"][-1].values) + '-' +
                           str(dummy_model_gm["time.day"][-1].values))]

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
            ttr_diff.append(model["gm"].mtnlwrf.sel(time=str(year)) - ceres['clim_gm'].mtnlwrf.values)
            tsr_diff.append(model["gm"].mtnswrf.sel(time=str(year)) - ceres['clim_gm'].mtnswrf.values)
            tnr_diff.append(model["gm"].tnr.sel(time=str(year)) - ceres['clim_gm'].tnr.values)
        # Concatenate the data along the 'time' dimension
        ttr_diff = xr.concat(ttr_diff, dim='time')
        tsr_diff = xr.concat(tsr_diff, dim='time')
        tnr_diff = xr.concat(tnr_diff, dim='time')
        # Plot the data for the current model
        ttr_diff.plot(ax=axes[0], color=linecolors[i], label=linelabels[i], x='time')
        tsr_diff.plot(ax=axes[1], color=linecolors[i], label=linelabels[i], x='time')
        ttr_diff.plot(ax=axes[2], color=linecolors[i], label=linelabels[i], x='time')

    samples_tmp = []
    for year in range(int(ceres["data"]["time.year"][0].values), int(ceres["data"]["time.year"][-1].values)-1):
        # select year and assign (fake) time coordinates so that the differencing works
        samples_tmp.append(ceres["gm"].sel(time=str(year)).assign_coords(time=ceres["clim_gm"].time) - ceres["clim_gm"])
    TOA_ceres_diff_samples_gm = xr.concat(samples_tmp, dim='ensemble')
    shading_data_list = []
    for year in years:
        new_data = TOA_ceres_diff_samples_gm.assign_coords(time=dummy_model_gm.sel(time=str(year)).time)
        shading_data_list.append(new_data)
        shading_data = xr.concat(shading_data_list, dim='time')
        long_time = np.append(shading_data['time'], shading_data['time'][::-1])

    axes[0].fill(long_time, np.append(shading_data['mtnlwrf'].min(dim='ensemble'),
                                      shading_data['mtnlwrf'].max(dim='ensemble')[::-1]),
                 color='lightgrey', alpha=0.6, label='ceres individual years', zorder=0)
    axes[0].set_title('LW', fontsize=16)
    axes[0].set_xticklabels([])
    axes[0].set_xlabel('')
    axes[0].legend(loc="upper left", frameon=False, fontsize='medium', ncol=3)

    axes[1].fill(long_time, np.append(shading_data['mtnswrf'].min(dim='ensemble'),
                                      shading_data['mtnswrf'].max(dim='ensemble')[::-1]),
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
        axes[i].plot(xlim, [0, 0], color='black', linestyle=':')
        axes[i].set_ylim([-ylim, ylim])

    plt.suptitle('Global mean TOA radiation bias relative to CERES climatology', fontsize=18)

    plt.tight_layout()

    if outputdir is not None:
        create_folder(folder=str(outputdir), loglevel='WARNING')
        # Save the data for each model to separate netCDF files
        for i, model in enumerate(models):
            model_name = linelabels[i].replace(' ', '_').lower()
            start_date = str(model["data"]["time.year"][0].values) + '-' + str(model["data"]["time.month"][0].values) +\
                '-'+str(model["data"]["time.day"][0].values)
            end_date = str(model["data"]["time.year"][-1].values) + '-' + str(model["data"]["time.month"][-1].values) +\
                '-'+str(model["data"]["time.day"][-1].values)
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


def plot_mean_bias(model=None, var=None, model_label=None, ceres=None, start_year=None, end_year=None,
                   outputdir=None, outputfig=None, seasons=False, statistics=False,
                   q_lower=0.05, q_upper=0.95, loglevel='WARNING'):
    """
    Plot the mean bias of the data over the specified time range and relative to CERES climatology.

    Args:
        model (xarray.Dataset): The model TOA radiation data.
        var (str): The variable to plot (e.g., 'mtnswrf', 'mtnlwrf', 'tnr').
        model_label (str): The label for the model.
        ceres (float): The CERES TOA radiation climatology.
        start_year (str): The start year of the time range for the model data.
        end_year (str): The end year of the time range for the model data.
        outputdir (str, optional): Directory where the output data will be saved. Defaults to None.
        outputfig (str, optional): Directory where the output figure will be saved. Defaults to None.
        seasons (bool, optional): If True, generate plots for each season (DJF, MAM, JJA, SON). Defaults to False.
        statistics (bool, optional): If True, add stipples where biases do not exceed
                                     the interannual variability of CERES data.
        q_lower (float, optional): Lower bound for statistic calculation. Defaults 0.05
        q_upper (float, optional): Upper bound for statistic calculation. Defaults 0.95
        loglevel (str, optional): The log level for the logger. Default is 'WARNING'.

    Returns:
        None. Displays the plot of the mean bias.
    """
    logger = log_configure(log_level=loglevel, log_name='Plot Mean Bias')

    if start_year is None or end_year is None:

        start_year = str(model["data"]["time.year"][0].values)\
            if len(model["data"].sel(time=str(model["data"]["time.year"][0].values)).time) == 12 \
            else str(model["data"]["time.year"][0].values + 1)
        end_year = str(model["data"]["time.year"][-1].values)\
            if len(model["data"].sel(time=str(model["data"]["time.year"][-1].values)).time) == 12 \
            else str(model["data"]["time.year"][-1].values - 1)
    if seasons:
        # Generate plots for each season
        season_months = {
            'DJF': [12, 1, 2],
            'MAM': [3, 4, 5],
            'JJA': [6, 7, 8],
            'SON': [9, 10, 11],
        }

        # Create a single subplot for all seasons
        fig, axs = plt.subplots(2, 2, subplot_kw={'projection': ccrs.PlateCarree()}, figsize=(12, 10))
        axs = axs.flatten()

        for i, (season, months) in enumerate(season_months.items()):
            # Calculate the mean bias over the specified time range and months
            if season == 'DJF':
                # Include December from the last year and only if it's available
                if 12 in months:
                    months = [12] + months[1:]
                    years = np.arange(int(start_year) - 1, int(end_year) + 1)
                else:
                    years = np.arange(int(start_year), int(end_year) + 1)
            else:
                years = np.arange(int(start_year), int(end_year) + 1)

            # print(f"Season: {season}, Months: {months}, Years: {years}")

            # Extract model data for the specific season
            model_season_data = (
                model["data"][var]
                .sel(time=(model["data"]["time.month"].isin(months)) & (model["data"]["time.year"].isin(years)))
                .mean(dim='time')
            )

            # print(model_season_data)

            # Convert masked values to NaN
            model_season_data = model_season_data.where(~model_season_data.isnull(), np.nan)

            # Extract CERES climatology for the specific season
            ceres_time_month = ceres["clim"]["time"]
            ceres_seasonal_climatology = ceres["clim"][var].sel(
                time=ceres_time_month[ceres_time_month.isin(months)]
            ).mean(dim='time')

            # Calculate the mean bias over the specified time range and months
            mean_bias = model_season_data - ceres_seasonal_climatology

            # Add cyclic longitude
            mean_bias = add_cyclic_lon(mean_bias)

            model_label = model["model"]+'_'+model["exp"]+'_'+model["source"] if model_label is None else model_label

            # model_label_season = f'{model_label}_{season}'

            # Plot on the current subplot
            contour_plot = mean_bias.plot.contourf(ax=axs[i], transform=ccrs.PlateCarree(), cmap='RdBu_r', levels=20,
                                                   add_colorbar=False, add_labels=False, extend='both')

            # Start significance testing

            if statistics:
                # this step calculates the lower and upper bounds based on the statistics of the CERES dataset
                # and then creates a boolean mask to identify where the biases are within these bounds.
                # Stipples are plotted at locations where biases are within the interannual variability.

                # Calculate differences between each year and climatology
                samples_tmp = []
                for year in range(int(ceres["data"]["time.year"][0].values), int(ceres["data"]["time.year"][-1].values) - 1):
                    samples_tmp.append(ceres["gm"].sel(time=str(year)).assign_coords(
                        time=ceres["clim_gm"].time) - ceres["clim_gm"])

                TOA_ceres_diff_samples = xr.concat(samples_tmp, dim='ensemble')
                # Rechunk along the 'ensemble' dimension
                TOA_ceres_diff_samples = TOA_ceres_diff_samples.chunk({'ensemble': -1})
                TOA_ceres_diff_samples['time'] = pd.to_datetime(TOA_ceres_diff_samples['time'].values)

                # Calculate the lower and upper bounds
                lower_bound = TOA_ceres_diff_samples.quantile(q_lower, dim='ensemble')
                upper_bound = TOA_ceres_diff_samples.quantile(q_upper, dim='ensemble')

                # Create a boolean mask where biases are within the bounds
                mask = np.logical_and(mean_bias > lower_bound, mean_bias < upper_bound)

                # Convert the xarray mask to a numpy array
                mask_np = mask.values

                # Plot filled contour plot with stipples where the biases are within the bounds
                mean_bias.plot.contourf(ax=axs[i], transform=ccrs.PlateCarree(), add_colorbar=False,
                                        add_labels=False, levels=20, hatches=["", "...."], cmap='RdBu_r')

                # Stipple non-significant points
                indices = np.argwhere(mask_np)

                if indices.size > 0:
                    y, x = indices[:, 0], indices[:, 1]
                    axs[i].scatter(mean_bias['lon'].values[x], mean_bias['lat'].values[y], marker='.', color='black',
                                   alpha=0.4, transform=ccrs.PlateCarree(), zorder=10)

                note_text = "Stipples indicate non-significant points within\nthe interannual variability bounds of the CERES dataset."  # noqa: E501
                fig.text(0.5, 0.02, note_text, ha='center', va='center', fontsize=7)

            # End significance testing

            # Explicitly convert masked elements to NaN for warning suppression
            contour_plot.collections[0].set_edgecolor("face")

            axs[i].coastlines(color='black', linewidth=0.5)
            axs[i].gridlines(linewidth=0.5)
            axs[i].set_title(f'{var.upper()} bias ({season} of the {model_label}\n climatology {start_year} to {end_year})\n relative to the CERES climatology (2001-2021)',  # noqa: E501
                             fontsize=12)
            axs[i].set_xlabel('Longitude')
            axs[i].set_ylabel('Latitude')
            axs[i].set_xticks(np.arange(-180, 181, 30), crs=ccrs.PlateCarree())
            axs[i].set_yticks(np.arange(-90, 91, 30), crs=ccrs.PlateCarree())
            axs[i].tick_params(axis='both', which='both', labelsize=8)
            axs[i].xaxis.set_ticklabels(
                ['-180°', '-150°', '-120°', '-90°', '-60°', '-30°', '0°', '30°', '60°', '90°', '120°', '150°', '180°'])
            axs[i].yaxis.set_ticklabels(['-90°', '-60°', '-30°', '0°', '30°', '60°', '90°'])

        # Add a colorbar for the entire figure
        cbar_ax = fig.add_axes([0.2, 0.08, 0.6, 0.02])
        fig.colorbar(contour_plot, cax=cbar_ax, orientation='horizontal',
                     label=f'{var.lower()} bias [' + mean_bias.units + ']')
        # Explicitly convert masked elements to NaN for warning suppression
        contour_plot.collections[0].set_edgecolor("face")

        if outputdir is not None:
            create_folder(folder=str(outputdir), loglevel='WARNING')
            # Save the data to a netCDF file
            ceres_model_name = ceres["model"] + '_' + ceres["exp"] + '_' + ceres["source"]
            filename = f"{outputdir}toa_mean_biases_{var}_{model_label}_{start_year}_{end_year}_{ceres_model_name}_seasons.pdf"
            plt.savefig(filename, dpi=300, format='pdf', bbox_inches="tight")
            logger.info(f"Data has been saved to {outputdir}.")

        if outputfig is not None:
            create_folder(folder=str(outputfig), loglevel='WARNING')
            ceres_model_name = ceres["model"] + '_' + ceres["exp"] + '_' + ceres["source"]
            filename = f"{outputfig}toa_mean_biases_{var}_{model_label}_{start_year}_{end_year}_{ceres_model_name}_seasons.pdf"
            plt.savefig(filename, dpi=300, format='pdf', bbox_inches="tight")
            logger.info(f"Plot has been saved to {outputfig}.")
        else:
            plt.show()
    else:  # no seasons
        # Extract model data for the entire time series
        model_data = model["data"][var].sel(time=slice(str(start_year), str(end_year)))

        mean_bias = (model_data.mean(dim='time') - ceres["clim"][var]).mean(dim='time')
        # Convert masked values to NaN
        mean_bias = mean_bias.where(~mean_bias.isnull(), np.nan)
        mean_bias = add_cyclic_lon(mean_bias)

        model_label = model["model"]+'_'+model["exp"]+'_'+model["source"] if model_label is None else model_label

        fig, ax = plt.subplots(subplot_kw={'projection': ccrs.PlateCarree()}, figsize=(8, 6))

        # Plot mean biases
        contour_plot = mean_bias.plot.contourf(ax=ax, transform=ccrs.PlateCarree(), cmap='RdBu_r', levels=20,
                                               add_colorbar=False, add_labels=False, extend='both')

        # Start significance testing

        if statistics:
            # This step calculates the lower and upper bounds based on the statistics of the CERES dataset
            # and then creates a boolean mask to identify where the biases are within these bounds.
            # Stipples are plotted at locations where biases are within the interannual variability.

            # Calculate differences between each year and climatology
            samples_tmp = []
            for year in range(int(ceres["data"]["time.year"][0].values), int(ceres["data"]["time.year"][-1].values) - 1):
                samples_tmp.append(ceres["gm"].sel(time=str(year)).assign_coords(
                    time=ceres["clim_gm"].time) - ceres["clim_gm"])

            TOA_ceres_diff_samples = xr.concat(samples_tmp, dim='ensemble')

            # Rechunk along the 'ensemble' dimension
            TOA_ceres_diff_samples = TOA_ceres_diff_samples.chunk({'ensemble': -1})
            TOA_ceres_diff_samples['time'] = pd.to_datetime(TOA_ceres_diff_samples['time'].values)

            # Calculate the lower and upper bounds
            lower_bound = TOA_ceres_diff_samples.quantile(q_lower, dim='ensemble')
            upper_bound = TOA_ceres_diff_samples.quantile(q_upper, dim='ensemble')

            # Create a boolean mask where biases are within the bounds
            mask = np.logical_and(mean_bias > lower_bound, mean_bias < upper_bound)

            # Convert the xarray mask to a numpy array
            mask_np = mask.values

            # Plot filled contour plot with stipples where the biases are within the bounds
            mean_bias.plot.contourf(ax=ax, transform=ccrs.PlateCarree(), add_colorbar=False, add_labels=False,
                                    levels=20, hatches=["", "...."], cmap='RdBu_r')

            # Stipple non-significant points
            indices = np.argwhere(mask_np)

            if indices.size > 0:
                y, x = indices[:, 0], indices[:, 1]
                ax.scatter(mean_bias['lon'].values[x], mean_bias['lat'].values[y], marker='.', color='black', alpha=0.4,
                           transform=ccrs.PlateCarree(), zorder=10)

            note_text = "Stipples indicate non-significant points within\nthe interannual variability bounds of the CERES dataset."  # noqa: E501
            fig.text(0.5, 0.05, note_text, ha='center', va='center', fontsize=7)

            # End significance testing

        fig.subplots_adjust(bottom=0.25, top=0.9, left=0.05, right=0.95,
                            wspace=0.1, hspace=0.5)
        cbar_ax = fig.add_axes([0.2, 0.15, 0.6, 0.02])
        try:
            fig.colorbar(contour_plot, cax=cbar_ax, orientation='horizontal',
                         label=mean_bias.long_name+' bias ['+mean_bias.units+']')
        except AttributeError:
            fig.colorbar(contour_plot, cax=cbar_ax, orientation='horizontal')

        ax.coastlines(color='black', linewidth=0.5)
        ax.gridlines(linewidth=0.5)
        ax.set_title(f'{var.upper()} bias of the {model_label.replace("_", " ")} climatology ({start_year} to {end_year})\n relative to the CERES climatology (2001-2021)', fontsize=14)  # noqa: E501
        ax.set_xlabel('Longitude')
        ax.set_ylabel('Latitude')
        ax.set_xticks(np.arange(-180, 181, 30), crs=ccrs.PlateCarree())
        ax.set_yticks(np.arange(-90, 91, 30), crs=ccrs.PlateCarree())
        ax.tick_params(axis='both', which='both', labelsize=10)
        ax.xaxis.set_ticklabels(['-180°', '-150°', '-120°', '-90°', '-60°', '-30°',
                                 '0°', '30°', '60°', '90°', '120°', '150°', '180°'])
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
