import os
import xarray as xr
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import calendar
from cdo import Cdo
import cartopy.crs as ccrs
from matplotlib.legend_handler import HandlerTuple
from aqua import Reader
import matplotlib.gridspec as gridspec

outputfig = "./output/figs/"
if not os.path.exists(outputfig):
    os.makedirs(outputfig)
outputdir = "./output/data/"
if not os.path.exists(outputdir):
    os.makedirs(outputdir)
cdo = Cdo(tempdir='./tmp/cdo-py')
tempdir='./tmp/cdo-py'
if not os.path.exists(tempdir):
    os.makedirs(tempdir)


def process_ceres_data(exp, source, TOA_icon_gm):
    """
    Extract CERES data for further analyis + create global means

    Args:
        exp:                            input experiment to be selected from the catalogue
        source:                         input source to be selected from the catalogue
        TOA_icon_gm:                    this is necessary to setting time axis to the same time axis as model output (modify if needed)

    Returns:
        TOA_ceres_clim_gm:
        TOA_ceres_ebaf_gm: 
        TOA_ceres_diff_samples_gm:
        reader_ceres_toa,
        TOA_ceres_clim,
        TOA_ceres_diff_samples: # returns the necessary ceres data for further evaluation
    """

    # reader_ceres_toa
    reader_ceres_toa = Reader(model='CERES', exp=exp, source=source)
    data_ceres_toa = reader_ceres_toa.retrieve(fix=True)

    # ceres_ebaf_ttr
    ceres_ebaf_ttr = data_ceres_toa.toa_lw_all_mon * -1

    # ceres_ebaf_tsr
    ceres_ebaf_tsr = data_ceres_toa.solar_mon - data_ceres_toa.toa_sw_all_mon

    # ceres_ebaf_tnr
    ceres_ebaf_tnr = data_ceres_toa.toa_net_all_mon

    # TOA_ceres_ebaf
    TOA_ceres_ebaf = ceres_ebaf_tsr.to_dataset(name='tsr')
    TOA_ceres_ebaf = TOA_ceres_ebaf.assign(ttr=ceres_ebaf_ttr)
    TOA_ceres_ebaf = TOA_ceres_ebaf.assign(tnr=ceres_ebaf_tnr)

    # limit to years that are complete
    TOA_ceres_ebaf = TOA_ceres_ebaf.sel(time=slice('2001', '2021'))

    # TOA_ceres_clim
    TOA_ceres_clim = TOA_ceres_ebaf.groupby('time.month').mean('time').rename({'month': 'time'}).assign_coords(time=TOA_icon_gm.sel(time='2020').time).transpose("time", ...)
    # TOA_ceres_clim = TOA_ceres_clim.assign_coords(time=TOA_icon_gm.sel(time='2020').time).transpose("time", ...)

    # TOA_ceres_clim_gm
    TOA_ceres_clim_gm = reader_ceres_toa.fldmean(TOA_ceres_clim)  # cdo.fldmean(input=TOA_ceres_clim, returnXDataset=True)

    # TOA_ceres_ebaf_gm
    TOA_ceres_ebaf_gm = reader_ceres_toa.fldmean(TOA_ceres_ebaf)  # cdo.fldmean(input=TOA_ceres_ebaf, returnXDataset=True)

    # samples_tmp
    samples_tmp = []
    for year in range(2001, 2021):
        # select year and assign (fake) time coordinates of 2020 so that the differencing works
        samples_tmp.append(TOA_ceres_ebaf.sel(time=str(year)).assign_coords(time=TOA_ceres_clim.time) - TOA_ceres_clim)

    # TOA_ceres_diff_samples
    TOA_ceres_diff_samples = xr.concat(samples_tmp, dim='ensemble').transpose("time", ...)

    # TOA_ceres_diff_samples_gm
    TOA_ceres_diff_samples_gm = reader_ceres_toa.fldmean(TOA_ceres_diff_samples)  # cdo.fldmean(input=TOA_ceres_diff_samples, returnXDataset=True).squeeze()
    TOA_ceres_clim['lon'] = TOA_ceres_clim['lon'] - 0.5
    TOA_ceres_diff_samples['lon'] = TOA_ceres_diff_samples['lon'] - 0.5
    
    return TOA_ceres_clim_gm, TOA_ceres_ebaf_gm, TOA_ceres_diff_samples_gm, reader_ceres_toa, TOA_ceres_clim, TOA_ceres_diff_samples

#-------------------------------------------------------------------------------------------------------------------------------------

def process_model_data(model, exp, source):
    """
    Extract Model output data for further analyis + create global means
    Example: TOA_ifs_4km_gm, reader_ifs_4km, data_ifs_4km, TOA_ifs_4km, TOA_ifs_4km_r360x180 = radiation_diag.process_model_data(model =  'IFS' , exp = 'tco2559-ng5-cycle3' , source = 'lra-r100-monthly')

    Args:
        model:                          input model to be selected from the catalogue
        exp:                            input experiment to be selected from the catalogue
        source:                         input source to be selected from the catalogue

    Returns:
        TOA_model_gm, reader_model, data_model, TOA_model, TOA_model_r360x180:       returns the necessary ceres data for further evaluation (xarrayDatSet, -Array, reader, regridded DataSet and Global                                                                                           means
    """

    reader_model = Reader(model=model, exp=exp, source=source)
    data_model = reader_model.retrieve()

    # Combine radiation data into a dataset
    TOA_model = data_model['mtntrf'].to_dataset()
    TOA_model = TOA_model.assign(tnr=data_model['mtntrf'] + data_model['mtnsrf'])
    TOA_model = TOA_model.assign(ttr=data_model['mtntrf'])
    TOA_model = TOA_model.assign(tsr=data_model['mtnsrf'])

    # Compute global mean using cdo.fldmean
    TOA_model_gm = reader_model.fldmean(TOA_model)  # cdo.fldmean(input=TOA_model, returnXDataset=True)
    TOA_model_r360x180 = cdo.remapcon('r360x180', input=TOA_model, returnXDataset=True)
   
    return TOA_model_gm, reader_model, data_model, TOA_model, TOA_model_r360x180

#-------------------------------------------------------------------------------------------------------------------------------------

def process_era5_data(exp, source):
    """
    Extract ERA5 data for further analyis
    Example: data_era5, reader_era5 = radiation_diag.process_era5_data(exp = "era5" , source = "monthly")

    Args:
        exp:                            input experiment to be selected from the catalogue
        source:                         input source to be selected from the catalogue

    Returns:
        data_era5, reader_era5:       returns the necessary ceres data for further evaluation (xarrayDatSet, reader)
    """

    reader_era5 = Reader(model="ERA5", exp=exp, source=source)
    data_era5 = reader_era5.retrieve(fix=True)
    return data_era5, reader_era5

#-------------------------------------------------------------------------------------------------------------------------------------

def process_ceres_sfc_data(exp, source, TOA_icon_gm):
    """
    Extract surface CERES data for further analyis + create global means

    Args:
        exp:                            input experiment to be selected from the catalogue
        source:                         input source to be selected from the catalogue
        TOA_icon_gm:                    this is necessary to setting time axis to the same time axis as model output (modify if needed)

    Returns:
        ceres_clim_gm_sfc, ceres_ebaf_gm_sfc, ceres_diff_samples_gm_sfc, reader_ceres_sfc, ceres_clim_sfc, ceres_diff_samples_sfc:       returns the necessary ceres data for further evaluation
    """

    # reader_ceres_toa
    reader_ceres_sfc = Reader(model='CERES', exp=exp, source=source)
    data_ceres_sfc = reader_ceres_sfc.retrieve(fix=True)

    # ceres_ebaf_ttr
    ceres_ebaf_ttr_sfc = data_ceres_sfc.sfc_net_lw_all_mon * -1

    # ceres_ebaf_tsr
    ceres_ebaf_tsr_sfc = data_ceres_sfc.sfc_net_sw_all_mon

    # ceres_ebaf_tnr
    ceres_ebaf_tnr_sfc = data_ceres_sfc.sfc_net_tot_all_mon

    # TOA_ceres_ebaf
    ceres_ebaf_sfc = ceres_ebaf_tsr_sfc.to_dataset(name='tsr')
    ceres_ebaf_sfc = ceres_ebaf_sfc.assign(ttr=ceres_ebaf_ttr_sfc)
    ceres_ebaf_sfc = ceres_ebaf_sfc.assign(tnr=ceres_ebaf_tnr_sfc)

    # limit to years that are complete
    ceres_ebaf_sfc = ceres_ebaf_sfc.sel(time=slice('2001', '2021'))

    # TOA_ceres_clim
    ceres_clim_sfc = ceres_ebaf_sfc.groupby('time.month').mean('time').rename({'month': 'time'}).assign_coords(time=TOA_icon_gm.sel(time='2020').time).transpose("time", ...)
    # TOA_ceres_clim = TOA_ceres_clim.assign_coords(time=TOA_icon_gm.sel(time='2020').time).transpose("time", ...)

    # TOA_ceres_clim_gm
    ceres_clim_gm_sfc = reader_ceres_sfc.fldmean(ceres_clim_sfc)  # cdo.fldmean(input=TOA_ceres_clim, returnXDataset=True)

    # TOA_ceres_ebaf_gm
    ceres_ebaf_gm_sfc = reader_ceres_sfc.fldmean(ceres_ebaf_sfc)  # cdo.fldmean(input=TOA_ceres_ebaf, returnXDataset=True)

    # samples_tmp
    samples_tmp = []
    for year in range(2001, 2021):
        # select year and assign (fake) time coordinates of 2020 so that the differencing works
        samples_tmp.append(ceres_ebaf_sfc.sel(time=str(year)).assign_coords(time=ceres_clim_sfc.time) - ceres_clim_sfc)

    # TOA_ceres_diff_samples
    ceres_diff_samples_sfc = xr.concat(samples_tmp, dim='ensemble').transpose("time", ...)

    # TOA_ceres_diff_samples_gm
    ceres_diff_samples_gm_sfc = reader_ceres_sfc.fldmean(ceres_diff_samples_sfc)  # cdo.fldmean(input=TOA_ceres_diff_samples, returnXDataset=True).squeeze()
    ceres_clim_sfc['lon'] = ceres_clim_sfc['lon'] - 0.5
    ceres_diff_samples_sfc['lon'] = ceres_diff_samples_sfc['lon'] - 0.5

    return ceres_clim_gm_sfc, ceres_ebaf_gm_sfc, ceres_diff_samples_gm_sfc, reader_ceres_sfc, ceres_clim_sfc, ceres_diff_samples_sfc

#-------------------------------------------------------------------------------------------------------------------------------------

def gregory_plot(obs_data, obs_reader, obs_time_range, model_label_obs, model_list, reader_dict):
    """
    Create Gregory Plot with various models and an observational dataset (e.g., ERA5).

    Args:
        obs_data (xarray.Dataset): Xarray Dataset containing the observational data.
        obs_reader: corresponding reader object for the observational dataset
        obs_time_range (tuple): A tuple containing the start and end dates of the time range for the observational data.
                                Format: ('YYYY-MM-DD', 'YYYY-MM-DD')
        model_list (list): A list of models that should be plotted.
        reader_dict (dict): A dictionary mapping model names to corresponding reader objects.
        model_label_obs (str): Desired label for the observational data.

    Returns:
        A Gregory Plot.

    """

    # Create the plot and axes
    _, ax = plt.subplots()
    # Colors for the plots
    colors = ["orange", "gray", "dodgerblue", "yellow", "indigo", "violet"]

    # Plot the data for each model
    handles = []
    labels = []

    # Plot the data for observation
    obs_2t = obs_data["2t"].sel(time=slice(*obs_time_range))
    obs_tsr = obs_data["mtnsrf"].sel(time=slice(*obs_time_range))
    obs_ttr = obs_data["mtntrf"].sel(time=slice(*obs_time_range))

    obs_2t_resampled = obs_2t.resample(time="M").mean()
    obs_tsr_resampled = obs_tsr.resample(time="M").mean()
    obs_ttr_resampled = obs_ttr.resample(time="M").mean()

    # Plot the data
    line = ax.plot(
        obs_reader.fldmean(obs_2t_resampled) - 273.15,
        obs_reader.fldmean(obs_tsr_resampled) + obs_reader.fldmean(obs_ttr_resampled),
        marker="o",
        color="mediumseagreen",
        linestyle="-",
        markersize=3,
        label=model_label_obs,
    )
    handles.append(line[0])  # Append the line object itself
    labels.append(model_label_obs)

    for i, model in enumerate(model_list):
        model_name = model.lower()
        model_reader = reader_dict[model_name]
        model_data = model_reader.retrieve(fix=True)
        model_color = colors[i % len(colors)]  # Rotate colors for each model

        ts = model_reader.fldmean(model_data["2t"].resample(time="M").mean()) - 273.15
        rad = model_reader.fldmean((model_data["mtnsrf"] + model_data["mtntrf"]).resample(time="M").mean())

        line, = ax.plot(
            ts,
            rad,
            color=model_color,
            linestyle="-",
            marker="o",
            markersize=5,
            label=model_name
        )
        handles.append(line)  # Append the line object itself
        labels.append(model_name)

        ax.plot(
            ts[0], rad[0],
            marker="*",
            color="black",
            linestyle="-",
            markersize=15,
        )
        ax.plot(
            ts[-1], rad[-1],
            marker="X",
            color="tab:red",
            linestyle="-",
            markersize=15,
        )

    # Set labels and title
    ax.set_xlabel("2m temperature [$^{\circ} C$]", fontsize=12)
    ax.set_ylabel("Net radiation TOA [Wm$^{-2}$]", fontsize=12)
    ax.set_title("Gregory Plot", fontsize=14)
    ax.legend(handles, labels + ["Start", "End"], handler_map={tuple: HandlerTuple(ndivide=None)})
    ax.text(0.5, -0.15, "Black stars indicate the first value of the dataseries\nRed X indicate the last value of the dataseries.",
            transform=ax.transAxes, fontsize=8, verticalalignment='top', horizontalalignment='center')
    ax.tick_params(axis="both", which="major", labelsize=10)
    ax.grid(True, linestyle="--", linewidth=0.5)

    
    
    # Save the data for each model to separate netCDF files
    for model in model_list:
        model_name = model.lower()
        model_reader = reader_dict[model_name]
        model_data = model_reader.retrieve(fix=True)

        model_data_resampled = model_data.resample(time="M").mean()
        model_data_resampled.to_netcdf(f"{outputdir}/Gregory_Plot_{model_name}.nc")
        filename = f"{outputfig}/Gregory_Plot_{model_name}.pdf"
        plt.savefig(filename, dpi=300, format='pdf')
        
    plt.show()
    print(f"Data has been saved to {outputdir}.")
    print(f"Plot has been saved to {outputfig}.")


#-------------------------------------------------------------------------------------------------------------------------------------

def barplot_model_data(datasets, model_names, year=None):
    """
    Create Bar Plot with various models and CERES. Variables ttr and tsr are plotted to show imbalances.
    Default mean for CERES data is the whole time range.

    Args:
        datasets:      a list of xarrayDataSets that should be plotted. Chose the global means (TOA_$model_gm)
        model_names:   your desired naming for the plotting
        year:          the year for which the plot is generated (optional)

    Returns:
        A bar plot

    Example:

    .. code-block:: python

        datasets = [TOA_ceres_clim_gm, TOA_icon_gm, TOA_ifs_4km_gm, TOA_ifs_9km_gm]
        model_names = ['ceres', 'ICON', 'IFS 4.4 km', 'IFS 9 km']
        barplot_model_data(datasets, model_names, year = 2022)
    """

    colors = ['red', 'blue']  # Longwave (lw) in red, Shortwave (sw) in blue
    variables = ['ttr', 'tsr']  # Fixed order: ttr (lw), tsr (sw)
    plt.figure(figsize=(12, 5))

    global_means = {}

    for i, dataset in enumerate(datasets):
        model_name = model_names[i]

        if model_name != 'ceres':
            if year is not None:
                dataset = dataset.sel(time=str(year))

        for j, variable in enumerate(variables):
            global_mean = dataset[variable].mean().compute()  # Convert to NumPy array

            if variable == 'ttr':
                global_mean *= -1  # Apply the sign (-1) to ttr

            plt.bar(f"{model_name}_{variable}", global_mean, color=colors[j])
            global_means[f"{model_name}_{variable}"] = global_mean

    plt.xlabel('Model')
    plt.ylabel('Global mean ($W/m^2$)')
    # Add legend with colored text
    legend_labels = ['ttr = net outgoing longwave (top thermal radiadion)', 'tsr = net incomming shortwave (total solar radiation)']
    legend_colors = ['red', 'blue']
    legend_handles = [plt.Rectangle((0, 0), 1, 1, color=color) for color in legend_colors]
    plt.legend(legend_handles, legend_labels, loc='upper right', facecolor='white', framealpha=1)
    plt.ylim(236, 250)
    if year is not None:
        plt.title(f"Global Mean TOA radiation for different models ({year}) (all CERES years from 2001 to 2021)")
    else:
        plt.title('Global Mean TOA radiation for different models (mean over all model times)')
    filename = f"{outputfig}/BarPlot.pdf"
    plt.savefig(filename, dpi=300, format='pdf')
    plt.show()

    # Save the data to a NetCDF file
    output_data = xr.Dataset(global_means)
    filename = f"{outputdir}/BarPlot.nc"
    output_data.to_netcdf(filename)

    print(f"Data has been saved to {outputdir}.")
    print(f"Plot has been saved to {outputfig}.")

#-------------------------------------------------------------------------------------------------------------------------------------

def plot_model_comparison_timeseries(models, linelabels, TOA_ceres_diff_samples_gm, TOA_ceres_clim_gm):
                        
    """
    Create time series bias plot with various models and CERES including the individual CERES years to show variabilities. 
    Variables ttr, tsr and tnr are plotted to show imbalances. Default mean for CERES data is the whole time range. 
    Example:
            models = [TOA_icon_gm.squeeze(), TOA_ifs_4km_gm.squeeze(), TOA_ifs_9km_gm.squeeze()]
            linelabels = ['ICON 5 km', 'IFS 4.4 km', 'IFS 9 km']
            radiation_diag.plot_model_comparison_timeseries(models, linelabels, TOA_ceres_diff_samples_gm, TOA_ceres_clim_gm)

    Args:
        models:                      a list of xarrayDataSets of the respective models. You can use squeeze() to remove single-dimensional entries from an array
                                         to ensure that the input arrays have the same dimensions and shape
        line_labels:                 your desired naming for the plotting (this will also be used in the filename)       
            
      
    Returns:
        A plot to show the model biases for the whole time range
    """
        
    fig, axes = plt.subplots(3, 1, figsize=(12, 8))
    linecolors = plt.cm.get_cmap('tab10').colors
    shading_data = xr.concat(
        (
            TOA_ceres_diff_samples_gm,
            TOA_ceres_diff_samples_gm.assign_coords(time=models[0].ttr.sel(time='2021').time),
            TOA_ceres_diff_samples_gm.assign_coords(time=models[0].ttr.sel(time='2022').time),
            TOA_ceres_diff_samples_gm.assign_coords(time=models[0].ttr.sel(time='2023').time),
            TOA_ceres_diff_samples_gm.assign_coords(time=models[0].ttr.sel(time='2024').time),
        ),
        dim='time'
    )
    long_time = np.append(shading_data['time'], shading_data['time'][::-1])
    
    #----------------------------- ttr-----------------------------
    for i, model in enumerate(models):
        ttr_diff = xr.concat(
            (
                (model.ttr.sel(time='2020').squeeze() - TOA_ceres_clim_gm.squeeze().ttr.values),
                (model.ttr.sel(time='2021').squeeze() - TOA_ceres_clim_gm.squeeze().ttr.values),
                (model.ttr.sel(time='2022').squeeze() - TOA_ceres_clim_gm.squeeze().ttr.values),
                (model.ttr.sel(time='2023').squeeze() - TOA_ceres_clim_gm.squeeze().ttr.values),
                (model.ttr.sel(time='2024').squeeze() - TOA_ceres_clim_gm.squeeze().ttr.values),
            ),
            dim='time'
        )
        ttr_diff.plot(ax=axes[0], color=linecolors[i], label=linelabels[i], x='time')

    axes[0].fill(long_time, np.append(shading_data['ttr'].min(dim='ensemble'), shading_data['ttr'].max(dim='ensemble')[::-1]), color='lightgrey', alpha=0.6, label='CERES individual years', zorder=0)
    axes[0].set_title('LW', fontsize=16)
    axes[0].set_xticklabels([])
    axes[0].set_xlabel('')
    axes[0].legend(loc="upper left", frameon=False, fontsize='medium', ncol=3)
    
    #----------------------------- tsr-----------------------------
    
    for i, model in enumerate(models):
        tsr_diff = xr.concat(
            (
                (model.tsr.sel(time='2020') - TOA_ceres_clim_gm.squeeze().tsr.values),
                (model.tsr.sel(time='2021') - TOA_ceres_clim_gm.squeeze().tsr.values),
                (model.tsr.sel(time='2022') - TOA_ceres_clim_gm.squeeze().tsr.values),
                (model.tsr.sel(time='2023') - TOA_ceres_clim_gm.squeeze().tsr.values),
                (model.tsr.sel(time='2024') - TOA_ceres_clim_gm.squeeze().tsr.values),
            ),
            dim='time'
        )
        tsr_diff.plot(ax=axes[1], color=linecolors[i], label=linelabels[i], x='time')

    axes[1].fill(long_time, np.append(shading_data['tsr'].min(dim='ensemble'), shading_data['tsr'].max(dim='ensemble')[::-1]), color='lightgrey', alpha=0.6, label='CERES individual years', zorder=0)
    axes[1].set_title('SW', fontsize=16)
    axes[1].set_xticklabels([])
    axes[1].set_xlabel('')
    
    #----------------------------- tnr-----------------------------
    
    for i, model in enumerate(models):
        tnr_diff = xr.concat(
            (
                (model.tnr.sel(time='2020') - TOA_ceres_clim_gm.squeeze().tnr.values),
                (model.tnr.sel(time='2021') - TOA_ceres_clim_gm.squeeze().tnr.values),
                (model.tnr.sel(time='2022') - TOA_ceres_clim_gm.squeeze().tnr.values),
                (model.tnr.sel(time='2023') - TOA_ceres_clim_gm.squeeze().tnr.values),
                (model.tnr.sel(time='2024') - TOA_ceres_clim_gm.squeeze().tnr.values),
            ),
            dim='time'
        )
        tnr_diff.plot(ax=axes[2], color=linecolors[i], label=linelabels[i], x='time')

    axes[2].fill(long_time, np.append(shading_data['tnr'].min(dim='ensemble'), shading_data['tnr'].max(dim='ensemble')[::-1]), color='lightgrey', alpha=0.6, label='CERES individual years', zorder=0)
    axes[2].set_title('net', fontsize=16)

    for i in range(3):
        axes[i].set_ylabel('$W/m^2$')
        axes[i].set_xlim([pd.to_datetime('2020-01-15'), pd.to_datetime('2024-12-15')])
        axes[i].plot([pd.to_datetime('2020-01-01'), pd.to_datetime('2030-12-31')], [0, 0], color='black', linestyle=':')
        axes[i].set_ylim([-6.5, 6.5])

    plt.suptitle('Global mean TOA radiation bias relative to CERES climatology - nextGEMS Cycle 3', fontsize=18)
    filename = f"{outputfig}/TimeSeries_{linelabels}.pdf"
    plt.savefig(filename, dpi=300, format='pdf')
    plt.tight_layout()
    plt.show()

    # Save the data for each model to separate netCDF files
    for i, model in enumerate(models):
        model_name = linelabels[i].replace(' ', '_').lower()
        model.to_netcdf(f"{outputdir}Timeseries_{linelabels}.nc")

    print(f"Data has been saved to {outputdir}.")
    print(f"Plot has been saved to {outputfig}.")
    
#-------------------------------------------------------------------------------------------------------------------------------------

def plot_bias(data, iax, title, plotlevels, lower, upper, index):
    """
    Plot the bias of the data on a map. The bias is calculated as the difference between the data and a reference and stippling is applied to highlight data points within the specified lower and upper thresholds.

    Args:
        data (xarray.DataArray):            The model data to plot.
        iax (matplotlib.axes.Axes):         The axes object to plot on.
        title (str):                        The title of the plot.
        plotlevels (int or list):           The levels for contour plot.
        lower (xarray.DataArray):           Lower threshold for stippling.
        upper (xarray.DataArray):           Upper threshold for stippling.
        index (int):                        The index of the data.

    Returns:
        A contour plot.

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


def plot_maps(TOA_model, var, model_label, TOA_ceres_diff_samples, TOA_ceres_clim, year='2020'):
    """
    The function plots maps of TOA bias for the specified variable and model using a Robinson projection.
    The TOA bias is calculated as the difference between the TOA model data and the TOA CERES climatology. Default year is 2020

    Args:
        TOA_model (xarray.DataArray):                The TOA model data.
        var (str):                                   The variable to plot ('tnr', 'tsr', or 'ttr').
        model_label (str):                           Desired label for the model (also used as filename to save figure, better avoid using characters like ' ',...)
        TOA_ceres_diff_samples (xarray.DataArray):   The TOA CERES difference samples data.
        TOA_ceres_clim (xarray.DataArray):           The TOA CERES climatology data.
        year (str, optional):                        The year to plot. Defaults to '2020'.

    Returns:
        Monthly bias plots of the chosen model, variable and year

    Example:

        plot_maps(TOA_model= TOA_ifs_4km_r360x180, TOA_ceres_diff_samples = TOA_ceres_diff_samples, TOA_ceres_clim = TOA_ceres_clim, var='tsr', model_label='Cycle 3 4.4 km IFS Fesom', year='2023')
        # Use the TOA_model_r360x180 DataSet to ensure that the gridding is right
        
    """

    # quantiles are a bit more conservative than the range, but interpolation from few values might not be robust
    # q05 = TOA_ceres_diff_samples.groupby('time.month').quantile(0.05,dim='ensemble')
    # q95 = TOA_ceres_diff_samples.groupby('time.month').quantile(0.95,dim='ensemble')
    # min-max range is more conservative, but could be sensitive to single extreme years
    range_min = TOA_ceres_diff_samples.groupby('time.month').min(dim='ensemble')
    range_max = TOA_ceres_diff_samples.groupby('time.month').max(dim='ensemble')
    TOA_model = TOA_model.sel(time=year)
    if var == 'tnr':
        label = 'net'
    elif var == 'tsr':
        label = 'SW'
    elif var == 'ttr':
        label = 'LW'

    if year == '2020':
        panel_1_off = True
    else:
        panel_1_off = False

    # what to use for significance testing
    # everything between these values will be considered not significant and hatched in the plot
    lower = range_min[var].rename({'time': 'month'})
    upper = range_max[var].rename({'time': 'month'})

    fig, ax = plt.subplots(4, 3, figsize=(22, 10), subplot_kw={'projection': ccrs.Robinson(central_longitude=180, globe=None)})
    plotlevels = np.arange(-50, 51, 10)
    global small_fonts
    small_fonts = 12
    # plt.figure(figsize=(8,4))
    # axes = plt.axes(projection=ccrs.PlateCarree())
    axes = ax.flatten()

    for index in range(len(TOA_model.time)):  # some experiments have less than 12 months, draw fewer panels for those
            plot = plot_bias(TOA_model[var].sel(time=year).isel(time=index)-TOA_ceres_clim[var].isel(time=index),
                            iax=axes[index], title=calendar.month_name[index+1], plotlevels=plotlevels,
                            lower=lower, upper=upper, index=index)

    for axi in axes:
        axi.coastlines(color='black', linewidth=0.5)

    if panel_1_off:
        axes[0].remove()

    # plt.tight_layout()
    # common colorbar
    fig.subplots_adjust(right=0.95)
    cbar_ax = fig.add_axes([0.96, 0.3, 0.02, 0.4])  # [left, bottom, width, height]
    cbar = fig.colorbar(plot, cax=cbar_ax)
    cbar.ax.tick_params(labelsize=small_fonts)
    cbar.set_label('$W m^{-2}$', labelpad=-32, y=-.08, rotation=0)

    plt.suptitle(label+' TOA bias ' + model_label + ' ' + year + '\nrel. to CERES climatology (2001-2021)', fontsize=small_fonts*2)
    # plt.tight_layout()
    filename = f"{outputfig}{label}_TOA_bias_maps_{year}_{model_label}.pdf"
    plt.savefig(filename, dpi=300, format='pdf')
    plt.show()
    
    # Save the data to a netCDF file
    data = TOA_model[var].sel(time=year) - TOA_ceres_clim[var].isel(time=0)
    filename = f"{outputdir}{var}_TOA_bias_maps_{year}_{model_label}.nc"
    data.to_netcdf(filename)

    print(f"Data has been saved to {outputdir}.")
    print(f"Plot has been saved to {outputfig}.")


def plot_mean_bias(TOA_model, var, model_label, TOA_ceres_clim, start_year, end_year):
    """
    Plot the mean bias of the data over the specified time range.

    Args:
        TOA_model (xarray.Dataset):       The model TOA radiation data.
        var (str):                       The variable to plot (e.g., 'tsr', 'ttr', 'tnr').
        model_label (str):               The label for the model.
        TOA_ceres_clim (float):          The CERES TOA radiation climatology.
        start_year (str):                The start year of the time range for the model data.
        end_year (str):                  The end year of the time range for the model data.
        ceres_start_year (str):          The start year of the time range for the CERES data (optional).
        ceres_end_year (str):            The end year of the time range for the CERES data (optional).

    Returns:
        None. Displays the plot of the mean bias.

    Example:
        plot_mean_bias(TOA_model, 'tsr', 'Cycle3_9km_IFS', TOA_ceres_clim, '2020', '2024', ceres_start_year='2000', ceres_end_year='2010')
    """
    plotlevels = np.arange(-50, 51, 10)

    # Calculate the mean bias over the specified time range
    mean_bias = (TOA_model[var].sel(time=slice(start_year, end_year)).mean(dim='time') - TOA_ceres_clim[var]).mean(dim='time')
    # Convert masked values to NaN
    mean_bias = mean_bias.where(~mean_bias.isnull(), np.nan)
    # Create the plot
    fig = plt.figure(figsize=(10, 6))
    gs = gridspec.GridSpec(2, 1, height_ratios=[10, 1], hspace=0.1)
    ax = plt.subplot(gs[0], projection=ccrs.PlateCarree())
    contour_plot = mean_bias.plot.contourf(ax=ax, transform=ccrs.PlateCarree(), cmap='RdBu_r', levels=20)
    ax.coastlines(color='black', linewidth=0.5)
    ax.gridlines(linewidth=0.5)
    ax.set_title(f'{var.upper()} Bias of the {model_label} climatology ({start_year} to {end_year})\n relative to the CERES climatology (2001-2021)', fontsize=14)
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    ax.set_xticks(np.arange(-180, 181, 30), crs=ccrs.PlateCarree())
    ax.set_yticks(np.arange(-90, 91, 30), crs=ccrs.PlateCarree())
    ax.tick_params(axis='both', which='both', labelsize=10)
    ax.xaxis.set_ticklabels(['-180°', '-150°', '-120°', '-90°', '-60°', '-30°', '0°', '30°', '60°', '90°', '120°', '150°', '180°'])
    ax.yaxis.set_ticklabels(['-90°', '-60°', '-30°', '0°', '30°', '60°', '90°'])

    #cbar_ax = plt.subplot(gs[1])
    #cbar = plt.colorbar(contour_plot, cax=cbar_ax, orientation='horizontal')
    #cbar.set_label('Bias (W/m²)')

    filename = f"{outputfig}{var}_{model_label}_TOA_mean_biases_{start_year}_{end_year}_CERES.pdf"
    plt.savefig(filename, dpi=300, format='pdf')
    plt.show()
    #Save the data to a netCDF file
    filename = f"{outputdir}{var}_{model_label}_TOA_mean_biases_{start_year}_{end_year}_CERES.nc"
    mean_bias.to_netcdf(filename)

    print(f"Data has been saved to {outputdir}.")
    print(f"Plot has been saved to {outputfig}.")

