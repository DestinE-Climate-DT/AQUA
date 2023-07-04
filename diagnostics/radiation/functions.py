import sys
import os
import xarray as xr
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import cartopy.crs as ccrs
import calendar 
from cdo import *
import dask
import seaborn as sns
from aqua import Reader, catalogue, inspect_catalogue

plotdir = './plots/RadiationTOA/'
if not os.path.exists(plotdir):
    os.makedirs(plotdir)

cdo = cdo.Cdo(tempdir='/scratch/b/b382257/tmp/cdo-py')
    
class radiation_diag:
    
    @staticmethod
    def process_ceres_data(exp, source,TOA_icon_gm):
        
        """
        Extract CERES data for further analyis + create global means

        Args:
            exp:                            input experiment to be selected from the catalogue
            source:                         input source to be selected from the catalogue
            TOA_icon_gm:                    this is necessary to setting time axis to the same time axis as model output (modify if needed)
      
        Returns:
            TOA_ceres_clim_gm, TOA_ceres_ebaf_gm, TOA_ceres_diff_samples_gm, reader_ceres_toa, TOA_ceres_clim, TOA_ceres_diff_samples:       returns the necessary ceres data for further evaluation
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
        #TOA_ceres_clim = TOA_ceres_clim.assign_coords(time=TOA_icon_gm.sel(time='2020').time).transpose("time", ...)
    
        # TOA_ceres_clim_gm
        TOA_ceres_clim_gm = reader_ceres_toa.fldmean(TOA_ceres_clim)  #= cdo.fldmean(input=TOA_ceres_clim, returnXDataset=True)

        # TOA_ceres_ebaf_gm
        TOA_ceres_ebaf_gm  =  reader_ceres_toa.fldmean(TOA_ceres_ebaf)#= cdo.fldmean(input=TOA_ceres_ebaf, returnXDataset=True)
        
        # samples_tmp
        samples_tmp = []
        for year in range(2001, 2021):
            # select year and assign (fake) time coordinates of 2020 so that the differencing works
            samples_tmp.append(TOA_ceres_ebaf.sel(time=str(year)).assign_coords(time=TOA_ceres_clim.time) - TOA_ceres_clim)

        # TOA_ceres_diff_samples
        TOA_ceres_diff_samples = xr.concat(samples_tmp, dim='ensemble').transpose("time", ...)

        # TOA_ceres_diff_samples_gm
        TOA_ceres_diff_samples_gm  = reader_ceres_toa.fldmean(TOA_ceres_diff_samples) #= cdo.fldmean(input=TOA_ceres_diff_samples, returnXDataset=True).squeeze()
        TOA_ceres_clim['lon'] = TOA_ceres_clim['lon'] - 0.5
        TOA_ceres_diff_samples['lon'] = TOA_ceres_diff_samples['lon'] - 0.5
        
        return TOA_ceres_clim_gm, TOA_ceres_ebaf_gm, TOA_ceres_diff_samples_gm, reader_ceres_toa, TOA_ceres_clim, TOA_ceres_diff_samples
        
    #--------------------------------------------------------------------------------------------------------------------------------------------
    #--------------------------------------------------------------------------------------------------------------------------------------------   
    
    @staticmethod
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
        TOA_model_gm = reader_model.fldmean(TOA_model) #= cdo.fldmean(input=TOA_model, returnXDataset=True)
        TOA_model_r360x180 = cdo.remapcon('r360x180',input=TOA_model,returnXDataset=True)
        
        return TOA_model_gm, reader_model, data_model, TOA_model, TOA_model_r360x180
    
    #--------------------------------------------------------------------------------------------------------------------------------------------
    #--------------------------------------------------------------------------------------------------------------------------------------------   
   

    @staticmethod
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
    
      
    #--------------------------------------------------------------------------------------------------------------------------------------------
    #--------------------------------------------------------------------------------------------------------------------------------------------   
   
    
    @staticmethod
    def process_ceres_sfc_data(exp, source,TOA_icon_gm):
        
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
        #TOA_ceres_clim = TOA_ceres_clim.assign_coords(time=TOA_icon_gm.sel(time='2020').time).transpose("time", ...)
    
        # TOA_ceres_clim_gm
        ceres_clim_gm_sfc = reader_ceres_sfc.fldmean(ceres_clim_sfc)  #= cdo.fldmean(input=TOA_ceres_clim, returnXDataset=True)

        # TOA_ceres_ebaf_gm
        ceres_ebaf_gm_sfc  =  reader_ceres_sfc.fldmean(ceres_ebaf_sfc)#= cdo.fldmean(input=TOA_ceres_ebaf, returnXDataset=True)
        
        # samples_tmp
        samples_tmp = []
        for year in range(2001, 2021):
            # select year and assign (fake) time coordinates of 2020 so that the differencing works
            samples_tmp.append(ceres_ebaf_sfc.sel(time=str(year)).assign_coords(time=ceres_clim_sfc.time) - ceres_clim_sfc)

        # TOA_ceres_diff_samples
        ceres_diff_samples_sfc = xr.concat(samples_tmp, dim='ensemble').transpose("time", ...)

        # TOA_ceres_diff_samples_gm
        ceres_diff_samples_gm_sfc  = reader_ceres_sfc.fldmean(ceres_diff_samples_sfc) #= cdo.fldmean(input=TOA_ceres_diff_samples, returnXDataset=True).squeeze()
        ceres_clim_sfc['lon'] = ceres_clim_sfc['lon'] - 0.5
        ceres_diff_samples_sfc['lon'] = ceres_diff_samples_sfc['lon'] - 0.5
        
        return ceres_clim_gm_sfc, ceres_ebaf_gm_sfc, ceres_diff_samples_gm_sfc, reader_ceres_sfc, ceres_clim_sfc, ceres_diff_samples_sfc
        
    #--------------------------------------------------------------------------------------------------------------------------------------------
    #--------------------------------------------------------------------------------------------------------------------------------------------           
        
    def gregory_plot(data_era5, model_list, reader_dict):
        
        """
        Create Gregory Plot with various models and ERA5 (plotted by default)
        Example: model_list = ['icon', 'ifs_9km', 'ifs_4km']
                 reader_dict = {
                                "icon" : reader_icon,
                                "ifs_9km" : reader_ifs_9km,
                                "ifs_4km" : reader_ifs_4km
                                }
             radiation_diag.gregory_plot(data_era5, model_list, reader_dict)

        Args:
           model_list:                      a list of model that should be plotted.
       
            data_era5:                      your xarrayDataSet provided from process_era5_data(exp, source)       
      
        Returns:
          A Gregory Plot
        """
        
        reader_era5 = Reader(model="ERA5", exp="era5", source="monthly")
        data_era5 = reader_era5.retrieve(fix=True)
        
        # Create the plot and axes
        fig, ax = plt.subplots()
        # Colors for the plots
        colors = ["b", "r", "c", "m", "y", "k"]
 
        # Plot the data for ERA5 (2000-2020)
        era5_2t_2000_2020 = data_era5["T2M"].sel(time=slice('2000-01-01', '2020-12-31'))
        era5_2t_2000_2020_resampled = era5_2t_2000_2020.resample(time="M").mean()
        era5_tsr_2000_2020 = data_era5["TSR"].sel(time=slice('2000-01-01', '2020-12-31'))
        era5_tsr_2000_2020_resampled = (era5_tsr_2000_2020.resample(time="M").mean())/86400 
        era5_ttr_2000_2020 = data_era5["TTR"].sel(time=slice('2000-01-01', '2020-12-31'))
        era5_ttr_2000_2020_resampled = (era5_ttr_2000_2020.resample(time="M").mean())/86400 
        ax.plot(
            reader_era5.fldmean(era5_2t_2000_2020_resampled)-273.15,
            reader_era5.fldmean(era5_tsr_2000_2020_resampled) + reader_era5.fldmean(era5_ttr_2000_2020_resampled),
            marker="o",
            color="g",
            linestyle="-",
            markersize=3,
            label="ERA5 2000-2020",
        )
    
        # Plot the data for each model
        for i, model in enumerate(model_list):
            model_name = model.lower()
            #model_reader = globals()["reader_" + model_name]
            #model_data = globals()["data_" + model_name]
            model_reader = reader_dict[model_name]
            model_data = model_reader.retrieve(fix=True)
            model_color = colors[(i + 1) % len(colors)]  # Rotate colors for each model
            ax.plot(
                #model_data["2t"].resample(time="M").mean().weighted(model_reader.areacella).mean(dim=["lon", "lat"]),
                #(model_data["mtnsrf"] + model_data["mtntrf"]).resample(time="M").mean().weighted(model_reader.areacella).mean(dim=["lon", "lat"]),
                model_reader.fldmean(model_data["2t"].resample(time="M").mean())-273.15,
                model_reader.fldmean((model_data["mtnsrf"] + model_data["mtntrf"]).resample(time="M").mean()),
                marker="o",
                color=model_color,
                linestyle="-",
                markersize=5,
                label = model_name
            )

        # Set labels and title
        ax.set_xlabel("2m temperature [$^{\circ} C$]", fontsize=12)
        ax.set_ylabel("Net radiation TOA [Wm$^{-2}$]", fontsize=12)
        ax.set_title("Gregory Plot", fontsize=14)
        ax.legend()
        ax.tick_params(axis="both", which="major", labelsize=10)
        ax.grid(True, linestyle="--", linewidth=0.5)
        plt.savefig(plotdir+'Gregory_Plot.png',dpi=300)
        plt.show()
    

    #--------------------------------------------------------------------------------------------------------------------------------------------
    #--------------------------------------------------------------------------------------------------------------------------------------------
    
    
    def barplot_model_data(datasets, model_names, year=None):
                
        """
        Create Bar Plot with various models and CERES. Variables ttr and tsr are plotted to show imbalances. 
        Default mean for CERES data is the whole time range. 
        Example:
                datasets = [TOA_ceres_clim_gm, TOA_icon_gm, TOA_ifs_4km_gm, TOA_ifs_9km_gm]
                model_names = ['ceres', 'icon', 'ifs 4.4 km', 'ifs 9 km']

                radiation_diag.barplot_model_data(datasets, model_names, year = 2022)

        Args:
           datasets:                      a list of xarrayDataSets that should be plotted. Chose the global means (TOA_"model"_gm)
            model_names:                  your desired naming for the plotting       
      
        Returns:
          A bar plot
        """
                
        colors = ['red', 'blue']  # Longwave (lw) in red, Shortwave (sw) in blue
        variables = ['ttr', 'tsr']  # Fixed order: ttr (lw), tsr (sw)
        plt.figure(figsize=(12, 5))

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
    
        plt.xlabel('Model')
        plt.ylabel('Global mean ($W/m^2$)')
        # Add legend with colored text
        legend_labels = ['ttr = net outgoing longwave (top thermal radiadion)', 'tsr = net incomming shortwave (total solar radiation)']
        legend_colors = ['blue', 'red']
        legend_handles = [plt.Rectangle((0, 0), 1, 1, color=color) for color in legend_colors]
        plt.legend(legend_handles, legend_labels, loc='upper right', facecolor='white', framealpha=1)
        plt.ylim(236, 250)
        if year is not None:
            plt.title(f"Global Mean TOA radiation for different models ({year}) (all CERES years from 2001 to 2021)")
        else:
            plt.title('Global Mean TOA radiation for different models (mean over all model times)')
        plt.savefig(plotdir+'BarPlot.png',dpi=300)
        plt.show()

        
    #--------------------------------------------------------------------------------------------------------------------------------------------
    #--------------------------------------------------------------------------------------------------------------------------------------------
    
    @staticmethod
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
            line_labels:                 your desired naming for the plotting       
            
      
        Returns:
          A plot to show the model biases for the whole time range
        """
        
        fig, axes = plt.subplots(3, 1, figsize=(12, 8))
        linecolors = ['red', 'green', 'blue']

        #linelabels = ['ICON 5 km', 'IFS 4.4 km', 'IFS 9 km']
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

        for i, model in enumerate(models):
            ttr_diff = xr.concat(
                (
                    (model.ttr.sel(time='2020') - TOA_ceres_clim_gm.squeeze().ttr.values),
                    (model.ttr.sel(time='2021') - TOA_ceres_clim_gm.squeeze().ttr.values),
                    (model.ttr.sel(time='2022') - TOA_ceres_clim_gm.squeeze().ttr.values),
                    (model.ttr.sel(time='2023') - TOA_ceres_clim_gm.squeeze().ttr.values),
                    (model.ttr.sel(time='2024') - TOA_ceres_clim_gm.squeeze().ttr.values),
                ),
                dim='time'
            )
            ttr_diff.plot(ax=axes[0], color=linecolors[i], label=linelabels[i], x='time')

        axes[0].fill(long_time, np.append(shading_data['ttr'].min(dim='ensemble'), shading_data['ttr'].max(dim='ensemble')[::-1]), color='lightgrey', alpha=0.6, label='CERES individual years', zorder=0)
        axes[0].set_title('long wave radiation (ttr)', fontsize=16)
        axes[0].set_xticklabels([])
        axes[0].set_xlabel('')
        axes[0].legend(loc="upper left", frameon=False, fontsize='medium', ncol=3)

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
        axes[1].set_title('short wave radiation (tsr)', fontsize=16)
        axes[1].set_xticklabels([])
        axes[1].set_xlabel('')

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
        axes[2].set_title('net radiation (imbalance)', fontsize=16)

        for i in range(3):
            axes[i].set_ylabel('$W/m^2$')
            axes[i].set_xlim([pd.to_datetime('2020-01-15'), pd.to_datetime('2024-12-15')])
            axes[i].plot([pd.to_datetime('2020-01-01'), pd.to_datetime('2030-12-31')], [0, 0], color='black', linestyle=':')
            axes[i].set_ylim([-6.5, 6.5])

        plt.suptitle('Global mean TOA radiation bias relative to CERES climatology - nextGEMS Cycle 3', fontsize=18)
        plt.savefig(plotdir+'TimeSeries.png',dpi=300)
        plt.tight_layout()
        plt.show()

        
    #------------------------------------------------------------------------------------------
    #------------------------------------------------------------------------------------------

    def plot_bias(data,iax,title,plotlevels,lower,upper,index):
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
        stipple_data = data.where(np.logical_and(data>lower.isel(month=index), data<upper.isel(month=index)))/data.where(np.logical_and(data>lower.isel(month=index), data<upper.isel(month=index)))
        plot2 = stipple_data.plot.contourf(ax=iax,levels=[-10,0,10], hatches=["","...."],add_colorbar=False, alpha=0, transform=ccrs.PlateCarree())


        iax.set_title(title,fontsize=small_fonts)
        # iax.set_title(data.label+' ('+str(len(data.ensemble))+')',fontsize=small_fonts)

        return plot

    def plot_maps(TOA_model, var, model_label, TOA_ceres_diff_samples, TOA_ceres_clim, year='2020'):
        
        """
        The function plots maps of TOA bias for the specified variable and model using a Robinson projection.
        The TOA bias is calculated as the difference between the TOA model data and the TOA CERES climatology. Default year is 2020
         Example:
                radiation_diag.plot_maps(TOA_model= TOA_ifs_4km_r360x180, TOA_ceres_diff_samples = TOA_ceres_diff_samples, TOA_ceres_clim = TOA_ceres_clim, var='tsr', model_label='Cycle 3 4.4 km IFS Fesom', year='2023')
                Use the TOA_"model"_r360x180 DataSet to ensure that the gridding is right
        Args:
            TOA_model (xarray.DataArray):                The TOA model data.
            var (str):                                   The variable to plot ('tnr', 'tsr', or 'ttr').
            model_label (str):                           Desired label for the model (also used as filename to save figure)
            TOA_ceres_diff_samples (xarray.DataArray):   The TOA CERES difference samples data.
            TOA_ceres_clim (xarray.DataArray):           The TOA CERES climatology data.
            year (str, optional):                        The year to plot. Defaults to '2020'.

        
        Returns:
        Monthly bias plots of the chosen model, variable and year
       
       """
        
        # quantiles are a bit more conservative than the range, but interpolation from few values might not be robust
        # q05 = TOA_ceres_diff_samples.groupby('time.month').quantile(0.05,dim='ensemble')
        # q95 = TOA_ceres_diff_samples.groupby('time.month').quantile(0.95,dim='ensemble')
        # min-max range is more conservative, but could be sensitive to single extreme years
        range_min = TOA_ceres_diff_samples.groupby('time.month').min(dim='ensemble')
        range_max = TOA_ceres_diff_samples.groupby('time.month').max(dim='ensemble')
        TOA_model = TOA_model.sel(time=year)
        if var=='tnr':
            label='net'
        elif var=='tsr':
            label='SW'
        elif var=='ttr':
            label='LW'
        
        if year == '2020':
            panel_1_off = True
        else:
            panel_1_off = False
    
        # what to use for significance testing
        # everything between these values will be considered not significant and hatched in the plot
        lower = range_min[var].rename({'time': 'month'})
        upper = range_max[var].rename({'time': 'month'})


        fig, ax = plt.subplots(3, 4,figsize=(24,10), subplot_kw={'projection': ccrs.Robinson(central_longitude=180, globe=None)})
        plotlevels=np.arange(-50,51,10)
        global small_fonts
        small_fonts = 8
        # plt.figure(figsize=(8,4))
        # axes = plt.axes(projection=ccrs.PlateCarree())
        axes = ax.flatten()

        for index in range( len(TOA_model.time) ): # some experiments have less than 12 months, draw fewer panels for those
                plot = radiation_diag.plot_bias(TOA_model[var].sel(time=year).isel(time=index)-TOA_ceres_clim[var].isel(time=index)
                                 ,iax=axes[index],title=calendar.month_name[index+1],plotlevels=plotlevels
                                 ,lower=lower,upper=upper,index=index)


        for axi in axes:
            axi.coastlines(color='black',linewidth=0.5)

        if panel_1_off:
            axes[0].remove()


        # plt.tight_layout()
        # common colorbar
        fig.subplots_adjust(right=0.95)
        cbar_ax = fig.add_axes([0.96, 0.3, 0.02, 0.4]) # [left, bottom, width, height]
        cbar = fig.colorbar(plot, cax=cbar_ax)
        cbar.ax.tick_params(labelsize=small_fonts)
        cbar.set_label('$W m^{-2}$', labelpad=-32, y=-.08, rotation=0)

        plt.suptitle(label+' TOA bias IFS ' + model_label + ' ' + year + '\nrel. to CERES climatology (2001-2021)',fontsize=small_fonts*2)
        # plt.tight_layout()
        plt.savefig(plotdir+label+'_TOA_bias_maps_'+year+'_'+model_label+'.png',dpi=300)
        plt.show()
        
  