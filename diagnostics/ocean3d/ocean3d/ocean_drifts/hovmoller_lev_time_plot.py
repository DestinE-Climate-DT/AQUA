
from .tools import *
from ocean3d import write_data
from ocean3d import export_fig
from ocean3d import split_ocean3d_req
import matplotlib.pyplot as plt

class hovmoller_lev_time_plot:
    def __init__(self, o3d_request):
        
        self = split_ocean3d_req(self,o3d_request)
        # self.data = o3d_request.get('data')
        # self.model = o3d_request.get('model')
        # self.exp = o3d_request.get('exp')
        # self.source = o3d_request.get('source')
        # self.region = o3d_request.get('region', None)
        # self.latS = o3d_request.get('latS', None)
        # self.latN = o3d_request.get('latN', None)
        # self.lonW = o3d_request.get('lonW', None)
        # self.lonE = o3d_request.get('lonE', None)
        # self.output = o3d_request.get('output')
        # self.output_dir = o3d_request.get('output_dir')
        
    def data_process_by_type(self, **kwargs):
        """
        Selects the type of timeseries and colormap based on the given parameters.

        Args:
            data (DataArray): Input data containing temperature (ocpt) and salinity (so).
            anomaly (bool, optional): Specifies whether to compute anomalies. Defaults to False.
            standardise (bool, optional): Specifies whether to standardize the data. Defaults to False.
            anomaly_ref (str, optional): Reference for the anomaly computation. Valid options: "t0", "tmean". Defaults to None.

        Returns:
            process_data (Dataset): Processed data based on the selected preprocessing approach.
            type (str): Type of preprocessing approach applied
            cmap (str): Colormap to be used for the plot.
        """
        data = kwargs["data"]
        anomaly = kwargs["anomaly"]
        anomaly_ref = kwargs["anomaly_ref"]
        standardise = kwargs["standardise"]
        
        
        process_data = xr.Dataset()

        
        if anomaly:
            anomaly_ref = anomaly_ref.lower().replace(" ", "").replace("_", "")
            if not standardise:
                if anomaly_ref in ['tmean', "meantime", "timemean"]:
                    cmap = "PuOr"
                    for var in list(data.data_vars.keys()):
                        process_data[var] = data[var] - data[var].mean(dim='time')
                    type = "anomaly wrt temporal mean"
                elif anomaly_ref in ['t0', "intialtime", "firsttime"]:
                    cmap = "PuOr"
                    for var in list(data.data_vars.keys()):
                        process_data[var] = data[var] - data[var].isel(time=0)
                    type = "anomaly wrt initial time"
                else:
                    raise ValueError(
                        "Select proper value of anomaly_ref: t0 or tmean, when anomaly = True ")
                logger.info(f"Data processed for {type}")
            if standardise:
                if anomaly_ref in ['t0', "intialtime", "firsttime"]:
                    cmap = "PuOr"
                    for var in list(data.data_vars.keys()):
                        var_data = data[var] - data[var].isel(time=0)
                        var_data.attrs['units'] = 'Stand. Units'
                        # Calculate the standard anomaly by dividing the anomaly by its standard deviation along the time dimension
                        process_data[var] = var_data / var_data.std(dim="time")
                    type = "Std. anomaly wrt initial time"
                elif anomaly_ref in ['tmean', "meantime", "timemean"]:
                    cmap = "PuOr"
                    for var in list(data.data_vars.keys()):
                        var_data = data[var] - data[var].mean(dim='time')
                        var_data.attrs['units'] = 'Stand. Units'
                        # Calculate the standard anomaly by dividing the anomaly by its standard deviation along the time dimension
                        process_data[var] = var_data / var_data.std(dim="time")
                    type = "Std. anomaly wrt temporal mean"
                else:
                    raise ValueError(
                        "Select proper value of type: t0 or tmean, when anomaly = True ")
                logger.info(
                    f"Data processed for {type}")

        else:
            cmap = 'jet'
            logger.info("Data processed for Full values as anomaly = False")
            type = "Full values"

            process_data = data
        # logger.info(f"Data processed for {type}")
        return process_data, type, cmap

    def define_lev_values(self, data_proc):
        # data_proc = args[1]["data_proc"]
        # To center the colorscale around zero when we plot temperature anomalies
        ocptmin = round(np.nanmin(data_proc.ocpt.values), 2)
        ocptmax = round(np.nanmax(data_proc.ocpt.values), 2)

        if ocptmin < 0:
            if abs(ocptmin) < ocptmax:
                ocptmin = ocptmax*-1
            else:
                ocptmax = ocptmin*-1

            ocptlevs = np.linspace(ocptmin, ocptmax, 21)

        else:
            ocptlevs = 20

        # And we do the same for salinity
        somin = round(np.nanmin(data_proc.so.values), 3)
        somax = round(np.nanmax(data_proc.so.values), 3)

        if somin < 0:
            if abs(somin) < somax:
                somin = somax*-1
            else:
                somax = somin*-1

            solevs = np.linspace(somin, somax, 21)

        else:
            solevs = 20
        return ocptlevs, solevs


                    
                    
    def data_for_hovmoller_lev_time_plot(self):
        data = self.data
        region = self.region
        latS = self.latS
        latN = self.latN
        lonW = self.lonW
        lonE = self.lonE
        output_dir = self.output_dir
        self.plot_info = {}
        
        data = weighted_area_mean(data, region, latS, latN, lonW, lonE)
        
        counter = 1
        for anomaly in [False,True]:
            for standardise in [False,True]:
                for anomaly_ref in ["t0","tmean"]:
                    data_proc, type, cmap = data_process_by_type(
                        data=data, anomaly=anomaly, standardise=standardise, anomaly_ref=anomaly_ref)
                    key = counter
                    
                    region_title = custom_region(region=region, latS=latS, latN=latN, lonW=lonW, lonE=lonE)

                    if self.output:
                        # if standardise:
                        #     type = f"{type} standardised"
                        plot_name = f'hovmoller_plot_{type.replace(" ","_")}'
                        output_path, fig_dir, data_dir, filename = dir_creation(data_proc,
                            region, latS, latN, lonW, lonE, output_dir, plot_name = plot_name)

                    # ocptlevs, solevs =self.define_lev_values(data_proc)
                    ocptlevs, solevs = 20, 20
                    plot_config = {"anomaly": anomaly,
                                   "standardise": standardise,
                                   "anomaly_ref": anomaly_ref}
                    
                    self.plot_info[key] = {"data": data_proc,
                                            "type": type,
                                            "cmap": cmap,
                                            "region_title": region_title,
                                        "solevs": solevs,
                                        "ocptlevs": ocptlevs,
                                            "output_path": output_path,
                                            "type": type,
                                            "fig_dir": fig_dir,
                                            "data_dir": data_dir,
                                            "filename": filename,
                                            "plot_config": plot_config}
                    counter += 1            
        return 
    
    def prepare_plot(self,key):
        
        plot_info = self.plot_info[key]
        data = plot_info['data']
        ocptlevs = plot_info['ocptlevs']
        solevs = plot_info['solevs']
        cmap = plot_info['cmap']
        region_title = plot_info['region_title']
        type = plot_info['type']
        filename = plot_info['filename']
        data_dir = plot_info['data_dir']
        fig_dir = plot_info['fig_dir']
        output_path = plot_info['output_path']
        
        logger.info("start plottiing")
        filename = f"{self.model}_{self.exp}_{self.source}_{filename}"
        
        fig, (axs) = plt.subplots(nrows=1, ncols=2, figsize=(14, 5))
        fig.suptitle(f"Spatially averaged {region_title} T,S {type}", fontsize=22)
        
        cs1 = axs[0].contourf(data.time, data.lev, data.ocpt.transpose(),
                            levels=ocptlevs, cmap=cmap, extend='both')
        
        cbar_ax = fig.add_axes([0.13, 0.1, 0.35, 0.05])
        fig.colorbar(cs1, cax=cbar_ax, orientation='horizontal', label=f'Potential temperature in {data.ocpt.attrs["units"]}')

        cs2 = axs[1].contourf(data.time, data.lev, data.so.transpose(),
                            levels=solevs, cmap=cmap, extend='both')
        cbar_ax = fig.add_axes([0.54, 0.1, 0.35, 0.05])
        fig.colorbar(cs2, cax=cbar_ax, orientation='horizontal', label=f'Salinity in {data.so.attrs["units"]}')

        # if self.output:
            # obs_clim.to_netcdf(f'{data_dir}/{filename}_Rho.nc')

        axs[0].invert_yaxis()
        axs[1].invert_yaxis()

        axs[0].set_ylim((max(data.lev).data, 0))
        axs[1].set_ylim((max(data.lev).data, 0))

        axs[0].set_title("Temperature", fontsize=15)
        axs[0].set_ylabel("Depth (in m)", fontsize=12)
        axs[0].set_xlabel("Time", fontsize=12)
        axs[0].set_xticklabels(axs[0].get_xticklabels(), rotation=30)
        axs[1].set_xticklabels(axs[0].get_xticklabels(), rotation=30)
        # max_num_ticks = 10  # Adjust this value to control the number of ticks
        # from matplotlib.ticker import MaxNLocator
        # locator = MaxNLocator(integer=True, prune='both', nbins=max_num_ticks)
        # axs[0].xaxis.set_major_locator(locator)
        # axs[1].xaxis.set_major_locator(locator)

        axs[1].set_title("Salinity", fontsize=15)
        axs[1].set_xlabel("Time", fontsize=12)
        axs[1].set_yticklabels([])

        plt.subplots_adjust(bottom=0.3, top=0.85, wspace=0.1)

        if self.output:
            plt.savefig(f"{fig_dir}/{filename}.pdf")
            write_data(f'{data_dir}/{filename}.nc', data)
            logger.info(
                "Figure and data used for this plot are saved here: %s", output_path)
        plt.close(fig)
        return 
    
    def loop_details(self, i, fig, axs):
        
        key = i + 3
        plot_info = self.plot_info[key]
        data = plot_info['data']
        ocptlevs = plot_info['ocptlevs']
        solevs = plot_info['solevs']
        cmap = plot_info['cmap']
        region_title = plot_info['region_title']
        type = plot_info['type']
        data_dir = plot_info["data_dir"]
        filename = plot_info["filename"]

        cs1_name = f'cs1_{i}'
        vars()[cs1_name]  = axs[i,0].contourf(data.time, data.lev, data.ocpt.transpose(),
                            levels=ocptlevs, cmap=cmap, extend='both')
        
        cbar_ax = fig.add_axes([.47, 0.77 - i* 0.115, 0.028, 0.08])
        
        fig.colorbar(vars()[cs1_name], cax=cbar_ax, orientation='vertical', label=f'Potential temperature in {data.ocpt.attrs["units"]}')
        
        cs2_name = f'cs2_{i}'
        vars()[cs2_name] = axs[i,1].contourf(data.time, data.lev, data.so.transpose(),
                            levels=solevs, cmap=cmap, extend='both')
        cbar_ax = fig.add_axes([.94,  0.77 - i* 0.115, 0.028, 0.08])
        fig.colorbar(vars()[cs2_name], cax=cbar_ax, orientation='vertical', label=f'Salinity in {data.so.attrs["units"]}')

        axs[i,0].invert_yaxis()
        axs[i,1].invert_yaxis()
        axs[i,0].set_ylim((max(data.lev).data, 0))
        axs[i,1].set_ylim((max(data.lev).data, 0))
        
        if i==0:
            axs[i,0].set_title("Temperature", fontsize=15) 
        axs[i,0].set_ylabel("Depth (in m)", fontsize=12)
        if i==4:
            axs[i,0].set_xlabel("Time", fontsize=12)
            axs[i,1].set_xlabel("Time", fontsize=12) 

            axs[i,0].set_xticklabels(axs[i,0].get_xticklabels(), rotation=30)
            axs[i,1].set_xticklabels(axs[i,0].get_xticklabels(), rotation=30)
        else:
            axs[i, 0].set_xticklabels([])
            axs[i, 1].set_xticklabels([])
        # max_num_ticks = 10  # Adjust this value to control the number of ticks
        # from matplotlib.ticker import MaxNLocator
        # locator = MaxNLocator(integer=True, prune='both', nbins=max_num_ticks)
        # axs[0].xaxis.set_major_locator(locator)
        # axs[1].xaxis.set_major_locator(locator)

        if i==0:
            axs[i,1].set_title("Salinity", fontsize=15) 
        axs[i,1].set_yticklabels([])

        #if self.output:
        #    write_data(f'{data_dir}/{filename}.nc', data)
        #    logger.info(
        #        "data used for this plot are saved here:", f'{data_dir}/{filename}.nc')
    

    def single_plot(self):
        
        self.data_for_hovmoller_lev_time_plot()

        filename = f"{self.model}_{self.exp}_{self.source}_{self.region}_hovmoller_combined_plot"
        
        fig, (axs) = plt.subplots(nrows=5, ncols=2, figsize=(14, 25))
        plt.subplots_adjust(bottom=0.3, top=0.85, wspace=0.5, hspace=0.5)
        
        self.loop_details(0, fig, axs)
        self.loop_details(1, fig, axs)
        self.loop_details(2, fig, axs)
        self.loop_details(3, fig, axs)
        self.loop_details(4, fig, axs)

        fig.suptitle(f"Spatially averaged {self.region}", fontsize=22, y=0.9)
        
        if self.output:
            export_fig(self.output_dir, filename , "jpg")

        return
    
    def plot(self):

        self.data_for_hovmoller_lev_time_plot()
        
        # Create subplots for temperature and salinity plots
        logger.info("Hovmoller plotting in process")
        
        for key, value in self.plot_info.items():
            if key not in [1,2,3]:
                self.prepare_plot(key)
                
        return


    def plot_paralell(self):

        self.data_for_hovmoller_lev_time_plot()
        
        # Create subplots for temperature and salinity plots
        logger.info("Hovmoller plotting in paralell process")
        
        import concurrent.futures
        import matplotlib
        # matplotlib.use('Agg')
        # for key, value in self.plot_info.items():
        #     if key not in [1,2,3]:
        #         self.prepare_plot(key)
        #         # self.single_plot(key)
                
        keys_to_parallelize = [key for key in self.plot_info.keys() if key not in [1, 2, 3]]
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
            executor.map(self.prepare_plot, keys_to_parallelize)
        
        logger.info("Hovmoller plotting in completed")
        
        return


# if __name__ == '__main__':
#     plot = hovmoller_lev_time_plot(o3d_request)
#     plot.plot()
