from .tools import *
from ocean3d import write_data
from ocean3d import export_fig
from ocean3d import split_ocean3d_req
import matplotlib.pyplot as plt
from aqua.logger import log_configure


class hovmoller_plot:
    def __init__(self, o3d_request):
        
        self = split_ocean3d_req(self,o3d_request)
        
    def define_lev_values(self, data_proc):
        logger = log_configure(self.loglevel, 'define_lev_values')
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
        logger = log_configure(self.loglevel, 'data_for_hovmoller_lev_time_plot')
        data = self.data
        region = self.region
        lat_s = self.lat_s
        lat_n = self.lat_n
        lon_w = self.lon_w
        lon_e = self.lon_e
        output_dir = self.output_dir
        self.plot_info = {}
        
        data = weighted_area_mean(data, region, lat_s, lat_n, lon_w, lon_e, loglevel=self.loglevel)
        
        counter = 1
        for anomaly in [False,True]:
            for standardise in [False,True]:
                for anomaly_ref in ["t0","tmean"]:
                    data_proc, type, cmap = data_process_by_type(
                        data=data, anomaly=anomaly, standardise=standardise, anomaly_ref=anomaly_ref, loglevel=self.loglevel)
                    key = counter
                    
                    region_title = custom_region(region=region, lat_s=lat_s, lat_n=lat_n, lon_w=lon_w, lon_e=lon_e, loglevel=self.loglevel)

                    if self.output:
                        # if standardise:
                        #     type = f"{type} standardised"
                        plot_name = f'hovmoller_plot_{type.replace(" ","_")}'
                        output_path, fig_dir, data_dir, filename = dir_creation(data_proc,
                            region, lat_s, lat_n, lon_w, lon_e, output_dir, plot_name = plot_name, loglevel=self.loglevel)

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

    def loop_details(self, i, fig, axs):
        logger = log_configure(self.loglevel, 'loop_details')
        
        key = i + 4
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
        
        cbar_ax = fig.add_axes([.47, 0.77 - i* 0.117, 0.028, 0.08])
        
        fig.colorbar(vars()[cs1_name], cax=cbar_ax, orientation='vertical', label=f'Potential temperature in {data.ocpt.attrs["units"]}')
        
        cs2_name = f'cs2_{i}'
        vars()[cs2_name] = axs[i,1].contourf(data.time, data.lev, data.so.transpose(),
                            levels=solevs, cmap=cmap, extend='both')
        cbar_ax = fig.add_axes([.94,  0.77 - i* 0.117, 0.028, 0.08])
        fig.colorbar(vars()[cs2_name], cax=cbar_ax, orientation='vertical', label=f'Salinity in {data.so.attrs["units"]}')
        

        axs[i,0].invert_yaxis()
        axs[i,1].invert_yaxis()
        axs[i,0].set_ylim((max(data.lev).data, 0))
        axs[i,1].set_ylim((max(data.lev).data, 0))
        

        if i==0:
            axs[i,1].set_title("Salinity", fontsize=20) 
            axs[i,0].set_title("Temperature", fontsize=20) 
        axs[i,0].set_ylabel(f"Depth (in {data.lev.units})", fontsize=12)
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

        axs[i,1].set_yticklabels([])

        axs[i, 0].text(-0.35, 0.2, type.replace("wrt", "\nwrt\n"), fontsize=15, color='dimgray', rotation=90, transform=axs[i, 0].transAxes, ha='center')

        if self.output:
            write_data(f'{data_dir}/{filename}.nc', data)
    

    def plot(self):
        logger = log_configure(self.loglevel, 'single_plot')
        logger.debug("Hovmoller plot started")
        
        self.data_for_hovmoller_lev_time_plot()

        filename = f"{self.model}_{self.exp}_{self.source}_{self.region}_hovmoller_plot"
        filename = filename.replace(" ", "_") 
        
        fig, (axs) = plt.subplots(nrows=5, ncols=2, figsize=(14, 25))
        plt.subplots_adjust(bottom=0.3, top=0.85, wspace=0.5, hspace=0.5)
        
        self.loop_details(0, fig, axs)
        self.loop_details(1, fig, axs)
        self.loop_details(2, fig, axs)
        self.loop_details(3, fig, axs)
        self.loop_details(4, fig, axs)

        fig.suptitle(f"Spatially averaged {self.region}", fontsize=25, y=0.9)

        if self.output:
            export_fig(self.output_dir, filename , "pdf")
        logger.debug("Hovmoller plot completed")

        return
    