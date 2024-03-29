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
        avg_thetaomin = round(np.nanmin(data_proc.avg_thetao.values), 2)
        avg_thetaomax = round(np.nanmax(data_proc.avg_thetao.values), 2)

        if avg_thetaomin < 0:
            if abs(avg_thetaomin) < avg_thetaomax:
                avg_thetaomin = avg_thetaomax*-1
            else:
                avg_thetaomax = avg_thetaomin*-1

            avg_thetaolevs = np.linspace(avg_thetaomin, avg_thetaomax, 21)

        else:
            avg_thetaolevs = 20

        # And we do the same for salinity
        somin = round(np.nanmin(data_proc.avg_so.values), 3)
        somax = round(np.nanmax(data_proc.avg_so.values), 3)

        if somin < 0:
            if abs(somin) < somax:
                somin = somax*-1
            else:
                somax = somin*-1

            solevs = np.linspace(somin, somax, 21)

        else:
            solevs = 20
        return avg_thetaolevs, solevs

                   
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
                # for anomaly_ref in ["t0","tmean"]:
                for anomaly_ref in ["t0"]:
                    data_proc, type, cmap = data_process_by_type(
                        data=data, anomaly=anomaly, standardise=standardise, anomaly_ref=anomaly_ref, loglevel=self.loglevel)
                    key = counter
                    
                    region_title = custom_region(region=region, lat_s=lat_s, lat_n=lat_n, lon_w=lon_w, lon_e=lon_e, loglevel=self.loglevel)

                    # avg_thetaolevs, solevs =self.define_lev_values(data_proc)
                    avg_thetaolevs, solevs = 21, 21
                    plot_config = {"anomaly": anomaly,
                                   "standardise": standardise,
                                   "anomaly_ref": anomaly_ref}
                    
                    self.plot_info[key] = {"data": data_proc,
                                            "type": type,
                                            "cmap": cmap,
                                            "region_title": region_title,
                                        "solevs": solevs,
                                        "avg_thetaolevs": avg_thetaolevs,
                                            "type": type,
                                            "plot_config": plot_config}
                    counter += 1            
        return 

    def loop_details(self, i, fig, axs):
        logger = log_configure(self.loglevel, 'loop_details')
        
        key = i + 2
        plot_info = self.plot_info[key]
        data = plot_info['data']
        avg_thetaolevs = plot_info['avg_thetaolevs']
        solevs = plot_info['solevs']
        cmap = plot_info['cmap']
        region_title = plot_info['region_title']
        type = plot_info['type']

        logger.debug("Plotting started for %s", type)
        
        if type != "Full values":
            abs_max_avg_thetao = max(abs(np.nanmax(data.avg_thetao)), abs(np.nanmin(data.avg_thetao)))
            abs_max_so = max(abs(np.nanmax(data.avg_so)), abs(np.nanmin(data.avg_so)))
            avg_thetaolevs = np.linspace(-abs_max_avg_thetao, abs_max_avg_thetao, avg_thetaolevs)
            solevs = np.linspace(-abs_max_so, abs_max_so, solevs)
        
        cs1_name = f'cs1_{i}'
        # axs[i, 0].set_yscale('log')
        # axs[i, 1].set_yscale('log')
        vars()[cs1_name]  = axs[i,0].contourf(data.time, data.lev, data.avg_thetao.transpose(),
                            levels=avg_thetaolevs, cmap=cmap, extend='both')
        # cbar_ax = fig.add_axes([.47, 0.77 - i* 0.117, 0.028, 0.08])
        cbar_ax = fig.add_axes([.47, 0.73 - i* 0.2, 0.023, 0.1])
        
        # fig.colorbar(vars()[cs1_name], cax=cbar_ax, orientation='vertical', label=f'Potential temperature in {data.avg_thetao.attrs["units"]}')
        fig.colorbar(vars()[cs1_name], cax=cbar_ax, orientation='vertical')
        
        cs2_name = f'cs2_{i}'
        vars()[cs2_name] = axs[i,1].contourf(data.time, data.lev, data.avg_so.transpose(),
                            levels=solevs, cmap=cmap, extend='both')
        # cbar_ax = fig.add_axes([.94,  0.77 - i* 0.117, 0.028, 0.08])
        cbar_ax = fig.add_axes([.91,  0.73 - i* 0.2, 0.023, 0.1])
        # fig.colorbar(vars()[cs2_name], cax=cbar_ax, orientation='vertical', label=f'Salinity in {data.avg_so.attrs["units"]}')
        fig.colorbar(vars()[cs2_name], cax=cbar_ax, orientation='vertical')
        

        axs[i,0].invert_yaxis()
        axs[i,1].invert_yaxis()
        axs[i,0].set_ylim((max(data.lev).data, 0))
        axs[i,1].set_ylim((max(data.lev).data, 0))
        avg_so_unit = data.avg_so.attrs["units"]
        avg_thetao_unit = data.avg_thetao.attrs["units"]
        if i==0:
            axs[i,1].set_title(f"Salinity (in {avg_so_unit})", fontsize=20) 
            axs[i,0].set_title(f"Pot. Temperature (in {avg_thetao_unit})", fontsize=20) 
        axs[i,0].set_ylabel(f"Depth (in {data.lev.units})", fontsize=12)
        if i==2:
            axs[i,0].set_xlabel("Time", fontsize=12)
            axs[i,1].set_xlabel("Time", fontsize=12) 

            axs[i,0].set_xticklabels(axs[i,0].get_xticklabels(), rotation=30)
            axs[i,1].set_xticklabels(axs[i,0].get_xticklabels(), rotation=30)
        else:
            axs[i, 0].set_xticklabels([])
            axs[i, 1].set_xticklabels([])
        axs[i,1].set_yticklabels([])

        # adding type in the plot
        axs[i, 0].text(-0.35, 0.33, type.replace("wrt", "\nwrt\n"), fontsize=15, color='dimgray', rotation=90, transform=axs[i, 0].transAxes, ha='center')

        if self.output:
            type = type.replace(" ","_").lower()
            filename =  f"{self.filename}_{type}"
            write_data(self.output_dir, filename, data)
    


    def plot(self):
        logger = log_configure(self.loglevel, 'single_plot')
        logger.debug("Hovmoller plot started")
        
        self.data_for_hovmoller_lev_time_plot()
        
        if self.output:
            self.filename = file_naming(self.region, self.lat_s, self.lat_n, self.lon_w, self.lon_e, 
                                    plot_name=f"{self.model}-{self.exp}-{self.source}_hovmoller_plot")

        # fig, (axs) = plt.subplots(nrows=5, ncols=2, figsize=(14, 25))
        fig, (axs) = plt.subplots(nrows=3, ncols=2, figsize=(14, 20))
        plt.subplots_adjust(bottom=0.3, top=0.85, wspace=0.3, hspace=0.1)
        
        self.loop_details(0, fig, axs)
        self.loop_details(1, fig, axs)
        self.loop_details(2, fig, axs)

        title = f"Spatially averaged {self.region}"
        fig.suptitle(title, fontsize=25, y=0.9)

        if self.output:
            export_fig(self.output_dir, self.filename , "pdf", metadata_value = title)
        logger.debug("Hovmoller plot completed")

        return
    