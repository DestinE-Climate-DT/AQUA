from .tools import *
from ocean3d import write_data
from ocean3d import export_fig
from ocean3d import split_ocean3d_req
import matplotlib.pyplot as plt
from aqua.logger import log_configure




class time_series:
    def __init__(self, o3d_request):
        self = split_ocean3d_req(self,o3d_request)

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
                    avg_thetaolevs, solevs = 20, 20
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
        customise_level = False

        if customise_level:
            if levels is None:
                raise ValueError(
                    "Custom levels are selected, but levels are not provided.")
        else:
            levels = [0, 100, 500, 1000, 2000, 3000, 4000, 5000]

        for level in levels:
            if level != 0:
                # Select the data at the specified level
                data_level = data.sel(lev=slice(None, level)).isel(lev=-1)
            else:
                # Select the data at the surface level (0)
                data_level = data.isel(lev=0)
           
            # Plot the temperature time series
            data_level.avg_thetao.plot.line(
                ax=axs[i,0], label=f"{round(int(data_level.lev.data), -2)}")

            # Plot the salinity time series
            data_level.avg_so.plot.line(
                ax=axs[i,1], label=f"{round(int(data_level.lev.data), -2)}")

        tunits = data_level.avg_thetao.attrs['units']
        sunits = data_level.avg_so.attrs['units']
        axs[i,0].set_title('')
        axs[i,1].set_title('')

        # axs[i,0].set_title("Temperature", fontsize=15)
        axs[i,0].set_ylabel(f"Pot. Temperature ", fontsize=12)
        # axs[i,0].set_xlabel("Time", fontsize=12)
        # axs[i,1].set_title("Salinity", fontsize=15)
        axs[i,1].set_ylabel(f"Salinity", fontsize=12)
        # axs[i,1].set_xlabel("Time (in years)", fontsize=12)
        axs[i,1].legend(loc='right')

        if i==0:
            axs[i,1].set_title(f"Salinity ({sunits})", fontsize=20) 
            axs[i,0].set_title(f"Temperature ({tunits})", fontsize=20) 
        if i==2:
            axs[i,0].set_xlabel("Time", fontsize=12)
            axs[i,1].set_xlabel("Time", fontsize=12) 

            axs[i,0].set_xticklabels(axs[i,0].get_xticklabels(), rotation=30)
            axs[i,1].set_xticklabels(axs[i,0].get_xticklabels(), rotation=30)
        else:
            axs[i,0].set_xlabel(" ")
            axs[i,1].set_xlabel(" ") 
            
            axs[i, 0].set_xticklabels([])
            axs[i, 1].set_xticklabels([])


        # axs[i,1].set_yticklabels([])

        axs[i, 0].text(-0.35, 0.3, type.replace("wrt", "\nwrt\n"), fontsize=15, color='dimgray', rotation=90, transform=axs[i, 0].transAxes, ha='center')

        if self.output:
            type = type.replace(" ","_").lower()
            filename =  f"{self.filename}_{type}"
            write_data(self.output_dir, filename, data)
    

    def plot(self):
        logger = log_configure(self.loglevel, 'single_plot')
        
        logger.debug("Time series plot started")
        
        self.data_for_hovmoller_lev_time_plot()

        if self.output:
            self.filename = file_naming(self.region, self.lat_s, self.lat_n, self.lon_w, self.lon_e, 
                                    plot_name=f"{self.model}-{self.exp}-{self.source}_time_series")

  

        fig, (axs) = plt.subplots(nrows=3, ncols=2, figsize=(14, 20))
        plt.subplots_adjust(bottom=0.3, top=0.85, wspace=0.3, hspace=0.1)
        
        self.loop_details(0, fig, axs)
        self.loop_details(1, fig, axs)
        self.loop_details(2, fig, axs)
        # self.loop_details(3, fig, axs)
        # self.loop_details(4, fig, axs)

        title = f"Time Series of {self.region}"
        fig.suptitle(title, fontsize=25, y=0.9)

        if self.output:
            export_fig(self.output_dir, self.filename , "pdf",
                        metadata_value = title)
        
        logger.debug("Time series plot completed")
        return


 