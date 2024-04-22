from ocean3d.ocean_drifts.tools import *
from ocean3d.ocean_drifts.hovmoller_plot import hovmoller_plot
import re
from datetime import datetime

class time_series:
    def __init__(self, o3d_request):
        self.data_dict = o3d_request.get('data_dict')
        self.region = o3d_request.get('region', None)
        self.lat_s = o3d_request.get('lat_s', None)
        self.lat_n = o3d_request.get('lat_n', None)
        self.lon_w = o3d_request.get('lon_w', None)
        self.lon_e = o3d_request.get('lon_e', None)
        self.level = o3d_request.get('level', None)
        self.output = o3d_request.get('output')
        self.output_dir = o3d_request.get('output_dir', None)
        self.loglevel = o3d_request.get('loglevel',"WARNING")

    def _calculate_time_series(self):
        self.data_time_series = {}
        for data_name in self.data_dict:
            self.data_dict[data_name] = self.data_dict[data_name].mean("lat").mean("lon")
            self.data_time_series[data_name], self.type, cmap  = data_process_by_type(
                data=self.data_dict[data_name], anomaly=True, standardise=True, anomaly_ref='t0', loglevel=self.loglevel)
            
            self.data_time_series[data_name] = self.data_time_series[data_name].load()
            
        return 

    def plot(self):
        self._calculate_time_series()

        
        
        
        nrows = len(self.data_time_series) 
        fig, axs = plt.subplots(nrows = nrows, ncols=2, figsize=(16, 4*nrows))
        fig.subplots_adjust(hspace=0.18, wspace=0.15, top=.8)
        
        levels = [0, 100, 500, 1500, 3000, 4500]
        if nrows == 1:
            axs = axs.reshape(1, -1)
        for num, data_name in enumerate(self.data_time_series):
            # data = self.data_time_series[data_name].load()
            start_year = self.data_time_series[data_name].time.dt.year[0].values
            end_year = self.data_time_series[data_name].time.dt.year[-1].values
            
            for level in levels:
                if level != 0:
                    # Select the data at the specified level
                    data_level = self.data_time_series[data_name].sel(lev=slice(None, level)).isel(lev=-1)
                else:
                    # Select the data at the surface level (0)
                    data_level = self.data_time_series[data_name].isel(lev=0)
            
                # Plot the temperature time series
                data_level.avg_thetao.plot.line(
                    ax=axs[num,0],
                    label=f"{round(int(data_level.lev.data),
                    -2)}")

                # Plot the salinity time series
                data_level.avg_so.plot.line(
                    ax=axs[num,1],
                    label=f"{round(int(data_level.lev.data),
                    -2)}")
                
                if re.search(r'Hist', data_name):
                    start_date = np.datetime64('1990-01-01')
                    end_date =  np.datetime64('2006-12-31')
                    axs[num, 1].set_xlim(start_date, end_date)
                    axs[num, 0].set_xlim(start_date, end_date)
                
                if re.search(r'ssp', data_name):
                    start_date = np.datetime64('2020-01-01')
                    end_date =  np.datetime64('2039-12-31')
                    axs[num, 1].set_xlim(start_date, end_date)
                    axs[num, 0].set_xlim(start_date, end_date)
                
                axs[num,0].set_title("", fontsize=18)
                axs[num,1].set_title("", fontsize=18)

            axs[num,0].set_ylabel(f"Pot. Temperature ", fontsize=12)
            axs[num,1].set_ylabel(f"Salinity", fontsize=12)
            # axs[num, 1].set_yticklabels([])
            # if num != len(self.data_time_series)-1:
            #     axs[num, 0].set_xticklabels([])
            #     axs[num, 1].set_xticklabels([])
            axs[0,0].set_title(f"Pot. Temperature", fontsize=18)
            axs[0,1].set_title(f"Salinity", fontsize=18)
            axs[num, 0].text(-0.22, 0.25, f"{data_name} \n ({start_year}-{end_year}) " , fontsize=18, color='black', rotation=90, transform=axs[num, 0].transAxes, ha='center')
           
        axs[0,1].legend(loc='right')
           
        region_title = custom_region(region=self.region, lat_s=self.lat_s, lat_n=self.lat_n, lon_w=self.lon_w, lon_e=self.lon_e)
        self.title = f'Time Series of the {region_title} (Std. anomaly)'
        plt.suptitle(self.title, fontsize=24)
        # if self.output:
        #     self._save_plot_data(data)


    def _save_plot_data(self, data):
        """
        Saves plot data.
        """
        filename = file_naming(self.region, self.lat_s, self.lat_n, self.lon_w, self.lon_e, plot_name=f"{self.model}-{self.exp}-{self.source}_multilevel_t_s_trend")
        write_data(self.output_dir, filename, data.interp(lev=self.levels[-1]))
        export_fig(self.output_dir, filename, "pdf", metadata_value=self.title, loglevel=self.loglevel)
        return