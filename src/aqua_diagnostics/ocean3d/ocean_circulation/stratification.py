from .ocean_circulation import *
from ocean3d import split_ocean3d_req

class stratification:
    def __init__(self, o3d_request):
        split_ocean3d_req(self, o3d_request)
        self.logger = log_configure(self.loglevel, 'stratification')

    def prepare_data_list(self):
        self.obs_data = crop_obs_overlap_time(self.data, self.obs_data)

        obs_data, time = prepare_data_for_stratification_plot(
            self.obs_data, self.region, self.time, self.lat_s, self.lat_n, self.lon_w, self.lon_e)
        data, self.time = prepare_data_for_stratification_plot(
            self.data, self.region, self.time, self.lat_s, self.lat_n, self.lon_w, self.lon_e)
        data_list, self.obs_data = compare_arrays(data, obs_data)

        data_list = list(
            filter(lambda value: value is not None, data_list))
        return data_list

    def plot(self):
        
        data_list = self.prepare_data_list()

        fig, axs = plt.subplots(nrows=1, ncols=3, figsize=(14, 8))
        self.logger.info("Stratification plot is in process")

        if self.output:
            filename = file_naming(self.region, self.lat_s, self.lat_n, self.lon_w, self.lon_e, plot_name=f"{self.model}-{self.exp}-{self.source}_stratification_{self.time}_clim")
                
        legend_list = []
        if self.time in ["Yearly"]:
            start_year = data_list[0].time[0].data
            end_year = data_list[0].time[-1].data
            logger.debug(end_year)
        else:
            start_year = data_list[0].time[0].dt.year.data
            end_year = data_list[0].time[-1].dt.year.data

        for i, var in zip(range(len(axs)), ["avg_thetao", "avg_so", "rho"]):
            axs[i].set_ylim((4500, 0))
            data_1 = data_list[0][var].mean("time")

            axs[i].plot(data_1, data_1.lev, 'g-', linewidth=2.0)
            legend_info = f"Model {start_year}-{end_year}"
            legend_list.append(legend_info)
            if self.output:
                new_filename = f"{filename}_{legend_info.replace(' ', '_')}"
                write_data(self.output_dir, new_filename, data_1)

            if len(data_list) > 1:
                if self.time in ["Yearly"]:
                    start_year_data_2 = data_list[1].time[0].data
                    end_year_data_2 = data_list[1].time[-1].data
                    logger.debug(end_year)
                else:
                    start_year_data_2 = data_list[1].time[0].dt.year.data
                    end_year_data_2 = data_list[1].time[-1].dt.year.data
                
                data_2 = data_list[1][var].mean("time")
                axs[i].plot(data_2, data_2.lev, 'b-', linewidth=2.0)

                legend_info_data_2 = f"Model {start_year_data_2}-{end_year_data_2}"
                legend_list.append(legend_info_data_2)
                if self.output:
                    new_filename = f"{filename}_{legend_info_data_2.replace(' ', '_')}"
                    write_data(self.output_dir, new_filename, data_2)

            if self.obs_data is not None:
                data_3 = self.obs_data[var].mean("time")
                axs[i].plot(data_3, data_3.lev, 'r-', linewidth=2.0)
                # if var == "avg_thetao":
                #     axs[i].plot(obs_data["thetao_uncertainty"].mean("time"), data_3.lev, 'b-', linewidth=1.0)
                # if var == "avg_so":
                #     axs[i].plot(obs_data["so_uncertainty"].mean("time"), data_3.lev, 'b-', linewidth=1.0)
                
                legend_info = f"Obs {start_year}-{end_year}"
                legend_list.append(legend_info)
                if self.output:
                    new_filename = f"{filename}_{legend_info.replace(' ', '_')}"
                    write_data(self.output_dir, new_filename, data_3)


        region_title = custom_region(region=self.region, lat_s=self.lat_s, lat_n= self.lat_n, lon_w= self.lon_w, lon_e= self.lon_e)

        title = f"Climatological {self.time.upper()} T, S and rho0 stratification in {region_title}"
        fig.suptitle(title, fontsize=20)
        axs[0].legend(legend_list, loc='best')
        
        axs[0].set_title("Temperature Profile", fontsize=16)
        axs[0].set_ylabel("Depth (m)", fontsize=15)
        axs[0].set_xlabel("Temperature (°C)", fontsize=12)

        axs[1].set_title("Salinity Profile", fontsize=16)
        axs[1].set_xlabel("Salinity (psu)", fontsize=12)
        # axs[1].set_ylabel("", fontsize=0)
        axs[1].set_yticklabels([])

        axs[2].set_title("Rho (ref 0) Profile", fontsize=16)
        axs[2].set_xlabel("Density Anomaly (kg/m³)", fontsize=12)
        # axs[2].set_ylabel("", fontsize=0)
        axs[2].set_yticklabels([])


        if self.output:
            export_fig(self.output_dir, filename , "pdf", metadata_value = title, loglevel= self.loglevel)

        return


