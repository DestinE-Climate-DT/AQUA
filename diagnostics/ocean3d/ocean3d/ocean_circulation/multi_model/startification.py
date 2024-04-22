from ocean3d.ocean_circulation.ocean_circulation import *
from ocean3d import split_ocean3d_req


class stratification:
    def __init__(self, o3d_request):
        self.data_dict = o3d_request.get('data_dict')
        self.region = o3d_request.get('region', None)
        self.lat_s = o3d_request.get('lat_s', None)
        self.lat_n = o3d_request.get('lat_n', None)
        self.lon_w = o3d_request.get('lon_w', None)
        self.lon_e = o3d_request.get('lon_e', None)
        self.level = o3d_request.get('level', None)
        self.region_list = o3d_request.get('region_list', None)
        
        self.time = o3d_request.get('time', None)
        # self.overlap = o3d_request.get('overlap', None)
        self.output = o3d_request.get('output')
        self.output_dir = o3d_request.get('output_dir', None)
        self.loglevel = o3d_request.get('loglevel',"WARNING")

        self.logger = log_configure(self.loglevel, 'stratification')

    def data_regions(self):
        self.region_data = {}
        for region in self.region_list:
            self.region_data[region] = self.prepare_data_list(region)
        return
        
    
    def prepare_data_list(self, region):
        # self.obs_data = crop_obs_overlap_time(self.data, self.obs_data)
        strat_data_dict = {}
        for data_name in self.data_dict:
            data, time = prepare_data_for_stratification_plot(
                                self.data_dict[data_name], region, self.time, self.lat_s, self.lat_n, self.lon_w, self.lon_e)
            strat_data_dict[data_name] = data
        return strat_data_dict

    def plot(self):
        
        self.data_regions()
        legend_list = [] 

        nrows = len(self.region_data)
        
        fig, axs = plt.subplots(nrows = nrows, ncols=3, figsize=(14, 7.5*nrows))
        self.logger.info("Stratification plot is in process")
        
        if nrows == 1:
            axs = axs.reshape(1, -1)
            
        for row, region_name in enumerate(self.region_data):
            axs[row, 0].text(-0.35, 0.33, region_name, fontsize=18, color='dimgray', rotation=90, transform=axs[row, 0].transAxes, ha='center')
            data_list = self.region_data[region_name]
            for column, var in enumerate(["avg_thetao", "avg_so", "rho"]):
                for data_name in data_list:
                    data = data_list[data_name][var]

                    if self.time in ["Yearly"]:
                        start_year = data.time[0].data
                        end_year = data.time[-1].data
                        logger.debug(end_year)
                    else:
                        start_year = data.time[0].dt.year.data
                        end_year = data.time[-1].dt.year.data
                    
                    data = data.mean('time')
                    line, = axs[row,column].plot(data, data.lev,
                                                 linewidth=1.0)
                    if column == 2 and row == 0:
                        line.set_label(data_name)
                        legend_list.append(f"{data_name} ({start_year}-{end_year})")
    
                axs[row, column].set_ylim((4500, 0))
                # axs[0,0].set_ylabel("Depth (m)", fontsize=15)

                # axs[0,1].set_title("Salinity Profile", fontsize=16)
                if column != 0:
                    axs[row, column].set_ylabel("", fontsize=0)
                    axs[row, column].set_yticklabels([])
                else:
                    axs[row, column].set_ylabel("Depth", fontsize=0)
                    # axs[row, column].set_yticklabels([])

                # if row != 3:
                #     axs[row, column].set_xlabel("", fontsize=0)
                #     axs[row, column].set_xticklabels([])
                
           
        axs[0, 0].set_title("Temperature", fontsize=16)
        axs[nrows-1, 0].set_xlabel("Temperature (°C)", fontsize=12)

        axs[0, 1].set_title("Salinity", fontsize=16)
        axs[nrows-1, 1].set_xlabel("Salinity (psu)", fontsize=12)

        axs[0, 2].set_title("Rho (ref 0)", fontsize=16)
        axs[nrows-1, 2].set_xlabel("Density Anomaly (kg/m³)", fontsize=12)


        axs[0, 2].legend(legend_list, loc='best')



        # if self.output:
        #     filename = file_naming(self.region, self.lat_s, self.lat_n, self.lon_w, self.lon_e, plot_name=f"{self.model}-{self.exp}-{self.source}_stratification_{self.time}_clim")
                
        # legend_list = []
        # if self.time in ["Yearly"]:
        #     start_year = data_list[0].time[0].data
        #     end_year = data_list[0].time[-1].data
        #     logger.debug(end_year)
        # else:
        #     start_year = data_list[0].time[0].dt.year.data
        #     end_year = data_list[0].time[-1].dt.year.data

        # for i, var in zip(range(len(axs)), ["avg_thetao", "avg_so", "rho"]):
        #     axs[0,i].set_ylim((4500, 0))
        #     data_1 = data_list[0][var].mean("time")

        #     axs[0,i].plot(data_1, data_1.lev, 'g-', linewidth=2.0)
        #     legend_info = f"Model {start_year}-{end_year}"
        #     legend_list.append(legend_info)
        #     if self.output:
        #         new_filename = f"{filename}_{legend_info.replace(' ', '_')}"
        #         write_data(self.output_dir, new_filename, data_1)

        #     if len(data_list) > 1:
        #         if time in ["Yearly"]:
        #             start_year_data_2 = data_list[1].time[0].data
        #             end_year_data_2 = data_list[1].time[-1].data
        #             logger.debug(end_year)
        #         else:
        #             start_year_data_2 = data_list[1].time[0].dt.year.data
        #             end_year_data_2 = data_list[1].time[-1].dt.year.data
                
        #         data_2 = data_list[1][var].mean("time")
        #         axs[0,i].plot(data_2, data_2.lev, 'b-', linewidth=2.0)

        #         legend_info_data_2 = f"Model {start_year_data_2}-{end_year_data_2}"
        #         legend_list.append(legend_info_data_2)
        #         if output:
        #             new_filename = f"{filename}_{legend_info_data_2.replace(' ', '_')}"
        #             write_data(self.output_dir, new_filename, data_2)

        #     if self.obs_data is not None:
        #         data_3 = self.obs_data[var].mean("time")
        #         axs[0,i].plot(data_3, data_3.lev, 'r-', linewidth=2.0)
        #         if var == "avg_thetao":
        #             axs[i].plot(obs_data["thetao_uncertainty"].mean("time"), data_3.lev, 'b-', linewidth=1.0)
        #         if var == "avg_so":
        #             axs[i].plot(obs_data["so_uncertainty"].mean("time"), data_3.lev, 'b-', linewidth=1.0)
                
        #         legend_info = f"Obs {start_year}-{end_year}"
        #         legend_list.append(legend_info)
        #         if self.output:
        #             new_filename = f"{filename}_{legend_info.replace(' ', '_')}"
        #             write_data(self.output_dir, new_filename, data_3)


        # region_title = custom_region(region=self.region, lat_s=self.lat_s, lat_n= self.lat_n, lon_w= self.lon_w, lon_e= self.lon_e)

        title = f"Climatological {self.time.upper()} T, S and rho0 stratification"
        fig.suptitle(title, fontsize=20)
        # axs[0,0].legend(legend_list, loc='best')
        



        # if self.output:
        #     export_fig(self.output_dir, filename , "pdf", metadata_value = title, loglevel= self.loglevel)

        return


