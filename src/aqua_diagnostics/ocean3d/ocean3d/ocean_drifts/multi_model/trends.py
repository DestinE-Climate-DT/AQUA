from ocean3d.ocean_drifts.tools import *
from ocean3d.ocean_drifts.trends import TrendCalculator 



class trend:
    def __init__(self, o3d_request):
        self.data_dict = o3d_request.get('data_dict')
        # self.model = o3d_request.get('model')
        # self.exp = o3d_request.get('exp')
        # self.source = o3d_request.get('source')
        # self.obs_data = o3d_request.get('obs_data', None)
        self.time = o3d_request.get('time', None)
        self.region = o3d_request.get('region', None)
        self.lat_s = o3d_request.get('lat_s', None)
        self.lat_n = o3d_request.get('lat_n', None)
        self.lon_w = o3d_request.get('lon_w', None)
        self.lon_e = o3d_request.get('lon_e', None)
        self.customise_level = o3d_request.get('customise_level', None)
        self.level = o3d_request.get('level', None)
        self.overlap = o3d_request.get('overlap', None)
        self.output = o3d_request.get('output')
        self.output_dir = o3d_request.get('output_dir', None)
        self.loglevel = o3d_request.get('loglevel',"WARNING")
        self.logger = log_configure(self.loglevel, 'data_process_by_type')

    def plot(self):
        self.TS_trend_dict = {}
        for i, data_name in enumerate(self.data_dict):
            data = self.data_dict[data_name]
            trend_data_name = f"{data_name}"
            data = area_selection(data, self.region, self.lat_s, self.lat_n, self.lon_w, self.lon_e)
            TS_trend_data = self._calculate_trend_data(data)
            TS_trend_data = TS_trend_data[["avg_thetao","avg_so"]]
            if self.level != None:
                TS_trend_data = TS_trend_data.interp(lev=self.level).squeeze()
            else:
                self.logger.info("leve is not define, selecting surface level")
                TS_trend_data = TS_trend_data.isel(lev=0).squeeze()
                self.level = TS_trend_data.lev.data
                
            self.TS_trend_dict[trend_data_name] = TS_trend_data.load()
    
        self._plot_multilevel_trend(TS_trend_data)
        return


    def _calculate_trend_data(self, data):
        """
        Calculates the trend data for temperature and salinity.
        """
        TS_trend_data = TrendCalculator.TS_3dtrend(data, loglevel=self.loglevel)
        TS_trend_data.attrs = data.attrs
        return TS_trend_data

    def _plot_multilevel_trend(self, data):
        """
        Plots the multilevel trend for temperature and salinity.
        """  
        # cbar_value = {'max_values': {"avg_thetao": [],
        #                              "avg_so": []},
        #               'min_values': {"avg_thetao": [],
        #                              "avg_so": []}}

        # for data_name in self.TS_trend_dict:
        #     data = self.TS_trend_dict[data_name]
        #     for variable in list(data.data_vars):
        #         max_value = np.max(data[variable])
        #         min_value = np.min(data[variable])
        #         cbar_value['max_values'][variable].append(max_value)
        #         cbar_value['min_values'][variable].append(min_value)

        # self.max_values = {}
        # self.min_values = {}

        # for variable, values_list in cbar_value['max_values'].items():
        #     self.max_values[variable] = np.max(values_list)

        # for variable, values_list in cbar_value['min_values'].items():
        #     self.min_values[variable] = np.min(values_list)

                    
        nrows = len(self.TS_trend_dict)
        
        

        fig, axs = plt.subplots(nrows= nrows, ncols=2, figsize=(16, 6 * nrows))
        fig.subplots_adjust(hspace=0.18, wspace=0.15, top=0.85)
        
        if nrows == 1:
            axs = axs.reshape(1, -1)
        
        # avg_thetao_max = np.round(max(np.abs(self.min_values["avg_thetao"]), np.abs(self.max_values["avg_thetao"])), 1)
        
        # if avg_thetao_max*10 % 2 == 0:
        #     num = 11
        # else:
        #     num = 10
            
        # avg_thetao_levels = np.linspace(-avg_thetao_max, avg_thetao_max, num=num).round(2)
        # avg_thetao_levels = np.insert(avg_thetao_levels, np.searchsorted(avg_thetao_levels, 0), 0)
        # avg_thetao_levels = np.insert(avg_thetao_levels, np.searchsorted(avg_thetao_levels, 0), 0)
        
        # avg_so_max = np.round(max(np.abs(self.min_values["avg_so"]), np.abs(self.max_values["avg_so"])), 1)
        # avg_so_levels = np.linspace(-avg_so_max, avg_so_max, num=18).round(2)
        # avg_so_levels = np.insert(avg_so_levels, np.searchsorted(avg_so_levels, 0), 0)


        avg_thetao_levels = np.linspace(-0.5, 0.5, 11)
        avg_so_levels = np.linspace(-1, 1, 21)




        for num, data_name in enumerate(self.TS_trend_dict):
            data = self.TS_trend_dict[data_name]
            start_year = self.data_dict[data_name].time.dt.year[0].values
            end_year = self.data_dict[data_name].time.dt.year[-1].values
            
            cs1 = axs[num,0].contourf(data.lon, data.lat, data["avg_thetao"], cmap="RdBu_r", levels=avg_thetao_levels)
            cs2 = axs[num,1].contourf(data.lon, data.lat, data["avg_so"], cmap="RdBu_r", levels=avg_so_levels)
            axs[num, 0].set_facecolor('grey')
            axs[num, 1].set_facecolor('grey')
            # axs[num, 0].set_ylabel("Latitude (in deg North)", fontsize=9)
            axs[num, 1].set_yticklabels([])
            # axs[num, 0].set_xlabel("Longitude (in de East)", fontsize=12)
            # axs[num, 0].set_ylabel("Latitude (in deg North)", fontsize=12)
            if num != len(self.TS_trend_dict)-1:
                axs[num, 0].set_xticklabels([])
                axs[num, 1].set_xticklabels([])
            axs[0,0].set_title(f"Pot. Temperature", fontsize=18)
            axs[0,1].set_title(f"Salinity", fontsize=18)
            axs[num, 0].text(-0.13, 0.33, f"{data_name} \n ({start_year}-{end_year}) " , fontsize=18, color='black', rotation=90, transform=axs[num, 0].transAxes, ha='center')
           
        # cb = fig.colorbar(cs1, ax=axs, location="bottom", pad=0.05, aspect=40, label='Mixed layer depth (in m)')
        # plt.subplots_adjust(bottom=0.30)
        cax = fig.add_axes([0.15, 0.07, 0.3, 0.015])
        cb1 = fig.colorbar(cs1, cax=cax, orientation='horizontal', label=f'Pot Temp trend in {data.avg_thetao.attrs["units"]}')
        
        # cb1 = fig.colorbar(cs1, ax=axs[num,0], location="bottom", pad=0.2, aspect=40, label=f'Pot Temp trend in {data.avg_thetao.attrs["units"]}')
        # cb_pos = cb1.ax.get_position()
        # cb1.ax.set_position([cb_pos.x0, -0.05, cb_pos.width, cb_pos.height])
        
        # cb2 = fig.colorbar(cs2, ax=axs[num,1], location="bottom", pad=0.2, aspect=40, label=f'Salinity trend in {data.avg_thetao.attrs["units"]}')
        
        cax = fig.add_axes([0.56, 0.07, 0.3, 0.015])
        cb2 = fig.colorbar(cs2, cax=cax, orientation='horizontal', label=f'Salinity trend in {data.avg_so.attrs["units"]}')
        # cb_pos = cb2.ax.get_position()
        # cb2.ax.set_position([cb_pos.x0, -0.02, cb_pos.width, cb_pos.height])
        
        region_title = custom_region(region=self.region, lat_s=self.lat_s, lat_n=self.lat_n, lon_w=self.lon_w, lon_e=self.lon_e)
        level = int(str(self.level).split('.')[0])
        self.title = f'Linear Trends of T,S at {level}m in the {region_title}'
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