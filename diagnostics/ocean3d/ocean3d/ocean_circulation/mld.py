from .ocean_circulation import *
from ocean3d import write_data
from ocean3d import export_fig
from ocean3d import split_ocean3d_req
from ocean3d import round_up
import IPython


class mld:
    def __init__(self, o3d_request):
        split_ocean3d_req(self,o3d_request)
        self.logger = log_configure(self.loglevel, 'plot_spatial_mld')
    def _process_data(self):
        self.mod_clim, self.time = prepare_data_for_stratification_plot(self.data, self.region, self.time,
                                                        self.lat_s, self.lat_n, self.lon_w, self.lon_e,
                                                        areamean= False, timemean= True,
                                                        compute_mld= True, loglevel= self.loglevel)  # To select the month and compute its climatology
        
        self.mod_clim = self.mod_clim[["mld"]].compute()
        
        if self.obs_data:
            self.obs_data = crop_obs_overlap_time(self.data, self.obs_data)
            self.data = crop_obs_overlap_time(self.obs_data, self.data)
            self.obs_clim, self.time = prepare_data_for_stratification_plot(self.obs_data, self.region, self.time,
                                                            self.lat_s, self.lat_n, self.lon_w, self.lon_e, areamean= False,
                                                            timemean= True, compute_mld= True, loglevel= self.loglevel )  # To select the month and compute its climatology
            self.obs_clim = self.obs_clim[["mld"]].compute()

    def plot(self):
        self._process_data()
        self.logger.debug("Spatial MLD plot is in process")
        fig, axs = plt.subplots(nrows=1, ncols=2, figsize=(20, 6.5))

        region_title = custom_region(region = self.region, lat_s = self.lat_s, lat_n = self.lat_n, lon_w = self.lon_w, lon_e = self.lon_e)
        title = f'Climatology of {self.time.upper()} mixed layer depth in {region_title}'
        fig.suptitle(title, fontsize=20)
        fig.set_figwidth(18)

        clev1 = 0.0
        if self.obs_data:
            clev2 = max(self.obs_clim["mld"].max(), self.mod_clim["mld"].max())
            
        else: 
            clev2 = self.mod_clim["mld"].max()
        clev2 = round_up(clev2)
        if clev2 < 200:
            inc = 10
        elif clev2 > 1500:
            inc = 100
        else:
            inc = 50

        nclev = int(clev2/inc)+1
        clev2 = float(clev2)

        cs1 = axs[0].contourf(self.mod_clim.lon, self.mod_clim.lat, self.mod_clim["mld"],
                            levels=np.linspace(clev1, clev2, nclev),
                            cmap='jet')
        # fig.colorbar(cs1, location="bottom", label='Mixed layer depth (in m)')
        cbar_ax = fig.add_axes([0.15, -0.05, 0.7, 0.03])
        cbar = fig.colorbar(cs1, cax=cbar_ax, orientation='horizontal')
        cbar.set_label('Mixed layer depth (in m)')

        axs[0].set_title(f"Model climatology {self.mod_clim.attrs['start_year']}-{self.mod_clim.attrs['end_year']}", fontsize=18)
        axs[0].set_ylabel("Latitude", fontsize=14)
        axs[0].set_xlabel("Longitude", fontsize=14)
        axs[0].set_facecolor('grey')
        if self.obs_data:
            cs1 = axs[1].contourf(self.obs_clim.lon, self.obs_clim.lat, self.obs_clim["mld"],
                                levels=np.linspace(clev1, clev2, nclev), cmap='jet')

            axs[1].set_title(f"EN4 OBS climatology {self.obs_clim.attrs['start_year']}-{self.obs_clim.attrs['end_year']}", fontsize=18)
            # axs[1].set_ylabel("Latitude", fontsize=12)
            axs[1].set_xlabel("Longitude", fontsize=14)
            axs[1].set_yticklabels([])
            axs[1].set_facecolor('grey')
        else:
            axs[1].set_title(f"Observation data not available", fontsize=18)
            

        plt.subplots_adjust(top=0.85, wspace=0.1)

        if self.output:
            filename = file_naming(self.region, self.lat_s, self.lat_n, self.lon_w, self.lon_e, plot_name=f"{self.model}-{self.exp}-{self.source}_spatial_MLD_{self.time}")
            write_data(self.output_dir,f"{filename}_mod_clim", self.mod_clim)
            if self.obs_data:
                write_data(self.output_dir,f"{filename}_obs_clim", self.obs_clim)
            export_fig(self.output_dir, filename , "pdf", metadata_value = title, loglevel= self.loglevel)

            if not IPython.get_ipython():  
                plt.close() 
        return