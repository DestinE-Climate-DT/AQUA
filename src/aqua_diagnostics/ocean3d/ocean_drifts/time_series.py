from .tools import *
from aqua.diagnostics.ocean3d import write_data
from aqua.diagnostics.ocean3d import export_fig
from aqua.diagnostics.ocean3d import split_ocean3d_req
import matplotlib.pyplot as plt
from .hovmoller_plot import hovmoller_plot
from aqua.logger import log_configure




class time_series(hovmoller_plot):
    """
    A class for generating time series plots from ocean3d data.

    Inherits from hovmoller_plot.

    Args:
        o3d_request: Request object containing necessary data for plot generation.

    Attributes:
        Inherits all attributes from the parent class.
    """
    def __init__(self, o3d_request):
        """
        Initializes the time_series object.

        Args:
            o3d_request: Request object containing necessary data for plot generation.
        """
        super().__init__(o3d_request)
  
    def loop_details(self, i, fig, axs):
        """
        Loop over plot details to generate time series plots.

        Args:
            i: Index indicating the loop iteration.
            fig: Figure object for plotting.
            axs: Axes object for plotting.
        """
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
        """
        Generate and display time series plots.

        Returns:
            None
        """
        logger = log_configure(self.loglevel, 'single_plot')
        
        logger.debug("Time series plot started")
        
        # self.data_for_hovmoller_lev_time_plot()
        super().data_for_hovmoller_lev_time_plot()


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


 