import os
import gc
import sys
import numpy as np
import pandas as pd
import xarray as xr
import cartopy.crs as ccrs
import matplotlib.pyplot as plt
from aqua import Reader
from aqua.exceptions import NotEnoughDataError, NoDataError, NoObservationError
from aqua.util import create_folder, coord_names, area_selection
from aqua import Reader, plot_single_map
from aqua.logger import log_configure

class sshVariabilityCompute():
    def __init__(self, data=None, variable=None, catalog=None, model=None, exp=None, source=None, startdate=None, enddate=None, regrid=None, outputdir=None, loglevel='WARNING'):
        """
        Initialize the sshVariability.

        This class is designed to load an xarray.Dataset and computes std. It can load either load data using the Reader class or takes in input xarray.Dataset and regrids it using the Reader.regrid method. It then returns the xarray.Dataset. 
        Args:
            variable (str): Variable name
            catalog (str): catalog 
            data: xarray.Dataset 
            model (str): Name of the data
            exp (str): Name of the experiment
            source (str): the source
            startdate (str): Start date 
            enddate  (str): End date 
            regrid (str): Regrid option for regridding
            outputdir (str): output directory
            loglevel (str): Default WARNING
        """
        self.variable = variable
        self.catalog = catalog
        self.model = model
        self.exp = exp
        self.source = source 
        self.regrid = regrid
        self.startdate = startdate
        self.enddate = enddate
        self.outputdir = outputdir + "/ssh"
        self.logger = log_configure(log_level=loglevel, log_name='SSH Variability Computation')
        self.data = data 

    def save_netcdf(self):
        """
        Save the standard deviation data to a NetCDF file.
        """
        # Create the file type folder within the output directory
        file_type_folder = os.path.join(self.outputdir,"netcdf")
        os.makedirs(file_type_folder, exist_ok=True)

        # Set the output file path
        if self.data is not None:
            output_file = os.path.join(file_type_folder, f"{self.model}_{self.exp}_{self.source}_{self.startdate}_to_{self.enddate}_std.nc")
            self.data.to_netcdf(output_file)
        else:
            self.logger.error("The data can not be saved")

    def run(self):
        # Retrieve model data and handle potential errors
        try:
            reader = Reader(catalog=self.catalog, model=self.model, exp=self.exp, source=self.source,
                            startdate=self.startdate, enddate=self.enddate, regrid=self.regrid)
            if self.data is not None and self.regrid:
                self.data = reader.regrid(self.data)
            elif self.data is not None and self.regrid is None:
                self.logger(f"Precomputed data for {self.model} may not be reggrid correctly. This could lead to errors in the ssh variabilty calculation")
            else:
                data = reader.retrieve(var=self.variable)
                data = data[self.variable]
                self.data = data.std(axis=0)
                if self.regrid:
                    self.data = reader.regrid(self.data)
                    self.save_netcdf()
                else:
                    self.logger.warning(
                    "No regridding applied. Data is in native grid, "
                    "this could lead to errors in the ssh variability calculation if the data is not in the same grid as the reference data."
                    )

        except Exception as e:
            self.logger.error(f"No model data found: {e}")
            sys.exit("SSH diagnostic terminated.")

        return self.data


class sshVariabilityPlot():
    def __init__(self, variable=None, data_ref=None, data_model=None, name_ref=None, name_model=None, exp_ref=None, exp_model=None, source_ref=None, source_model=None, startdate_model=None, enddate_model=None, startdate_ref=None, enddate_ref=None, outputdir=None, loglevel='WARNING', **kwargs):
        """
        Initialize the sshVariability.

        Args:
            variable (str): Variable name
            data_ref: xarray.Dataset of the obeservational data
            data_model: xarray.Dataset of the model data
            name_ref (str): Name of the observational data
            name_model (str): Name of the model data
            exp_ref (str): Name of the obervational experiment
            exp_model (str): Name of the model experiment
            startdate_ref (str): Start date obs. 
            enddate_ref (str): End date obs.
            startdate_model (str): Start date model
            enddate_model (str): End date model
            outputdir (str): output directory
            loglevel (str): Default WARNING
        """
        self.variable = variable
        self.data_ref = data_ref
        self.data_model = data_model
        self.name_ref = name_ref
        self.name_model = name_model
        self.exp_ref = exp_ref
        self.exp_model = exp_model
        self.source_ref = source_ref
        self.source_model = source_model
        self.startdate_model = startdate_model or pd.to_datetime(data_model.time[0].values).strftime('%Y-%m-%d')
        self.enddate_model = enddate_model or pd.to_datetime(data_model.time[-1].values).strftime('%Y-%m-%d')
        self.startdate_ref = startdate_ref or pd.to_datetime(data_ref.time[0].values).strftime('%Y-%m-%d')
        self.enddate_ref = enddate_ref or pd.to_datetime(data_ref.time[-1].values).strftime('%Y-%m-%d')
        self.outputdir = outputdir + "/ssh"
        self.logger = log_configure(log_level=loglevel, log_name='SSH Variability')
        # Plot options with default options. New values can be defined in the config file
        plot_options = kwargs.get('plot_options',{})
        self.scale_min = plot_options.get('scale_min',0.1)
        self.scale_max = plot_options.get('scale_max',0.4)
        self.cmap = plot_options.get('cmap','viridis')
        self.difference_plots = plot_options.get('difference_plots',False)
        self.region_selection = plot_options.get('region_selection',False)
        #self.fig_format = plot_options.get('fig_format','png')
        self.contours = plot_options.get('contours',0)
        #self.dpi = plot_option.get('dpi',300)
        # Region selection. New values can be defined in the config file
        region = kwargs.get('sub_region', {})
        self.region_name = region.get('name','Agulhas')
        self.lon_lim = region.get('lon_lim',[5,50])
        self.lat_lim = region.get('lat_lim',[-10,-50])
        # Retrieve the masking flags and boundary latitudes from the configuration, Specific to ICON
        mask_options = kwargs.get('mask_options',{})
        self.mask_northern_boundary = mask_options.get("mask_northern_boundary", True)
        self.mask_southern_boundary = mask_options.get("mask_southern_boundary", True)
        self.northern_boundary_latitude = mask_options.get("northern_boundary_latitude", 70)
        self.southern_boundary_latitude = mask_options.get("southern_boundary_latitude", -62)


    def plot_std(self, data_std=None, model_name=None, exp_name=None, source_name=None, startdate=None, enddate=None, contours=21):
        """
        Visualize the SSH variability using Cartopy.

        Args:
            data_std: xarray.Dataset as input to be plotted.
            The following Args are used for the title in the plot
            model_name (str): Name of the Dataset.
            exp_name (str): Name of the experiment.
            source (str): source defined in the catalog file
            startdate (str): start date
            enddate (str): end date
            contours (int): Number of contours for plot (default: 21). If 0 or None the pcolormesh is used.
        """
        self.logger.info(f'Plotting std for: {model_name}.')
        fig, ax = plt.subplots(figsize=(12, 6),subplot_kw={'projection': ccrs.PlateCarree()})
        # Apply masking if the model is "ICON" and the flags are enabled with boundary latitudes provided
        if "ICON" in model_name and self.mask_northern_boundary and self.northern_boundary_latitude:
            data_std = data_std.where(data.lat < self.northern_boundary_latitude)
        if "ICON" in model_name and self.mask_southern_boundary and self.southern_boundary_latitude:
            data_std = data_std.where(data_std.lat > self.southern_boundary_latitude)
       
        lonname, latname = coord_names(data_std)

        if contours:
            lon, lat = np.meshgrid(data_std[lonname].values, data_std[latname].values)
            levels = np.linspace(self.scale_min,self.scale_max, contours)  # XXX num of levels hardcode, could be selected from config
            contf = ax.contourf(lon, lat, data_std, transform=ccrs.PlateCarree(), 
                                levels=levels, extend='both',cmap=self.cmap,
                                transform_first=True)  # It was failing with transform_first=False
        else:
            contf = ax.pcolormesh(data_std[lonname].values, data_std[latname].values, data_std,
                                  transform=ccrs.PlateCarree(), 
                                  vmin=self.scale_min, vmax=self.scale_max, cmap=self.cmap)
        ax.set_title(f"SSH Variability for {model_name} {exp_name} {source_name} {startdate} to {enddate}") ### here add further data in the tiltle 
        ax.coastlines()               
        
        # Add a colorbar for each subplot
        cbar = fig.colorbar(contf, ax=ax, orientation='vertical', shrink=0.9)
        cbar.set_label('SSH Variability (m)')
        fig.tight_layout()
        if model_name is None:
            model_name = "model"
        if exp_name is None:
            exp_name = "exp"
        if source_name is None:
            source_name = "source"
        filename = model_name + "_" + exp_name + "_" + source_name
        self.save_plot(filename, fig)

    def plot_std_diff(self, contours=21):
        """
        Visualize the difference in SSH variability data between each model and the AVISO model.

        Args:
            contours (int): Number of contours for plot (default: 21). If 0 or None the pcolormesh is used.
        """
        self.logger.info(f'Plotting the difference in std between: {self.name_model} and {self.name_ref}.')
        fig, ax = plt.subplots(figsize=(12, 6), subplot_kw={'projection': ccrs.PlateCarree()})
        #to remove large fill values
        data_ref = xr.where(self.data_ref < 100, self.data_ref, np.nan)
        if self.data_ref is not None and self.data_model is not None:
            #to remove large fill values
            data_ref = xr.where(self.data_ref < 100, self.data_ref, np.nan)
            diff_data = data_ref - self.data_model
        else:
            raise ValueError("Data is missing for for plotting the difference std plot")

        if "ICON" in self.name_model:
            if self.mask_northern_boundary and self.northern_boundary_latitude:
                diff_data = diff_data.where(data.lat < self.northern_boundary_latitude)
            if self.mask_southern_boundary and self.southern_boundary_latitude:
                diff_data = diff_data.where(data.lat > self.southern_boundary_latitude)

        lonname, latname = coord_names(self.data_model)

        if contours:
            lon, lat = np.meshgrid(self.data_model[lonname].values, self.data_model[latname].values)
            levels = np.linspace(-0.4, 0.4, contours)

            contf = ax.contourf(lon, lat, diff_data, transform=ccrs.PlateCarree(), 
                                levels=levels, extend='both',
                                cmap='RdBu', transform_first=True)
        else:
            contf = ax.pcolormesh(self.data_model[lonname], self.data_model[latname], diff_data,
                                transform=ccrs.PlateCarree(),
                                vmin=-0.4,
                                vmax=0.4,
                                cmap="RdBu")

        # ax.set_title(f"{model_name} - Difference from {ref_model_name}")
        ax.coastlines()
        ax.set_title(f"{self.name_model} - Difference from {self.name_ref}")
        # Add a colorbar for each subplot
        cbar = fig.colorbar(contf, ax=ax, orientation='vertical', shrink=0.9)
        cbar.set_label('SSH Variability Difference (m)')
        fig.tight_layout()
        filename = "difference_plot_" + self.name_model + "-" + self.name_ref
        self.save_plot(filename, fig)

    def subregion_plot(self,data=None, model_name=None, exp_name=None):
        """
        Plotting the subregion and saving as pdf and png
        """
        self.logger.info(f'Plotting the sub-region plots: {model_name}.')
        fig = plt.figure(figsize=(12, 8))
        # Apply masking if necessary
        if "ICON" in model_name and self.mask_northern_boundary and self.northern_boundary_latitude:
            data = data.where(data.lat < self.northern_boundary_latitude)
        if "ICON" in model_name and self.mask_southern_boundary and self.southern_boundary_latitude:
            data = data.where(data.lat > self.southern_boundary_latitude)
        
        ssh_sel = area_selection(data, lon=self.lon_lim, lat=self.lat_lim, drop=True)
            
        ax = fig.add_subplot(1, 1, 1)
        
        # pdf plot
        file_type_folder = os.path.join(self.outputdir,"pdf")
        os.makedirs(file_type_folder, exist_ok=True)
            
        plot_single_map(ssh_sel, ax=ax, title=f"{self.region_name} - {model_name}", 
                        vmin=self.scale_min, vmax=self.scale_max, 
                        cmap=self.cmap, save=True,
                        outputdir=file_type_folder,
                        filename=f"SSH_{model_name}_{exp_name}_region_selection.plot.pdf",
                        format='pdf')
        # png plot
        file_type_folder = os.path.join(self.outputdir,"png")
        os.makedirs(file_type_folder, exist_ok=True)
            
        plot_single_map(ssh_sel, ax=ax, title=f"{self.region_name} - {model_name}", 
                        vmin=self.scale_min, vmax=self.scale_max, 
                        cmap=self.cmap, save=True,
                        outputdir=file_type_folder,
                        filename=f"SSH_{model_name}_{exp_name}_region_selection.plot.png",
                        format='png')


    def save_plot(self, filename, fig):
        """
        Saves the plots as a pdf and png image file.
        """
        # Saving plot a pdf
        self.logger.info("Saving figure to pdf")
        file_type_folder = os.path.join(self.outputdir, "pdf")
        self.logger.debug(f"Saving figure to {file_type_folder}")
        file_type_folder = os.path.join(self.outputdir, "pdf")
        os.makedirs(file_type_folder, exist_ok=True)
        output_file = os.path.join(file_type_folder, filename)
        output_file = output_file + '.pdf'
        # Save the figure as a PDF file. fig.savefig() or plt.savefig() should accomplish the same task of saving the figure to a file. (DPI = dots per inch)
        fig.savefig(output_file, format='pdf')
        
        # Saving plot a png
        self.logger.info("Saving figure to png")
        file_type_folder = os.path.join(self.outputdir, "png")
        self.logger.debug(f"Saving figure to {file_type_folder}")
        file_type_folder = os.path.join(self.outputdir, "png")
        os.makedirs(file_type_folder, exist_ok=True)
        output_file = os.path.join(file_type_folder, filename)
        output_file = output_file + '.png'
        # Save the figure as a PDF file. fig.savefig() or plt.savefig() should accomplish the same task of saving the figure to a file. (DPI = dots per inch)
        fig.savefig(output_file, format='png')

    def run(self):
        # Plotting the ssh variability for observations
        if self.data_ref is not None:
            self.plot_std(self.data_ref, self.name_ref, self.exp_ref, self.source_ref, self.startdate_ref, self.enddate_ref, contours=21)
            if self.region_selection:
                self.subregion_plot(self.data_ref, self.name_ref, self.exp_ref)

        # Plotting the ssh variability for model data
        if self.data_model is not None:
            self.plot_std(self.data_model, self.name_model, self.exp_model, self.source_model, self.startdate_model, self.enddate_model, contours=21)
            if  self.region_selection:
                self.subregion_plot(self.data_model, self.name_model, self.exp_model)

        # Plot the difference between the Reference std and Model std
        if self.difference_plots:
            self.plot_std_diff(contours=21)
