import os
import gc

import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.gridspec as gridspec
from cartopy.mpl.ticker import LongitudeFormatter, LatitudeFormatter
from aqua.logger import log_configure
from aqua.exceptions import NoObservationError, NoDataError
from aqua.util import create_folder
from aqua.util import add_pdf_metadata

xr.set_options(keep_attrs=True)

class EnsembleTimeseries():
    """
    This class computes mean and standard deviation of the ensemble.mulit-model ensemble. 
    This also plots ensemble mean and standard deviation.  
    """
    def __init__(self,var=None,
                 mon_model_dataset=None,
                 ann_model_dataset=None,
                 mon_ref_data=None,
                 ann_ref_data=None,
                 ensemble_dimension_name="Ensembles",
                 loglevel='WARNING'):
        """
        Args:
            var (str): Variable name.
            mon_model_dataset: xarray Dataset of ensemble members of monthly timeseries.
                     The ensemble memebers are concatenated along a new dimension "Ensmebles".
                     This can be editted with the function edit_attributes.
            ann_model_dataset: xarray Dataset of ensemble members of annual timeseries.
                     Similarly, the members are concatenated along the dimension "Ensembles" 
            ensemble_dimension_name="Ensembles" (str): a default name given to the
                     dimensions along with the individual Datasets were concatenated.
            mon_ref_data: xarray Dataset of monthly reference data.
            ann_ref_data: xarray Dataset of annual reference data.
            loglevel (str): Log level. Default is "WARNING".
        """
        self.loglevel = loglevel
        self.logger = log_configure(log_level=self.loglevel, log_name='Ensemble Timeseries')
        self.var = var
        self.mon_model_dataset = mon_model_dataset
        self.ann_model_dataset = ann_model_dataset
        self.mon_ref_data = mon_ref_data
        self.ann_ref_data = ann_ref_data

        self.plot_label = True

        self.mon_dataset_mean = None
        self.mon_dataset_std = None

        self.ann_dataset_mean = None
        self.ann_dataset_std = None

        self.plot_std = True
        self.plot_ensemble_members = True
        self.plot_ref = True
        self.figure_size = [10, 5]
        self.outfile = None
        self.outdir = 'Ensemble_Timeseries'
        self.label_size = 7.5
        self.ensemble_label = 'Ensemble'
        self.ref_label = 'ERA5'
        self.plot_title = 'Ensemble statistics for '+self.var
        self.dim = ensemble_dimension_name
        self.label_ncol = 3
        self.pdf_save = True
        if self.pdf_save is False:
            self.logger.info("Figure will not be saved")
        self.netcdf_save = True

    def ensemble_mean(self, dataset):
        """
        compute mean of the dataset
        """
        return dataset[self.var].mean(dim=self.dim)

    def ensemble_std(self, dataset):
        """
        compute std of the dataset
        """
        return dataset[self.var].std(dim=self.dim)

    def compute(self):
        """
        A function to compute the mean and standard devivation of the input dataset
        It is import to make sure that the dim along which the mean is compute is correct.
        The default dim="Ensembles". This can be reassianged with the method "edit_attributes".
        """
        if self.mon_model_dataset != None:
            self.mon_dataset_mean = self.mon_model_dataset[self.var].mean(dim=self.dim)
        if self.ann_model_dataset != None:
            self.ann_dataset_mean = self.ann_model_dataset[self.var].mean(dim=self.dim)

        if self.mon_model_dataset != None:
            self.mon_dataset_std = self.mon_model_dataset[self.var].std(dim=self.dim)
        if self.ann_model_dataset != None:
            self.ann_dataset_std = self.ann_model_dataset[self.var].std(dim=self.dim)

    def edit_attributes(self, **kwargs):
        """
        This method helps edit the default attributes. 
        """
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
            else:
                print(f"Attribute '{key}' does not exist.")

    def plot(self):
        """
        This plots the ensemble mean and standard deviation of the ensemble statistics.
        To edit the default settings please call the method "edit_attributes"
        """
        self.logger.info('Plotting the ensemble timeseries')
        var = self.var  # Only one variable can be plotted at a time
        plt.rcParams["figure.figsize"] = (self.figure_size[0], self.figure_size[1])
        fig, ax = plt.subplots(1, 1)
        color_list = ["#1898e0", "#8bcd45", "#f89e13", "#d24493", "#00b2ed", "#dbe622",
                      "#fb4c27", "#8f57bf", "#00bb62", "#f9c410", "#fb4865", "#645ccc"]
        
        # Plotting monthly ensemble data
        if self.mon_dataset_mean is not None:
            if isinstance(self.mon_dataset_mean,xr.Dataset):
                mon_dataset_mean = self.mon_dataset_mean[var]
            else:
                mon_dataset_mean = self.mon_dataset_mean
            if isinstance(self.mon_dataset_std,xr.Dataset):
                mon_dataset_std = self.mon_dataset_std[var]
            else:
                mon_dataset_std = self.mon_dataset_std
            mon_dataset_mean.plot(ax=ax, label=self.ensemble_label+'-mon-mean', color=color_list[0], zorder=2)
            if self.plot_std:
                ax.fill_between(mon_dataset_mean.time, mon_dataset_mean - 2.*mon_dataset_std, mon_dataset_mean +
                                2.*mon_dataset_std, facecolor=color_list[0], alpha=0.25, label=self.ensemble_label+'-mon-mean'+r'$\pm2$std', zorder=0)
            if self.plot_ensemble_members:
                for i in range(0,len(self.mon_model_dataset[var][:,0])):
                    self.mon_model_dataset[var][i, :].plot(ax=ax, color='grey', lw=0.7, zorder=1)
        
        # Plotting monthy reference
        if self.mon_ref_data is not None:
            if isinstance(self.mon_ref_data,xr.Dataset):
                mon_ref_data = self.mon_ref_data[var]
            else:
                 mon_ref_data = self.mon_ref_data
            mon_ref_data.plot(ax=ax, label=self.ref_label+'-mon', color='black', lw=0.95, zorder=2)
        
        # Plotting annual ensemble data
        if self.ann_dataset_mean is not None:
            if isinstance(self.ann_dataset_mean,xr.Dataset):
                ann_dataset_mean = self.ann_dataset_mean[var]
            else:
                ann_dataset_mean = self.ann_dataset_mean

            if isinstance(self.ann_dataset_std,xr.Dataset):
                ann_dataset_std = self.ann_dataset_std[var]
            else:
                ann_dataset_std = self.ann_dataset_std
            ann_dataset_mean.plot(ax=ax, label=self.ensemble_label+'-ann-mean',
                                            color=color_list[2], linestyle='--', zorder=2)
            if self.plot_std:
                ax.fill_between(ann_dataset_mean.time,ann_dataset_mean - 2.*ann_dataset_std,ann_dataset_mean +
                                2.*ann_dataset_std, facecolor=color_list[2], alpha=0.25, label=self.ensemble_label+'-ann-mean'+r'$\pm2$std', zorder=0)
            if self.plot_ensemble_members:
                for i in range(0,len(self.ann_model_dataset[var][:,0])):
                    self.ann_model_dataset[var][i, :].plot(ax=ax, color='grey', lw=0.7, linestyle='--', zorder=1)
        
        # Plotting annual reference
        if self.ann_ref_data is not None:
            if isinstance(self.ann_ref_data,xr.Dataset):
                ann_ref_data = self.ann_ref_data[var]
            else:
                ann_ref_data = self.ann_ref_data
            ann_ref_data.plot(ax=ax, label=self.ref_label+'-ann', color='black', linestyle='--', lw=0.95, zorder=2)

        ax.set_title(self.plot_title)
        ax.legend(ncol=self.label_ncol, fontsize=self.label_size, framealpha=0)
        
        # Save plot as pdf
        if self.pdf_save:
            self.logger.info("Saving figure to pdf")
            if self.outfile is None:
                outfile = f'ensemble_time_series_timeseries_{self.var}'
            else:
                outfile = self.outfile
            pdf_path = os.path.join(self.outdir, 'pdf')
            self.logger.debug(f"Saving figure to {pdf_path}")
            create_folder(pdf_path, self.loglevel)
            outfile += '.pdf'
            self.logger.debug(f"pdf output: {outfile}")
            fig.savefig(os.path.join(pdf_path, outfile),bbox_inches='tight', pad_inches=0.1)

    def run(self):
        self.compute()
        self.edit_attributes()
        self.plot()

class EnsembleLatLon():
    """
    A class to compute ensemble mean and standard deviation of a 2D (lon-lat) Dataset.
    Make sure that the dataset has correct lon-lat dimensions.
    """
    def __init__(self, var=None, dataset=None, ensemble_dimension_name="Ensembles", loglevel='WARNING'):
        """
        Args:
            var (str): Variable name.
            dataset: xarray Dataset composed of ensembles 2D lon-lat data, i.e., 
                     the individual Dataset (lon-lat) are concatenated along.
                     a new dimension "Ensemble". This Ensemble name can be changed.
            ensemble_dimension_name="Ensembles" (str): a default name given to the 
                     dimensions along with the individual Datasets were concatenated.
            loglevel (str): Log level. Default is "WARNING".
        """
        self.loglevel = loglevel
        self.logger = log_configure(log_level=self.loglevel, log_name='Ensemble')

        self.var = var
        self.dataset = dataset
        if self.dataset is not None:
            self.units = self.dataset[self.var].units
        else:
            self.units = "units"
        self.dim = ensemble_dimension_name
        self.plot_std = True
        self.figure_size = [15,15]
        self.plot_label = True
        self.outdir = 'Ensemble_LatLon'
        self.outfile = None
        self.pdf_save = True
        self.mean_plot_title = 'Map of '+self.var+' for Ensemble mean'
        self.std_plot_title = 'Map of '+self.var+' for Ensemble standard deviation'
        self.cbar_label = self.var+' in '+self.units

        if self.pdf_save is False:
            self.logger.info("Figure will not be saved")
        self.dataset_mean = None
        self.dataset_std = None

    def compute(self):
        """
        A function to compute the mean and standard devivation of the input dataset
        It is import to make sure that the dim along which the mean is compute is correct.
        The default dim="Ensembles". This can be reassianged with the method "edit_attributes".
        """
        if self.dataset is not None:
            self.dataset_mean = self.dataset.mean(dim=self.dim)
            self.dataset_std = self.dataset.std(dim=self.dim)

    def edit_attributes(self, **kwargs):
        """
        This method helps edit the default attributes. 
        """
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
            else:
                print(f"Attribute '{key}' does not exist.")

    def plot(self):
        """
        This plots the ensemble mean and standard deviation of the ensemble statistics.
        To edit the default settings please call the method "edit_attributes"
        """
        self.logger.info('Plotting the ensemble computation')
        projection = ccrs.PlateCarree()
        var = self.var
        if self.dataset_mean is not None:
            if isinstance(self.dataset_mean,xr.Dataset):
                dataset_mean = self.dataset_mean[var]
            else:
                dataset_mean = self.dataset_mean
            

            fig0 = plt.figure(figsize=(self.figure_size[0], self.figure_size[1]))
            levels = np.linspace(dataset_mean.values.min(), dataset_mean.values.max(), num=21)
            create_folder(self.outdir, self.loglevel)
            if self.outfile is None:
                self.outfile = f'global_2D_map_{var}'
            gs = gridspec.GridSpec(1, 1, figure=fig0)
            ax0 = fig0.add_subplot(gs[0, :], projection=projection)
            ax0.coastlines()
            ax0.add_feature(cfeature.LAND, facecolor='lightgray')
            ax0.add_feature(cfeature.OCEAN, facecolor='lightblue')
            ax0.xaxis.set_major_formatter(LongitudeFormatter())
            ax0.yaxis.set_major_formatter(LatitudeFormatter())
            ax0.gridlines(draw_labels=True)
            im = ax0.contourf(dataset_mean.lon, dataset_mean.lat,
                              dataset_mean, cmap='RdBu_r', levels=levels, extend='both')
            ax0.set_title(self.mean_plot_title)
            ax0.set_xlabel('Longitude')
            ax0.set_ylabel('Latitude')
            cbar = fig0.colorbar(im, ax=ax0, shrink=0.43, extend='both')
            cbar.set_label(self.cbar_label)
            self.logger.info("Saving ensemble mean map to pdf")
            outfig = os.path.join(self.outdir, 'mean_pdf')
            self.logger.debug(f"Saving figure to {outfig}")
            create_folder(outfig, self.loglevel)
            outfile = self.outfile + '_mean.pdf'
            self.logger.debug(f"Outfile: {outfile}")
            fig0.savefig(os.path.join(outfig, outfile),bbox_inches='tight', pad_inches=0.1)

        if self.dataset_std is not None:
            if isinstance(self.dataset_std,xr.Dataset):
                dataset_std = self.dataset_std[var]
            else:
                dataset_std = self.dataset_std

            fig1 = plt.figure(figsize=(self.figure_size[0], self.figure_size[1]))
            levels = np.linspace(dataset_std.values.min(), dataset_std.values.max(), num=21)
            gs = gridspec.GridSpec(1, 1, figure=fig1)
            ax1 = fig1.add_subplot(gs[0, :], projection=projection)
            ax1.coastlines()
            ax1.add_feature(cfeature.LAND, facecolor='lightgray')
            ax1.add_feature(cfeature.OCEAN, facecolor='lightblue')
            ax1.xaxis.set_major_formatter(LongitudeFormatter())
            ax1.yaxis.set_major_formatter(LatitudeFormatter())
            ax1.gridlines(draw_labels=True)
            im = ax1.contourf(dataset_std.lon, dataset_std.lat,
                              dataset_std, cmap='OrRd', levels=20, extend='both')
            ax1.set_title(self.std_plot_title)
            ax1.set_xlabel('Longitude')
            ax1.set_ylabel('Latitude')
            cbar = fig1.colorbar(im, ax=ax1, shrink=0.43, extend='both')
            cbar.set_label(self.cbar_label)
            self.logger.info("Saving ensemble std map to pdf")
            outfig = os.path.join(self.outdir, 'std_pdf')
            self.logger.debug(f"Saving figure to {outfig}")
            create_folder(outfig, self.loglevel)
            outfile = self.outfile + '_std.pdf'
            self.logger.debug(f"Outfile: {outfile}")
            fig1.savefig(os.path.join(outfig, outfile))
            self.logger.debug(f"Outfile: {outfile}")
            fig1.savefig(os.path.join(outfig, outfile),bbox_inches='tight', pad_inches=0.1)

    def run(self):
        self.compute()
        self.edit_attributes()
        self.plot()

class EnsembleZonal():
    """
    A class to compute ensemble mean and standard deviation of the Zonal averages
    Make sure that the dataset has correct lev-lat dimensions.
    """
    def __init__(self, var=None, dataset=None, ensemble_dimension_name="Ensembles", loglevel='WARNING'):
        """
        Args:
            var (str): Variable name.
            dataset: xarray Dataset composed of ensembles 2D Zonal data, i.e., 
                     the individual Dataset (lev-lat) are concatenated along.
                     a new dimension "Ensemble". This Ensemble name can be changed.
            ensemble_dimension_name="Ensembles" (str): a default name given to the 
                     dimensions along with the individual Datasets were concatenated.
            loglevel (str): Log level. Default is "WARNING".
        """
        self.loglevel = loglevel
        self.logger = log_configure(log_level=self.loglevel, log_name='Ensembles')

        self.var = var
        self.dataset = dataset
        # if self.dataset != None: self.units = self.dataset[self.var].units
        self.dim = ensemble_dimension_name
        self.plot_std = True
        self.figure_size = [15, 15]
        self.plot_label = True
        self.outdir = 'Ensemble_Zonal'
        self.outfile = None
        self.pdf_save = True
        self.mean_plot_title = 'Ensemble mean of zonal average of '+self.var
        self.std_plot_title = 'Ensemble standard deviation of zonal average of '+self.var
        self.cbar_label = self.var

        if self.pdf_save is False:
            self.logger.info("Figure will not be saved")
        self.dataset_mean = None
        self.dataset_std = None

    def compute(self):
        """
        A function to compute the mean and standard devivation of the input dataset
        It is import to make sure that the dim along which the mean is compute is correct.
        The default dim="Ensembles". This can be reassianged with the method "edit_attributes".
        """
        if self.dataset != None:
            self.dataset_mean = self.dataset.mean(dim=self.dim)
            self.dataset_std = self.dataset.std(dim=self.dim)

    def edit_attributes(self, **kwargs):
        """
        This method helps edit the default attributes. 
        """
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
            else:
                print(f"Attribute '{key}' does not exist.")

    def plot(self):
        """
        This plots the ensemble mean and standard deviation of the ensemble statistics.
        To edit the default settings please call the method "edit_attributes"
        """
        self.logger.info('Plotting the ensemble computation')
        # projection = ccrs.PlateCarree()
        var = self.var
        if self.dataset_mean is not None:
            if isinstance(self.dataset_mean,xr.Dataset):
                dataset_mean = self.dataset_mean[var]
            else:
                dataset_mean = self.dataset_mean
 
            fig0 = plt.figure(figsize=(self.figure_size[0], self.figure_size[1]))
            if self.outfile is None:
                self.outfile = f'zonal_average_{var}'
            ax0 = fig0.add_subplot(111)
            im = ax0.contourf(dataset_mean.lat,dataset_mean.lev,dataset_mean,cmap='RdBu_r', levels=20, extend='both')
            ax0.set_ylim((5500, 0))
            ax0.set_ylabel("Depth (in m)", fontsize=9)
            ax0.set_xlabel("Latitude (in deg North)", fontsize=9)
            ax0.set_facecolor('grey')
            ax0.set_title(self.mean_plot_title)
            cbar = fig0.colorbar(im, ax=ax0, shrink=0.9, extend='both')
            cbar.set_label(self.cbar_label)
            self.logger.info("Saving ensemble mean map to pdf")
            outfig = os.path.join(self.outdir, 'mean_pdf')
            self.logger.debug(f"Saving figure to {outfig}")
            create_folder(outfig, self.loglevel)
            outfile = self.outfile + '_mean.pdf'
            self.logger.debug(f"Outfile: {outfile}")
            fig0.savefig(os.path.join(outfig, outfile),bbox_inches='tight', pad_inches=0.1)

        if self.dataset_std is not None:
            if isinstance(self.dataset_std,xr.Dataset):
                dataset_std = self.dataset_std[var]
            else:
                dataset_std = self.dataset_std
 
            fig1 = plt.figure(figsize=(self.figure_size[0], self.figure_size[1]))
            if self.outfile is None:
                self.outfile = f'zonal_average_{var}'
            ax1 = fig1.add_subplot(111)
            #im = self.dataset_std[var].contourf(ax=ax1, cmap=cmap, levels=20, extend='both');
            im = ax1.contourf(dataset_std.lat,dataset_std.lev,dataset_std,cmap='OrRd',extend='both')
            ax1.set_ylim((5500, 0))
            ax1.set_ylabel("Depth (in m)", fontsize=9)
            ax1.set_xlabel("Latitude (in deg North)", fontsize=9)
            ax1.set_facecolor('grey')
            ax1.set_title(self.std_plot_title)
            cbar = fig1.colorbar(im, ax=ax1,shrink=0.9, extend='both')
            cbar.set_label(self.cbar_label)
            self.logger.info("Saving ensemble std map to pdf")
            outfig = os.path.join(self.outdir, 'std_pdf')
            self.logger.debug(f"Saving figure to {outfig}")
            create_folder(outfig, self.loglevel)
            outfile = self.outfile + '_std.pdf'
            self.logger.debug(f"Outfile: {outfile}")
            fig1.savefig(os.path.join(outfig, outfile),bbox_inches='tight', pad_inches=0.1)

    def run(self):
        self.compute()
        self.edit_attributes()
        self.plot()
