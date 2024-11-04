import os
import gc

import xarray as xr
import numpy as np
from aqua import Reader
from aqua.logger import log_configure
from aqua.exceptions import NoObservationError, NoDataError
from aqua.util import create_folder
from aqua.util import add_pdf_metadata,time_to_string
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.gridspec as gridspec
from cartopy.mpl.ticker import LongitudeFormatter, LatitudeFormatter
xr.set_options(keep_attrs=True)

class EnsembleLatLon():
    def __init__(self,var=None,model=None,exp=None,source=None,plot_std=True,figure_size=None,plot_label=True,label_size=None,outdir=None,outfile=None,pdf_save=True,loglevel='WARNING'):
        """
        """
        self.loglevel = loglevel
        self.logger = log_configure(log_level=self.loglevel,log_name='Multi-model Ensemble')
        
        self.var = var
        self.model = model
        self.exp = exp
        self.source = source

        if isinstance(self.model,str):
            self.model = [self.model]
        if isinstance(self.exp,str):
            self.model = [self.exp]
        if isinstance(self.source,str):
            self.source = [self.source]
        
        self.plot_std = plot_std
        self.figure_size = figure_size
        self.plot_label = plot_label
        self.label_size = label_size
        self.outdir = outdir
        self.outfile = outfile
        self.pdf_save = pdf_save
        self.loglevel = loglevel
        if self.pdf_save is False:
            self.logger.info("Figure will not be saved")
        self.dataset_mean = None
        self.dataset_std = None
         
    def retrieve_data(self):
        """
        """
        self.logger.debug("Retrieve data")
        data_list = []
        if self.model != []:
            for i,model in enumerate(self.model):
                i_model=model; i_exp=self.exp[i]; i_source=self.source[i]
                self.logger.info(f'Retrieve data for {model} {self.exp[i]} {self.source[i]}')
                #print(f'Retrieve data for {model} {self.exp[i]} {self.source[i]}')
                try:
                    reader = Reader(model=i_model,exp=i_exp,source=i_source,areas=False)
                    data = reader.retrieve()
                    data_list.append(data)
                except Exception as e:
                    self.logger.debug(f'Error while retrieving data')
                    self.logger.warning(f'No data found for {model} {self.exp[i]} {self.source[i]}')
            combined_data = xr.concat(data_list,dim="Ensembles")
            self.dataset_mean = combined_data.mean(dim="Ensembles")
            self.dataset_std = combined_data.std(dim="Ensembles")
            # Clean up    
            del reader
            del data
            del data_list
            del combined_data
            gc.collect()
        if self.model !=[] and self.dataset_mean.sizes == 0:
            raise NoDataError('No computation performed')

    def plot(self):
        """
        """
        self.logger.info('Plotting the ensemble computation')
        projection = ccrs.PlateCarree()
        cmap = 'RdBu_r'
        var = self.var[0]
        if self.dataset_mean.sizes != 0:
            fig0 = plt.figure(figsize=(self.figure_size[0], self.figure_size[1]))
            levels = np.linspace(self.dataset_mean[var].values.min(),self.dataset_mean[var].values.max(),num=21)
            gs = gridspec.GridSpec(1,1,figure=fig0)
            ax0 = fig0.add_subplot(gs[0,:],projection=projection)
            ax0.coastlines()
            ax0.add_feature(cfeature.LAND, facecolor='lightgray')
            ax0.add_feature(cfeature.OCEAN, facecolor='lightblue')
            ax0.xaxis.set_major_formatter(LongitudeFormatter())
            ax0.yaxis.set_major_formatter(LatitudeFormatter())
            ax0.gridlines(draw_labels=True)
            im = ax0.contourf(self.dataset_mean.lon,self.dataset_mean.lat,self.dataset_mean[var],cmap=cmap,levels=levels,extend='both')
            ax0.set_title(f'Map of {var} for Ensemble Multi-Model Mean')
            ax0.set_xlabel('Longitude')
            ax0.set_ylabel('Latitude')
            cbar = fig0.colorbar(im, ax=ax0, shrink=0.4, extend='both')
            cbar.set_label(f"{self.dataset_mean[var].GRIB_name} in {self.dataset_mean[var].units}")
            self.logger.info("Saving ensemble mean map to pdf")
            outfig = os.path.join(self.outdir, 'mean_pdf')
            self.logger.debug(f"Saving figure to {outfig}")
            create_folder(outfig, self.loglevel)
            if self.outfile is None:
                self.outfile = f'multimodel_global_2D_map_{var}'
            self.outfile += '_mean.pdf'
            self.logger.debug(f"Outfile: {self.outfile}")
            fig0.savefig(os.path.join(outfig, self.outfile))
        
        if self.dataset_std.sizes != 0:
            fig1 = plt.figure(figsize=(self.figure_size[0], self.figure_size[1]))
            levels = np.linspace(self.dataset_std[var].values.min(),self.dataset_std[var].values.max(),num=21)
            gs = gridspec.GridSpec(1,1,figure=fig1)
            ax1 = fig1.add_subplot(gs[0,:],projection=projection)
            ax1.coastlines()
            ax1.add_feature(cfeature.LAND, facecolor='lightgray')
            ax1.add_feature(cfeature.OCEAN, facecolor='lightblue')
            ax1.xaxis.set_major_formatter(LongitudeFormatter())
            ax1.yaxis.set_major_formatter(LatitudeFormatter())
            ax1.gridlines(draw_labels=True)
            im = ax1.contourf(self.dataset_std.lon,self.dataset_std.lat,self.dataset_std[var],cmap=cmap,levels=levels,extend='both')
            ax1.set_title(f'Map of {var} for Ensemble Multi-Model Standard Deviation')
            ax1.set_xlabel('Longitude')
            ax1.set_ylabel('Latitude')
            cbar = fig1.colorbar(im, ax=ax1, shrink=0.4, extend='both')
            cbar.set_label(f"{self.dataset_std[var].GRIB_name} in {self.dataset_std[var].units}")
            self.logger.info("Saving ensemble std map to pdf")
            outfig = os.path.join(self.outdir, 'std_pdf')
            self.logger.debug(f"Saving figure to {outfig}")
            create_folder(outfig, self.loglevel)
            if self.outfile is None:
                self.outfile = f'multimodel_global_2D_map_{var}'
            self.outfile += '_std.pdf'
            self.logger.debug(f"Outfile: {self.outfile}")
            fig1.savefig(os.path.join(outfig, self.outfile))

    def run(self):
        self.retrieve_data()
        self.plot()
