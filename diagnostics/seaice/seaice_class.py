"""Sea ice diagnostics"""
import os
import matplotlib.pyplot as plt
import xarray as xr
import numpy as np
from aqua import Reader
from aqua.exceptions import NoDataError
from aqua.util import load_yaml, create_folder
from aqua.logger import log_configure
import numpy as np

import cartopy.crs as ccrs
import cartopy.feature as cfeature
from   cartopy.util import add_cyclic_point
import sys
sys.path.insert(0, '..')
from colInterpolatOr import *
from matplotlib.colors import LinearSegmentedColormap


class SeaIceExtent:
    """Sea ice extent class"""

    def __init__(self, config, loglevel: str = 'WARNING', threshold=0.15,
                 regions_definition_file=None, outputdir=None):
        """
        The SeaIceExtent constructor.

        Args:
            config (str or dict):   If str, the path to the yaml file
                                    containing the configuration. If dict, the
                                    configuration itself.
            loglevel (str):     The log level
                                Default: WARNING
            threshold (float):  The sea ice extent threshold
                                Default: 0.15
            regions_definition_file (str):  The path to the file that specifies the regions boundaries

        Returns:
            A SeaIceExtent object.

        """
        # Configure logger
        self.loglevel = loglevel
        self.logger = log_configure(self.loglevel, 'Seaice')

        if regions_definition_file is None:
            regions_definition_file = os.path.dirname(os.path.abspath(__file__)) + "/regions_definition.yml"
            self.logger.debug("Using default regions definition file %s",
                              regions_definition_file)

        self.regionDict = load_yaml(regions_definition_file)
        self.thresholdSeaIceExtent = threshold

        if outputdir is None:
            outputdir = os.path.dirname(os.path.abspath(__file__)) + "/output/"
            self.logger.info("Using default output directory %s", outputdir)
        self.outputdir = outputdir

        if config is str:
            self.logger.debug("Reading configuration file %s", config)
            config = load_yaml(config)
        else:
            self.logger.debug("Configuration is a dictionary")

        self.logger.debug("CONFIG:" + str(config))

        self.configure(config)

    def configure(self, config=None):
        """
        Set the number of regions and the list of regions.
        Set also the list of setups.
        """
        if config is None:
            raise ValueError("No configuration provided")

        try:
            self.myRegions = config['regions']
        except KeyError:
            self.logger.error("No regions specified in configuration file")
            self.logger.warning("Using all regions")
            self.myRegions = None

        if self.myRegions is None:
            self.myRegions = ["Arctic", "Hudson Bay",
                              "Southern Ocean", "Ross Sea",
                              "Amundsen-Bellingshausen Seas",
                              "Weddell Sea", "Indian Ocean",
                              "Pacific Ocean"]

        self.logger.debug("Regions: " + str(self.myRegions))
        self.nRegions = len(self.myRegions)
        self.logger.debug("Number of regions: " + str(self.nRegions))

        try:
            self.mySetups = config['models']

            # Attribute a color for plotting
            reserveColorList = ["#1898e0", "#00b2ed", "#00bb62",
                                "#8bcd45", "#dbe622", "#f9c410",
                                "#f89e13", "#fb4c27", "#fb4865",
                                "#d24493", "#8f57bf", "#645ccc",]
            js = 0
            for s in self.mySetups:
                if s["model"] == "OSI-SAF":
                    s["color_plot"] = [0.2, 0.2, 0.2]
                else:
                    s["color_plot"] = reserveColorList[js]
                js += 1
        except KeyError:
            raise NoDataError("No models specified in configuration")

    def run(self):
        """
        The run diagnostic method.

        The method produces as output a figure with the seasonal cycles
        of sea ice extent in the regions for the setups and a netcdf file
        containing the time series of sea ice extent in the regions for
        each setup.
        """
        self.computeExtent()
        self.plotExtent()
        self.createNetCDF()

    def computeExtent(self):
        """Method which computes the seaice extent."""

        # Instantiate the various readers (one per setup) and retrieve the
        # corresponding data
        self.myExtents = list()
        for jSetup, setup in enumerate(self.mySetups):
            # Acquiring the setup
            self.logger.debug("Setup: " + str(setup))
            model = setup["model"]
            exp = setup["exp"]
            # We use get because the reader can try to take
            # automatically the first source available
            source = setup.get("source", None)
            regrid = setup.get("regrid", None)
            var = setup.get("var", 'avg_siconc')
            timespan = setup.get("timespan", None)

            self.logger.info(f"Retrieving data for {model} {exp} {source}")

            # Instantiate reader
            try:
                reader = Reader(model=model, exp=exp, source=source,
                                regrid=regrid, loglevel=self.loglevel)
                tmp = reader.retrieve()
            except Exception as e:
                self.logger.error("An exception occurred while instantiating reader: %s", e)
                raise NoDataError("Error while instantiating reader")

            try:
                data = reader.retrieve(var=var)
            except KeyError:
                self.logger.error("Variable %s not found in dataset", var)
                raise NoDataError("Variable not found in dataset")
            if timespan is None:
                # if timespan is set to None, retrieve the timespan
                # from the data directly
                self.logger.warning("Using timespan based on data availability")
                self.mySetups[jSetup]["timespan"] = [np.datetime_as_string(data.time[0].values, unit='D'),
                                                     np.datetime_as_string(data.time[-1].values, unit='D')]
            if regrid:
                self.logger.info("Regridding data")
                data = reader.regrid(data)

            areacello = reader.grid_area
            try:
                lat = data.coords["lat"]
                lon = data.coords["lon"]
            except KeyError:
                raise NoDataError("No lat/lon coordinates found in dataset")

            # Important: recenter the lon in the conventional 0-360 range
            lon = (lon + 360) % 360
            lon.attrs["units"] = "degrees"

            # Compute climatology

            # Create mask based on threshold
            try:
                ci_mask = data[var].where((data[var] > self.thresholdSeaIceExtent) &
                                          (data[var] < 1.0))
            except Exception:
                raise NoDataError("No sea ice concentration data found in dataset")

            self.regionExtents = list()  # Will contain the time series
            # for each region and for that setup
            # Iterate over regions
            for jr, region in enumerate(self.myRegions):
                self.logger.info("Producing diagnostic for region %s", region)
                # Create regional mask
                try:
                    latS, latN, lonW, lonE = (

                        self.regionDict[region]["latS"],
                        self.regionDict[region]["latN"],
                        self.regionDict[region]["lonW"],
                        self.regionDict[region]["lonE"],
                    )
                except KeyError:
                    self.logger.info("Error: region not defined")
                    print("Region " + region + " does not exist in regions_definition.yml")
                    raise KeyError("Region not defined")

                # Dealing with regions straddling the 180° meridian
                if lonW > lonE:
                    regionMask = (
                        (lat >= latS)
                        & (lat <= latN)
                        & ((lon >= lonW) | (lon <= lonE))
                    )
                else:
                    regionMask = (
                        (lat >= latS)
                        & (lat <= latN)
                        & (lon >= lonW)
                        & (lon <= lonE)
                    )

                # Print area of region
                if source == "lra-r100-monthly" or model == "OSI-SAF":
                    if source == "lra-r100-monthly":
                        dim1Name, dim2Name = "lon", "lat"
                    elif model == "OSI-SAF":
                        dim1Name, dim2Name = "xc", "yc"
                    myExtent = areacello.where(regionMask).where(
                        ci_mask.notnull()).sum(dim=[dim1Name, dim2Name]) / 1e12
                else:
                    myExtent = areacello.where(regionMask).where(
                        ci_mask.notnull()).sum(dim="value") / 1e12

                myExtent.attrs["units"] = "million km^2"
                myExtent.attrs["long_name"] = "Sea ice extent"
                self.regionExtents.append(myExtent)

            # Save set of diagnostics for that setup
            self.myExtents.append(self.regionExtents)

    def plotExtent(self):
        """
        Method to produce figures plotting seaice extent.
        """

        # First figure: raw time series (useful to check any possible suspicious
        # data that could contaminate statistics like averages: fig1

        # Second figure: seasonal cycles (useful for evaluation): fig2
        monthsNumeric = range(1, 12 + 1)  # Numeric months
        monthsNames = ["J", "F", "M", "A", "M", "J", "J", "A", "S", "O", "N", "D"]

        fig1, ax1 = plt.subplots(self.nRegions, figsize=(13, 3 * self.nRegions))
        fig2, ax2 = plt.subplots(self.nRegions, figsize=(6, 4 * self.nRegions))

        for jr, region in enumerate(self.myRegions):
            for js, setup in enumerate(self.mySetups):
                strTimeInfo = " to ".join(setup["timespan"])
                label = setup["model"] + " " + setup["exp"] + " " + setup["source"] + " " + strTimeInfo
                color_plot = setup["color_plot"]
                self.logger.debug(f"Plotting {label} for region {region}")
                extent = self.myExtents[js][jr]

                # Monthly cycle
                extentCycle = np.array([extent.sel(time=extent['time.month'] == m).mean(dim='time').values
                                        for m in monthsNumeric])

                # One standard deviation of the temporal variability
                extentStd = np.array([extent.sel(time=extent['time.month'] == m).std(dim='time').values
                                      for m in monthsNumeric])

                # Don't plot osisaf nh in the south and conversely
                if (setup["model"] == "OSI-SAF" and setup["source"] == "nh-monthly" and
                    self.regionDict[region]["latN"] < 20.0) or (
                        setup["model"] == "OSI-SAF" and setup["source"] == "sh-monthly"
                        and self.regionDict[region]["latS"] > -20.0):
                    self.logger.debug("Not plotting osisaf nh in the south and conversely")
                    pass
                else:
                    ax1[jr].plot(extent.time, extent, label=label, color=color_plot)
                    ax2[jr].plot(monthsNumeric, extentCycle, label=label, lw=3, color=color_plot)

                    # Plot ribbon of uncertainty
                    if setup["model"] == "OSI-SAF":
                        mult = 2.0
                        ax2[jr].fill_between(monthsNumeric, extentCycle - mult * extentStd, extentCycle + mult * extentStd,
                                             alpha=0.5, zorder=0, color=color_plot, lw=0)

                ax1[jr].set_title("Sea ice extent: region " + region)

                ax1[jr].legend(fontsize=6, ncols=6, loc="best")
                ax1[jr].set_ylabel(extent.units)
                # ax1[jr].set_ylim(bottom = 0, top = None)
                ax1[jr].grid()
                ax1[jr].set_axisbelow(True)
                fig1.tight_layout()

                ax2[jr].set_title("Sea ice extent seasonal cycle: region " + region)
                ax2[jr].legend(fontsize=6, loc="best")
                ax2[jr].set_ylabel(extent.units)
                # Ticks
                ax2[jr].set_xticks(monthsNumeric)
                ax2[jr].set_xticklabels(monthsNames)
                # ax2[jr].set_ylim(bottom = 0, top = None)
                ax2[jr].grid()
                ax2[jr].set_axisbelow(True)

            fig2.tight_layout()
            create_folder(self.outputdir, loglevel=self.loglevel)

            for fmt in ["pdf"]:
                outputfig = self.outputdir + "/" + fmt
                create_folder(outputfig, loglevel=self.loglevel)
                fig1Name = "SeaIceExtent_" + "all_models" + "." + fmt
                fig2Name = "SeaIceExtentCycle_" + "all_models" + "." + fmt
                self.logger.info("Saving figure %s", fig1Name)
                self.logger.info("Saving figure %s", fig2Name)
                fig1.savefig(outputfig + "/" + fig1Name, dpi=300)
                fig2.savefig(outputfig + "/" + fig2Name, dpi=300)

    def createNetCDF(self):
        """Method to create NetCDF files."""
        # NetCDF creation (one per setup)
        outputdir = os.path.join(self.outputdir, "netcdf")
        create_folder(outputdir, loglevel=self.loglevel)

        for js, setup in enumerate(self.mySetups):
            dataset = xr.Dataset()
            for jr, region in enumerate(self.myRegions):

                if (setup["model"] == "OSI-SAF" and setup["source"] == "nh-monthly" and
                    self.regionDict[region]["latN"] < 20.0) or (
                        setup["model"] == "OSI-SAF" and setup["source"] == "sh-monthly"
                        and self.regionDict[region]["latS"] > -20.0):
                    self.logger.debug("Not saving osisaf nh in the south and conversely")
                    pass
                else:  # we save the data
                    # NetCDF variable
                    varName = setup["model"] + "_" + setup["exp"] + "_" + setup["source"] + "_" + region.replace(' ', '')
                    dataset[varName] = self.myExtents[js][jr]

                    filename = outputdir + "/" + varName + ".nc"
                    self.logger.info("Saving NetCDF file %s", filename)
                    dataset.to_netcdf(filename)



class SeaIceConcentration:
    def __init__(self, config, loglevel: str = 'WARNING',
            outputdir=None):
        
        """
        The SeaIceConcentration constructor.

        Args:
            config (str or dict):   If str, the path to the yaml file
                                    containing the configuration. If dict, the
                                    configuration itself.
            loglevel (str):     The log level
                                Default: WARNING
        Returns:
            A SeaIceConcentration object.

        """
         
        # Configure the logger
        self.loglevel = loglevel
        self.logger = log_configure(self.loglevel, 'Seaice')

        if outputdir is None:
            outputdir = os.path.dirname(os.path.abspath(__file__)) + "/output/"
            self.logger.info("Using default output directory %s", outputdir)
        self.outputdir = outputdir

        if config is str:
            self.logger.debug("Reading configuration file %s", config)
            config = load_yaml(config)
        else:
            self.logger.debug("Configuration is a dictionary")

        self.logger.debug("CONFIG:" + str(config))

        self.configure(config)
    

    
    def configure(self, config=None):
        """Sets the list of setupts
        """
        if config is None:
            raise ValueError("No configuration provided")

        try:
            self.mySetups = config['models']

            self.nModels  = len(self.mySetups)
        except KeyError:
            raise NoDataError("No models specified in configuration")

    def run(self):
        """
        The run diagnostic method.

        The method produces as output a figure with the winter and summer spatial
        distributions of sea ice concentration averaged in time (Arctic and Antarctic)

        """

        self.plotConcentration()

    def plotConcentration(self):



        # One figure per projection (impossible to instantiate a figure with different central_latitude

        for jHemi, hemi in enumerate(["nh", "sh"]):
            if hemi == "nh":
                central_latitude = 90
            else:
                central_latitude = -90

            projection = ccrs.Orthographic(central_longitude=0.0, central_latitude=central_latitude)

            fig1, ax1 = plt.subplots(nrows = 1, ncols = self.nModels, subplot_kw={'projection': projection}, figsize=(5 * self.nModels, 5))

            if self.nModels == 1:
                ax1 = [ax1]
            else:
                ax1 = ax1.flatten()

            for jSetup, setup in enumerate(self.mySetups):
                # Acquiring the setup
                self.logger.debug("Setup: " + str(setup))
                model = setup["model"]
                exp = setup["exp"]
                # We use get because the reader can try to take
                # automatically the first source available
                source = setup.get("source", None)
                regrid = setup.get("regrid", None)
                var = setup.get("var", "avg_siconc")
                timespan = setup.get("timespan", None)

                self.logger.info(f"Retrieving data for {model} {exp} {source}")

                # Instantiate reader
                try:
                    reader = Reader(model=model, exp=exp, source=source,
                                regrid=regrid, loglevel=self.loglevel)

                    tmp = reader.retrieve()
                except Exception as e:
                    self.logger.error("An exception occurred while instantiating reader: %s", e)
                    raise NoDataError("Error while instantiating reader")

                try:
                    data= reader.retrieve(var=var)
                except KeyError:
                    self.logger.error("Variable %s not found in dataset", var)
                    raise NoDataError("Variable not found in dataset")
                if timespan is None:
                    # if timespan is set to None, retrieve the timespan
                    # from the data directly
                    self.logger.warning("Using timespan based on data availability")
                    timespan = [np.datetime_as_string(data.time[0].values, unit='D'),
                                                     np.datetime_as_string(data.time[-1].values, unit='D')]
                if regrid:
                    self.logger.info("Regridding data")
                    data = reader.regrid(data)

                try:
                    lat = data.coords["lat"]
                    lon = data.coords["lon"]
                except KeyError:
                    raise NoDataError("No lat/lon coordinates found in dataset")

                # Important: recenter the lon in the conventional 0-360 range
                lon = (lon + 360) % 360
                lon.attrs["units"] = "degrees"

                label = setup["model"] + " " + setup["exp"] + " " + setup["source"]
            

                if len(lon.shape) == 1: # If we are using a regular grid, we need to meshgrid it
                    lon2D, lat2D = np.meshgrid(lon, lat)
                else:
                    lon2D, lat2D = lon, lat

                month_diagnostic = 3 # Classical (non-Pythonic) convention
                monthNames = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]

                self.logger.warning("WARNING: TO BE IMPLEMENTED: SUB SELECT BASED ON TIME SPAN")
                maskTime = (data['time.month'] == month_diagnostic)

                dataPlot = data[var].where(maskTime, drop=True).mean("time").values

                # Create color sequence for sic
                sourceColors = [[0.0, 0.0, 0.2], [0.0, 0.0, 0.0],[0.5, 0.5, 0.5], [0.6, 0.6, 0.6], [0.7, 0.7, 0.7], [0.8, 0.8, 0.8], [0.9, 0.9, 0.9],[1.0, 1.0, 1.0]]
                myCM = LinearSegmentedColormap.from_list('myCM', sourceColors, N = 15)

                # Create a figure and axis with the specified projection

                # Add cyclic points to avoid a white Greenwich meridian
                #varShow, lon1DCyclic = add_cyclic_point(dataPlot, coord = lon1D, axis = 1)

                # Plot the field data using contourf
                levels = np.arange(0.0, 1.05, 0.05)
                levelsShow = np.arange(0.0, np.max(levels), 0.1)

                contour = ax1[jSetup].pcolormesh(lon, lat, dataPlot,  \
                          transform=ccrs.PlateCarree(), cmap = myCM
                          )


                # Add coastlines and gridlines
                ax1[jSetup].coastlines()
                #ax.gridlines()
                ax1[jSetup].add_feature(cfeature.LAND, edgecolor='k')

                # Add colorbar
                cbar = plt.colorbar(contour, ax=ax1[jSetup], orientation='vertical', pad=0.05)
                cbar.set_label('fractional')
                cbar.set_ticks(levelsShow)

                # Set title
                ax1[jSetup].set_title(str(model) + "-" + str(exp) + "-" + str(source)  + '\n(average over ' + " - ".join(timespan) + ")")

            fig1.suptitle(monthNames[month_diagnostic - 1] + ' sea ice concentration ')
            fig1.savefig(self.outputdir + "./maps_" + hemi + ".pdf")


class SeaIceThickness:
    def __init__(self, config, loglevel: str = 'WARNING',
            outputdir=None):
        
        """
        The SeaIceThickness constructor.

        Args:
            config (str or dict):   If str, the path to the yaml file
                                    containing the configuration. If dict, the
                                    configuration itself.
            loglevel (str):     The log level
                                Default: WARNING
        Returns:
            A SeaIceThickness object.

        """
         
        # Configure the logger
        self.loglevel = loglevel
        self.logger = log_configure(self.loglevel, 'Seaice')

        if outputdir is None:
            outputdir = os.path.dirname(os.path.abspath(__file__)) + "/output/"
            self.logger.info("Using default output directory %s", outputdir)
        self.outputdir = outputdir

        if config is str:
            self.logger.debug("Reading configuration file %s", config)
            config = load_yaml(config)
        else:
            self.logger.debug("Configuration is a dictionary")

        self.logger.debug("CONFIG:" + str(config))

        self.configure(config)
    

    
    def configure(self, config=None):
        """Sets the list of setupts
        """
        if config is None:
            raise ValueError("No configuration provided")

        try:
            self.mySetups = config['models']

            self.nModels  = len(self.mySetups)
        except KeyError:
            raise NoDataError("No models specified in configuration")

    def run(self):
        """
        The run diagnostic method.

        The method produces as output a figure with the winter and summer spatial
        distributions of sea ice thickness averaged in time (Arctic and Antarctic)

        """

        self.plotThickness()

    def plotThickness(self):



        # One figure per projection (impossible to instantiate a figure with different central_latitude

        for jHemi, hemi in enumerate(["nh", "sh"]):
            if hemi == "nh":
                central_latitude = 90
                ylims = [50,90]
            else:
                central_latitude = -90
                ylims = [-90,-50]

            projection = ccrs.Orthographic(central_longitude=0.0, central_latitude=central_latitude)
            #ax = plt.axes(projection= \
            #                projection)
            xlims = [-180,180]
            

            fig1, ax1 = plt.subplots(nrows = 1, ncols = self.nModels, subplot_kw={'projection': projection}, figsize=(5 * self.nModels, 5))


            if self.nModels == 1:
                ax1 = [ax1]
            else:
                ax1 = ax1.flatten()

            for jSetup, setup in enumerate(self.mySetups):
                # Acquiring the setup
                self.logger.debug("Setup: " + str(setup))
                model = setup["model"]
                exp = setup["exp"]
                # We use get because the reader can try to take
                # automatically the first source available
                source = setup.get("source", None)
                regrid = setup.get("regrid", None)
                var = setup.get("var", "avg_siconc")
                timespan = setup.get("timespan", None)

                self.logger.info(f"Retrieving data for {model} {exp} {source}")

                # Instantiate reader
                try:
                    reader = Reader(model=model, exp=exp, source=source,
                                regrid=regrid, loglevel=self.loglevel)

                    tmp = reader.retrieve()
                except Exception as e:
                    self.logger.error("An exception occurred while instantiating reader: %s", e)
                    raise NoDataError("Error while instantiating reader")

                try:
                    data= reader.retrieve(var=var)
                except KeyError:
                    self.logger.error("Variable %s not found in dataset", var)
                    raise NoDataError("Variable not found in dataset")
                if timespan is None:
                    # if timespan is set to None, retrieve the timespan
                    # from the data directly
                    self.logger.warning("Using timespan based on data availability")
                    timespan = [np.datetime_as_string(data.time[0].values, unit='D'),
                                                     np.datetime_as_string(data.time[-1].values, unit='D')]
                if regrid:
                    self.logger.info("Regridding data")
                    data = reader.regrid(data)

                try:
                    lat = data.coords["lat"]
                    lon = data.coords["lon"]
                except KeyError:
                    raise NoDataError("No lat/lon coordinates found in dataset")

                # Important: recenter the lon in the conventional 0-360 range
                lon = (lon + 360) % 360
                lon.attrs["units"] = "degrees"

                label = setup["model"] + " " + setup["exp"] + " " + setup["source"]
            

                if len(lon.shape) == 1: # If we are using a regular grid, we need to meshgrid it
                    lon2D, lat2D = np.meshgrid(lon, lat)
                else:
                    lon2D, lat2D = lon, lat

                month_diagnostic = 3 # Classical (non-Pythonic) convention
                monthNames = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]

                self.logger.warning("WARNING: TO BE IMPLEMENTED: SUB SELECT BASED ON TIME SPAN")
                maskTime = (data['time.month'] == month_diagnostic)
                dataPlot = data[var].where(maskTime, drop=True).mean("time").values

                # Create color sequence for sit
                masterColors = [[0.0, 0.0, 0.2],[0.0, 0.5, 0.5],[0.0, 0.5, 0.0], [1.0, 0.5, 0.0], [0.5, 0.0, 0.0] ]

                listCol = list()
                for m in masterColors:
                    alpha = 0.80
                    tmp = colInterpolatOr([m, [mm + alpha * (1 - mm) for mm in m]], 5)
                    listCol += tmp
                myCM = LinearSegmentedColormap.from_list('myCM', listCol, N = len(listCol))
                
                # Create a figure and axis with the specified projection

                # Add cyclic points to avoid a white Greenwich meridian
                #varShow, lon1DCyclic = add_cyclic_point(dataPlot, coord = lon1D, axis = 1)

                # Plot the field data using contourf
                if hemi == "nh":
                    levels = np.arange(0.0, 5.05, 1.0)
                    levelsShow = levels
                else:
                    levels = np.arange(0.0, 2.55, 0.5)
                    levelsShow = levels

                ax1[jSetup].set_extent(xlims+ylims, crs=ccrs.PlateCarree())
                ax1[jSetup].coastlines("110m", linewidth=0.5, color="black")

                contour = ax1[jSetup].pcolormesh(lon, lat, dataPlot,  \
                          transform=ccrs.PlateCarree(), cmap = myCM, vmin = 0.0, vmax = np.max(levels)
                          )


                # Add coastlines and gridlines
                ax1[jSetup].coastlines()
                #ax.gridlines()
                ax1[jSetup].add_feature(cfeature.LAND, edgecolor='k')

                # Add colorbar
                cbar = plt.colorbar(contour, ax=ax1[jSetup], orientation='vertical', pad=0.05)
                cbar.set_label('meters')
                #cbar.set_ticks(levelsShow)

                # Set title
                ax1[jSetup].set_title(str(model) + "-" + str(exp) + "-" + str(source)  + '\n(average over ' + " - ".join(timespan) + ")")

            fig1.suptitle(monthNames[month_diagnostic - 1] + ' sea ice thickness ')
            fig1.savefig(self.outputdir + "./pdf/maps_thickness_" + hemi + ".pdf") 
        
