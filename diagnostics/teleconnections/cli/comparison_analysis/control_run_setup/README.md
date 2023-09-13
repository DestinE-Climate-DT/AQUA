# Control run analysis CLI setup

This folder contains the setup that has been used to run the control run analysis with the CLI on levante.
Two files, one for NAO and one for ENSO teleconnection, are provided. 
They contain the list of the experiments that have been run, which specific source has been used for the data, together with technical information on the setup of the analysis, like resolution, time aggregation, etc.

Since for ENSO the variable 'sst' was not available, we used '2t' to try to infer some results. A study of ENSO done with ERA5 data and a comparison of the index evaluated with '2t' compared with NCAR data is available in the notebooks folder.

If you'd like to run the analysis on your own, you can use these files as a template, please note that you will need to change the paths to the data and the output folder to not overwrite the existing results.
The analysis can be also performed on lumi, since the same entries with the same properties are available on both machines.

The analysis will generate the same data provided in the deliverable. 
The automatic generation of the figures included in the CLI may differ from the final figures provided in the deliverable, as they have been sometimes recreated opening the netCDF files and improving the layout with respect to the automatic generation.