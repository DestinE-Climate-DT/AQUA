# AQUA console and Reader class

[TOC]

## AQUA console

AQUA provides easy access to data in different formats (netCDF, GRIB, FDB, Zarr, ...), hiding the technicalities inside yaml files that are part of the **catalog**.

In order to use AQUA we need then to deploy these files in our machine. This is done with the AQUA console, that has the purpose of helping the user in the process of deploying the requested catalogs and setting user informations. The detailed documentation is available [here](https://aqua-web-climatedt.2.rahtiapp.fi/documentation/aqua_console.html).

### Initializing the .aqua folder

Let's start by initializing our installation folder:

```bash
mamba activate aqua # if AQUA is installed with a conda manager
aqua install <machine-name> -e $AQUA/config
```

This will create a folder `$HOME/.aqua` where a config-aqua.yaml file with user setting is stored. Also the catalogs we want to add will be stored in this folder. 

:::info
The `-e $AQUA/config` will guarantee that instead of copying other necessary files (grid and fix definitions), we link the files of our source code. This is important if we want to customize them or to keep them always updated when we pull the source code from github.
:::

:::warning
:exclamation: Do you have already an `.aqua` folder but you want to follow this part? You can always use `aqua uninstall` to delete the folder and start from scratch with this tutorial.
:::

### Installing a catalog

A catalog can be added to the `$HOME/.aqua` folder with the following command:

```bash
aqua add <catalog-name>
```

This will download from the catalog repository the most updated version of the selected catalog, if found. This will work also if you don't have access to the repository.

To add the observations, for example:

```bash
aqua add obs
```

However, it is possible to link our files, because we're working with a developer version of the catalog or we're working on a new catalog. AQUA has the possibility to add new catalogs not part of the official repository, in order to support other projects.

Let's add the other catalogs used in the session:

```bash
aqua add nextgems3
aqua add nextgems4
```

As for the install, the `-e` flag will link the selected catalog instead of copying the files.

Finally we can check which catalogs we have installed with the command:

```bash
aqua list
```

This will specify also if a catalog si added in editable mode (with links) and where are the original file stored.

Other commands for more complicated task (uninstall, add fix or grid custom files, etc..) are available, check the [documentation](https://aqua-web-climatedt.2.rahtiapp.fi/documentation/aqua_console.html).

## Reader class

In this section we will explore what is it the Reader class, the main class with which AQUA access data.

:::info
This and the following sections will be only shortly introduced in this document, while notebooks with working examples are available in your AQUA repository and specifically in the folder `$AQUA/notebooks/aquathon/plenary`.
:::

### Exercise 1

Let's try to do a little exercise with the ERA5 data and the IFS-NEMO historical simulation from phase1.

You can open this two entries:

- catalog='nextgems4', model='IFS-FESOM', exp= 'historical-1990', source='lra-r100-monthly'
- catalog='obs', model='ERA5', exp='era5', source='monthly'

Both the sources are regular lon-lat grids, so we can compute for the two easily the global mean temperature (`var=2t`) and plot a time series of it. Can you do it for the period 1990-2005 with the usage of the Reader to retrieve the data and the xarray functionalities for the analysis?

<details>
  <summary>Need an hint? (Spoiler alert)</summary>
  
- **hint1**: the global mean in a regular grid requires the computation of the area weighted mean, which can be done with the `weighted` method of the `xarray.Dataset` object.
- **hint2**: the Reader returns always an xarray.Dataset object, also when asking for only one variable.
  
</details>

<details>
  <summary>Need the solution? (Spoiler alert)</summary>
    
    from aqua import Reader
    import matplotlib.pyplot as plt
    import numpy as np

    # Instantiate the Reader and retrieve the data
    reader1 = Reader(catalog='obs', model='ERA5', exp='era5', source='monthly')
    reader2 = Reader(catalog='nextgems4', model='IFS-FESOM', exp='historical-1990', source='lra-r100-monthly')

    data1 = reader1.retrieve(var='2t', startdate='1990-01-01', enddate='2010-12-31')
    data2 = reader2.retrieve(var='2t', startdate='1990-01-01', enddate='2010-12-31')

    # Move to the xarray dataarray
    data1 = data1['2t']
    data2 = data2['2t']

    # Evaluate the global mean on a regular grid
    lat1 = data1.lat
    lat2 = data2.lat
    wgt1 = np.cos(np.deg2rad(lat1))
    wgt2 = np.cos(np.deg2rad(lat2))

    timeseries1 = data1.weighted(wgt1).mean(dim=['lat', 'lon'])
    timeseries2 = data2.weighted(wgt2).mean(dim=['lat', 'lon'])

    # Plot the timeseries
    plt.figure()
    timeseries1.plot(label='ERA5')
    timeseries2.plot(label='IFS-FESOM')

    plt.legend()
    plt.show()
</details>
