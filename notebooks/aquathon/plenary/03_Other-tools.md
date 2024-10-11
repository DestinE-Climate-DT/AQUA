# Other tools

[TOC]

## Introduction

The core of AQUA is to deal with the access to the data and the burden of preprocess needed for the scientific analysis.
However inside the Climate DT, the role of AQUA is to provide the quality assessment of the production simulations. In order to do so, diagnostics making use of all the potentiality of AQUA have been developed.

In this section we will highlight with an overview and some practical examples what you can find in the code, so that you will be able to make use of them in your daily scientific analysis.

## AQUA framework structure

Let's take a look at the folder structure. The core code is in the `aqua` folder inside the main path of the repo. Here you will find:

- `cli`: the folder contains the aqua console code and in the future it will contains tools that are important part of the framework (LRA and catalog generator are WIP)
- `graphics`: graphical tools available to show the analysis results. Maps, timeseries and seasonal cycles are now available.
- `gsv`: the intake driver to access the FDB data. Quite hidden for the normal user, nothing to explore here.
- `lra_generator`: also here quite technical code, the cli will be available in the `cli` folder
- `reader`: here we have the core of AQUA, the `Reader` class and its methods are here, together with the tools to inspect the catalog. New methods and expansions of the preprocessing capabilities can be developed here.
- `slurm`: a package dedicated to use a dask cluster in an interactive node, but opening it while being in the login node. Useful for quick tests of more heavy functionalities.
- `util`: collection of scientific and technical utilities such as yaml reading and merging, zarr creation, time conversions. Before creating a new utility for your analysis give a look here!

## Main Reader methods

Let's move to the notebooks in order to be able to test with real datasets the main methods available for the Reader. The file is ```AQUA/notebooks/aquathon/plenary/03_tools.ipynb```.


### Exercise 2

Can you reproduce the result of the exercise 1, but making use of AQUA built-in methods?
**Bonus**: can you also make use of the AQUA tools to plot the timeseries?

<details>
  <summary>Need a hint? (Spoiler alert)</summary>

- **hint1**: the area evaluation of AQUA will automatically take into account the correct weights, no need to worry about it in this exercise!
- **hint2**: to plot timeseries you can use the function `plot_timeseries()` from `aqua.graphics`.

</details>

<details>
  <summary>Need the solution? (Spoiler alert)</summary>
    
    from aqua import Reader
    from aqua.graphics import plot_timeseries

    reader_era5 = Reader(catalog='obs', model='ERA5', exp='era5', source='monthly')
    reader_ifs_fesom = Reader(catalog='nextgems4', model='IFS-FESOM', exp='historical-1990', source='lra-r100-monthly')

    data_era5 = reader_era5.retrieve(var='2t', startdate='1990-01-01', enddate='2005-12-31')
    data_ifs_fesom = reader_ifs_fesom.retrieve(var='2t', startdate='1990-01-01', enddate='2005-12-31')

    ts_era5 = reader_era5.fldmean(data_era5['2t'])
    ts_ifs_fesom = reader_ifs_fesom.fldmean(data_ifs_fesom['2t'])

    plot_timeseries(monthly_data=ts_ifs_fesom, ref_monthly_data=ts_era5,
                    data_labels=['IFS-FESOM historical'], ref_label='ERA5',
                    title='2m Temperature')
</details>

## Important CLI and other infos

AQUA is undergoing important changes in the code. We're organizing the important CLI in our main code in the `aqua` folder, but in the meanwhile, many tools are in the `cli` folder inside the main repo path.

Please take a look at the dedicated page in the [documentation](https://aqua-web-climatedt.2.rahtiapp.fi/documentation/cli.html) to have a full overwiev of what is it available and how to use the differen tools.

In particular, not mentioned so far, AQUA provides a `aqua-analysis.sh` script that allows the user to run all the available diagnostics on a experiment (monthly and regridded to 1 deg). This will be explored in our breakout session.

If you want to explore and expand our diagnostics, please take a look at the dedicated section of the [documentation](https://aqua-web-climatedt.2.rahtiapp.fi/documentation/diagnostics/index.html) and contact the AQUA team on GitHub or our [mattermost channel](https://mattermost.mpimet.mpg.de/destine/channels/aqua) for every doubt.
