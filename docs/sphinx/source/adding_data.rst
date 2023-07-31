Adding data to AQUA
===================

To add your data to AQUA, you have to provide an intake catalogue that describes your data, 
and in particular, the location of the data.

The best way to explain the process is to follow the example of adding some fake dataset.

Let's imagine we have a dataset called `yearly_SST` that consists of the following:

- 2 netCDF files, each file contains one year of data (`/data/path/1990.nc` and `/data/path/1991.nc`)
- data are stored in 2D arrays, with dimensions `lat` and `lon`
- coordinate variables are `lat` and `lon`, and the time variable is `time`, all one dimensional
- data located on the Levante machine

We will create a catalogue that will describe this dataset, and then we will add it to AQUA. 
The most straightforward intake catalogue describing this dataset will look like this: 

.. code-block:: yaml

    plugins:
    source:
        - module: intake_xarray

    sources:
      annual:
        description: my amazing yearly_SST dataset    
        driver: netcdf
        args:
            chunks:
                time: 1
            urlpath:
            - /data/path/1990.nc
            - /data/path/1991.nc

We name this catalogue `yearly_SST.yaml` and save it in the `config/machines/levante/catalog/yearly_SST` directory (that we should create first).
We have to add another file to the `config/machines/levante/catalog/yearly_SST` directory called `main.yaml`. 
In this file, we point to `yearly_SST.yaml`, but we can point to them here if we have several catalogues for the same dataset/model.
In our case, the `main.yaml` file will look like this:

.. code-block:: yaml

    sources:
      yearly_sst:
        description: amazing yearly_SST dataset
        driver: yaml_file_cat
        args:
          path: "{{CATALOG_DIR}}/yearly_SST.yaml"


The next step is to add this catalogue to the `config/machines/levante/catalogs.yaml` file. 
The additional entry in this file will look like this:

.. code-block:: yaml

    yearly_SST:
        description: amazing yearly_SST dataset
        driver: yaml_file_cat
        args:
          path: "{{CATALOG_DIR}}/yearly_SST/main.yaml"

The last thing to do is to add grid infromation about the data to the `config/machines/levante/regrid.yaml` file. 

In the `source_grids` section of the file we add the following entry:

.. code-block:: yaml

    yearly_SST:
        yearly_sst:
            default:
                space_coord: ["lon", "lat"]

The grid is regular, and all the info is contained inside, so there should be no problem for cdo to compute grid areas and interpolation weights.

Now we can access our dataset from AQUA with the following command:

.. code-block:: python

    from aqua import Reader
    reader = Reader(model="yearly_SST", exp="yearly_sst", source="annual", regrid='r100')
    data = reader.retrieve(fix=False)


You can add fixes to your dataset by following examples in the `config/fixes/` directory.






