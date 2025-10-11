# How to convert to zarr using nc2zarr

## Installation

You will need the nc2zarr tool, available from [github](https://github.com/bcdev/nc2zarr) and [conda-forge](https://anaconda.org/conda-forge/nc2zarr).

If your machine allows it, adding it to a conda environment is the easiest solution. We provide a sample file `env.yml`.
Unless added to the AQUA environment, on lumi you will need to install it as a separate container-wrapper.
We provide the `install.sh` script to this end (please edit the target directory).
To use the tool you will then need to add the path to this directory `$OUTDIR` to your path with 
`export path=$OUTDIR:$PATH` (see the `setnc2zarr.sh` script for an example).

## Usage

Once installed, a simple configuration file can be used. For example the enclosed `era5_monthly.yml` can be used to convert ERA5 data on lumi (edit the paths if needed):

```
nc2zarr -vv -c era5_montly.yml
```

If the configuration file contains multiple directories as a source, to guarantee that the time axis is sorted correctly we recommend adding the `-s name` option to the command.

The sample configuration file contains `overwrite: false` to avoid overwriting the production dataset. Please change anyway the output filename to test.
The option `multi_file: true` activates dask for the tool and seems in general more efficient.

The enclosed job script `nc2zarr_era5.job` shows an example of how to run it on lumi.

## Notes

Chunking is an aspect to pay attention to before creating a zarr. Please inspect the current chunking of your data. For AQUA the best option is to have no chunking in space and to chunk along other dimensions (time, levels), possibly with a step of 1. The sample configuration file provided here implements this. In some cases it has been better (e.g. MSWEP), to reduce the workload of the tool, to first rechunk the data using cdo and afterward produce the zarr.

