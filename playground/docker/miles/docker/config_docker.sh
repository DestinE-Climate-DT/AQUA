#!/bin/bash

####################################
# ---- folder definition --------- #
####################################

# ---- this is a template for unix machines ----- #

# INDIR ->data folder: all the geopotential height data should be here
# you must customoze this according to the dataset you analyze and your local file structure
# you can play with the path structure with variable $dataset, $expid and $ens
# this will correspond to $dataset_exp and $expid_exp and to $ens from wrapper_miles.sh
INDIR=/data

# to look for some specific file structure
# otherwise the program will look for all the netcdf or grib files in the folder
#export expected_input_name=*.nc

#program folder where MiLES is placed
export PROGDIR=$(pwd)
#data folder where place output (Z500 files, NetCDF files and figures)
export OUTPUTDIR=/miles_output

####################################
# ----  program definition  ------ #
####################################

#program definition

#CDO
#if you CDO is not equipped of NetCDF4 compression change "cdo4" command here
cdo=cdo
cdonc="$cdo -f nc"
cdo4="$cdo -f nc4 -z zip"

#Rscript is the script-launcher by R
Rscript=Rscript


####################################
#no need to change below this line #
####################################

#NetCDF output dir
export FILESDIR=$OUTPUTDIR/files
#figures folder
export FIGDIR=$OUTPUTDIR/figures

# file type
export output_file_type
export map_projection


#creating folders
mkdir -p $FIGDIR $FILESDIR

#safety check
echo "Check if CDO has been loaded"
command -v $cdo -v >/dev/null 2>&1 || { echo "CDO module is not loaded. Aborting." >&2; exit 1; }
echo "CDO found: proceeding..."

echo "Check if R has been loaded"
command -v $Rscript >/dev/null 2>&1 || { echo "R module is not loaded. Aborting." >&2; exit 1; }
echo "R found: proceeding..."

#R check for key packages
$Rscript $PROGDIR/config/installpack.R


