#!/bin/bash
# This script copies files which are needed by recent eccodes binaries to older versions of eccodes

dir=/work/bb1153/b382075/aqua/eccodes
src=2.36.0

for dst in 2.32.6
do
for f in stepUnits.def template.4.20.def template.4.forecast_time_44.def template.4.forecast_time.def template.4.localtime.def
do
   cp $dir/eccodes-$src/definitions/grib2/$f $dir/eccodes-$dst/definitions/grib2/$f
done
done

