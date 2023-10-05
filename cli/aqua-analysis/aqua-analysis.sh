#!/bin/bash

# User defined variables
# ----------------------
# The following variables can be changed by the user.
# ---------------------------------------------------
model_atm="IFS"
model_oce="NEMO"
exp="control-1950-devcon"
source="lra-r100-monthly"

outputdir="/work/bb1153/b382267/cli_test"
aqua="/work/bb1153/b382267/AQUA"

# Set as true the diagnostics you want to run
# -------------------------------------------
atmglobalmean=false
dummy=false # dummy is a test diagnostic
ecmean=false
global_time_series=false
ocean3d=false
radiation=false
seaice=false
teleconnection=false
tropical_rainfall=true

# End of user defined variables
# -----------------------------

args_atm="--model $model_atm --exp $exp --source $source"
args_oce="--model $model_oce --exp $exp --source $source"
args="--model_atm $model_atm --model_oce $model_oce --exp $exp --source $source"

# use $AQUA if defined otherwise use aqua
if [[ -z "${AQUA}" ]]; then
  echo "AQUA path is not defined, using user defined aqua in the script"
else
  echo "AQUA path is defined, using $AQUA"
  aqua=$AQUA
fi

if [ "$atmglobalmean" = true ] ; then
  echo "Running atmglobalmean"
  python $aqua/diagnostics/atmglobalmean/cli/cli_atmglobalmean.py $args_atm --outputdir $outputdir/atmglobalmean
fi

if [ "$dummy" = true ] ; then
  echo "Running dummy"
  python $aqua/diagnostics/dummy/cli/cli_dummy.py $args --outputdir $outputdir/dummy
fi

if [ "$ecmean" = true ] ; then
  echo "Running ecmean"
  python $aqua/diagnostics/ecmean/cli/cli_ecmean.py $args --outputdir $outputdir/ecmean
fi

if [ "$global_time_series" = true ] ; then
  echo "Running global_time_series"
  python $aqua/diagnostics/global_time_series/cli/cli_global_time_series.py $args_atm --outputdir $outputdir/global_time_series --config $aqua/diagnostics/global_time_series/cli/config_atm.yaml
  python $aqua/diagnostics/global_time_series/cli/cli_global_time_series.py $args_oce --outputdir $outputdir/global_time_series --config $aqua/diagnostics/global_time_series/cli/config_oce.yaml
fi

if [ "$ocean3d" = true ] ; then
  echo "Running ocean3d"
  python $aqua/diagnostics/ocean3d/cli/cli_ocean3d.py $args_oce --outputdir $outputdir/ocean3d
fi

if [ "$radiation" = true ] ; then
  echo "Running radiation"
  python $aqua/diagnostics/radiation/cli/cli_radiation.py $args_atm --outputdir $outputdir/radiation
fi

if [ "$seaice" = true ] ; then
  echo "Running seaice"
  python $aqua/diagnostics/seaice/cli/cli_seaice.py $args_oce --outputdir $outputdir/seaice
fi

if [ "$teleconnection" = true ] ; then
  echo "Running teleconnection"
  python $aqua/diagnostics/teleconnection/cli/cli_teleconnection.py $args_atm --outputdir $outputdir/teleconnection --config $aqua/diagnostics/teleconnection/cli/config_atm.yaml
  python $aqua/diagnostics/teleconnection/cli/cli_teleconnection.py $args_oce --outputdir $outputdir/teleconnection --config $aqua/diagnostics/teleconnection/cli/config_oce.yaml
fi

if [ "$tropical_rainfall" = true ] ; then
  echo "Running tropical rainfall"
  cd $aqua/diagnostics/tropical_rainfall/cli
  python $aqua/diagnostics/tropical_rainfall/cli/cli_tropical_rainfall.py $args_atm --outputdir $outputdir/tropical_rainfall
  cd $aqua/cli/aqua-analysis
fi

echo "Finished"