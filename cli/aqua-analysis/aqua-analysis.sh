#!/bin/bash

# User defined variables
# ----------------------
# The following variables can be changed by the user.
# ---------------------------------------------------
model_atm="IFS"
model_oce="FESOM"
exp="tco2559-ng5-cycle3"
source="lra-r100-monthly"

# LUMI
# outputdir="/scratch/project_465000454/nurissom/cli_outpturdir"
# aqua="/users/nurissom/AQUA"

# LEVANTE
outputdir="/scratch/b/b382289/cli_test"
aqua="/home/b/b382289/AQUA"

machine="levante" # will change the aqua config file

# When available, this will set the loglevel for the diagnostics
loglevel="WARNING" # DEBUG, INFO, WARNING, ERROR, CRITICAL

# Set as true the diagnostics you want to run
# -------------------------------------------
run_dummy=true # dummy is a diagnostic that checks if the setup is correct
run_atmglobalmean=true
run_ecmean=true
# ---------------------------------------
# Command line extra arguments for ecmean
# -c --config ecmean config file
# -i --interface custom interface file
# ---------------------------------------
run_global_time_series=true
run_ocean3d=true
run_radiation=true
# ------------------------------------------
# Command line extra arguments for radiation
# --config (readiation config file)
# ------------------------------------------
run_seaice=true
# ------------------------------------------
# Command line extra arguments for seaice
# --all-regions (if set it will plot all regions)
#               (default is False)
# --config (seaice config file)
# --regrid (regrid data to a different grid)
# ------------------------------------------
run_teleconnections=true
# teleconnections additional flags
# ------------------------------------------------------------------
# --dry, -d (dry run, if set it will run without producing plots)
# --ref (if set it will analyze also the reference data, it is set
#        by default)
# ------------------------------------------------------------------
run_tropical_rainfall=true

# End of user defined variables
# -----------------------------

# Define colors for echo output
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

colored_echo() {
  local color=$1
  shift
  echo -e "${color}$@${NC}"
}

# Define the arguments for the diagnostics
args_atm="--model $model_atm --exp $exp --source $source"
args_oce="--model $model_oce --exp $exp --source $source"
args="--model_atm $model_atm --model_oce $model_oce --exp $exp --source $source"

# use $AQUA if defined otherwise use aqua
if [[ -z "${AQUA}" ]]; then
  colored_echo $GREEN "AQUA path is not defined, using user defined aqua in the script"
else
  colored_echo $GREEN "AQUA path is defined, using $AQUA"
  aqua=$AQUA
fi

# set the correct machine in the config file
if [[ "$OSTYPE" == "darwin"* ]]; then
  # Mac OSX
  sed -i '' "/^machine:/c\\
machine: $machine" "$aqua/config/config-aqua.yaml"
else
  # Linux
  sed -i "/^machine:/c\\machine: $machine" "$aqua/config/config-aqua.yaml"
fi

# print the output directory
colored_echo $GREEN "Output directory: $outputdir"

if [ "$run_dummy" = true ] ; then
  colored_echo $GREEN "Running setup checker"
  python $aqua/diagnostics/dummy/cli/cli_dummy.py $args -l $loglevel

  # exit if dummy fails
  if [ $? -ne 0 ]; then
    colored_echo $RED "Setup checker failed, exiting"
    exit 1
  fi
  colored_echo $GREEN "Finished setup checker"
fi

if [ "$run_atmglobalmean" = true ] ; then
  colored_echo $GREEN "Running atmglobalmean"
  cd $aqua/diagnostics/atmglobalmean/cli
  python cli_atm_mean_bias.py $args_atm --outputdir $outputdir/atmglobalmean -l $loglevel
  cd $aqua/cli/aqua-analysis
  colored_echo $GREEN "Finished atmglobalmean"
fi

if [ "$run_ecmean" = true ] ; then
  colored_echo $GREEN "Running ecmean"
  scriptpy="$aqua/diagnostics/ecmean/cli/ecmean_cli.py"
  python $scriptpy $args -o $outputdir/ecmean -l $loglevel
  colored_echo $GREEN "Finished ecmean"
fi

if [ "$run_global_time_series" = true ] ; then
  colored_echo $GREEN "Running global_time_series"

  filepy="$aqua/diagnostics/global_time_series/cli/single_analysis/cli_global_time_series.py"
  conf_atm="$aqua/diagnostics/global_time_series/cli/single_analysis/config_time_series_atm.yaml"
  conf_oce="$aqua/diagnostics/global_time_series/cli/single_analysis/config_time_series_oce.yaml"

  python $filepy $args_atm --outputdir $outputdir/global_time_series --config $conf_atm -l $loglevel
  python $filepy $args_oce --outputdir $outputdir/global_time_series --config $conf_oce -l $loglevel
  colored_echo $GREEN "Finished global_time_series"
fi

if [ "$run_ocean3d" = true ] ; then
  colored_echo $GREEN "Running ocean3d"
  # Moving to ocean3d directory to run the ocean3d_cli.py script
  cd $aqua/diagnostics/ocean3d/cli
  python $aqua/diagnostics/ocean3d/cli/ocean3d_cli.py $args_oce --outputdir $outputdir/ocean3d

  # Moving back to aqua-analysis directory
  cd $aqua/cli/aqua-analysis

  colored_echo $GREEN "Finished ocean3d"
fi

if [ "$run_radiation" = true ] ; then
  colored_echo $GREEN "Running radiation"
  python $aqua/diagnostics/radiation/cli/cli_radiation.py $args_atm --outputdir $outputdir/radiation -l $loglevel
  colored_echo $GREEN "Finished radiation"
fi

if [ "$run_seaice" = true ] ; then
  colored_echo $GREEN "Running seaice"
  python $aqua/diagnostics/seaice/cli/seaice_cli.py $args_oce --outputdir $outputdir/seaice -l $loglevel
  colored_echo $GREEN "Finished seaice"
fi

if [ "$run_teleconnections" = true ] ; then
  colored_echo $GREEN "Running teleconnections"
  scriptpy="$aqua/diagnostics/teleconnections/cli/cli_teleconnections.py"

  python $scriptpy $args_atm --outputdir $outputdir/teleconnections --config cli_config_atm.yaml -l $loglevel --ref
  python $scriptpy $args_oce --outputdir $outputdir/teleconnections --config cli_config_oce.yaml -l $loglevel --ref
  colored_echo $GREEN "Finished teleconnections"
fi

if [ "$run_tropical_rainfall" = true ] ; then
  colored_echo $GREEN "Running tropical rainfall"
  cd $aqua/diagnostics/tropical_rainfall/cli
  python $aqua/diagnostics/tropical_rainfall/cli/cli_tropical_rainfall.py $args_atm --outputdir $outputdir/tropical_rainfall -l $loglevel
  cd $aqua/cli/aqua-analysis
  colored_echo $GREEN "Finished tropical rainfall"
fi

colored_echo $GREEN "Finished all diagnostics"
