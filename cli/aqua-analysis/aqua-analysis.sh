#!/bin/bash

# User defined variables
# ---------------------------------------------------
# The following variables can be changed by the user.
# They can be overwritten by using the command line
# arguments.
# ---------------------------------------------------
model_atm="IFS"
model_oce="FESOM"
exp="tco2559-ng5-cycle3"
source="lra-r100-monthly"
outputdir="/scratch/b/b382289/cli_test"
loglevel="WARNING" # DEBUG, INFO, WARNING, ERROR, CRITICAL
machine="levante" # will change the aqua config file

# AQUA path, can be defined as $AQUA env variable
# if not defined it will use the aqua path in the script
aqua="/home/b/b382289/AQUA"

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

# Command line arguments

# Define accepted log levels
accepted_loglevels=("info" "debug" "error" "warning" "critical" "INFO" "DEBUG" "ERROR" "WARNING" "CRITICAL")

# Parse command-line options
while [[ $# -gt 0 ]]; do
  case "$1" in
    -a|--model_atm)
      model_atm="$2"
      shift 2
      ;;
    -o|--model_oce)
      model_oce="$2"
      shift 2
      ;;
    -e|--exp)
      exp="$2"
      shift 2
      ;;
    -s|--source)
      source="$2"
      shift 2
      ;;
    -d|--outputdir)
      outputdir="$2"
      shift 2
      ;;
    -m|--machine)
      machine="$2"
      shift 2
      ;;
    -l|--loglevel)
      # Check if the specified log level is in the accepted list
      if [[ " ${accepted_loglevels[@]} " =~ " $2 " ]]; then
        loglevel="$2"
      else
        colored_echo $RED "Invalid log level. Accepted values are: ${accepted_loglevels[@]}"
        # Setting loglevel to WARNING
        loglevel="WARNING"
      fi
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

colored_echo $GREEN "Setting loglevel to $loglevel"
colored_echo $GREEN "Atmospheric model: $model_atm"
colored_echo $GREEN "Oceanic model: $model_oce"
colored_echo $GREEN "Experiment: $exp"
colored_echo $GREEN "Source: $source"
colored_echo $GREEN "Machine: $machine"
colored_echo $GREEN "Output directory: $outputdir"

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
colored_echo $GREEN "Machine set to $machine in the config file"

if [ "$run_dummy" = true ] ; then
  colored_echo $GREEN "Running setup checker"
  scriptpy="$aqua/diagnostics/dummy/cli/cli_dummy.py"
  python $scriptpy $args -l $loglevel
  
  # Store the error code of the dummy script
  dummy_error=$?

  # exit if dummy fails in finding both atmospheric and oceanic model
  if [ $dummy_error -ne 0 ]; then
    if [ $dummy_error -eq 1 ]; then # if error code is 1, then the setup checker failed
      colored_echo $RED "Setup checker failed, exiting"
      exit 1
    fi
    # if error code is 2 or 3, then the setup checker
    # passed but there are some warnings
    if [ $dummy_error -eq 2 ]; then
      colored_echo $RED "Atmospheric model is not found, it will be skipped"
    fi
    if [ $dummy_error -eq 3 ]; then
      colored_echo $RED "Oceanic model is not found, it will be skipped"
    fi
  fi
  colored_echo $GREEN "Finished setup checker"
fi

if [ "$run_atmglobalmean" = true ] ; then
  colored_echo $GREEN "Running atmglobalmean"
  scriptpy="$aqua/diagnostics/atmglobalmean/cli/cli_atm_mean_bias.py"
  python $scriptpy $args_atm --outputdir $outputdir/atmglobalmean -l $loglevel
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

  scriptpy="$aqua/diagnostics/global_time_series/cli/single_analysis/cli_global_time_series.py"
  conf_atm="$aqua/diagnostics/global_time_series/cli/single_analysis/config_time_series_atm.yaml"
  conf_oce="$aqua/diagnostics/global_time_series/cli/single_analysis/config_time_series_oce.yaml"

  python $scriptpy $args_atm --outputdir $outputdir/global_time_series --config $conf_atm -l $loglevel
  python $scriptpy $args_oce --outputdir $outputdir/global_time_series --config $conf_oce -l $loglevel
  colored_echo $GREEN "Finished global_time_series"
fi

if [ "$run_ocean3d" = true ] ; then
  colored_echo $GREEN "Running ocean3d"
  scriptpy="$aqua/diagnostics/ocean3d/cli/ocean3d_cli.py"
  python $scriptpy $args_oce --outputdir $outputdir/ocean3d -l $loglevel
  colored_echo $GREEN "Finished ocean3d"
fi

if [ "$run_radiation" = true ] ; then
  colored_echo $GREEN "Running radiation"
  scriptpy="$aqua/diagnostics/radiation/cli/cli_radiation.py"
  python $scriptpy $args_atm --outputdir $outputdir/radiation -l $loglevel
  colored_echo $GREEN "Finished radiation"
fi

if [ "$run_seaice" = true ] ; then
  colored_echo $GREEN "Running seaice"
  scriptpy="$aqua/diagnostics/seaice/cli/seaice_cli.py"
  python $scriptpy $args_oce --outputdir $outputdir/seaice -l $loglevel
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
  scriptpy="$aqua/diagnostics/tropical_rainfall/cli/cli_tropical_rainfall.py"
  python $scriptpy $args_atm --outputdir $outputdir/tropical_rainfall -l $loglevel
  colored_echo $GREEN "Finished tropical rainfall"
fi

colored_echo $GREEN "Finished all diagnostics"
