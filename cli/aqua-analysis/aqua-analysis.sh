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
outputdir="./output"
loglevel="WARNING" # DEBUG, INFO, WARNING, ERROR, CRITICAL
machine="levante" # will change the aqua config file

# AQUA path, can be defined as $AQUA env variable
# if not defined it will use the aqua path in the script
aqua="/work/bb1153/b382267/AQUA"

# ---------------------------------------
# The max_threads variable serves as a mechanism to control the maximum number of threads
# or parallel processes that can run simultaneously.
# - If max_threads is set to 0 or a negative value: There is no limit on the number of threads,
#                                                   and all processes run in parallel without waiting. 
#                                                   This is suitable for situations where you want to utilize
#                                                   the maximum available resources without any restrictions.
# - If max_threads is set to a positive value: It limits the number of concurrent threads to the specified value. 
#                                              After launching the designated number of threads, the script waits
#                                              for these threads to complete before launching additional ones. 
#                                              This is useful when you are working on a system with limitations
#                                              on the number of concurrent threads, like a login node.
# ---------------------------------------
max_threads=-1  # Set to the desired maximum number of threads, or leave it as 0 for no limit

# Define the array of atmospheric diagnostics, add more if needed or available
atm_diagnostics=("tropical_rainfall" "global_time_series" "radiation" "teleconnections" "atmglobalmean")
# Define the array of oceanic diagnostics, add more if needed or available
oce_diagnostics=("global_time_series" "teleconnections" "ocean3d" "seaice")
# Define the array of diagnostics combining atmospheric and oceanic data, add more if needed or available
atm_oce_diagnostics=("ecmean")

# Combine all diagnostics into a single array
all_diagnostics=("${atm_diagnostics[@]}" "${oce_diagnostics[@]}" "${atm_oce_diagnostics[@]}")

run_dummy=true

# Define an associative array for atmospheric extra arguments
declare -A atm_extra_args
# Define an associative array for oceanic extra arguments
declare -A oce_extra_args
# Define an associative array for combined atmospheric and oceanic extra arguments
declare -A atm_oce_extra_args

# Set default value for all keys in atmospheric extra arguments
default_value=" "
for diagnostic in ${atm_diagnostics[@]}; do
  atm_extra_args["$diagnostic"]=$default_value
done

# Set default value for all keys in oceanic extra arguments
for diagnostic in ${oce_diagnostics[@]}; do
  oce_extra_args["$diagnostic"]=$default_value
done

# Set default value for all keys in combined atmospheric and oceanic extra arguments
for diagnostic in ${atm_oce_diagnostics[@]}; do
  atm_oce_extra_args["$diagnostic"]=$default_value
done
# Here we can set the extra arguments for each diagnostic
# ----------------------------------------------------
# Command line extra arguments for global_time_series:
# --config (config file)
# Concatenate the new part to the existing content
atm_extra_args["global_time_series"]="${atm_extra_args["global_time_series"]} \
--config $aqua/diagnostics/global_time_series/cli/single_analysis/config_time_series_atm.yaml"
oce_extra_args["global_time_series"]="${oce_extra_args["global_time_series"]} \
--config $aqua/diagnostics/global_time_series/cli/single_analysis/config_time_series_oce.yaml"
# ----------------------------------------
# Command line extra arguments for ecmean:
# -c --config (ecmean config file)
# -i --interface (custom interface file)
# -------------------------------------------
# Command line extra arguments for radiation:
# --config (readiation config file)
# ------------------------------------------
# Command line extra arguments for seaice:
# --all-regions (if set it will plot all regions)
#               (default is False)
# --config (seaice config file)
# --regrid (regrid data to a different grid)
# ----------------------------------------------------------------
# Command line extra arguments for teleconnections:
# --dry, -d (dry run, if set it will run without producing plots)
# --ref (if set it will analyze also the reference data, it is set
#        by default)
# Concatenate the new part to the existing content
atm_extra_args["teleconnections"]="${atm_extra_args["teleconnections"]} \
--config cli_config_atm.yaml --ref"
oce_extra_args["teleconnections"]="${oce_extra_args["teleconnections"]} \
--config cli_config_oce.yaml --ref"
# End of user defined variables
# -----------------------------

# Trap Ctrl-C to clean up and kill the entire process group
trap 'kill 0' SIGINT

# Declare the path and cli name if it is not standard, i.e., not AQUA/diagnostics/dummy/cli/cli_dummy.py.
declare -A script_path

# Iterate over all diagnostics and set the default script path
for diagnostic in ${all_diagnostics[@]}; do
  script_path["$diagnostic"]="$diagnostic/cli/cli_$diagnostic.py"
done

# Set specific value for "global_time_series"
script_path["global_time_series"]="global_time_series/cli/single_analysis/cli_global_time_series.py"

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
    -t|--threads)
      max_threads="$2"
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

# Create output directory if it does not exist
colored_echo $GREEN "Creating output directory $outputdir"
mkdir -p "$outputdir"

if [ "$run_dummy" = true ] ; then
  colored_echo $GREEN "Running setup checker"
  scriptpy="$aqua/diagnostics/dummy/cli/cli_dummy.py"
  python $scriptpy $args -l $loglevel > "$outputdir/setup_checker.log" 2>&1
  
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

thread_count=0
# Run diagnostics in parallel
for diagnostic in "${all_diagnostics[@]}"; do
  colored_echo $GREEN "Running $diagnostic"

  if [[ "${atm_diagnostics[@]}" =~ "$diagnostic" ]]; then
    python "$aqua/diagnostics/${script_path[$diagnostic]}" $args_atm ${atm_extra_args[$diagnostic]} \
    -l $loglevel --outputdir $outputdir/$diagnostic > "$outputdir/$diagnostic.log" 2>&1 &
  elif [[ "${oce_diagnostics[@]}" =~ "$diagnostic" ]]; then
    python "$aqua/diagnostics/${script_path[$diagnostic]}" $args_oce ${oce_extra_args[$diagnostic]} \
    -l $loglevel --outputdir $outputdir/$diagnostic > "$outputdir/$diagnostic.log" 2>&1 &
  elif [[ "${atm_oce_diagnostics[@]}" =~ "$diagnostic" ]]; then
    python "$aqua/diagnostics/${script_path[$diagnostic]}" $args ${atm_oce_extra_args[$diagnostic]} \
    -l $loglevel --outputdir $outputdir/$diagnostic > "$outputdir/$diagnostic.log" 2>&1 &
  fi
  if [ $max_threads -gt 0 ]; then
    ((thread_count++))

    # Check if the maximum number of threads has been reached
    if [ $thread_count -ge $max_threads ]; then
      # Wait for the background processes to finish
      wait
      # Reset the thread count
      thread_count=0
    fi
  fi
done
# Wait for all background processes to finish
wait
colored_echo $GREEN "Finished all diagnostics"