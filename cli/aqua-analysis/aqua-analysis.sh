#!/bin/bash
# Check if AQUA is set and the file exists
if [[ -z "$AQUA" ]]; then
    echo -e "\033[0;31mError: The AQUA environment variable is not defined."
    echo -e "\x1b[38;2;255;165;0mPlease define the AQUA environment variable with the path to your 'AQUA' directory."
    echo -e "For example: export AQUA=/path/to/aqua\033[0m"
    exit 1  # Exit with status 1 to indicate an error
else
    source "${AQUA}/cli/util/logger.sh"
    log_message INFO "Sourcing logger.sh from: ${AQUA}/cli/util/logger.sh"
    # Your subsequent commands here
fi
setup_log_level 2 # 1=DEBUG, 2=INFO, 3=WARNING, 4=ERROR, 5=CRITICAL
aqua=$AQUA

# User defined variables
# ---------------------------------------------------
# The following variables can be changed by the user.
# They can be overwritten by using the command line
# arguments.
# ---------------------------------------------------
model_atm="IFS-NEMO"
model_oce="IFS-NEMO"
exp="historical-1990"
source="lra-r100-monthly"
outputdir="${AQUA}/cli/aqua-analysis/output" # Prefer absolute paths, e.g., "/path/to/aqua/my/output"
loglevel="WARNING" # DEBUG, INFO, WARNING, ERROR, CRITICAL
machine="lumi" # will change the aqua config file

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
atm_diagnostics=("tropical_rainfall" "global_time_series" "seasonal_cycles" "radiation" "teleconnections" "atmglobalmean")
# Define the array of oceanic diagnostics, add more if needed or available
oce_diagnostics=("global_time_series" "teleconnections" "ocean3d_drift" "ocean3d_circulation" "seaice_extent" "seaice_conc" "seaice_volume" "seaice_thick")
# Define the array of diagnostics combining atmospheric and oceanic data, add more if needed or available
atm_oce_diagnostics=("ecmean")

# Combine all diagnostics into a single array
all_diagnostics=("${atm_diagnostics[@]}" "${oce_diagnostics[@]}" "${atm_oce_diagnostics[@]}")

log_message DEBUG "Running diagnostics: ${all_diagnostics[@]}"

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
--config ${aqua}/diagnostics/global_time_series/cli/config_time_series_atm.yaml"
oce_extra_args["global_time_series"]="${oce_extra_args["global_time_series"]} \
--config ${aqua}/diagnostics/global_time_series/cli/config_time_series_oce.yaml"
# ----------------------------------------
# Command line extra arguments for ecmean:
# -c --config (ecmean config file)
# -i --interface (custom interface file)
atm_oce_extra_args["ecmean"]="${atm_oce_extra_args["ecmean"]} \
--interface ${aqua}/diagnostics/ecmean/config/interface_AQUA_destine-v1.yml"
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
# --interface (custom interface file)
# Concatenate the new part to the existing content
atm_extra_args["teleconnections"]="${atm_extra_args["teleconnections"]} \
--config cli_config_atm.yaml --ref"
oce_extra_args["teleconnections"]="${oce_extra_args["teleconnections"]} \
--config cli_config_oce.yaml --ref"
# Concatenate the new part to the existing content
atm_extra_args["tropical_rainfall"]="--regrid=r100 --freq=M --xmax=75"
# End of user defined variables
# ---------------------------------------------------
# Command line extra arguments for seasonal_cycles:
# It's still under time_series folder
# --config (seasonal cycles config file)
atm_extra_args["seasonal_cycles"]="${atm_extra_args["seasonal_cycles"]} \
--config ${aqua}/diagnostics/global_time_series/cli/config_seasonal_cycles_atm.yaml"
# -----------------------------
# Command line extra arguments for ocean3d:
#
oce_extra_args["ocean3d_circulation"]="${atm_extra_args["ocean3d_circulation"]} \
--config ${aqua}/diagnostics/ocean3d/cli/config.circulation.yaml"
oce_extra_args["ocean3d_drift"]="${atm_extra_args["ocean3d_drift"]} \
--config ${aqua}/diagnostics/ocean3d/cli/config.drift.yaml"

# -----------------------------
# Command line extra arguments for seaice:
#
oce_extra_args["seaice_extent"]="${atm_extra_args["seaice_extent"]} \
--config ${aqua}/diagnostics/seaice/cli/config_Extent.yaml"
oce_extra_args["seaice_conc"]="${atm_extra_args["seaice_conc"]} \
--config ${aqua}/diagnostics/seaice/cli/config_Concentration.yaml"
oce_extra_args["seaice_volume"]="${atm_extra_args["seaice_volume"]} \
--config ${aqua}/diagnostics/seaice/cli/config_Volume.yaml"
oce_extra_args["seaice_thick"]="${atm_extra_args["seaice_thick"]} \
--config ${aqua}/diagnostics/seaice/cli/config_Thickness.yaml"

# Trap Ctrl-C to clean up and kill the entire process group
trap 'kill 0' SIGINT

# Declare the path and cli name if it is not standard, i.e., not AQUA/diagnostics/dummy/cli/cli_dummy.py.
declare -A script_path

# Iterate over all diagnostics and set the default script path
for diagnostic in ${all_diagnostics[@]}; do
  script_path["$diagnostic"]="$diagnostic/cli/cli_$diagnostic.py"
done

# Set the path if it is not standard
script_path["seasonal_cycles"]="global_time_series/cli/cli_global_time_series.py"
script_path["ocean3d_drift"]="ocean3d/cli/cli_ocean3d.py"
script_path["ocean3d_circulation"]="ocean3d/cli/cli_ocean3d.py"
script_path["seaice_volume"]="seaice/cli/cli_seaice.py"
script_path["seaice_thick"]="seaice/cli/cli_seaice.py"
script_path["seaice_conc"]="seaice/cli/cli_seaice.py"
script_path["seaice_extent"]="seaice/cli/cli_seaice.py"

# Command line arguments
# Define accepted log levels
accepted_loglevels=("info" "debug" "error" "warning" "critical" "INFO" "DEBUG" "ERROR" "WARNING" "CRITICAL")
distributed=0

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
    -p|--parallel)
      distributed=1
      shift 1
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
        log_message ERROR "Invalid log level. Accepted values are: ${accepted_loglevels[@]}"
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

log_message INFO "Setting loglevel to $loglevel"
log_message INFO "Atmospheric model: $model_atm"
log_message INFO "Oceanic model: $model_oce"
log_message INFO "Experiment: $exp"
log_message INFO "Source: $source"
log_message INFO "Machine: $machine"
log_message INFO "Output directory: $outputdir"

# Set extra arguments in distributed case
if [ $distributed -eq 1 ]; then
  log_message INFO "Running with distributed cluster"
  atm_extra_args["atmglobalmean"]="${atm_extra_args['atmglobalmean']} --nworkers 16"
  atm_extra_args["global_time_series"]="${atm_extra_args['global_time_series']} --nworkers 16"
  atm_extra_args["seasonal_cycles"]="${atm_extra_args['seasonal_cycles']} --nworkers 16"
  atm_extra_args["radiation"]="${atm_extra_args['radiation']} --nworkers 8"
  atm_extra_args["teleconnections"]="${atm_extra_args['teleconnections']} --nworkers 8"
  atm_extra_args["tropical_rainfall"]="${atm_extra_args['tropical_rainfall']} --nworkers 16"
  oce_extra_args["global_time_series"]="${oce_extra_args['global_time_series']} --nworkers 8"
  oce_extra_args["ocean3d_drift"]="${oce_extra_args['ocean3d_drift']} --nworkers 8"
  oce_extra_args["ocean3d_circulation"]="${oce_extra_args['ocean3d_circulation']} --nworkers 4"
  oce_extra_args["seaice_extent"]="${oce_extra_args['seaice_extent']} --nworkers 4"
  oce_extra_args["seaice_conc"]="${oce_extra_args['seaice_conc']} --nworkers 4"
  oce_extra_args["seaice_volume"]="${oce_extra_args['seaice_volume']} --nworkers 4"
  oce_extra_args["seaice_thick"]="${oce_extra_args['seaice_thick']} --nworkers 4"
  oce_extra_args["teleconnections"]="${oce_extra_args['teleconnections']} --nworkers 4"
  atm_oce_extra_args["ecmean"]="${atm_oce_extra_args['ecmean']} --nworkers 4"
fi

# Define the outputdir for ocanic and atmospheric diagnostics
outputdir_atm="$outputdir/$model_atm/$exp"
outputdir_oce="$outputdir/$model_oce/$exp"

# Define the arguments for the diagnostics
args_atm="--model $model_atm --exp $exp --source $source"
args_oce="--model $model_oce --exp $exp --source $source"
args="--model_atm $model_atm --model_oce $model_oce --exp $exp --source $source"

# set the correct machine in the config file
if [[ "$OSTYPE" == "darwin"* ]]; then
  # Mac OSX
  sed -i '' "/^machine:/c\\
machine: $machine" "${aqua}/config/config-aqua.yaml"
else
  # Linux
  sed -i "/^machine:/c\\machine: $machine" "${aqua}/config/config-aqua.yaml"
fi
log_message INFO "Machine set to $machine in the config file"

# Create output directory if it does not exist
log_message INFO "Creating output directory $outputdir"
mkdir -p "$outputdir_atm"
mkdir -p "$outputdir_oce"

atm_extra_args["tropical_rainfall"]="${atm_extra_args["tropical_rainfall"]} \
--bufferdir=${outputdir_atm}/tropical_rainfall/"

cd $AQUA
if [ "$run_dummy" = true ] ; then
  log_message INFO "Running setup checker"
  scriptpy="${aqua}/diagnostics/dummy/cli/cli_dummy.py"
  python $scriptpy $args -l $loglevel > "$outputdir_atm/setup_checker.log" 2>&1
  
  # Store the error code of the dummy script
  dummy_error=$?

  # exit if dummy fails in finding both atmospheric and oceanic model
  if [ $dummy_error -ne 0 ]; then
    if [ $dummy_error -eq 1 ]; then # if error code is 1, then the setup checker failed
      log_message CRITICAL "Setup checker failed, exiting"
      exit 1
    fi
    # if error code is 2 or 3, then the setup checker
    # passed but there are some warnings
    if [ $dummy_error -eq 2 ]; then
      log_message ERROR "Atmospheric model is not found, it will be skipped"
    fi
    if [ $dummy_error -eq 3 ]; then
      log_message ERROR "Oceanic model is not found, it will be skipped"
    fi
  fi
  log_message INFO "Finished setup checker"
  # copy the setup checker log to the oceanic output directory to be available for the oceanic diagnostics
  cp "$outputdir_atm/setup_checker.log" "$outputdir_oce/setup_checker.log"
fi

thread_count=0
# Run diagnostics in parallel
for diagnostic in "${all_diagnostics[@]}"; do
  log_message INFO "Running $diagnostic"

  if [[ "${atm_diagnostics[@]}" =~ "$diagnostic" ]]; then
    log_message DEBUG "Atmospheric diagnostic: $diagnostic"
    log_message DEBUG "Script path: ${script_path[$diagnostic]}"
    log_message DEBUG "Arguments: $args_atm ${atm_extra_args[$diagnostic]} -l $loglevel --outputdir $outputdir_atm/$diagnostic"
  elif [[ "${oce_diagnostics[@]}" =~ "$diagnostic" ]]; then
    log_message DEBUG "Oceanic diagnostic: $diagnostic"
    log_message DEBUG "Script path: ${script_path[$diagnostic]}"
    log_message DEBUG "Arguments: $args_oce ${oce_extra_args[$diagnostic]} -l $loglevel --outputdir $outputdir_oce/$diagnostic"
  elif [[ "${atm_oce_diagnostics[@]}" =~ "$diagnostic" ]]; then
    log_message DEBUG "Atmospheric and oceanic diagnostic: $diagnostic"
    log_message DEBUG "Script path: ${script_path[$diagnostic]}"
    log_message DEBUG "Arguments: $args ${atm_oce_extra_args[$diagnostic]} -l $loglevel --outputdir $outputdir_atm/$diagnostic"
  fi

  if [[ "${atm_diagnostics[@]}" =~ "$diagnostic" ]]; then
    python "${aqua}/diagnostics/${script_path[$diagnostic]}" $args_atm ${atm_extra_args[$diagnostic]} \
    -l $loglevel --outputdir $outputdir_atm/$diagnostic > "$outputdir_atm/atm_$diagnostic.log" 2>&1 &
    # Remove diagnostic from atm_diagnostics array
    atm_diagnostics=(${atm_diagnostics[@]/$diagnostic})
  elif [[ "${oce_diagnostics[@]}" =~ "$diagnostic" ]]; then
    python "${aqua}/diagnostics/${script_path[$diagnostic]}" $args_oce ${oce_extra_args[$diagnostic]} \
    -l $loglevel --outputdir $outputdir_oce/$diagnostic > "$outputdir_oce/oce_$diagnostic.log" 2>&1 &
    # Remove diagnostic from oce_diagnostics array
    oce_diagnostics=(${oce_diagnostics[@]/$diagnostic})
  elif [[ "${atm_oce_diagnostics[@]}" =~ "$diagnostic" ]]; then
    # NOTE: atm_oce diagnostics are run in the atmospheric output directory
    python "${aqua}/diagnostics/${script_path[$diagnostic]}" $args ${atm_oce_extra_args[$diagnostic]} \
    -l $loglevel --outputdir $outputdir_atm/$diagnostic > "$outputdir_atm/$diagnostic.log" 2>&1 &
    # Remove diagnostic from atm_oce_diagnostics array
    atm_oce_diagnostics=(${atm_oce_diagnostics[@]/$diagnostic})
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
log_message INFO "Finished all diagnostics"
