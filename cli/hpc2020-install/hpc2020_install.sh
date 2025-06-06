#!/bin/bash

# Install AQUA framework and diagnostics.

# Usage
# bash hpc2020_install.sh 
# or
# AQUA=/path/to/aqua bash hpc2020_install.sh 

set -e

# Check if AQUA is set and the file exists
if [[ -z "$AQUA" ]]; then
    echo -e "\033[0;31mWarning: The AQUA environment variable is not defined."
    echo -e "\033[0;31mWarning: We are assuming AQUA is installed in your HPCPERM, i.e. $HPCPERM/AQUA"
    echo -e "\x1b[38;2;255;165;0mAlternatively, define the AQUA environment variable with the path to your 'AQUA' directory."
    echo -e "For example: export AQUA=/path/to/aqua\033[0m"
    AQUA=$HPCPERM/AQUA
fi

if [[ ! -d  $AQUA ]] ; then
    echo -e "\033[0;31mWarning: I cannot find AQUA, exiting..."
    exit 1  # Exit with status 1 to indicate an error
else
    source "$AQUA/cli/util/logger.sh"
    log_message INFO "AQUA found in $AQUA"
    log_message INFO "Sourcing logger.sh from: $AQUA/cli/util/logger.sh"
fi

setup_log_level 2 # 1=DEBUG, 2=INFO, 3=WARNING, 4=ERROR, 5=CRITICAL

log_message WARNING "Installation on a login node may fail. It is better to ask for adequate resources using for example"
log_message WARNING "ecinteractive -c 8 -m 20 -s 30"
log_message WARNING "NOTE: If this is the first time that you use ecinteractive, remember to run the command 'ssh-key-setup' first (this needs to be done only once)."

#####################################################################
# Begin of user input

# define installation path
export INSTALLATION_PATH="$HPCPERM/tykky/aqua"

# End of user input
#####################################################################
log_message INFO "Installation path has been set to ${INSTALLATION_PATH}"

install_aqua() {

  # load modules. tykky substitutes all other modules, so that a reset is not needed
  module load tykky
  log_message INFO "tykky module has been loaded."
  
  # Fix environment.yml
  SCRIPTDIR="${AQUA}/cli/hpc2020-install"
  sed 's/- imagemagick/# - imagemagick/' ../../environment.yml >$SCRIPTDIR/environment_hpc2020.yml  # imagemagick has a buggy dependency, it needs to be installed separately 
  sed -i.bak "s;- -e .;- -e $AQUA;" $SCRIPTDIR/environment_hpc2020.yml  # replace relative paths with $AQUA
  
  # update.sh is needed to fix the imagemagick bug
  echo "#!/bin/bash" > $SCRIPTDIR/update.sh
  echo "conda install -y -c conda-forge imagemagick" >> $SCRIPTDIR/update.sh

  # install AQUA framework and diagnostics
  conda-containerize new --post-install $SCRIPTDIR/update.sh --prefix "${INSTALLATION_PATH}" $SCRIPTDIR/environment_hpc2020.yml

  rm $SCRIPTDIR/environment_hpc2020.yml $SCRIPTDIR/environment_hpc2020.yml.bak $SCRIPTDIR/update.sh 
  log_message INFO "AQUA framework and diagnostics have been installed."
}

# if INSTALLATION_PATH does not exist, create it
if [[ ! -d "${INSTALLATION_PATH}" ]]; then
  mkdir -p "${INSTALLATION_PATH}"
  log_message INFO "Installation path ${INSTALLATION_PATH} has been created."
else
  log_message INFO "Installation path ${INSTALLATION_PATH} already exists."
fi

# if INSTALLATION_PATH is empty, install AQUA
if [[ -z "$(ls -A ${INSTALLATION_PATH})" ]]; then
  log_message INFO "Installing AQUA..."
  # install AQUA
  install_aqua
else
  log_message INFO "AQUA is already installed."
  # check if reinstallation is wanted

  log_message $next_level_msg_type "Do you want to reinstall AQUA? (y/n) "
  # Read the user's input
  read -n 1 -r
  echo

  if [[ $REPLY =~ ^[Yy]$ ]]
  then
    # run code to reinstall AQUA
    log_message INFO "Removing AQUA..."
    log_message $next_level_msg_type "Are you sure you want to delete ${INSTALLATION_PATH}? (y/n) "
    # Read the user's input
    read -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]
    then
      rm -rf "${INSTALLATION_PATH}"
      mkdir -p "${INSTALLATION_PATH}"
    else
      log_message CRITICAL "Deletion cancelled."
      exit 1
    fi
    log_message INFO "Installing AQUA..."
    install_aqua
  else
    log_message ERROR "AQUA will not be reinstalled."
  fi
fi

# ask if you want to add tykky to the bash profile
log_message $next_level_msg_type "On HPC2020 you can use AQUA either by using the tykky module (tykky activate aqua) or by adding AQUA to your PATH."
log_message $next_level_msg_type "Please note that adding AQUA to your PATH will make you use the aqua environment for all activities on HPC2020, so the first option is highly recommended."

# ask if you want to make the container the default
while true; do
  log_message $next_level_msg_type "Would you add AQUA to your PATH in .bash_profile (not recommended)? (y/n) "
  # Read the user's input
  read -n 1 -r
  echo
  case $REPLY in
    [Yy])
      if ! grep -q 'export PATH="'$INSTALLATION_PATH'/bin:$PATH"' ~/.bash_profile; then
        echo 'export PATH="'$INSTALLATION_PATH'/bin:$PATH"' >>  ~/.bash_profile
        log_message INFO 'AQUA addaed to your PATH in your .bash_profile.'
      else
        log_message WARNING 'AQUA is already in PATH in your bash profile, not adding it again!'
      fi
      break
      ;;
    [Nn])
      log_message WARNING "AQUA not added to PATH in .bash_profile"
      log_message $next_level_msg_type "You can use AQUA by loading the tykky module 'module load tykky' and then activating the environment 'tykky activate aqua'"
      break
      ;;
    *)
      log_message ERROR "Invalid response. Please enter 'y' or 'n'."
      ;;
  esac
done

log_message WARNING "AQUA environment has been installed, please remember to run 'aqua install hpc2020' and 'aqua add <your-catalogue-name> -e <path/to/your/catalogue>'"
log_message WARNING "hpc2020 is the name of this machine in AQUA syntax. You will need to provide a catalogue for experiments on hpc2020 in order to use the diagnostics."
