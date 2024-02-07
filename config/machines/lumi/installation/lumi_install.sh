#!/bin/bash

# Install AQUA framework and diagnostics.

# Usage
# bash lumi_install.sh 

set -e

#####################################################################
# Begin of user input
machine=lumi 
configfile=config-aqua.yaml
user=$USER # change this to your username if automatic detection fails
MAMBADIR="$HOME/mambaforge" #check if $HOME does not exist
load_aqua_file="$HOME/load_aqua.sh" #check if $HOME does not exist
# End of user input
#####################################################################

# define AQUA path
if [[ -z "${AQUA}" ]]; then
  #export AQUA="/users/${user}/AQUA"
  export AQUA=$(realpath $(dirname "$0")"/../../../..")
  echo "AQUA path has been set to ${AQUA}"
else
  echo "AQUA path is already defined as ${AQUA}"
fi

# define installation path
export INSTALLATION_PATH="$MAMBADIR/aqua_common"
echo "Installation path has been set to ${INSTALLATION_PATH}"

# Remove the installation path from the $PATH. 
# This is AI-based block which creates a new $PATH removing path including 'aqua'

# Word to check and remove from $PATH
word_to_remove="aqua"

# Function to check if a path contains the specified word
contains_word() {
  [[ "$1" == *"$word_to_remove"* ]]
}

# Split the $PATH into individual components using ":" as the separator
IFS=":" read -ra path_components <<< "$PATH"

# Create a new array to store the modified path components
new_path_components=()

# Loop through each path component and check if it contains the specified word
for component in "${path_components[@]}"; do
  if ! contains_word "$component"; then
    # If the component does not contain the word, add it to the new array
    new_path_components+=("$component")
  fi
done

# Join the new array back into a single string with ":" as the separator
new_path=$(IFS=":"; echo "${new_path_components[*]}")

# Update the $PATH variable with the new value
export PATH="$new_path"

echo "Paths containing '$word_to_remove' have been removed from \$PATH."

#####################################################################

# change machine name in config file
sed -i "/^machine:/c\\machine: ${machine}" "${AQUA}/config/$configfile"
echo "Machine name in config file has been set to ${machine}"

sed -i "/^  lumi:/c\\  lumi: ${INSTALLATION_PATH}/bin/cdo" "${AQUA}/config/$configfile"
echo "CDO in config file now points to ${INSTALLATION_PATH}/bin/cdo"

install_aqua() {
  # clean up environment
  module --force purge
  echo "Environment has been cleaned up."

  # load modules
  module load LUMI/22.08
  module load lumi-container-wrapper
  echo "Modules have been loaded."

  
  # install AQUA framework and diagnostics
  conda-containerize new --mamba --prefix "${INSTALLATION_PATH}" "${AQUA}/config/machines/lumi/installation/environment_lumi_common.yml"
  conda-containerize update "${INSTALLATION_PATH}" --post-install "${AQUA}/config/machines/lumi/installation/pip_lumi_common.txt"
  echo "AQUA framework and diagnostics have been installed."
}

# if INSTALLATION_PATH does not exist, create it
if [[ ! -d "${INSTALLATION_PATH}" ]]; then
  mkdir -p "${INSTALLATION_PATH}"
  echo "Installation path ${INSTALLATION_PATH} has been created."
else
  echo "Installation path ${INSTALLATION_PATH} already exists."
fi

# if INSTALLATION_PATH is empty, install AQUA
if [[ -z "$(ls -A ${INSTALLATION_PATH})" ]]; then
  echo "Installing AQUA..."
  # install AQUA
  install_aqua
else
  echo "AQUA is already installed."
  # check if reinstallation is wanted
  read -p "Do you want to reinstall AQUA? (y/n) " -n 1 -r
  echo # move to a new line
  if [[ $REPLY =~ ^[Yy]$ ]]
  then
    # run code to reinstall AQUA
    echo "Removing AQUA..."
    read -p "Are you sure you want to delete ${INSTALLATION_PATH}? (y/n) " -n 1 -r
    echo # move to a new line
    if [[ $REPLY =~ ^[Yy]$ ]]
    then
      rm -rf "${INSTALLATION_PATH}"
      mkdir -p "${INSTALLATION_PATH}"
    else
      echo "Deletion cancelled."
      exit 1
    fi
    echo "Installing AQUA..."
    install_aqua
  else
    echo "AQUA will not be reinstalled."
  fi
fi

# check if load_aqua_file exist and clean it
if [ -f "$load_aqua_file" ]; then
  read -p "Existing ${load_aqua_file} found. Would you like to remove it? Safer to say yes (y/n) " -n 1 -r
  if [[ $REPLY =~ ^[Yy]$ ]]; then
    rm $load_aqua_file
    echo "Existing ${load_aqua_file} removed."
  elif [[ $REPLY =~ ^[Nn]$ ]]; then
    echo "Keeping the old $load_aqua_file"
  else
    echo "Invalid response. Please enter 'y' or 'n'."
  fi
fi

if ! grep -q 'module use /project/project_465000454/devaraju/modules/LUMI/23.03/C'  "~/load_aqua.sh" ; then
#if [ ! -f $load_aqua_file ] ; then
  echo '# Use ClimateDT paths' >> $load_aqua_file
  echo 'module use /project/project_465000454/devaraju/modules/LUMI/23.03/C' >> $load_aqua_file

  echo '# Load modules' >> $load_aqua_file
  echo 'module purge' >> $load_aqua_file
  echo 'module load ecCodes/2.33.0-cpeCray-23.03' >> $load_aqua_file
  echo 'module load fdb/5.11.94-cpeCray-23.03' >> $load_aqua_file
  echo 'module load eckit/1.25.0-cpeCray-23.03' >> $load_aqua_file
  echo 'module load metkit/1.11.0-cpeCray-23.03' >> $load_aqua_file
    
  # Config FDB: check load_modules_lumi.sh on GSV repo https://earth.bsc.es/gitlab/digital-twins/de_340/gsv_interface/-/blob/main/load_modules_lumi.sh
  echo 'export FDB5_CONFIG_FILE=/scratch/project_465000454/igonzalez/fdb-test/config.yaml' >>  $load_aqua_file
  echo "exports for FDB5 added to .bashrc. Please run 'source ~/.bashrc' to load the new configuration."

  # Config GSV: check load_modules_lumi.sh on GSV repo https://earth.bsc.es/gitlab/digital-twins/de_340/gsv_interface/-/blob/main/load_modules_lumi.sh
  echo 'export GSV_WEIGHTS_PATH=/scratch/project_465000454/igonzalez/gsv_weights' >>  $load_aqua_file
  echo 'export GSV_TEST_FILES=/scratch/project_465000454/igonzalez/gsv_test_files' >> $load_aqua_file
  echo 'export GRID_DEFINITION_PATH=/scratch/project_465000454/igonzalez/grid_definitions' >>  $load_aqua_file
  echo "export for GSV has been added to .bashrc. Please run 'source  $load_aqua_file' to load the new configuration."

  # Install path
  echo "# AQUA installation path" >>  $load_aqua_file
  echo 'export PATH="'$INSTALLATION_PATH'/bin:$PATH"' >>  $load_aqua_file
  echo "export PATH has been added to .bashrc. Please run 'source $load_aqua_file' to load the new configuration."
else
  echo "A $(basename $load_aqua_file) is already available in your home. Nothing to add!"
fi

# ask if you want to add this to the bash profile
read -p "Would you like to source $load_aqua_file in your .bash_profile? (y/n) " -n 1 -r
echo
# ask if you want to add this to the bash profile
while true; do
  read -p "Would you like to source $load_aqua_file in your .bash_profile? (y/n) " -n 1 -r
  echo
  case $REPLY in
    [Yy])
      if ! grep -q "source  $load_aqua_file" ~/.bash_profile; then
        echo "source  $load_aqua_file" >> ~/.bash_profile
        echo 'load_aqua.sh added to your .bash_profile.'
      else
        echo 'load_aqua.sh is already in your bash profile, not adding it again!'
      fi
      break
      ;;
    [Nn])
      echo "source load_aqua.sh not added to .bash_profile"
      break
      ;;
    *)
      echo "Invalid response. Please enter 'y' or 'n'."
      ;;
  esac
done
