#!/bin/bash

#####################################################################
# Begin of user input
machine=lumi 
user=$USER # change this to your username

# define AQUA path
if [[ -z "${AQUA}" ]]; then
  #export AQUA="/users/${user}/AQUA"
  export AQUA=$(realpath $(dirname "$0")"/../../../..")
  echo "AQUA path has been set to ${AQUA}"
else
  echo "AQUA path is already defined as ${AQUA}"
fi

# define installation path
export INSTALLATION_PATH="/users/${user}/mambaforge/aqua"
echo "Installation path has been set to ${INSTALLATION_PATH}"

# End of user input
#####################################################################

# change machine name in config file
sed -i "/^machine:/c\\machine: ${machine}" "${AQUA}/config/config.yaml"
echo "Machine name in config file has been set to ${machine}"

sed -i "/^  lumi:/c\\  lumi: ${INSTALLATION_PATH}/bin/cdo" "${AQUA}/config/config.yaml"
echo "CDO in config file now points to ${INSTALLATION_PATH}/bin/cdo"

install_aqua() {
  # clean up environment
  module --force purge
  echo "Environment has been cleaned up."

  # load modules
  module load LUMI/22.08
  module load lumi-container-wrapper
  echo "Modules have been loaded."

  conda-containerize new --mamba --prefix "${INSTALLATION_PATH}" "${AQUA}/config/machines/lumi/installation/environment_lumi.yml"
  
  conda-containerize update "${INSTALLATION_PATH}" --post-install "${AQUA}/config/machines/lumi/installation/pip_lumi.txt"
  echo "AQUA has been installed."
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
    else
      echo "Deletion cancelled."
      return 1
    fi
    echo "Installing AQUA..."
    install_aqua
  else
    echo "AQUA will not be reinstalled."
  fi
fi

# check if the line is already present in the load_aqua.sh file
if ! grep -q 'module use /project/project_465000454/devaraju/modules/LUMI/22.08/C' ~/load_aqua.sh; then
  # if not, append it to the end of the file
  echo 'module use /project/project_465000454/devaraju/modules/LUMI/22.08/C' >> ~/load_aqua.sh
  echo 'module purge' >> ~/load_aqua.sh
  echo 'module load pyfdb/0.0.2-cpeCray-22.08' >> ~/load_aqua.sh
  echo 'module load ecCodes/2.30.0-cpeCray-22.08' >> ~/load_aqua.sh
  echo 'module load python-climatedt/3.11.3-cpeCray-22.08.lua' >> ~/load_aqua.sh
  
  # Config FDB: check load_modules_lumi.sh on GSV repo https://earth.bsc.es/gitlab/digital-twins/de_340/gsv_interface/-/blob/main/load_modules_lumi.sh
  echo 'export FDB5_CONFIG_FILE=/scratch/project_465000454/igonzalez/fdb-test/config.yaml' >> ~/load_aqua.sh
  echo "exports for FDB5 added to .bashrc. Please run 'source ~/.bashrc' to load the new configuration."

  # Config GSV: check load_modules_lumi.sh on GSV repo https://earth.bsc.es/gitlab/digital-twins/de_340/gsv_interface/-/blob/main/load_modules_lumi.sh
  echo 'export GSV_WEIGHTS_PATH=/scratch/project_465000454/igonzalez/gsv_weights' >> ~/load_aqua.sh
  echo 'export GSV_TEST_FILES=/scratch/project_465000454/igonzalez/gsv_test_files' >> ~/load_aqua.sh
  echo 'export GRID_DEFINITION_PATH=/scratch/project_465000454/igonzalez/grid_definitions' >> ~/load_aqua.sh
  echo "export for GSV has been added to .bashrc. Please run 'source ~/load_aqua.sh' to load the new configuration."

  # Install path
  echo "# AQUA installation path" >> ~/load_aqua.sh
  echo 'export PATH="'$INSTALLATION_PATH'/bin:$PATH"' >> ~/load_aqua.sh
  echo "export PATH has been added to .bashrc. Please run 'source ~/load_aqua.sh' to load the new configuration."
else
  echo "A load_aqua.sh is already available in your home!"
fi

# ask if you want to add this to the bash profile
read -p "Would you like to source load_aqua.sh in your .bash_profile? " -n 1 -r
echo 
if [[ $REPLY =~ ^[Yy]$ ]]; then
  if ! grep -q 'source  ~/load_aqua.sh' ~/.bash_profile; then
    echo 'source  ~/load_aqua.sh' >> ~/.bash_profile
  else 
    echo 'load_aqua.sh is already in your bash profile, not adding it again!'
  fi
else
  echo "source load_aqua.sh not added to .bash_profile"
fi
 