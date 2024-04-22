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

# This script will create the avg_tos files from the avg_thetao files,
# selecting the first level of the temperature data.
# If you need, add here the module load cdo command.

model="ICON"
exp="historical-1990"

for variable in avg_sos avg_tos ; do


    LRA_folder="/projappl/project_465000454/data/AQUA/LRA"
    source_folder="${LRA_folder}/${model}/${exp}/r100/monthly"

    log_message INFO "Source folder: ${source_folder}"

    # moving to the source folder
    cd "${source_folder}"

    if [[ $variable == "avg_tos" ]] ; then
        varsource="avg_thetao"
    elif [[ $variable == "avg_sos" ]] ; then
        varsource="avg_so"
    fi

    log_message INFO "Creating ${variable} files from ${varsource} files for ${model} ${exp} LRA."

    for file in ${varsource}_*.nc ; do
        # Extract the filename without the extension
        filename=$(basename -- "$file")

        log_message DEBUG "Processing file: ${filename}"

        filename_without_extension="${filename%.nc}"

        # Ottenere gli ultimi 6 caratteri
        last_six_chars="${filename_without_extension: -6}"

        # Cut 'avg_thetao' from the beginning of each filename
        filename_novar="${filename#${varsource}}"

        # Add 'avg_tos' to the beginning of each filename
        tmp_filename="${variable}_tmp.nc"
        new_filename="${variable}${filename_novar}"

        # Verificare se gli ultimi 6 caratteri sono numeri
        if [[ $last_six_chars =~ ^[0-9]{6}$ ]]; then
            log_message WARNING "${new_filename} is a monthly file: remove it for safety"
            rm -f "${new_filename}"
        fi

        log_message DEBUG "New filename: ${new_filename}"

        if [[ ! -f $new_filename ]] ; then
            log_message INFO "Creating ${new_filename} file."
            cdo -setname,${variable} -sellevel,1 "$file" "${tmp_filename}"
            cdo -setzaxis,surface "${tmp_filename}" "${new_filename}"
            rm -rf "${tmp_filename}"
        fi


    done

    # Moving back to the original directory
    cd -

done