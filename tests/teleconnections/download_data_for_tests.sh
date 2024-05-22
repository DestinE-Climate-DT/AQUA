#!/bin/bash

file_url="https://swift.dkrz.de/v1/dkrz_a973e394-5f24-4f4d-8bbf-1a83bd387ccb/AQUA/teleconnections/AQUA_tests_teleconnectionsv3.tar.gz?temp_url_sig=01710bd3c8491923fb683e097c3b31f858cf8425&temp_url_expires=2027-02-02T15:42:23Z"
file_path="AQUA_tests_teleconnectionsv3.tar.gz"

if [ ! -f "$file_path" ]; then
    echo "Downloading file..."
    curl -o "$file_path" "$file_url"
    echo "File downloaded."
    echo "Extracting file..."
    tar -xzf "$file_path"
else
    echo "File already exists."
fi

cp ./config/config-aqua.yaml ./config/config-aqua.yaml.bak
sed -i "/^machine:/c\\machine: ci" "./config/config-aqua.yaml"