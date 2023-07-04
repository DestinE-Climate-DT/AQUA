#!/bin/bash

file_url="https://swift.dkrz.de/v1/dkrz_a973e394-5f24-4f4d-8bbf-1a83bd387ccb/AQUA/teleconnections/AQUA_tests_teleconnections.tar.gz?temp_url_sig=a471539e626e3fabde351f7b97cb14837dc4a59c&temp_url_expires=2023-10-30T08:06:34Z"
file_path="AQUA_tests_teleconnections.tar.gz"

if [ ! -f "$file_path" ]; then
    echo "Downloading file..."
    curl -o "$file_path" "$file_url"
    echo "File downloaded."
    echo "Extracting file..."
    tar -xzf "$file_path"
else
    echo "File already exists."
fi

cp ./config/config.yaml ./config/config.yaml.bak
sed -i "/^machine:/c\\machine: ci" "./config/config.yaml"