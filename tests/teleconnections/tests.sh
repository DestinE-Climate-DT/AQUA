#!/bin/bash

file_url="https://swift.dkrz.de/v1/dkrz_a973e394-5f24-4f4d-8bbf-1a83bd387ccb/AQUA/teleconnections/AQUA_tests_teleconnections.tar.gz?temp_url_sig=2d7f5264256c8164241974015c24d094ffbf5a38&temp_url_expires=2023-04-16T11:06:03Z"
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

# cp ./config/config.yaml ./config/config.yaml.bak
# sed -i "/^machine:/c\\machine: ci" "./config/config.yaml"
# python -m pytest ./tests/teleconnections/test_teleconnections.py
# mv ./config/config.yaml.bak ./config/config.yaml