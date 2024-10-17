#!/bin/bash

# This script downloads the data needed for the tests.
file_url="https://link/to/your/file/"
file_path="filename.tar.gz"

# Download the file if it does not exist.
if [ ! -f "$file_path" ]; then
    echo "Downloading file..."
    curl -o "$file_path" "$file_url"
    echo "File downloaded."
    echo "Extracting file..."
    tar -xzf "$file_path"
else
    echo "File already exists."
fi

# Change the catalog name in the config file.
cp ./config/config.yaml ./config/config.yaml.bak
sed -i "/^catalog:/c\\catalog: ci" "./config/config.yaml"