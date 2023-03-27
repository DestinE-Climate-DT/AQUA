#!/bin/bash

file_url="https://swift.dkrz.de/v1/dkrz_035d8f6ff058403bb42f8302e6badfbc/AQUA/AQUA_tests_short.tar.gz"
file_path="AQUA_tests_short.tar.gz"

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
#python -m pytest ./tests/test_basic.py
#mv ./config/config.yaml.bak ./config/config.yaml


