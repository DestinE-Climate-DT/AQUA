#!/bin/bash

file_url="https://swift.dkrz.de/v1/dkrz_a973e394-5f24-4f4d-8bbf-1a83bd387ccb/AQUA/framework/AQUA_tests.tar.gz?temp_url_sig=cbc57346e214b29bf44b094ee079eb12c8f4cb22&temp_url_expires=2025-07-10T09:59:23Z"
file_path="AQUA_tests.tar.gz"

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

if [[ "$OSTYPE" == "darwin"* ]]; then
  # Mac OSX
  sed -i '' "/^machine:/c\\
machine: ci" "./config/config-aqua.yaml"
else
  # Linux
  sed -i "/^machine:/c\\machine: ci" "./config/config-aqua.yaml"
fi

#python -m pytest ./tests/test_basic.py
#mv ./config/config.yaml.bak ./config/config.yaml


