#!/bin/bash

file_url="https://swift.dkrz.de/v1/dkrz_a973e394-5f24-4f4d-8bbf-1a83bd387ccb/AQUA/framework/AQUA_testsv4.tar.gz?temp_url_sig=0edd32127555f6c9ff3e0eb750746d622d1d8d4f&temp_url_expires=2025-01-06T11:35:45Z"
file_path="AQUA_testsv4.tar.gz"

if [ ! -f "$file_path" ]; then
    echo "Downloading file..."
    curl -o "$file_path" "$file_url"
    echo "File downloaded."
    echo "Extracting file..."
    tar -xzf "$file_path"
else
    echo "File already exists."
fi

# if [ -f "./config/config-aqua.yaml" ] ; then
#     cp ./config/config-aqua.yaml ./config/config-aqua.yaml.bak
# else
#     cp ./config/config-aqua.tmpl ./config/config-aqua.yaml
# fi

# if [[ "$OSTYPE" == "darwin"* ]]; then
#   # Mac OSX
#   sed -i '' "/^catalog:/c\\
# catalog: ci" "./config/config-aqua.yaml"
# else
#   # Linux
#   sed -i "/^catalog:/c\\catalog: ci" "./config/config-aqua.yaml"
# fi

#python -m pytest ./tests/test_basic.py
#mv ./config/config.yaml.bak ./config/config.yaml
