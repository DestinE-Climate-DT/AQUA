#!/bin/bash

file_url="https://swift.dkrz.de/v1/dkrz_a973e394-5f24-4f4d-8bbf-1a83bd387ccb/AQUA/framework/AQUA_testsv6.tar.gz?temp_url_sig=38bfddb3ff0b9584db673f695167895896e6a2d6&temp_url_expires=2027-05-06T09:26:02Z"
file_path="AQUA_testsv6.tar.gz"
# file_url="https://swift.dkrz.de/v1/dkrz_a973e394-5f24-4f4d-8bbf-1a83bd387ccb/AQUA/framework/AQUA_testsv5.tar.gz?temp_url_sig=0738c078c9d994c3cee25ceae4f739a47844ee8d&temp_url_expires=2026-11-16T16:31:15Z"
# file_path="AQUA_testsv5.tar.gz"

if [ ! -f "$file_path" ]; then
    echo "Downloading file..."
    curl -o "$file_path" "$file_url"
    echo "File downloaded."
    echo "Extracting file..."
    tar -xzf "$file_path"
else
    echo "File already exists."
fi

# if [[ "$OSTYPE" == "darwin"* ]]; then
#   # Mac OSX
#   sed -i '' "/^catalog:/c\\
# catalog: ci" "./config/config-aqua.yaml"
# else
#   # Linux
#   sed -i "/^catalog:/c\\catalog: ci" "./config/config-aqua.yaml"
# fi

#python -m pytest ./tests/test_basic.py
