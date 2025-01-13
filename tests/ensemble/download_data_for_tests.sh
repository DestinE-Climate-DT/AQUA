#!/bin/bash

##TODO include the link once pushed to the server

file_url=""
file_path=""

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
