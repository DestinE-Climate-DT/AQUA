#!/bin/bash

diag_dir=$(pwd)

conda install  conda-merge 
cd ../..
conda-merge environment.yml  $diag_dir/enviroment/env-tropical-rainfall.yml > $diag_dir/enviroment/merged.yml

conda env create -f $diag_dir/enviroment/merged.yml

#conda activate tropical-rainfall_2
