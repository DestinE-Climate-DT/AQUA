#!/bin/bash


#conda install  conda-merge 

conda-merge ../../environment.yml  enviroment/env-tropical-rainfall.yml >> enviroment/merged.yml

conda env create -f enviroment/merged.yml
