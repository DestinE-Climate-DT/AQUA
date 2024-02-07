#!/bin/bash

# enter container
#bash /pfs/lustrep3/scratch/project_465000454/kkeller/lab/aqua_container/AQUA/config/machines/lumi/container/load_aqua_lumi.sh

model_atm="IFS"
model_oce="NEMO"
exp="a0er-LUMI-C"
outputdir=/pfs/lustrep3/scratch/project_465000454/kkeller/lab/aqua_container/replicability/out/$exp #"$AQUA/../aqua_output"
mkdir -p $outputdir
loglevel="WARNING" # DEBUG, INFO, WARNING, ERROR, CRITICAL
machine="lumi" # will change the aqua config file

# AQUA path, can be defined as $AQUA env variable
# if not defined it will use the aqua path in the script
aqua=$AQUA

for member in fc{0..9}; do
    source="${member}-lra-r100-monthly"
    
    python $aqua/diagnostics/ecmean/cli/ecmean_cli.py \
        --model_atm $model_atm \
        --model_oce $model_oce \
        --exp $exp \
        --source $source \
        -o $outputdir \
        -l $loglevel

    mv ${outputdir}/YAML/PI4_EC23_${exp}_AQUA_r1i1p1f1_1990_2020.yml  ${outputdir}/YAML/${member}.yml
    mv ${outputdir}/PDF/PI4_EC23_${exp}_AQUA_r1i1p1f1_1990_2020.pdf  ${outputdir}/PDF/${member}.pdf
done





