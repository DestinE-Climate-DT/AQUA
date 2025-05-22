#!/bin/bash

OUTPUTDIR=/scratch/project_462000911/mnurisso/prec_italy_new_selection/climatedt-phase1/IFS-FESOM

exps=(story-2017-control story-2017-historical story-2017-T2K)

resolution=(r005 r050)

while true; do
    echo "--------------------------------"
    date
    for exp in "${exps[@]}"; do
        for res in "${resolution[@]}"; do
            # Evaluate the size of the output directory
            output_dir="$OUTPUTDIR/$exp/$res/daily/"
            if [ -d "$output_dir" ]; then
                size=$(du -sh "$output_dir" | cut -f1)
                echo 
                echo "Size of $exp $res: $size"
                echo "Last generated file:"
                ls -lt $output_dir | head -n 2 | tail -n 1
            fi
        done
    done
    echo "--------------------------------"
    sleep 60
done