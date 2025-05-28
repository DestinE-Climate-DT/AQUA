#!/bin/bash
#SBATCH --partition=small
#SBATCH --job-name=3d-italy-T2K
#SBATCH --output=3d_italy_T2K_%j.out
#SBATCH --error=3d_italy_T2K_%j.err
#SBATCH --account=project_462000911
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=12
#SBATCH --time=02:00:00
#SBATCH --mem=200G 
set -e

var=w
levels=(70000) #50000)
catalog=climatedt-phase1
model=IFS-FESOM
exp=story-2017-T2K
source=hourly-hpz9-atm3d
freq=daily
regrid=r005

for lev in "${levels[@]}"; do
    python lra_prec_italy_3d.py --var $var --lev $lev --catalog $catalog --model $model --exp $exp --source $source --freq $freq --regrid $regrid
done
