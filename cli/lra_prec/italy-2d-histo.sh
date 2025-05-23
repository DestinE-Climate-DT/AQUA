#!/bin/bash
#SBATCH --partition=small
#SBATCH --job-name=2d-italy-histo
#SBATCH --output=2d_italy_histo_%j.out
#SBATCH --error=2d_italy_histo_%j.err
#SBATCH --account=project_462000911
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=12
#SBATCH --time=02:00:00
#SBATCH --mem=64G 
set -e

vars=(tcwv 2t tprate)
catalog=climatedt-phase1
model=IFS-FESOM
exp=story-2017-historical
source=hourly-hpz9-atm2d
freq=daily
regrid=r005

for var in "${vars[@]}"; do
    python lra_prec_italy.py --var $var --catalog $catalog --model $model --exp $exp --source $source --freq $freq --regrid $regrid
done
