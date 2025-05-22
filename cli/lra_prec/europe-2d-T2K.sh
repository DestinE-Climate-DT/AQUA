#!/bin/bash
#SBATCH --partition=small
#SBATCH --job-name=2d-europe-T2K
#SBATCH --output=2d_europe_T2K_%j.out
#SBATCH --error=2d_europe_T2K_%j.err
#SBATCH --account=project_462000911
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=12
#SBATCH --time=02:00:00
#SBATCH --mem=64G 
set -e

vars=(skt msl)
catalog=climatedt-phase1
model=IFS-FESOM
exp=story-2017-T2K
source=hourly-hpz9-atm2d
freq=daily
regrid=r050

for var in "${vars[@]}"; do
    python lra_prec_europe.py --var $var --catalog $catalog --model $model --exp $exp --source $source --freq $freq --regrid $regrid
done
