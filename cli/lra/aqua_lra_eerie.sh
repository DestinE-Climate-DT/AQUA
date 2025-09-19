#!/bin/bash
#SBATCH --job-name=LRA_eerie
#SBATCH --partition=compute
#SBATCH --account=ab0995
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=128
#SBATCH --time=08:00:00
#SBATCH --error=my_job.%j.err
#SBATCH --output=my_job.%j.out


source ~/.bashrc

ulimit -n 65535

conda activate /work/ab0995/a270260/aqua


aqua lra -l info -c LRA_levante_eerie.yaml -d

