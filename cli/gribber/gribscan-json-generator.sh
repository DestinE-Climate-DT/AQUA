# This script tries to create a json entry using gribscan to access 
# IFS files from catalog using a zarr interface

#!/bin/bash
#set -e

# find mamba/conda (to be refined)
#whereconda=$(which mamba | rev | cut -f 3-10 -d"/" | rev)
#source $whereconda/etc/profile.d/conda.sh
#module load Miniconda3
# activate AQUA environment
#conda activate /gpfs/projects/bsc32/bsc32655/aquaenv

# expid
expid=tco399-orca025-kai

# define folder and grib files
tmpdir=/gpfs/scratch/bsc32/bsc32655/scratch/gribscan/$expid
jsondir=/gpfs/scratch/bsc32/bsc32655/scratch/gribscan-json/$expid
datadir=/gpfs/scratch/bsc32/bsc32655/scratch/IFS-NEMO40-INSTALL/ifs-source/flexbuild/bin/SLURM/marenostrum4/rundir/tco79l137/hvi1/hres/intel.mn4.undef/mn4.intel.sp/h104712.N3.T24xt6xh1.nextgems_pl6h_sfc6h.i16r0w24.eORCA1_Z75.htco79-27833884/
gribfiles='ICMGG????+*'

# number of parallel procs
nprocs=16

# create folders
mkdir -p $jsondir $tmpdir

# create file links to avoid messing with original data
echo "Linking files..."
for file in $(ls $datadir/$gribfiles) ; do
    ln -sf $file $tmpdir/
done


# creating the indices
echo "Creating GRIB indices..."
cd $tmpdir
gribscan-index $tmpdir/$gribfiles -n $nprocs

# running the json creation
echo "Building JSON file..."
gribscan-build -o $jsondir --magician ifs --prefix ${datadir}/ *.index

# clean tmpdir
echo "Cleaning..."
rm $tmpdir/$gribfiles
#rm $tmpdir/*.index
rmdir $tmpdir

echo "Good job my friend, have yourself an icecream!"