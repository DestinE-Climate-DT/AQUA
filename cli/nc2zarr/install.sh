module load LUMI
module load lumi-container-wrapper
OUTDIR=/pfs/lustrep3/projappl/project_465000454/jvonhar/containers/nc2zarr
mkdir -p $OUTDIR
conda-containerize new --prefix $OUTDIR env.yml

