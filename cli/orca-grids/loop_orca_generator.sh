#!/bin/bash
TGTDIR=/pfs/lustrep3/projappl/project_465000454/data/AQUA/grids/NEMO2
grid=$1


if [[ $grid == eORCA12 ]] ; then
	inputfile=/pfs/lustrep3/scratch/project_465000454/pool/data/IFS/inputs_de_48r1/nemo/V40/eORCA12_Z75/common/mesh_mask.nc
elif [[ $grid = eORCA1 ]] ; then
	inputfile=/pfs/lustrep4/users/padavini/mesh_mask.nc
else
	echo 'wrong grid'
	exit 1
fi

for stagg in T U V F ; do
	echo $stagg
	rm -f tmp.nc
	./orca_bounds_new.py $inputfile tmp.nc --stagg ${stagg} --unstructured
	cdo -f nc4 -z zip selname,dummy tmp.nc $TGTDIR/${grid}_mesh_sfc_grid_${stagg}.nc
done

for name in height level ; do
	echo "3d"
	stagg=T
	rm -f tmp.nc
	./orca_bounds_new.py $inputfile tmp.nc --stagg ${stagg} --unstructured --level --zname $name
	cdo -f nc4 -z zip selname,dummy tmp.nc $TGTDIR/${grid}_mesh_3d_grid_${stagg}_${name}.nc
done
rm tmp.nc
