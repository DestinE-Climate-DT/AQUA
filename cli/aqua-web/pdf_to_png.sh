#!/bin/bash
#
# Simple script to convert pdf to png
# Usage: ./pdf_to_png.sh [catalog] [model] [experiment] [realization]
# Realization is optional, if not given a directory structure with only three levels will be assumed.

cd content
mkdir -p png
cd pdf

d0=${1:-*}. # Default to all directories
d1=${2:-*}
d2=${3:-*}
d3=$4  # ok if empty

# If no argument given assume we are using ensembles
if [ -z "$d3" ] && [ "$d0" = "*" ] && [ "$d1" = "*" ] && [ "$d2" = "*" ]; then
  d3="*"
fi

for dir0 in $d0
do
  cd $dir0
  for dir1 in $d1
  do
    cd $dir1
    for dir2 in $d2
    do
      cd $dir2
      if [ -z "$d3" ]; then
        dest="../../../../png/${dir0}/${dir1}/${dir2}"
        mkdir -p $dest
        echo "Performing pdf to png conversion in $dest"
	      magick mogrify -verbose -format png -density 100 -trim -path $dest *.pdf
      else
        for dir3 in $d3
        do
          cd $dir3
          dest="../../../../png/${dir0}/${dir1}/${dir2}/${dir3}"
          mkdir -p $dest
          echo "Performing pdf to png conversion in $dest"
	        magick mogrify -verbose -format png -density 100 -trim -path $dest *.pdf
        done
        cd ..
      fi
      cd ..
    done
    cd ..
  done
  cd ..
done
