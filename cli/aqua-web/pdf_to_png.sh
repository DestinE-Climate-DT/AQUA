#!/bin/bash
#
# Simple script to convert pdf to png
# Usage: ./pdf_to_png.sh [catalog] [model] [experiment]

cd content
mkdir -p png
cd pdf

d0="*"
d1="*"
d2="*"

if [ "$1" != "" ]; then
    d0="$1"
fi

if [ "$2" != "" ]; then
    d1="$2"
fi

if [ "$3" != "" ]; then
    d2="$3"
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
        dest="../../../../png/${dir0}/${dir1}/${dir2}"
        mkdir -p $dest
        echo "Performing pdf to png conversion in $dest"
	magick mogrify -verbose -format png -density 100 -trim -path $dest *.pdf
        cd ..
    done
    cd ..
  done
  cd ..
done
