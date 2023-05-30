#!/usr/bin/env python3
"""Loop on multiple dataset to crete weights using the Reader"""

from aqua import Reader

resos=['r025', 'r100']
model=['ICON']
exp=['ngc3026', 'ngc3028']
source=['P1D']


for reso in resos:
    for m in model:
        for e in exp:
            for s in source:
                for zoom in range(0, 9):
                    reader = Reader(model=m, exp=e, source=s, regrid=reso, zoom=zoom)
