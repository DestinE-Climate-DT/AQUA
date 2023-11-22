#!/usr/bin/env python3
"""Loop on multiple dataset to crete weights using the Reader"""

from aqua import Reader

resos = ['r025', 'r100']
model = ['FESOM']
exp = ['tco2559-ng5-cycle3']
source = ['3D_daily_native', '2D_daily_native', '2D_monthly_native_elem',
          '2D_monthly_native', '2D_1h_native']


for reso in resos:
    for m in model:
        for e in exp:
            for s in source:
                # If ICON is used, the zoom level is needed
                # please uncomment and loop on zoom
                # for zoom in range(0, 9):
                #    reader = Reader(model=m, exp=e, source=s, regrid=reso, zoom=zoom)
                reader = Reader(model=m, exp=e, source=s, regrid=reso)
