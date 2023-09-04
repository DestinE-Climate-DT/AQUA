# import ptvsd
# import pdb

# # Enable the ptvsd debugger
# ptvsd.enable_attach(address=("0.0.0.0", 7001))
# ptvsd.wait_for_attach()

import sys
# sys.path.append("/users/sughosh/AQUA")
from aqua import Reader
import xarray as xr

from ecmean.global_mean import global_mean
from ecmean.performance_indices import performance_indices
from wand.image import Image as WImage # to open PDFs in jupyter

model_atm = 'IFS'
model_oce = 'FESOM'
exp = 'tco79-orca1'
year1 = 2021
year2 = 2022
numproc = 1
interface = '../config/interface_AQUA.yml'
config = '../config/ecmean_wf_config.yml'

clim=xr.open_mfdataset("/pfs/lustrep3/scratch/project_465000454/kkeller/AQUA/diagnostics/ecmean/ECmean4/ecmean/climatology/EC23/r360x180/*")
clim= clim.isel(time=0).squeeze().ta

# reader_atm2d = Reader(model="IFS", exp="tco79-eORCA2", source="atm2d", areas=False)
# data_atm2d = reader_atm2d.retrieve(fix=True)
# reader_atm3d = Reader(model="IFS", exp="tco79-eORCA2", source="atm3d", areas=False)
# data_atm3d = reader_atm3d.retrieve(fix=True)
data_path= "/scratch/project_465000454/tmp/a09x/"

# data_atm2d = xr.open_mfdataset(data_path+"/atm_2d.nc")
# data_atm3d = xr.open_mfdataset(data_path+"/atm_3d.nc")
# data = data_atm3d.merge(data_atm2d)

data = xr.open_mfdataset(data_path+"/*nc")

data["2t_mean"].attrs["units"]=data["2t_mean"].attrs["GRIB_units"]
# data["msl"].attrs["units"]=data["msl"].attrs["GRIB_units"]
# data["tprate"].attrs["units"]=data["tprate"].attrs["GRIB_units"]
# data["t"].attrs["units"]=data["t"].attrs["GRIB_units"]
# data["u"].attrs["units"]=data["u"].attrs["GRIB_units"]
# data["v"].attrs["units"]=data["v"].attrs["GRIB_units"]
# data["q"].attrs["units"]=data["q"].attrs["GRIB_units"]

# data= data.rename({"height":"plev"})
data = data.interp_like(clim)

print(data)
performance_indices(exp, year1, year2, numproc = numproc, config = config, 
            interface = interface, loglevel = 'warning', xdataset = data)

img = WImage(filename=f'/pfs/lustrep3/scratch/project_465000454/AQUA-workflow/ecmean/figures/PI4_EC23_{exp}_AQUA_r1i1p1f1_{year1}_{year2}.pdf')
filename=f'/pfs/lustrep3/scratch/project_465000454/AQUA-workflow/ecmean/figures/PI4_EC23_{exp}_AQUA_r1i1p1f1_{year1}_{year2}.pdf'
print("check this file:", filename)
# img