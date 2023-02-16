import yaml
import glob
import xarray as xr
from collections import defaultdict

def find_paths():
    fn_by_time = defaultdict(list)

    for fn in glob.glob("/work/bm1235/a270046/cycle2-sync/tco2559-ng5/04_01Apr-30Apr2020_fesom/*fesom*.nc"):
        ds = xr.open_dataset(fn)
        fn_by_time[ds.dims["time"]].append(fn.split("/")[-1])

    lens = [("3d", 240, {"time": 1, "nz": 1, "nz1": 1}), ("2d", 720, {"time": 1})]
    for name, size, chunks in lens:
        yield (f"original_{name}", {
                "description": f"original {name} output",
                "driver": "netcdf",
                "args": {
                    "urlpath": list(sorted([fn
                         for f in fn_by_time[size]
                         for fn in glob.glob("/work/bm1235/a270046/cycle2-sync/tco2559-ng5/*_fesom/" + f)])),
                    "chunks": chunks,
                }
            })

SOURCES = {
    "node_grid": {
        "driver": "netcdf",
        "args": {
            "urlpath": "/work/bm1235/a270046/meshes/NG5_griddes_nodes_IFS.nc",
        },
    },
    "elem_grid": {
        "driver": "netcdf",
        "args": {
            "urlpath": "/work/bm1235/a270046/meshes/NG5_griddes_elems_IFS.nc",
        },
    },
}

PLUGINS = {
    "source": [
        {"module": "intake_xarray"},
        {"module": "gribscan"},
    ],
}

def main():
    sources = {
        **dict(find_paths()),
        **SOURCES,
    }
    with open("tco2559-ng5.yaml", "w") as outfile:
        yaml.dump({
            "plugins": PLUGINS,
            "sources": sources,
        }, outfile)

if __name__ == "__main__":
    main()
