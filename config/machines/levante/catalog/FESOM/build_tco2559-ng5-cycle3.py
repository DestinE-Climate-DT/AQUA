#!/usr/bin/env python3

import os
import glob
import yaml
import intake

# Path to FESOM data
base_dirs = ["/work/bm1235/a270046/cycle3/tco2559l137/hzfy/hres/intel.levante.openmpi/lvt.intel.sp/Cycle3_",
"/work/bm1235/a270046/cycle3/tco2559l137_2023/tco2559l137/hzfy/hres/intel.levante.openmpi/lvt.intel.sp/Cycle3_"]

minimal_mesh = "/work/bm1344/AWI/Cycle3/experimental_grid/fesom_ng5.nc"

out_catalog_name = "tco2559-ng5-cycle3.yaml"
# Dictionary to map variables to their sources
variable_sources = {
    "2D_daily_native": [
        "atmice_x",
        "atmice_y",
        "atmoce_x",
        "atmoce_y",
        "m_ice",
        "m_snow",
        "MLD1",
        "MLD2",
        "prec",
        "ssh",
        "uice",
        "vice",
        'fw'
    ],
    "2D_1h_native": [
        "a_ice",
        "evap",
        "fh",
        "sss",
        "sst",
    ],
    "2D_monthly_native": ["runoff"],
    "2D_monthly_native_elem": ["tx_sur", "ty_sur"],
    "3D_daily_native": ["salt", "temp","Kv", "w"],
    "3D_daily_native_elem": ["u", "v"] #, "Av"]
    #"3D_3h_native": ["salt_upper", "temp_upper", "w_upper"],
    #"3D_3h_native_elem": ["u_upper", "v_upper"]
}

chunks = {
    "2D_daily_native": {"time": 1, "nod2": -1},
    "2D_1h_native": {"time": 1, "nod2": -1},
    "2D_monthly_native": {"time": 1, "nod2": -1},
    "2D_monthly_native_elem": {"time": 1, "elem": -1},

    "3D_daily_native": {"time": 1, "nod2": -1, "nz1": -1, 'nz': -1},
    "3D_daily_native_elem": {"time": 1, "elem": -1, "nz1": -1},

    "3D_3h_native": {"time": 1, "nod2": -1, "nz1_upper": -1, "nz_upper": -1},
    "3D_3h_native_elem": {"time": 1, "elem": -1, "nz1_upper": -1, "nz_upper": -1},
    # "3d_vertices_nz_daily": {"time": 1, "nod2": -1, "nz": -1},
    # "3d_vertices_nz-upper_3hourly": {"time": 1, "nod2": -1, "nz_upper": -1},
    # "3d_elements_nz1_daily": {"time": 1, "elem": -1, "nz1": -1},
    # "3d_elements_nz1-upper_3hourly": {"time": 1, "elem": -1, "nz1_upper": -1},
    # "3d_elements_nz_daily": {"time": 1, "elem": -1, "nz": -1},
}

# Fixed catalog entries
# Function to extract source from file name
def extract_source(file_name):
    for source, variables in variable_sources.items():
        for variable in variables:
            # this is very FESOM specific
            if file_name.split(".")[0] == variable:
                return source
    return None


# Fixed catalog entries
fixed_catalog_entries = {
    "plugins": {"source": [{"module": "intake_xarray"}]},
    "sources": {
        "elem_grid": {
            "args": {"urlpath": "/work/bm1235/a270046/meshes/NG5_griddes_elems_IFS.nc"},
            "driver": "netcdf",
        },
        "node_grid": {
            "args": {"urlpath": "/work/bm1235/a270046/meshes/NG5_griddes_nodes_IFS.nc"},
            "driver": "netcdf",
        },
    },
}

# Function to search for netCDF files and create catalog
def create_intake_catalog():
    # base_dir = "Cycle3_"
    catalog_entries = {}
    
    for base_dir in base_dirs:
        for year in range(2020, 2025):
            for month in range(1, 13):
                directory = f"{base_dir}{month:02d}{year}"
                if os.path.exists(directory):
                    for nc_file in glob.glob(f"{directory}/*.nc"):
                        file_name = os.path.basename(nc_file)
                        source = extract_source(file_name)
                        if source:
                            if source not in catalog_entries:
                                catalog_entries[source] = {
                                    "driver": "netcdf",
                                    "args": {"urlpath": [], "chunks": chunks[source]},
                                    "metadata": {"source": source},
                                }
                                #catalog_entries[source]["args"]["urlpath"].append(minimal_mesh)
                            catalog_entries[source]["args"]["urlpath"].append(nc_file)

    # Merge fixed_catalog_entries with generated catalog_entries
    fixed_catalog_entries["sources"].update(catalog_entries)

    # Save catalog to YAML file
    with open(out_catalog_name, "w") as outfile:
        outfile.write("# Sources:\n")
        outfile.write(f"# - node_grid: grid for vertices (also known as nods)\n")
        outfile.write(f"# - elem_grid: grid for elements (also known as triangles)\n")
        for source, variables in variable_sources.items():
            outfile.write(f"# - {source}: {', '.join(variables)}\n")
        outfile.write("\n")

        yaml.dump(fixed_catalog_entries, outfile, default_flow_style=False)

    # Load and return the catalog
    return intake.open_catalog(out_catalog_name)


# Create and use the catalog
catalog = create_intake_catalog()

# Example usage
# ds = catalog["2d_elements_monthly"].to_dask()
# print(ds)