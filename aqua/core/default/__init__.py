"""Central default constants to avoid circular imports."""
DEFAULT_REALIZATION = "r1"

# Reader/backend defaults
DEFAULT_DATAMODEL = "aqua"
DEFAULT_CONVENTION = "eccodes"
DEFAULT_REGRID_METHOD = "ycon" #used also in regridder
DEFAULT_ENGINE = "gsv"
DEFAULT_NPROC = 4

# Spatial defaults
DEFAULT_COORDS = {"lat_min": -90, "lat_max": 90, "lon_min": 0, "lon_max": 360}

# Define internal names for coordinates
AQUA_LONGITUDE = "longitude"
AQUA_LATITUDE = "latitude"
AQUA_TIME = "time"
AQUA_ISOBARIC = "isobaric"
AQUA_DEPTH = "depth"
AQUA_HEIGHT = "height"

# Single source of truth for all valid internal coordinate names
AQUA_COORDS = (AQUA_LATITUDE, AQUA_LONGITUDE, AQUA_TIME, AQUA_ISOBARIC, AQUA_DEPTH, AQUA_HEIGHT)

# Possible basic names for coordinates: used for CoordIdentifier matching
DEFAULT_COORD_NAMES = {
    AQUA_LATITUDE: ["latitude","lat", "y"],
    AQUA_LONGITUDE: ["longitude", "lon", "x"],
    AQUA_TIME: ["time", "time_counter"],
    AQUA_ISOBARIC: ["plev"],
    AQUA_DEPTH: ["depth"],
    AQUA_HEIGHT: ["height"],
}

# Fixer defaults
DEFAULT_DELTAT = 1
# Time defaults
DEFAULT_TIME_UNIT = "us"
DEFAULT_CALENDAR = "gregorian"

# Drop defaults
DEFAULT_DROP_GRID = "lon-lat-r100"
DEFAULT_DROP_CHUNK_TARGET = "128MiB"  # target size of the read chunks written in DROP catalog entries

# Graphics defaults (font sizes are set in the aqua.mplstyle style file)
DEFAULT_FIGSIZE_HORIZONTAL = (10, 5)

# Regridder defaults
DEFAULT_WEIGHTS_AREAS_PARAMETERS = ["zoom"]
DEFAULT_DIMENSION = "2d"
DEFAULT_DIMENSION_MASK = "2dm"
DEFAULT_GRID_DATA = "data"  # reserved grid path value: the grid is taken from the dataset
DEFAULT_DIMENSION_SKIPNA = "skipna"  # single grid path regridded with smmregrid skipna
# grid path keys regridded with 2D weights (no mask_dim in smmregrid)
DEFAULT_2D_DIMENSIONS = [DEFAULT_DIMENSION, DEFAULT_DIMENSION_MASK, DEFAULT_DIMENSION_SKIPNA]
# max fraction of missing source weights before a target cell is NaN: smmregrid default without skipna
DEFAULT_NA_THRES = 0.5
