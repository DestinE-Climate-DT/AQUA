"""Central default constants to avoid circular imports."""
DEFAULT_REALIZATION = "r1"

# Reader/backend defaults
DEFAULT_DATAMODEL = "aqua"
DEFAULT_CONVENTION = "eccodes"
DEFAULT_REGRID_METHOD = "ycon"
DEFAULT_ENGINE = "fdb"
DEFAULT_NPROC = 4

# Spatial defaults
DEFAULT_COORDS = {"lat_min": -90, "lat_max": 90, "lon_min": 0, "lon_max": 360}

# Data model coord names
DEFAULT_COORD_NAMES = {
    "latitude": ["lat", "latitude", "y"],
    "longitude": ["lon", "longitude", "x"],
    "time": ["time"],
    "isobaric": ["plev", "level"],
    "depth": ["depth"],
    "height": ["height"],
}

# Fixer defaults
DEFAULT_DELTAT = 1
# Time defaults
DEFAULT_TIME_UNIT = "us"
DEFAULT_CALENDAR = "gregorian"

# Drop defaults
DEFAULT_DROP_GRID = "lon-lat-r100"

# Regridder defaults
DEFAULT_WEIGHTS_AREAS_PARAMETERS = ["zoom"]
DEFAULT_GRID_METHOD = "ycon"
DEFAULT_DIMENSION = "2d"
DEFAULT_DIMENSION_MASK = "2dm"
