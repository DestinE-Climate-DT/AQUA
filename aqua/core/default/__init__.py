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

# Regridder defaults
DEFAULT_WEIGHTS_AREAS_PARAMETERS = ["zoom"]
DEFAULT_DIMENSION = "2d"
DEFAULT_DIMENSION_MASK = "2dm"
