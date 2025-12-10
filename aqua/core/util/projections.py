import cartopy.crs as ccrs

projections_maps = {
    "plate_carree": ccrs.PlateCarree,
    "albers_equal_area": ccrs.AlbersEqualArea,
    "azimuthal_equidistant": ccrs.AzimuthalEquidistant,
    "equidist_conic": ccrs.EquidistantConic,
    "lambert_conformal": ccrs.LambertConformal,
    "lambert_cylindrical": ccrs.LambertCylindrical,
    "mercator": ccrs.Mercator,
    "miller": ccrs.Miller,
    "mollweide": ccrs.Mollweide,
    "orthographic": ccrs.Orthographic,
    "robinson": ccrs.Robinson,
    "sinusoidal": ccrs.Sinusoidal,
    "stereographic": ccrs.Stereographic,
    "transverse_mercator": ccrs.TransverseMercator,
    "interrupted_gh": ccrs.InterruptedGoodeHomolosine,
    "rotated_pole": ccrs.RotatedPole,
    "europp": ccrs.EuroPP,
    "geostationary": ccrs.Geostationary,
    "nearside": ccrs.NearsidePerspective,
    "nh_polar_stereo": ccrs.NorthPolarStereo,
    "sh_polar_stereo": ccrs.SouthPolarStereo,
    "eckert_ii": ccrs.EckertII,
    "eckert_iv": ccrs.EckertIV,
    "eckert_vi": ccrs.EckertVI,
    "aitoff": ccrs.Aitoff,
    "equal_earth": ccrs.EqualEarth,
    "hammer": ccrs.Hammer,
    "lambert_equal_area": ccrs.LambertAzimuthalEqualArea,
    "gnomonic": ccrs.Gnomonic
    }

def get_projection(projname: str, **kwargs) -> ccrs.Projection:
    """
    Return a Cartopy projection by name. Refer to the Cartopy 
    documentation (https://scitools.org.uk/cartopy/) to review the 
    supported keyword arguments for each projection class.

    Args:
        projname (str): Name of the projection.
        **kwargs: Additional keyword arguments passed to the projection class.

    Returns:
        cartopy.crs.Projection: An instance of the projection.
    """
    projname = projname.lower()
    if projname not in projections_maps:
        raise ValueError(f"Unsupported projection: '{projname}'. "
                         f"Available options are: {list(projections_maps.keys())}")
    
    projection_class = projections_maps[projname]
    return projection_class(**kwargs)
