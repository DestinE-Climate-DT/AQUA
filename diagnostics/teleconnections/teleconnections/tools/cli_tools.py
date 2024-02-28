"""Module containing cli tools"""


def set_filename(filename=None, fig_type=None):
    """
    Modify the filename provided by the class by adding the figure type.

    Args:
        - filename (str):   name of the file
        - fig_type (str):   type of the figure

    Returns:
        - filename (str):   modified filename
    """
    if filename is None:
        raise ValueError("We need a filename to set the figure")

    if fig_type is None:
        raise ValueError("We need to define the figure type and the teleconnection")

    # We need to inspect the filename and add the fig_type
    # after the telec
    filename = filename.split("_")
    filename.insert(2, fig_type)

    # We need to join the list into a string
    filename = "_".join(filename)

    return filename


def set_figs(telec=None, model=None, exp=None, ref=False,
             filename=None,
             cor=False, reg=False,
             full_year=None, seasons=None,
             reg_full=None, cor_full=None,
             reg_season=None, cor_season=None,
             ref_reg_full_year=None, ref_cor_full_year=None,
             ref_reg_season=None, ref_cor_season=None):
    """
    Set the figures for the teleconnections diagnostics.

    Full years first, seasons then, if both reg and cor, reg is first,
    cor is second.

    Args:
        - telec (str):   name of the teleconnection
        - model (str):  name of the model
        - exp (str):    name of the experiment
        - ref (str):    name of the reference model.
                        If None reference is ignored.
        - filename (str): name of the file to be set
        - cor (bool):   if True correlation maps are set
        - reg (bool):   if True regression maps are set
        - full_year (bool): if True full year maps are set
        - seasons (list):   list of the seasons to be set
        - reg_full (list):  list of the full year regression maps
        - cor_full (list):  list of the full year correlation maps
        - reg_season (list): list of the seasonal regression maps
        - cor_season (list): list of the seasonal correlation maps
        - ref_reg_full_year (xr.DataArray): reference full year regression map
        - ref_cor_full_year (xr.DataArray): reference full year correlation map
        - ref_reg_season (list): reference seasonal regression maps
        - ref_cor_season (list): reference seasonal correlation maps

    Returns:
        - map_names (list): list of the names of the maps
        - maps (list):       list of the maps
        - ref_maps (list):   list of the reference maps
        - titles (list):     list of the titles
        - description (str): description of the figure to be set as pdf metadata
        - cbar_labels (list): list of the colorbar labels

    Raise:
        - ValueError:   if telec is None
        - ValueError:   if model or exp is None
    """
    if telec is None:
        raise ValueError("We need to define the teleconnection name")

    if model is None or exp is None:
        raise ValueError("We need to define the model and the experiment")

    map_names = []
    maps = []
    titles = []
    descriptions = []
    cbar_labels = []
    ref_maps = []

    if telec == "NAO":
        if reg:
            # Regressions
            if full_year:
                # Converting map to hPa
                reg_full = reg_full / 100
                if ref:
                    ref_reg_full_year = ref_reg_full_year / 100
                maps.append(reg_full)
                filename_def = set_filename(filename, "regression")
                title = f'NAO {model} {exp} regression map (msl)'
                description = f'NAO {model} {exp} regression map (msl)'

                if ref:
                    title += f' compared to {ref}'
                    filename_def += f'_{ref}'
                    description += f' compared to {ref}'
                    description += '\nThe contour lines are the model regression map and the filled contour map is the difference between the model and the reference regression map' # noqa
                    ref_maps.append(ref_reg_full_year)
                else:
                    description += '\nThe contour plot is the model regression map.'
                filename_def += '.pdf'
                map_names.append(filename_def)
                titles.append(title)
                descriptions.append(description)
                cbar_labels.append('msl [hPa]')

            if seasons:
                for i, season in enumerate(seasons):
                    # Converting map to hPa
                    reg_season[i] = reg_season[i] / 100
                    if ref:
                        ref_reg_season[i] = ref_reg_season[i] / 100
                    maps.append(reg_season[i])
                    filename_def = set_filename(filename, f"regression_{season}")
                    title = f'NAO {model} {exp} regression map (msl) for {season}'
                    description = f'NAO {model} {exp} regression map (msl) for {season}'

                    if ref:
                        title += f' compared to {ref}'
                        filename_def += f'_{ref}'
                        description += f' compared to {ref}'
                        description += '\nThe contour lines are the model regression map and the filled contour map is the difference between the model and the reference regression map' # noqa
                        ref_maps.append(ref_reg_season[i])
                    else:
                        description += '\nThe contour plot is the model regression map.'
                    filename_def += '.pdf'
                    map_names.append(filename_def)
                    titles.append(title)
                    descriptions.append(description)
                    cbar_labels.append('msl [hPa]')

        # Correlations
        if cor:
            if full_year:
                maps.append(cor_full)
                filename = set_filename(filename, "correlation")
                title = f'NAO {model} {exp} correlation map (msl)'
                description = f'NAO {model} {exp} correlation map (msl)'

                if ref:
                    title += f' compared to {ref}'
                    filename_def += f'_{ref}'
                    description += f' compared to {ref}'
                    description += '\nThe contour lines are the model correlation map and the filled contour map is the difference between the model and the reference correlation map'
                    ref_maps.append(ref_cor_full_year)
                else:
                    description += '\nThe contour plot is the model correlation map.'
                filename_def += '.pdf'
                map_names.append(filename_def)
                titles.append(title)
                descriptions.append(description)
                cbar_labels.append('Pearson correlation coefficient')

            if seasons:
                for i, season in enumerate(seasons):
                    maps.append(cor_season[i])
                    filename_def = set_filename(filename, f"correlation_{season}")
                    title = f'NAO {model} {exp} correlation map (msl) for {season}'
                    description = f'NAO {model} {exp} correlation map (msl) for {season}'

                    if ref:
                        title += f' compared to {ref}'
                        filename_def += f'_{ref}'
                        description += f' compared to {ref}'
                        description += '\nThe contour lines are the model correlation map and the filled contour map is the difference between the model and the reference correlation map'
                        ref_maps.append(ref_cor_season[i])
                    else:
                        description += '\nThe contour plot is the model correlation map.'
                    filename_def += '.pdf'
                    map_names.append(filename_def)
                    titles.append(title)
                    descriptions.append(description)
                    cbar_labels.append('Pearson correlation coefficient')

    elif telec == "ENSO":
        if reg:
            # Regressions
            if full_year:
                maps.append(reg_full)
                filename_def = set_filename(filename, "regression")
                title = f'ENSO {model} {exp} regression map (msl)'
                description = f'ENSO {model} {exp} regression map (msl)'

                if ref:
                    title += f' compared to {ref}'
                    filename_def += f'_{ref}'
                    description += f' compared to {ref}'
                    description += '\nThe contour lines are the model regression map and the filled contour map is the difference between the model and the reference regression map'
                    ref_maps.append(ref_reg_full_year)
                else:
                    description += '\nThe contour plot is the model regression map.'
                filename_def += '.pdf'
                map_names.append(filename_def)
                titles.append(title)
                descriptions.append(description)
                cbar_labels.append('avg_tos [K]')

            if seasons:
                for i, season in enumerate(seasons):
                    maps.append(reg_season[i])
                    filename_def = set_filename(filename, f"regression_{season}")
                    title = f'ENSO {model} {exp} regression map (msl) for {season}'
                    description = f'ENSO {model} {exp} regression map (msl) for {season}'

                    if ref:
                        title += f' compared to {ref}'
                        filename_def += f'_{ref}'
                        description += f' compared to {ref}'
                        description += '\nThe contour lines are the model regression map and the filled contour map is the difference between the model and the reference regression map'
                        ref_maps.append(ref_reg_season[i])
                    else:
                        description += '\nThe contour plot is the model regression map.'
                    filename_def += '.pdf'
                    map_names.append(filename_def)
                    titles.append(title)
                    descriptions.append(description)
                    cbar_labels.append('avg_tos [K]')

        # Correlations
        if cor:
            if full_year:
                maps.append(cor_full)
                filename_def = set_filename(filename, "correlation")
                title = f'ENSO {model} {exp} correlation map (msl)'
                description = f'ENSO {model} {exp} correlation map (msl)'

                if ref:
                    title += f' compared to {ref}'
                    filename_def += f'_{ref}'
                    description += f' compared to {ref}'
                    description += '\nThe contour lines are the model correlation map and the filled contour map is the difference between the model and the reference correlation map'
                    ref_maps.append(ref_cor_full_year)
                else:
                    description += '\nThe contour plot is the model correlation map.'
                filename_def += '.pdf'
                map_names.append(filename_def)
                titles.append(title)
                descriptions.append(description)
                cbar_labels.append('Pearson correlation coefficient')

            if seasons:
                for i, season in enumerate(seasons):
                    maps.append(cor_season[i])
                    filename_def = set_filename(filename, f"correlation_{season}")
                    title = f'ENSO {model} {exp} correlation map (msl) for {season}'
                    description = f'ENSO {model} {exp} correlation map (msl) for {season}'

                    if ref:
                        title += f' compared to {ref}'
                        filename_def += f'_{ref}'
                        description += f' compared to {ref}'
                        description += '\nThe contour lines are the model correlation map and the filled contour map is the difference between the model and the reference correlation map'
                        ref_maps.append(ref_cor_season[i])
                    else:
                        description += '\nThe contour plot is the model correlation map.'
                    filename_def += '.pdf'
                    map_names.append(filename_def)
                    titles.append(title)
                    descriptions.append(description)
                    cbar_labels.append('Pearson correlation coefficient')

    return map_names, maps, ref_maps, titles, descriptions, cbar_labels
