"""Module containing cli tools"""

def set_figs(telec=None, catalog=None, model=None, exp=None, ref=False,
             loglevel='WARNING',
             cor=False, reg=False,
             full_year=None, seasons=None,
             reg_full=None, cor_full=None,
             reg_season=None, cor_season=None,
             ref_reg_full_year=None, ref_cor_full_year=None,
             ref_reg_season=None, ref_cor_season=None,
             filename_keys: list = None):
    """
    Set the figures for the teleconnections diagnostics.

    Full years first, seasons then, if both reg and cor, reg is first,
    cor is second.

    Args:
        - telec (str):   name of the teleconnection
        - catalog (str): name of the catalog
        - model (str):  name of the model
        - exp (str):    name of the experiment
        - ref (str):    name of the reference model. If None reference is ignored.
        - loglevel (str, optional): Log level. Defaults to 'WARNING'.
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
        - filename_keys (list, optional): List of keys to keep in the filename. Default is None, which includes all keys.

    Returns:
        - common_save_args (dict): common args for a future images
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

    maps = []
    titles = []
    descriptions = []
    cbar_labels = []
    ref_maps = []

    common_save_args_list = []
    common_save_args = {}
    if ref:
        common_save_args.update({'model_2': ref})
    if telec == "NAO":
        if reg:
            common_save_args.update({'diagnostic_product': telec + '_' + 'regression'})
            # Regressions
            if full_year:
                # Converting map to hPa
                reg_full = reg_full / 100
                if ref:
                    ref_reg_full_year = ref_reg_full_year / 100
                maps.append(reg_full)
                title = f'NAO {model} {exp} regression map (msl)'
                description = f'NAO {model} {exp} regression map (msl)'
                if ref:
                    title += f' compared to {ref}'
                    description += f' compared to {ref}'
                    description += '\nThe contour lines are the model regression map and the filled contour map is the difference between the model and the reference regression map' # noqa
                    ref_maps.append(ref_reg_full_year)
                else:
                    description += '\nThe contour plot is the model regression map.'
                common_save_args_list.append(common_save_args)
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
                    season_save_args = common_save_args.copy()
                    season_save_args.update({'seasons': season})
                    title = f'NAO {model} {exp} regression map (msl) for {season}'
                    description = f'NAO {model} {exp} regression map (msl) for {season}'
                    if ref:
                        title += f' compared to {ref}'
                        description += f' compared to {ref}.'
                        description += '\nThe contour lines are the model regression map and the filled contour map is the difference between the model and the reference regression map.' # noqa
                        ref_maps.append(ref_reg_season[i])
                    else:
                        description += '\nThe contour plot is the model regression map.'
                    common_save_args_list.append(season_save_args)
                    titles.append(title)
                    descriptions.append(description)
                    cbar_labels.append('msl [hPa]')

        # Correlations
        if cor:
            common_save_args.update({'diagnostic_product': telec + '_' + 'correlation'})
            if full_year:
                maps.append(cor_full)
                title = f'NAO {model} {exp} correlation map (msl)'
                description = f'NAO {model} {exp} correlation map (msl)'
                if ref:
                    title += f' compared to {ref}'
                    description += f' compared to {ref}.'
                    description += '\nThe contour lines are the model correlation map and the filled contour map is the difference between the model and the reference correlation map.' # noqa
                    ref_maps.append(ref_cor_full_year)
                else:
                    description += '\nThe contour plot is the model correlation map.'
                common_save_args_list.append(common_save_args)
                titles.append(title)
                descriptions.append(description)
                cbar_labels.append('Pearson correlation coefficient')

            if seasons:
                for i, season in enumerate(seasons):
                    maps.append(cor_season[i])
                    season_save_args = common_save_args.copy()
                    season_save_args.update({'seasons': season})
                    title = f'NAO {model} {exp} correlation map (msl) for {season}'
                    description = f'NAO {model} {exp} correlation map (msl) for {season}'
                    if ref:
                        title += f' compared to {ref}'
                        description += f' compared to {ref}.'
                        description += '\nThe contour lines are the model correlation map and the filled contour map is the difference between the model and the reference correlation map.' # noqa
                        ref_maps.append(ref_cor_season[i])
                    else:
                        description += '\nThe contour plot is the model correlation map.'
                    common_save_args_list.append(season_save_args)
                    titles.append(title)
                    descriptions.append(description)
                    cbar_labels.append('Pearson correlation coefficient')

    elif telec == "ENSO":
        if reg:
            common_save_args.update({'diagnostic_product': telec + '_' + 'regression'})
            # Regressions
            if full_year:
                maps.append(reg_full)
                title = f'ENSO {model} {exp} regression map (tos)'
                description = f'ENSO {model} {exp} regression map (tos)'
                if ref:
                    title += f' compared to {ref}'
                    description += f' compared to {ref}.'
                    description += '\nThe contour lines are the model regression map and the filled contour map is the difference between the model and the reference regression map.' # noqa
                    ref_maps.append(ref_reg_full_year)
                else:
                    description += '\nThe contour plot is the model regression map.'
                common_save_args_list.append(common_save_args)
                titles.append(title)
                descriptions.append(description)
                cbar_labels.append('tos [K]')

            if seasons:
                for i, season in enumerate(seasons):
                    maps.append(reg_season[i])
                    title = f'ENSO {model} {exp} regression map (tos) for {season}'
                    description = f'ENSO {model} {exp} regression map (tos) for {season}'
                    season_save_args = common_save_args.copy()
                    season_save_args.update({'seasons': season})
                    if ref:
                        title += f' compared to {ref}'
                        description += f' compared to {ref}.'
                        description += '\nThe contour lines are the model regression map and the filled contour map is the difference between the model and the reference regression map.' # noqa
                        ref_maps.append(ref_reg_season[i])
                    else:
                        description += '\nThe contour plot is the model regression map.'
                    common_save_args_list.append(season_save_args)
                    titles.append(title)
                    descriptions.append(description)
                    cbar_labels.append('tos [K]')

        # Correlations
        if cor:
            common_save_args.update({'diagnostic_product': telec + '_' + 'correlation'})
            if full_year:
                maps.append(cor_full)
                title = f'ENSO {model} {exp} correlation map (tos)'
                description = f'ENSO {model} {exp} correlation map (tos)'

                if ref:
                    title += f' compared to {ref}'
                    description += f' compared to {ref}'
                    description += '\nThe contour lines are the model correlation map and the filled contour map is the difference between the model and the reference correlation map.' # noqa
                    ref_maps.append(ref_cor_full_year)
                else:
                    description += '\nThe contour plot is the model correlation map.'
                common_save_args_list.append(common_save_args)
                titles.append(title)
                descriptions.append(description)
                cbar_labels.append('Pearson correlation coefficient')

            if seasons:
                for i, season in enumerate(seasons):
                    maps.append(cor_season[i])
                    title = f'ENSO {model} {exp} correlation map (tos) for {season}'
                    description = f'ENSO {model} {exp} correlation map (tos) for {season}'
                    season_save_args = common_save_args.copy()
                    season_save_args.update({'seasons': season})
                    if ref:
                        title += f' compared to {ref}'
                        description += f' compared to {ref}.'
                        description += '\nThe contour lines are the model correlation map and the filled contour map is the difference between the model and the reference correlation map.' # noqa
                        ref_maps.append(ref_cor_season[i])
                    else:
                        description += '\nThe contour plot is the model correlation map.'
                    common_save_args_list.append(season_save_args)
                    titles.append(title)
                    descriptions.append(description)
                    cbar_labels.append('Pearson correlation coefficient')

    return common_save_args_list, maps, ref_maps, titles, descriptions, cbar_labels
