import os
import xarray as xr
from aqua.logger import log_configure
from aqua.configurer import ConfigPath
from aqua.util import load_yaml, select_season, to_list
from aqua.util import convert_data_units, get_realizations
from aqua.diagnostics.core import Diagnostic, OutputSaver

xr.set_options(keep_attrs=True)


class BaseMixin(Diagnostic):
    def __init__(self, telecname: str, catalog: str = None, model: str = None,
                 exp: str = None, source: str = None,
                 regrid: str = None,
                 startdate: str = None, enddate: str = None,
                 configdir: str = None,
                 definition: str = 'teleconnections-destine',
                 loglevel: str = 'WARNING'):
        """
        Initialize the Base class.
        Args:
            telecname (str): The name of the teleconnection.
            catalog (str): The catalog to be used. If None, the catalog will be determined by the Reader.
            model (str): The model to be used.
            exp (str): The experiment to be used.
            source (str): The source to be used.
            regrid (str): The target grid to be used for regridding. If None, no regridding will be done.
            startdate (str): The start date of the data to be retrieved.
                             If None, all available data will be retrieved.
            enddate (str): The end date of the data to be retrieved.
                           If None, all available data will be retrieved.
            configdir (str): The directory where the definition file is located.
                             If None, the default directory will be used.
            definition (str): The filename of the definition file.
                             Default is 'teleconnections-destine'.
            loglevel (str): The log level to be used. Default is 'WARNING'.
        """
        super().__init__(catalog=catalog, model=model, exp=exp, source=source, regrid=regrid,
                         startdate=startdate, enddate=enddate, loglevel=loglevel)

        self.definition = self.load_definition(configdir=configdir, definition=definition,
                                               telecname=telecname)
        # Initialize the possible results
        self.index = None

    def compute_regression(self, var: str = None,
                           dim: str = 'time', season: str = None):
        """
        Compute the regression of the data on the index.

        Args:
            var (str): The variable to be used. If None, the variable is the same of the index.
            dim (str): The dimension to be used for the regression. Default is 'time'.
            season (str): The season to be used. If None, no season will be selected.

        Returns:
            xr.DataArray: The regression of the data on the index.
        """
        data, index = self._prepare_statistic(var=var, season=season)
        reg = xr.cov(index, data, dim=dim)/index.var(dim=dim, skipna=True).values
        return reg

    def compute_correlation(self, var: str = None,
                            dim: str = 'time', season: str = None):
        """
        Compute the correlation of the data on the index.

        Args:
            var (str): The variable to be used. If None, the variable is the same of the index.
            dim (str): The dimension to be used for the regression. Default is 'time'.
            season (str): The season to be used. If None, no season will be selected.

        Returns:
            xr.DataArray: The regression of the data on the index.
        """
        data, index = self._prepare_statistic(var=var, season=season)
        corr = xr.corr(index, data, dim=dim)

        # Modify the attributes to match the correlation
        corr.attrs['long_name'] = f'Correlation of {data.long_name} with index evaluated with {index.long_name}'
        corr.attrs['shortName'] = f'Pearson_correlation'
        corr.attrs['units'] = '1'

        return corr

    def _prepare_statistic(self, var: str = None, season: str = None):
        """Hidden method to prepare the data and index for the statistic."""
        # Preparing data and index. Both have to be xr.DataArray
        if self.index is None:
            raise ValueError("Index is not set. Please compute the index first.")
        else:
            index = self.index
        if not var:
            if isinstance(self.data, xr.Dataset):
                data = self.data[self.var]
        else:
            data, _, _ = super()._retrieve(model=self.model, exp=self.exp, source=self.source,
                                           var=var, catalog=self.catalog, startdate=self.startdate,
                                           enddate=self.enddate, regrid=self.regrid, loglevel=self.loglevel)
            data = data[var]

        if season:
            data = select_season(data, season)
            index = select_season(index, season)

        return data, index

    def load_definition(self, configdir: str = None, definition: str = 'teleconnections-destine',
                       telecname: str = None):
        """
        Load the definition for the teleconnections.

        Args:
            configdir (str): The directory where the definition file is located.
                              If None, the default directory will be used.
            definition (str): The filename of the definition file.
                             Default is 'teleconnections-destine'.
            telecname (str): The name of the teleconnection. It selects the subset of the definition.

        Returns:
            dict: The definition file as a dictionary.
        """
        # Add yaml to definition if not present
        if not definition.endswith('.yaml'):
            definition = f'{definition}.yaml'
        if not configdir:
            configdir = ConfigPath().get_config_dir()
            configdir = os.path.join(configdir, 'tools', 'teleconnections', 'definitions')

        definition_file = os.path.join(configdir, definition)
        self.logger.debug(f'Loading definition file: {definition_file}')

        definition_dict = load_yaml(definition_file)

        return definition_dict[telecname] if telecname else definition_dict


class PlotBaseMixin():
    """PlotBaseMixin class is used for the PlotNAO and the PlotENSO classes."""
    def __init__(self, indexes=None, ref_indexes=None, diagnostic: str = None, outputdir: str = './',
                 rebuild: bool = True, loglevel: str = 'WARNING'):
        """
        Initialize the PlotBaseMixin class.

        Args:
            indexes (list): The list of indexes to be used. Default is None.
            ref_indexes (list): The list of reference indexes to be used. Default is None.
            diagnostic (str): The name of the diagnostic. Default is None.
            outputdir (str): The directory where the output files will be saved. Default is './'.
            rebuild (bool): If True, the output files will be rebuilt. Default is True.
            loglevel (str): The log level to be used. Default is 'WARNING'.
        """
        # Data info initalized as empty
        self.loglevel = loglevel
        self.logger = log_configure(self.loglevel, 'PlotBaseMixin')
        self.catalogs = None
        self.models = None
        self.exps = None
        self.ref_catalogs = None
        self.ref_models = None
        self.ref_exps = None

        self.indexes = to_list(indexes)
        self.ref_indexes = to_list(ref_indexes)

        self.len_data = len(self.indexes)
        self.len_ref = len(self.ref_indexes)

        self.get_data_info()

        self.outputsaver = OutputSaver(diagnostic=diagnostic,  catalog=self.catalogs, model=self.models,
                                       exp=self.exps, catalog_ref=self.ref_catalogs, model_ref=self.ref_models,
                                       exp_ref=self.ref_exps, outputdir=outputdir,
                                       realization = self.realizations, loglevel=self.loglevel)

    def get_data_info(self):
        """
        We extract the data needed for labels, description etc
        from the data arrays attributes.

        The attributes are:
        - AQUA_catalog
        - AQUA_model
        - AQUA_exp
        """
        if self.indexes is not None:
            self.catalogs = [d.AQUA_catalog for d in self.indexes]
            self.models = [d.AQUA_model for d in self.indexes]
            self.exps = [d.AQUA_exp for d in self.indexes]
            self.realizations = get_realizations(self.indexes)
        self.logger.debug(f'Catalogs: {self.catalogs}')
        self.logger.debug(f'Models: {self.models}')
        self.logger.debug(f'Exps: {self.exps}')

        if self.ref_indexes is not None:
            self.ref_catalogs = [d.AQUA_catalog for d in self.ref_indexes]
            self.ref_models = [d.AQUA_model for d in self.ref_indexes]
            self.ref_exps = [d.AQUA_exp for d in self.ref_indexes]
            self.logger.debug(f'Ref Catalogs: {self.ref_catalogs}')
            self.logger.debug(f'Ref Models: {self.ref_models}')
            self.logger.debug(f'Ref Exps: {self.ref_exps}')

    def set_index_title(self, diagnostic: str = None):
        """
        Set the title of the index.

        Args:
            diagnostic (str): The name of the diagnostic. Default is None.
        
        Returns:
            str: The title of the index plot.
        """
        titles_dataset = [f'{diagnostic} index for {self.models[i]} {self.exps[i]}'
                          for i in range(self.len_data)]
        titles_ref = [f'{diagnostic} index for {self.ref_models[i]} {self.ref_exps[i]}'
                      for i in range(self.len_ref)]
        titles = titles_dataset + titles_ref

        return titles

    def set_labels(self):
        """
        Set the labels for the plot.

        Returns:
            list: The list of labels for the plot.
        """
        labels_dataset = [f'{self.models[i]} {self.exps[i]}'
                          for i in range(self.len_data)]
        labels_ref = [f'{self.ref_models[i]} {self.ref_exps[i]}'
                      for i in range(self.len_ref)]
        labels = labels_dataset + labels_ref
        return labels

    def set_index_description(self, index_name: str = None):
        """
        Set the description of the index. This is used to
        generate the caption of the figure.

        Args:
            index_name (str): The name of the index. Default is None.

        Returns:
            str: The caption of the figure.
        """
        description = f"{index_name} index for"

        dataset = [f"{self.models[i]} {self.exps[i]}" for i in range(self.len_data)]
        refs = [f"{self.ref_models[i]} {self.ref_exps[i]}" for i in range(self.len_ref)]

        if self.len_data > 0:
            description += f" {', '.join(dataset)}"
        if self.len_ref > 0:
            description += " using reference data from"
            description += f" {', '.join(refs)}"
        description += "."

        self.logger.debug(f'Index description: {description}')
        return description

    def save_plot(self, fig, diagnostic_product: str = None, extra_keys: dict = None,
                  rebuild: bool = True,
                  dpi: int = 300, format: str = 'png', metadata: dict = None):
        """
        Save the plot to a file.

        Args:
            fig (matplotlib.figure.Figure): The figure to be saved.
            diagnostic_product (str): The name of the diagnostic product. Default is None.
            extra_keys (dict): Extra keys to be used for the filename (e.g. season). Default is None.
            rebuild (bool): If True, the output files will be rebuilt. Default is True.
            dpi (int): The dpi of the figure. Default is 300.
            format (str): The format of the figure. Default is 'png'.
            metadata (dict): The metadata to be used for the figure. Default is None.
                             They will be complemented with the metadata from the outputsaver.
                             We usually want to add here the description of the figure.
        """
        if format == 'png':
            _ = self.outputsaver.save_png(fig, diagnostic_product=diagnostic_product, rebuild=rebuild,
                                          extra_keys=extra_keys, metadata=metadata, dpi=dpi)
        elif format == 'pdf':
            _ = self.outputsaver.save_pdf(fig, diagnostic_product=diagnostic_product, rebuild=rebuild,
                                          extra_keys=extra_keys, metadata=metadata)

    def set_map_description(self, maps=None, ref_maps=None, statistic: str = None, telecname: str = None):
        """
        Set the description for the maps.

        Args:
            maps (list): List of maps to plot.
            ref_maps (list): List of reference maps to plot.
            statistic (str): Statistic to plot. Default is None.
            telecname (str): The name of the teleconnection. Default is None.

        Returns:
            str: Description of the maps.
        """
        description = f"{telecname} {statistic} map "

        maps, ref_maps = _homogeneize_maps(maps=maps, ref_maps=ref_maps)

        if isinstance(maps, xr.DataArray):
            if statistic == 'correlation':
                var = maps.long_name if hasattr(maps, 'long_name') else maps.shortName
            else:
                var = maps.shortName if hasattr(maps, 'shortName') else maps.long_name
            description += f"({var}) "
            description += f"{maps.AQUA_model} {maps.AQUA_exp}"
            if hasattr(maps, 'AQUA_season'):
                description += f" ({maps.AQUA_season})"
        elif isinstance(maps, list):
            var = maps[0].shortName if hasattr(maps[0], 'shortName') else maps[0].long_name
            description += f"({var}) "
            for map in maps:
                description += f"{map.AQUA_model} {map.AQUA_exp}, "
            description = description[:-2]
            if hasattr(maps[0], 'AQUA_season'):
                description += f" ({maps[0].AQUA_season})"
        if isinstance(ref_maps, xr.DataArray):
            var = ref_maps.shortName if hasattr(ref_maps, 'shortName') else ref_maps.long_name
            description += f" compared to {ref_maps.AQUA_model} {ref_maps.AQUA_exp}"
        elif isinstance(ref_maps, list):
            var = ref_maps[0].shortName if hasattr(ref_maps[0], 'shortName') else ref_maps[0].long_name
            description += f" compared to {ref_maps[0].AQUA_model} {ref_maps[0].AQUA_exp}"
            for map in ref_maps:
                description += f"{map.AQUA_model} {map.AQUA_exp}, "
            description = description[:-2]
        description += "."
        if ref_maps is not None:
            description += f" The contour lines are the model regression map and the filled contour map is the difference between the model and the reference {statistic} map."
        self.logger.debug(f'Map description: {description}')

        return description


def _homogeneize_maps(maps, ref_maps=None, var=None):
    """
    Homogenize the maps. If a list has length 1, convert it to a single xarray.
    If the units are in 'Pa', convert to 'hPa'. If var is None, it is inferred
    from the data variable name.

    Args:
        maps (list or xarray.DataArray): The list of maps or a single map.
        ref_maps (list or xarray.DataArray): The list of reference maps or a single map.
        var (str, optional): The variable name to pass to the unit conversion. 
                             If None, inferred from each DataArray.

    Returns:
        tuple: The homogenized maps and reference maps.
    """
    maps = to_list(maps)
    maps = [
        convert_data_units(data, var=data.name if var is None else var, units='hPa')
        if getattr(data, 'units', None) == 'Pa' else data
        for data in maps
    ]

    if ref_maps is not None:
        ref_maps = to_list(ref_maps)
        ref_maps = [
            convert_data_units(data, var=data.name if var is None else var, units='hPa')
            if getattr(data, 'units', None) == 'Pa' else data
            for data in ref_maps
        ]

    if len(maps) == 1:
        maps = maps[0]
    if ref_maps is not None and len(ref_maps) == 1:
        ref_maps = ref_maps[0]

    return maps, ref_maps
