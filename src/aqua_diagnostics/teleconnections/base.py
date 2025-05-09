import os
import xarray as xr
from aqua.diagnostics.core import Diagnostic, OutputSaver
from aqua.logger import log_configure
from aqua.util import ConfigPath
from aqua.util import load_yaml, select_season, to_list

xr.set_options(keep_attrs=True)


class BaseMixin(Diagnostic):
    def __init__(self, telecname: str, catalog: str = None, model: str = None,
                 exp: str = None, source: str = None,
                 regrid: str = None,
                 startdate: str = None, enddate: str = None,
                 configdir: str = None,
                 interface: str = 'teleconnections-destine',
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
            configdir (str): The directory where the interface file is located.
                             If None, the default directory will be used.
            interface (str): The filename of the interface file.
                             Default is 'teleconnections-destine'.
            loglevel (str): The log level to be used. Default is 'WARNING'.
        """
        super().__init__(catalog=catalog, model=model, exp=exp, source=source, regrid=regrid,
                         startdate=startdate, enddate=enddate, loglevel=loglevel)

        self.interface = self.load_interface(configdir=configdir, interface=interface,
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
        corr = xr.corr(index, data, dim=dim, skipna=True)
        return corr
    
    def _prepare_statistic(self, var: str = None, season: str = None):
        """Hidden method to prepare the data and index for the statistic."""
        # Preparing data and index. Both have to be xr.DataArray
        if not var:
            data = self.data[self.var]
        else:
            data, _, _ = super()._retrieve(model=self.model, exp=self.exp, source=self.source,
                                           var=var, catalog=self.catalog, startdate=self.startdate,
                                           enddate=self.enddate, regrid=self.regrid, loglevel=self.loglevel)
            data = data[var]

        index = self.index
        if season:
            data = select_season(data, season)
            index = select_season(index, season)

        return data, index

    def load_interface(self, configdir: str = None, interface: str = 'teleconnections-destine',
                       telecname: str = None):
        """
        Load the interface for the teleconnections.

        Args:
            configdir (str): The directory where the interface file is located.
                              If None, the default directory will be used.
            interface (str): The filename of the interface file.
                             Default is 'teleconnections-destine'.
            telecname (str): The name of the teleconnection. It selects the subset of the interface.
        
        Returns:
            dict: The interface file as a dictionary.
        """
        # Add yaml to interface if not present
        if not interface.endswith('.yaml'):
            interface = f'{interface}.yaml'
        if not configdir:
            configdir = ConfigPath().get_config_dir()
            configdir = os.path.join(configdir, 'diagnostics', 'teleconnections', 'config')

        interface_file = os.path.join(configdir, interface)
        self.logger.debug(f'Loading interface file: {interface_file}')

        interface_dict = load_yaml(interface_file)

        return interface_dict[telecname] if telecname else interface_dict


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
                                  exp_ref=self.ref_exps, outdir=outputdir, rebuild=rebuild, loglevel=self.loglevel)

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
                  dpi: int = 300, format: str = 'png', metadata: dict = None):
        """
        Save the plot to a file.

        Args:
            fig (matplotlib.figure.Figure): The figure to be saved.
            diagnostic_product (str): The name of the diagnostic product. Default is None.
            extra_keys (dict): Extra keys to be used for the filename (e.g. season). Default is None.
            dpi (int): The dpi of the figure. Default is 300.
            format (str): The format of the figure. Default is 'png'.
            metadata (dict): The metadata to be used for the figure. Default is None.
                             They will be complemented with the metadata from the outputsaver.
                             We usually want to add here the description of the figure.
        """
        if format == 'png':
            _ = self.outputsaver.save_png(fig, diagnostic_product=diagnostic_product,
                                          extra_keys=extra_keys, metadata=metadata)
        elif format == 'pdf':
            _ = self.outputsaver.save_pdf(fig, diagnostic_product=diagnostic_product,
                                          extra_keys=extra_keys, metadata=metadata)

def _homogeneize_maps(maps, ref_maps):
    """
    Homogeneize the maps. If a list has length 1, we convert it to a single xarray.

    Args:
        maps (list): The list of maps to be homogenized.
        ref_maps (list): The list of reference maps to be homogenized.

    Returns:
        tuple: The homogenized maps and reference maps.
    """
    if isinstance(maps, list) and len(maps) == 1:
        maps = maps[0]
    if isinstance(ref_maps, list) and len(ref_maps) == 1:
        ref_maps = ref_maps[0]

    return maps, ref_maps