import os
import healpy as hp
import xarray as xr
from matplotlib import pyplot as plt
from aqua.util import create_folder
from aqua.diagnostics.core import Diagnostic


class PlotETCCDI(Diagnostic):
    def __init__(self, catalog: str = None, model: str = None,
                 exp: str = None, source: str = None, index: str = None,
                 loglevel: str = 'WARNING'):
        """
        Plot ETCCDI index

        Args:
            catalog (str): catalog name
            model (str): model name
            exp (str): experiment name
            source (str): source name
            index (str): ETCCDI index name
            loglevel (str): logging level. Default is 'WARNING'
        """
        super().__init__(catalog=catalog, model=model, exp=exp, source=source, loglevel=loglevel)

        if index is None:
            raise ValueError('ETCCDI index name must be provided')
        self.index_name = index

    def plot_index(self, index: xr.DataArray, year: int,
                   outputdir: str = "./", format: str = 'pdf',
                   fillna: float = None, title: str = None, unit: str = None,
                   norm: str = 'lin', cmap: str = 'viridis', min: float = None,
                   max: float = None):
        """
        Plot ETCCDI index

        Args:
            index (xr.DataArray): ETCCDI index
            year (int): year of the data
            format (str): output format. Default is 'pdf'
            outputdir (str): output directory. Default is "./"
            fillna (float): fill NaN values with this value. Default is None, no fill applied
            title (str): plot title. Default is auto-generated
            unit (str): unit of the index. Default is derived from the index.units attribute
            norm (str): plot normalization. Default is 'lin'
            cmap (str): colormap. Default is 'viridis'
            min (float): minimum value for the colorbar. Default is None
            max (float): maximum value for the colorbar. Default is None
        """
        if fillna is not None:
            index = index.fillna(fillna)

        if title is None:
            title = f"ETCCDI {self.index_name} {year} {self.model} {self.exp}"

        if unit is None:
            unit = index.units

        hp.mollview(index, title=title, unit=unit, norm=norm, cmap=cmap,
                    flip='geo', nest=True, min=min, max=max)

        # HACK: healpy does not return any figure object, so that the default OutputSaver
        # will not work. We need to save the figure manually
        outputdir = os.path.join(outputdir, format)
        create_folder(outputdir, loglevel=self.loglevel)

        filename = f"ETCCDI.{self.index_name}.{year}.{self.model}.{self.exp}.{format}"

        plt.savefig(os.path.join(outputdir, filename), format=format)
