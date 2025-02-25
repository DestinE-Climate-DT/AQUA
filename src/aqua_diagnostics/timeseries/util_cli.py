from aqua.logger import log_configure
from aqua.diagnostics.timeseries import Timeseries

class TimeseriesCLI():

    def __init__(self, config_dict: dict, var: str, formulae: bool = False,
                 loglevel: str = 'WARNING'):
        """
        Initialize the TimeseriesCLI class.
        
        Args:
            config_dict (dict): The configuration dictionary.
            var (str): The variable to run the timeseries diagnostics for.
            formulae (bool): Whether the requested variable is a formula.
            loglevel (str): The logging level. Default is 'WARNING'.
        """
        
        self.logger = log_configure(log_level=loglevel, log_name='Timeseries CLI')
        self.loglevel = loglevel
        self.config_dict = config_dict
        # We need the lengths to have a list of results with the correct length
        self.len_datasets = len(self.config_dict['datasets'])
        self.len_references = len(self.config_dict['references'])
        self.var = var
        self.formulae = formulae
    
    def run(self, regrid: str = None, std_startdate: str = None, std_enddate: str = None,
            region: str = None, long_name: str = None, units: str = None,
            standard_name: str = None, freq: list = ['monthly', 'annual'],
            outputdir: str = './', rebuild: bool = True):
        """
        Run the timeseries diagnostics for the given variable and configuration."
        """
        init_args = {'regrid': regrid, 'region': region, 'loglevel': self.loglevel}
        run_args = {'var': self.var, 'formulae': self.formulae, 'long_name': long_name,
                    'units': units, 'standard_name': standard_name, 'freq': freq,
                    'outputdir': outputdir, 'rebuild': rebuild}
        # Run the datasets
        for dataset in self.config_dict['datasets']:
            self.logger.debug(f'Running dataset: {dataset}, variable: {self.var}')
            dataset_args = {'catalog': dataset['catalog'], 'model': dataset['model'],
                            'exp': dataset['exp'], 'source': dataset['source']}
            timeseries = Timeseries(**dataset_args, **init_args)
            timeseries.run(**run_args)
        
        # Run the references
        for reference in self.config_dict['references']:
            self.logger.debug(f'Running reference: {reference}, variable: {self.var}')
            reference_args = {'catalog': reference['catalog'], 'model': reference['model'],
                              'exp': reference['exp'], 'source': reference['source'],
                              'std_startdate': std_startdate, 'std_enddate': std_enddate}
            timeseries = Timeseries(**reference_args, **init_args)
            timeseries.run(std=True, **run_args)