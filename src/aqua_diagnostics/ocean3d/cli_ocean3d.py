import argparse
import os
import sys

from dask.distributed import Client, LocalCluster

from aqua import Reader
from aqua.util import load_yaml

from aqua.diagnostics.ocean3d import check_variable_name
from aqua.diagnostics.ocean3d import stratification
from aqua.diagnostics.ocean3d import plot_spatial_mld_clim
from aqua.diagnostics.ocean3d import load_obs_data

from aqua.diagnostics.ocean3d import hovmoller_plot
from aqua.diagnostics.ocean3d import time_series
from aqua.diagnostics.ocean3d import multilevel_trend
from aqua.diagnostics.ocean3d import zonal_mean_trend

from aqua.logger import log_configure


class Ocean3DCLI:
    def __init__(self, args):
        self.args = args
        self.loglevel = {}
        self.config = {}
        self.data = {}

    def get_arg(self, arg, default):
        """
        Support function to get arguments

        Args:
            args: the arguments
            arg: the argument to get
            default: the default value

        Returns:
            The argument value or the default value
        """

        res = getattr(self.args, arg)
        if not res:
            res = default
        return res

    def get_value_with_default(self, dictionary, key, default_value):
        try:
            return dictionary[key]
        except KeyError:
            return default_value

    def ocean3d_config_process(self, file):
        self.ocean3d_config_dict = load_yaml(file)

        self.logger.debug(f"Configuration file: {self.ocean3d_config_dict}")

        self.config["catalog"] = self.get_arg('catalog', self.ocean3d_config_dict['catalog'])
        self.config["model"] = self.get_arg('model', self.ocean3d_config_dict['model'])
        self.config["exp"] = self.get_arg('exp', self.ocean3d_config_dict['exp'])
        self.config["source"] = self.get_arg('source', self.ocean3d_config_dict['source'])
        self.config["outputdir"] = self.get_arg('outputdir', self.ocean3d_config_dict['outputdir'])

        self.config["custom_region"] = self.get_value_with_default(self.ocean3d_config_dict,
                                                                   "custom_region", None)
        self.config["ocean_drift"] = self.get_value_with_default(self.ocean3d_config_dict,
                                                                 "ocean_drift", [])
        self.config["ocean_circulation"] = self.get_value_with_default(self.ocean3d_config_dict,
                                                                       "ocean_circulation", [])
        self.config["select_time"] = self.get_value_with_default(self.ocean3d_config_dict, "select_time", None)
        if self.config["select_time"] is True:
            time_range = self.get_value_with_default(self.ocean3d_config_dict, "time_range", [])
            self.config["start_year"] = self.get_value_with_default(time_range, "start_year", [])
            self.config["end_year"] = self.get_value_with_default(time_range, "end_year", [])
        self.config["compare_model"] = self.get_value_with_default(self.ocean3d_config_dict, "compare_model_with_obs", None)

        # if self.ocean3d_config_dict['custom_region'] :
        #     self.config["custom_region"] = self.get_value_with_default(self.ocean3d_config_dict,"custom_region", [])

    def data_retrieve(self):
        catalog = self.config["catalog"]
        model = self.config["model"]
        exp = self.config["exp"]
        source = self.config["source"]
        self.logger.info(f"Reader selecting for model={model}, exp={exp}, source={source}")

        reader = Reader(catalog=catalog, model=model, exp=exp, source=source,
                        fix=True, loglevel=self.loglevel)
        
        self.logger.info(f"data retrieved for catalog= {catalog}, model={model}, exp={exp}, source={source}")
        
        if self.config["select_time"] == True:
            self.data["catalog_data"] = reader.retrieve(startdate= str(self.config["start_year"]),
                                                        enddate= str(self.config["end_year"]))
        else:
            self.data["catalog_data"] = reader.retrieve()
            
        self.data["catalog_data"] = check_variable_name(self.data["catalog_data"])

        if self.config["compare_model"]== True:
            self.data["obs_data"] = load_obs_data(model='EN4', exp='en4', source='monthly')
        self.data["obs_data"] = check_variable_name(self.data["obs_data"])
        
        return

    def make_request(self, kwargs):
        region = kwargs.get("region", None)
        lon_s = kwargs.get("lon_s", None)
        lon_n = kwargs.get("lon_n", None)
        lon_w = kwargs.get("lon_w", None)
        lon_e = kwargs.get("lon_e", None)

        o3d_request = {
                    "catalog": self.config["catalog"],
                    "model": self.config["model"],
                    "exp": self.config["exp"],
                    "source": self.config["source"],
                    "data": self.data["catalog_data"],
                    "region": region,
                    "lon_s": lon_s,
                    "lon_n": lon_n,
                    "lon_w": lon_w,
                    "lon_e": lon_e,
                    "output": True,
                    "output_dir": self.config["outputdir"],
                    "loglevel": self.loglevel
                    }
        if self.config["compare_model"]:
            o3d_request["obs_data"] = self.data["obs_data"]

        return o3d_request

    def ocean_drift_diag_list(self, **kwargs):
        region = kwargs.get("region", None)
        o3d_request = self.make_request(kwargs)

        self.logger.warning("Running the Ocean Drift diags for %s", region)
        self.logger.info("Evaluating Hovmoller plot")
        hovmoller_plot_init = hovmoller_plot(o3d_request)
        hovmoller_plot_init.plot()

        self.logger.info("Evaluating time series plot")
        time_series_plot = time_series(o3d_request)
        time_series_plot.plot()

        self.logger.info("Evaluating multilevel trend")
        trend = multilevel_trend(o3d_request)
        trend.plot()

        self.logger.info("Evaluating zonal mean trend")
        zonal_trend = zonal_mean_trend(o3d_request)
        zonal_trend.plot()

        self.logger.warning(f"Finished the diags for {region}")

    def ocean_circulation_diag_list(self, **kwargs):
        region = kwargs.get("region", None)
        self.logger.warning("Running the Ocean circulation diags for %s", region)

        time = kwargs.get("time")
        o3d_request = self.make_request(kwargs)

        self.logger.info("Evaluating stratification")
        o3d_request["time"] = time
        strat = stratification(o3d_request)
        strat.plot()
        
        self.logger.info("Evaluating Mixed layer depth")
        o3d_request["time"] = time
        plot_spatial_mld_clim(o3d_request)
        
        self.logger.warning(f"Finished the diags for {region}")

    def ocean_drifts_diags(self):
        if self.config["ocean_drift"]["regions"]:
            regions = self.config["ocean_drift"]["regions"]  # Add fix if not present
            for region in regions:
                self.logger.debug("Analysing predefined regions")
                self.logger.debug("region: %s", region)
                self.ocean_drift_diag_list(region=region)
        return

    def ocean_circulation_diags(self):
        if self.config["ocean_circulation"]["regions"]:
            regions = self.config["ocean_circulation"]["regions"]
            for region, clim_time in regions.items():
                self.ocean_circulation_diag_list(region=region, time=clim_time)

    def run_diagnostic(self):

        self.loglevel = self.get_arg('loglevel', 'WARNING')
        self.logger = log_configure(log_name='Ocean3D CLI', log_level=self.loglevel)

        # Dask distributed cluster
        nworkers = self.get_arg('nworkers', None)
        if nworkers:
            cluster = LocalCluster(n_workers=nworkers, threads_per_worker=1)
            client = Client(cluster)
            self.logger.info(f"Running with {nworkers} dask distributed workers.")

        # Change the current directory to the one of the CLI so that relative paths work
        abspath = os.path.abspath(__file__)
        dname = os.path.dirname(abspath)
        if os.getcwd() != dname:
            os.chdir(dname)
            self.logger.info(f'Moving from current directory to {dname} to run!')

        self.logger.info("Running ocean3d diagnostic...")

        # Read configuration file
        file = self.get_arg('config', '~/.aqua/diagnostics/ocean3d/cli/config.yaml')
        file = os.path.expanduser(file)
        self.logger.info('Reading configuration yaml file..')

        self.ocean3d_config_process(file)

        self.data_retrieve()
        if self.config["ocean_drift"]:
            self.ocean_drifts_diags()
        if self.config["ocean_circulation"]:
            self.ocean_circulation_diags()

        self.logger.warning("Ocean3D diagnostic terminated!")


def parse_arguments(args):
    """Parse command line arguments"""

    parser = argparse.ArgumentParser(description='Ocean3D CLI')

    parser.add_argument('--config', type=str,
                        help='yaml configuration file')
    parser.add_argument('-n', '--nworkers', type=int,
                        help='number of dask distributed workers')
    parser.add_argument('-l', '--loglevel', type=str,
                        help='log level [default: WARNING]')

    # This arguments will override the configuration file is provided
    parser.add_argument('--catalog', type=str, help='catalog name')
    parser.add_argument('--model', type=str, help='Model name')
    parser.add_argument('--exp', type=str, help='Experiment name')
    parser.add_argument('--source', type=str, help='Source name')
    parser.add_argument('--outputdir', type=str,
                        help='Output directory')

    return parser.parse_args(args)


if __name__ == '__main__':
    args = parse_arguments(sys.argv[1:])

    ocean3d_cli = Ocean3DCLI(args)
    ocean3d_cli.run_diagnostic()
