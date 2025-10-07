import argparse
import os
import sys
import socket

from dask.distributed import Client, LocalCluster

from aqua import Reader
from aqua.util import load_yaml, get_arg

from ocean3d import check_variable_name
from ocean3d import stratification
from ocean3d import mld

from ocean3d import hovmoller_plot
from ocean3d import time_series
from ocean3d import multilevel_trend
from ocean3d import zonal_mean_trend

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

    def dask_cluster(self):
        if self.nworkers or self.cluster:
            if not self.cluster:
                self.cluster = LocalCluster(n_workers=self.nworkers, threads_per_worker=1)
                self.logger.info(f"Initializing private cluster {self.cluster.scheduler_address} with {self.nworkers} workers.")
                self.private_cluster = True
            else:
                self.logger.info(f"Connecting to cluster {self.cluster}.")
            self.client = Client(self.cluster)
            self.logger.info(self.client.dashboard_link)
        else:
            self.client = None
        return

    def ocean3d_config_process(self, file):
        self.logger.info('Reading configuration yaml file..')
        self.ocean3d_config_dict = load_yaml(file)
        self.logger.debug(f"Configuration file: {self.ocean3d_config_dict}")
        
        self.loglevel = getattr(self.args, "loglevel") or self.get_value_with_default(self.ocean3d_config_dict, 'loglevel', 'warning')
        self.logger = log_configure(log_name='Ocean3D CLI', log_level=self.loglevel)
        
        self.nworkers = getattr(self.args, "nworkers") or self.get_value_with_default(self.ocean3d_config_dict, 'nworkers', None)
        self.cluster = getattr(self.args, "cluster") or self.get_value_with_default(self.ocean3d_config_dict, 'cluster', None)

        
        self.config["model"] = self.get_arg('model', self.ocean3d_config_dict['model'])
        self.config["exp"] = self.get_arg('exp', self.ocean3d_config_dict['exp'])
        self.config["source"] = self.get_arg('source', self.ocean3d_config_dict['source'])
        self.config["regrid"] = self.get_arg('regrid', self.ocean3d_config_dict.get('regrid', None))
        self.config["outputdir"] = self.get_arg('outputdir', self.ocean3d_config_dict['outputdir'])
        self.config["outputdir"] = os.path.realpath(self.config['outputdir'])
        self.logger.info(f"output will be saved here: {self.config['outputdir']}")
        self.config["variables"] = self.get_value_with_default(self.ocean3d_config_dict, "variables", None)
        self.config["custom_region"] = self.get_value_with_default(self.ocean3d_config_dict,
                                                                   "custom_region", None)
        self.config["ocean_drift"] = self.get_value_with_default(self.ocean3d_config_dict,
                                                                 "ocean_drift", [])
        self.config["ocean_circulation"] = self.get_value_with_default(self.ocean3d_config_dict,
                                                                       "ocean_circulation", [])
        self.config["select_time"] = self.get_value_with_default(self.ocean3d_config_dict, "select_time", None)
        if self.config["select_time"]:
            self.config["start_year"] = self.get_value_with_default(self.config["select_time"], "start_year", [])
            self.config["end_year"] = self.get_value_with_default(self.config["select_time"], "end_year", [])
        if self.config["ocean_circulation"]:
            self.config["compare_model"] = self.get_value_with_default(self.config["ocean_circulation"], "compare_model_with_obs", None)

        # if self.ocean3d_config_dict['custom_region'] :
        #     self.config["custom_region"] = self.get_value_with_default(self.ocean3d_config_dict,"custom_region", [])

    def data_retrieve(self):
        model = self.config["model"]
        exp = self.config["exp"]
        source = self.config["source"]
        regrid = self.config["regrid"]
        self.logger.info(f"Reader selecting for model={model}, exp={exp}, source={source}, regrid={regrid}")

        reader = Reader(model=model, exp=exp, source=source,
                        fix=True, regrid=regrid, loglevel=self.loglevel)
        
        if self.config["variables"] == []:
            self.config["variables"] = None
        else: 
            self.logger.info(f"retrieving {self.config['variables']}")

        if self.config["select_time"]:
            self.data["catalog_data"] = reader.retrieve(var=self.config["variables"],
                                                        startdate=str(self.config["start_year"]),
                                                        enddate=str(self.config["end_year"]))
        else:
            self.data["catalog_data"] = reader.retrieve(var=self.config["variables"])
        if regrid:
            self.data["catalog_data"] = reader.regrid(self.data["catalog_data"])

        self.logger.info(f"data retrieved for model={model}, exp={exp}, source={source}")
        self.logger.debug("model data: %s", self.data["catalog_data"])   
        self.data["catalog_data"] = check_variable_name(self.data["catalog_data"])
        self.logger.debug("model data: %s", self.data["catalog_data"])   
        if self.config["ocean_circulation"]:
            if self.config["compare_model"]== True:
                self.logger.info("Loading Observation data")
                obs_reader = Reader("EN4", "v4.2.2", "monthly", loglevel=self.loglevel, regrid=regrid)
                self.data["obs_data"] = obs_reader.retrieve()
                if regrid:
                    self.data["obs_data"] = obs_reader.regrid(self.data["obs_data"])
                self.data["obs_data"] = check_variable_name(self.data["obs_data"], loglevel=self.loglevel)
                self.logger.info("Loaded Observation data")

                self.data["obs_data"] = self.data["obs_data"].astype('float64')
                self.data["obs_data"].coords["lev"] = self.data["obs_data"].coords["lev"].astype('float64')
                self.logger.debug("obs_data: %s", self.data["obs_data"])
        return

    def make_request(self, kwargs):
        region = kwargs.get("region", None)
        lon_s = kwargs.get("lon_s", None)
        lon_n = kwargs.get("lon_n", None)
        lon_w = kwargs.get("lon_w", None)
        lon_e = kwargs.get("lon_e", None)

        o3d_request = {
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
        if self.config["ocean_circulation"]:
            if self.config["compare_model"]== True:
                o3d_request["obs_data"] = self.data["obs_data"]
            else:
                o3d_request["obs_data"] = None

        return o3d_request

    def ocean_drift_diag_list(self, **kwargs):
        region = kwargs.get("region", None)
        o3d_request = self.make_request(kwargs)

        self.logger.warning("Running the Ocean Drift diags for %s", region)
        
        if "hovmoller" in self.config["ocean_drift"]["plots"]:
            self.logger.info("Evaluating Hovmoller plot")
            hovmoller_plot_init = hovmoller_plot(o3d_request)
            hovmoller_plot_init.plot()
            self.logger.warning("Hovmoller plot completed")

        if "time_series" in self.config["ocean_drift"]["plots"]:
            self.logger.info("Evaluating time series plot")
            time_series_plot = time_series(o3d_request)
            time_series_plot.plot()
            self.logger.warning("Time series plot completed")

        if "multilevel_trend" in self.config["ocean_drift"]["plots"]:
            # if self.client:
            #     self.client.restart()
            self.logger.info("Evaluating multilevel trend")
            trend = multilevel_trend(o3d_request)
            trend.plot()
            self.logger.warning("Multi-level trend plot completed")
            
        if "zonal_trend" in self.config["ocean_drift"]["plots"]:
            # if self.client:
            #     self.client.restart()
            self.logger.info("Evaluating zonal mean trend")
            zonal_trend = zonal_mean_trend(o3d_request)
            zonal_trend.plot()
            self.logger.warning("Zonal trend plot completed")

        self.logger.warning(f"Finished ocean drift diags for {region}")

    def ocean_circulation_diag_list(self, **kwargs):
        region = kwargs.get("region", None)
        self.logger.warning("Running the Ocean circulation diags for %s", region)
        
        time = kwargs.get("time")
        o3d_request = self.make_request(kwargs)

        if "stratification" in self.config["ocean_circulation"]["plots"]:
            self.logger.info("Evaluating stratification")
            o3d_request["time"] = time
            strat = stratification(o3d_request)
            strat.plot()
            self.logger.warning("Stratification plot completed")
        
        if "MLD" in self.config["ocean_circulation"]["plots"]:
            self.logger.info("Evaluating Mixed layer depth")
            o3d_request["time"] = time
            mld_init = mld(o3d_request)
            mld_init.plot()
            self.logger.warning("Mixed-layer-depth plot completed")
        
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
        self.nworkers = get_arg(args, 'nworkers', None)
        self.cluster = get_arg(args, 'cluster', None)


        # Change the current directory to the one of the CLI so that relative paths work
        abspath = os.path.abspath(__file__)
        dname = os.path.dirname(abspath)
        if os.getcwd() != dname:
            os.chdir(dname)
            self.logger.info(f'Moving from current directory to {dname} to run!')

        self.logger.info("Running ocean3d diagnostic...")

        # Read configuration file
        file = self.get_arg('config', 'config.yaml')

        self.ocean3d_config_process(file)

        self.dask_cluster()
        
        self.data_retrieve()
        if self.config["ocean_drift"]:
            self.ocean_drifts_diags()
        if self.config["ocean_circulation"]:
            self.ocean_circulation_diags()

        if self.client:
            self.client.close()
            self.logger.debug("Dask client closed.")

        if getattr(self, 'private_cluster', None):
            self.cluster.close()
            self.logger.debug("Dask cluster closed.")

        self.logger.warning("Ocean3D diagnostic has finished.")


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
    parser.add_argument('--catalog', type=str, help='Catalog name', required=False) # not used yet
    parser.add_argument('--model', type=str, help='Model name')
    parser.add_argument('--exp', type=str, help='Experiment name')
    parser.add_argument('--source', type=str, help='Source name')
    parser.add_argument('--regrid', type=str, help='Regrid target grid')
    #TODO: not used yet
    parser.add_argument('--realization', type=str, default=None,
                        help='Realization name (default: None)')
    parser.add_argument('--outputdir', type=str,
                        help='Output directory')
    parser.add_argument("--cluster", type=str,
                        required=False, help="dask cluster address")

    return parser.parse_args(args)


if __name__ == '__main__':
    args = parse_arguments(sys.argv[1:])

    ocean3d_cli = Ocean3DCLI(args)
    ocean3d_cli.run_diagnostic()
