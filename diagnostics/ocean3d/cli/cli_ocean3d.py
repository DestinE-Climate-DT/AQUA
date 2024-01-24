import argparse
import os
import sys

AQUA = os.environ.get("AQUA")
if AQUA is not None:
    ocean3d_path = os.path.join(AQUA, 'diagnostics/ocean3d')
    sys.path.insert(0, ocean3d_path)
    print(f"Attached Ocean3d from this path {ocean3d_path}")
else:
    print("AQUA environment variable is not defined. Going to use default ocean3d package!")

from aqua import Reader
from aqua.util import load_yaml, get_arg, create_folder

from ocean3d import check_variable_name
from ocean3d import time_slicing
from ocean3d import plot_stratification
from ocean3d import plot_spatial_mld_clim

from ocean3d import hovmoller_lev_time_plot
from ocean3d import time_series_multilevs
from ocean3d import multilevel_t_s_trend_plot
from ocean3d import zonal_mean_trend_plot

from aqua.util import find_vert_coord
from aqua.logger import log_configure


class Ocean3DCLI:
    def __init__(self, args):
        self.args = args
        self.loglevel = {}
        self.logger = log_configure(log_name='Ocean3D CLI', log_level='WARNING')  # Initialize the logger
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

        self.config["model"] = self.get_arg('model', self.ocean3d_config_dict['model'])
        self.config["exp"] = self.get_arg('exp', self.ocean3d_config_dict['exp'])
        self.config["source"] = self.get_arg('source', self.ocean3d_config_dict['source'])
        self.config["outputdir"] = self.get_arg('outputdir', self.ocean3d_config_dict['outputdir'])

        self.config["custom_region"] = self.get_value_with_default(self.ocean3d_config_dict,
                                                "custom_region", None)
        self.config["predefined_regions"] = self.get_value_with_default(self.ocean3d_config_dict,
                                                    "predefined_regions", [])
        self.config["time_selection"] = self.get_value_with_default(self.ocean3d_config_dict,"time_selection", None)
        if self.config["time_selection"] == True:
            self.config["start_year"] = self.get_value_with_default(self.ocean3d_config_dict,"start_year", [])
            self.config["end_year"] = self.get_value_with_default(self.ocean3d_config_dict,"end_year", [])
        
        # if self.ocean3d_config_dict['custom_region'] :
        #     self.config["custom_region"] = self.get_value_with_default(self.ocean3d_config_dict,"custom_region", [])
            
    def data_retrieve(self):
        model = self.config["model"]
        exp = self.config["exp"]
        source = self.config["source"]
        self.logger.info(f"Reader selecting for model={model}, exp={exp}, source={source}")
        
        reader = Reader(model=model, exp=exp, source=source,
                        fix=True, loglevel=self.loglevel)
        data = reader.retrieve()
        data=data.rename({"time_counter":"time"})
        # data=data.rename_dims({"deptht":"lev"})
        # data=data.rename_vars({"toce_mean":"ocpt"})
        # data=data.rename_vars({"soce_mean":"so"})
        # data=data.drop_dims("bnds")
        # data=data[["ocpt","so"]]
        
        data = check_variable_name(data)
        if self.config["time_selection"] == True:
            self.data["catalog_data"] = time_slicing(data,self.config["start_year"],
                                                     self.config["end_year"])
        else:
            self.data["catalog_data"]= data
            
    def ocean3d_diags(self, region=None,
                  latS: float = None,
                  latN: float = None,
                  lonW: float = None,
                  lonE: float = None,
                  ):
        
        o3d_request={
            "model":self.config["model"],
            "exp":self.config["exp"],
            "source":self.config["source"],
            "data":self.data["catalog_data"],
            "region":region,
            "latS":latS,
            "latN":latN,
            "lonW":lonW,
            "lonE":lonE,
            "output":True,
            "output_dir":self.config["outputdir"]
        }
        
        

        
        self.logger.debug("Evaluating Hovmoller plots")
         
        
        hovmoller_plot = hovmoller_lev_time_plot(o3d_request)
        hovmoller_plot.plot()

        self.logger.debug("Evaluating time series multilevels")
        time_series_multilevs(o3d_request,
                            anomaly=False, standardise=False, customise_level=False, levels=list)
        time_series_multilevs(o3d_request,
                            anomaly=True, standardise=False, anomaly_ref="tmean", customise_level=False, levels=list)
        time_series_multilevs(o3d_request,
                            anomaly=True, standardise=False, anomaly_ref="t0", customise_level=False, levels=list)
        time_series_multilevs(o3d_request,
                            anomaly=True, standardise=True, anomaly_ref="tmean", customise_level=False, levels=list)
        time_series_multilevs(o3d_request,
                            anomaly=True, standardise=True, anomaly_ref="t0", customise_level=False, levels=list)

        self.logger.debug("Evaluating multilevel_t_s_trend_plot")
        multilevel_t_s_trend_plot(o3d_request,
                                customise_level=False, levels=None)

        self.logger.debug("Evaluating zonal_mean_trend_plot")
        zonal_mean_trend_plot(o3d_request)

        for time in range(1, 18):  # 1 to 12 is the months, then each number directs the seasonals and the yearly climatology
            self.logger.debug("Evaluating plot_stratification, time: %s", time)
            plot_stratification(o3d_request,
                                time=time)
            plot_spatial_mld_clim(o3d_request,
                                  time = time, overlap= True)

    def custom_region_diag(self):
        if self.config["custom_region"] != None:
            self.logger.info("Analysing custom regions")
            custom_regions = self.config["custom_region"] ### add fix if not present
            custom_region_dict = {}
            for custom_region in custom_regions:
                for coord in custom_region:
                    custom_region_dict.update(coord)
                lonE = custom_region_dict["lonE"]
                lonW = custom_region_dict["lonW"]
                latS = custom_region_dict["latS"]
                latN = custom_region_dict["latN"]

                self.logger.debug("lonE: %s, lonW: %s, latS: %s, latN: %s",
                             lonE, lonW, latS, latN)

                self.ocean3d_diags(region=None, latS=latS,
                                   latN=latN, lonW=lonW, lonE=lonE)

    def predefined_region_diag(self):
        if self.config["predefined_regions"]:
            predefined_regions = self.config["predefined_regions"] ### add fix if not present
            for predefined_region in predefined_regions:
                self.logger.info("Analysing predefined regions")
                self.logger.debug("predefined_region: %s", predefined_region)
                self.ocean3d_diags(region=predefined_region)

    def run_diagnostic(self):

        self.loglevel = self.get_arg('loglevel', 'WARNING')
        self.logger = log_configure(log_name='Ocean3D CLI', log_level= self.loglevel)

        # Change the current directory to the one of the CLI so that relative paths work
        abspath = os.path.abspath(__file__)
        dname = os.path.dirname(abspath)
        if os.getcwd() != dname:
            os.chdir(dname)
            self.logger.info(f'Moving from current directory to {dname} to run!')

        self.logger.info("Running ocean3d diagnostic...")

        # Read configuration file
        file = self.get_arg('config', 'config.yaml')
        self.logger.info('Reading configuration yaml file..')

        self.ocean3d_config_process(file)
        
        self.data_retrieve()
        
        self.custom_region_diag()
        self.predefined_region_diag()


def parse_arguments(args):
    """Parse command line arguments"""

    parser = argparse.ArgumentParser(description='Ocean3D CLI')

    parser.add_argument('--config', type=str,
                        help='yaml configuration file')
    parser.add_argument('-l', '--loglevel', type=str,
                        help='log level [default: WARNING]')

    # This arguments will override the configuration file is provided
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
