from aqua import Reader,catalogue, inspect_catalogue
from aqua.util import load_yaml


class Global_OceanDiagnostic:
    def __init__(self, model, exp, source):
        self.reader = Reader(model=model, exp=exp, source=source)
        self.yearly_data= self.reader.retrieve()[["ocpt","so"]].resample(time="Y").mean()
        self.yearly_data= self.yearly_data.rename({"nz1":"lev"})
        self.yearly_data= self.yearly_data.rename({"ocpt":"thetao"})
        self.config = load_yaml("config.yaml")
        self.outputfig= self.config["outputfig"]

        
    def process_data(self):
        self.global_mean_anom = fn.std_anom_wrt_initial(self.yearly_data, use_predefined_region=True, region="Global Ocean")
        self.atlantic_mean_anom = fn.std_anom_wrt_initial(self.yearly_data, use_predefined_region=True, region="Atlantic Ocean")
        self.pacific_mean_anom = fn.std_anom_wrt_initial(self.yearly_data, use_predefined_region=True, region="Pacific Ocean")
        self.indian_mean_anom = fn.std_anom_wrt_initial(self.yearly_data, use_predefined_region=True, region="Indian Ocean")
        self.arctic_mean_anom = fn.std_anom_wrt_initial(self.yearly_data, use_predefined_region=True, region="Arctic Ocean")
        self.southern_mean_anom = fn.std_anom_wrt_initial(self.yearly_data, use_predefined_region=True, region="Southern Ocean")


    def plot_profile(self):
        
        fn.thetao_so_anom_plot(self.global_mean_anom,"GLOBAL Ocean", self.outputfig)
        fn.thetao_so_anom_plot(self.atlantic_mean_anom,"Atlantic Ocean", self.outputfig)
        fn.thetao_so_anom_plot(self.pacific_mean_anom,"Pacific Ocean", self.outputfig)
        fn.thetao_so_anom_plot(self.indian_mean_anom,"Indian Ocean", self.outputfig)
        fn.thetao_so_anom_plot(self.arctic_mean_anom,"Arctic Ocean", self.outputfig)
        fn.thetao_so_anom_plot(self.southern_mean_anom,"Southern Ocean", self.outputfig)

        fn.time_series(self.global_mean_anom,'Global Ocean', self.outputfig)
        fn.time_series(self.atlantic_mean_anom,'Atlantic Ocean', self.outputfig)
        fn.time_series(self.pacific_mean_anom,'Pacific Ocean', self.outputfig)
        fn.time_series(self.indian_mean_anom,'Indian Ocean', self.outputfig)
        fn.time_series(self.arctic_mean_anom,'Arctic Ocean', self.outputfig)
        fn.time_series(self.southern_mean_anom,'Southern Ocean', self.outputfig)
    
    def run(self):
        self.process_data()
        self.plot_profile()