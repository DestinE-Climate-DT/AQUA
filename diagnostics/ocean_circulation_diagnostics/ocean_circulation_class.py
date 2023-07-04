from aqua import Reader,catalogue, inspect_catalogue
import ocean_circulation_func as fn
from aqua.util import load_yaml


class Ocean_circulationDiagnostic:
    def __init__(self, model, exp, source):
        self.reader = Reader(model=model, exp=exp, source=source)
        self.data = self.reader.retrieve()
        self.data = self.data.rename({"nz1":"lev"})
        self.data = self.data.rename({"ocpt":"thetao"})
        self.data = self.data[["thetao","so"]]
        
    def process_data(self):
        yearly_data = self.data.resample(time="Y").mean()
        self.labrador_sea_mean=fn.weighted_area_mean(yearly_data,50, 65, 300, 325)
        self.converted_labrador_sea_mean= fn.convert_variables(self.labrador_sea_mean)
        
    def plot_profile(self):
        fn.plot_temporal_split(self.converted_labrador_sea_mean, "Labrador Sea")
        
    
    def run_diagnostics(self):
        self.process_data()
        self.plot_profile()