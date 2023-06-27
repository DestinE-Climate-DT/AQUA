from aqua import Reader,catalogue, inspect_catalogue
import global_ocean_func as fn


class Global_OceanDiagnostic:
    def __init__(self, model, exp, source):
        self.reader = Reader(model=model, exp=exp, source=source)
        self.data = self.reader.retrieve()
        self.data = self.data.rename({"nz1":"lev"})
        self.data = self.data.rename({"ocpt":"thetao"})
        self.data = self.data[["thetao","so"]]
        
    def process_data(self):
        yearly_data = self.data.resample(time="Y").mean()

        self.global_mean_anom=fn.std_anom_wrt_initial(yearly_data,-90,90,0,360)
        self.atlantic_mean_anom=fn.std_anom_wrt_initial(yearly_data,-35,65,-80,30)
        self.pacific_mean_anom=fn.std_anom_wrt_initial(yearly_data,-55,65,120,290)
        self.indian_mean_anom=fn.std_anom_wrt_initial(yearly_data,-35,30,30,115)
        self.arctic_mean_anom=fn.std_anom_wrt_initial(yearly_data,65,90,0,360)
        self.southern_mean_anom=fn.std_anom_wrt_initial(yearly_data,-80,-55,-180,180)


    def plot_profile(self):
        
        fn.thetao_so_anom_plot(self.global_mean_anom,"Standardised GLOBAL Ocean Anomalies (wrt first value)")
        fn.thetao_so_anom_plot(self.atlantic_mean_anom,"Standardised Atlantic Ocean [35S-65N; 80W-30E] Anomalies (wrt first value)")
        fn.thetao_so_anom_plot(self.pacific_mean_anom,"Standardised Pacific Ocean [55S-65N;120E-70W] Anomalies (wrt first value)")
        fn.thetao_so_anom_plot(self.indian_mean_anom,"Standardised Indian Ocean [35S-30N;30E-115E] Anomalies (wrt first value)")
        fn.thetao_so_anom_plot(self.arctic_mean_anom,"Standardised Arctic Ocean [65N-90N] Anomalies (wrt first value)")
        fn.thetao_so_anom_plot(self.southern_mean_anom,"Standardised Southern Ocean [80S-55S] Anomalies (wrt first value)")

        fn.time_series(self.global_mean_anom,'Standardised Global Ocean T,S Anomalies (wrt first value)')
        fn.time_series(self.atlantic_mean_anom,'Standardised Atlantic Ocean T,S Anomalies (wrt first value)')
        fn.time_series(self.pacific_mean_anom,'Standardised Pacific Ocean T,S Anomalies (wrt first value)')
        fn.time_series(self.indian_mean_anom,'Standardised Indian Ocean T,S Anomalies (wrt first value)')
        fn.time_series(self.arctic_mean_anom,'Standardised Arctic Ocean T,S Anomalies (wrt first value)')
        fn.time_series(self.southern_mean_anom,'Standardised Southern Ocean T,S Anomalies (wrt first value)')
    
    def run_diagnostics(self):
        self.process_data()
        self.plot_profile()