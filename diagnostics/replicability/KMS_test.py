from aqua.util import load_yaml, get_arg, create_folder, dump_yaml
import pandas as pd
from pandas import json_normalize
import glob

machines = ["LUMI-C", "LUMI-G"]
# machines = ["lumi", "mn4"]
# ensembles = ["p000", "p001", "p002", "p003"]
ensembles = ["fc0", "fc1", "fc2", "fc3","fc4", "fc5", "fc6", "fc7", "fc8", "fc9"]

consolidated_df = pd.DataFrame()
data= {}
for machine in machines:
    data_ensembles= {}
    for ensemble in ensembles:
        # pattern = f"/scratch/project_465000454/sughosh/ecmean_files/YAML/*{machine}*/*{ensemble}*yml"
        pattern = f"/pfs/lustrep3/scratch/project_465000454/kkeller/lab/aqua_container/replicability/out/*{machine}*/*/*{ensemble}*yml"
        print(pattern)
        print(machine, ensemble)
        yaml_files = glob.glob(pattern)
        loaded_yaml= load_yaml(yaml_files[0])
        data_ensembles[ensemble] = loaded_yaml
        # print(pattern)
    data[machine] = data_ensembles
    
    
machines = []
ensembles = []
variables = []
seasons = []
regions = []
values = []

for machine in data:
    for ensemble in data[machine]:
        for variable in data[machine][ensemble]:
            for season in data[machine][ensemble][variable]:
                for region in data[machine][ensemble][variable][season]:
                        machines.append(machine)
                        ensembles.append(ensemble)
                        variables.append(variable)
                        seasons.append(season)
                        regions.append(region)
                        value = data[machine][ensemble][variable][season][region]
                        values.append( value )

                   
# Create a DataFrame
df = pd.DataFrame({'machines': machines, 'ensembles': ensembles, 'variables': variables, 'seasons': seasons, 'locations': regions, 'pi_score': values})
# df



vars = []
seasons = []
regions = []
p_values = []
KS_score = {}

for variable in df.variables.unique():
        for season in df.seasons.unique():
                for location in df.locations.unique():
                        filtered_df= df[(df["variables"] == variable) & (df["seasons"] == season) & (df["locations"]== location)]
                        machine1= filtered_df[(filtered_df["machines"]==df.machines.unique()[0])]
                        machine2= filtered_df[(filtered_df["machines"]==df.machines.unique()[1])]
                        vars.append(variable)
                        seasons.append(season+"_"+location)
                        regions.append(location)
                        
                        ks_score, pvalue = ks_2samp(machine1.pi_score.values, machine2.pi_score.values) 
                        p_values.append(pvalue)
                        
                        
                        
                        
data = {'variable': vars, "season": seasons,  'p_value': p_values}
df_new = pd.DataFrame(data)
df_new




import matplotlib.pyplot as plt
import seaborn as sns
sns.set()

flights = df_new.pivot_table(index="variable", columns="season", values="p_value", aggfunc='mean')

fig, ax = plt.subplots(figsize=(15,10)) 
sns.heatmap(flights, annot=True, linewidths=.5)
plt.title("P values of Kolmogorov-Smirnov test (LUMI C vs LUMI G)",fontsize=17, fontweight='bold')
# sns.heatmap(glue)
# plt.savefig("/gpfs/scratch/dese28/dese28422/ecmean_files/figs/mn4_lumi_KS_test_p_value.png", bbox_inches='tight')
plt.savefig("/scratch/project_465000454/sughosh/AQUA/diagnostics/ecmean/notebook/lumi_C_G_KS_test_p_value.png", bbox_inches='tight')