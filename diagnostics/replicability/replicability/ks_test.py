from aqua.util import load_yaml, get_arg, create_folder, dump_yaml
import pandas as pd
from scipy.stats import ks_2samp
import numpy as np
import glob
import matplotlib.pyplot as plt
import seaborn as sns
sns.set()

class ks_test:
    def __init__(self, *args):
        args = args[0]
        self.ensemble1 = args.get("ensemble1")
        self.ensemble2 = args.get("ensemble2")
        
        self.ensemble1_PI_dir = args.get("ensemble1_PI_dir")
        self.ensemble2_PI_dir = args.get("ensemble2_PI_dir")
        
        self.members = args.get("members")
        self.outputdir = args.get("outputdir")
        
    
    def PI_dict(self, ensemble, ensemble_PI_dir, members):
        pi_score = {}
        data_members= {}
        for member in members:
            pattern = f"{ensemble_PI_dir}/*{member}*yml"
            yaml_files = glob.glob(pattern)
            loaded_yaml= load_yaml(yaml_files[0])
            data_members[member] = loaded_yaml
        pi_score[ensemble] = data_members
        return pi_score
    
    def combine_PI_dict(self):
        ensemble1_PI_score = self.PI_dict(self.ensemble1, self.ensemble1_PI_dir, self.members)
        ensemble2_PI_score = self.PI_dict(self.ensemble2, self.ensemble2_PI_dir, self.members)
        self.pi_score = {
            **ensemble1_PI_score,
            **ensemble2_PI_score,
        }
        return 
    
    def pi_score_df(self):
        pi_score_dict = self.pi_score
        ensembles = []
        members = []
        variables = []
        seasons = []
        regions = []
        values = []

        for ensemble in pi_score_dict:
            for member in pi_score_dict[ensemble]:
                for variable in pi_score_dict[ensemble][member]:
                    for season in pi_score_dict[ensemble][member][variable]:
                        for region in pi_score_dict[ensemble][member][variable][season]:
                            ensembles.append(ensemble)
                            members.append(member)
                            variables.append(variable)
                            seasons.append(season)
                            regions.append(region)
                            value = pi_score_dict[ensemble][member][variable][season][region]
                            values.append(value)
        # Create a DataFrame
        self.df = pd.DataFrame({'ensembles': ensembles, 'members': members, 'variables': variables, 'seasons': seasons, 'locations': regions, 'pi_score': values})
        return 

    def categorize(self, value):
        if value < 0.01:
            return "p-values < 0.01"
        elif 0.01 <= value < 0.05:
            return "0.05 > p-values > 0.01"
        elif value >= 0.05:
            return "0.05 < p-values"
        elif value == np.nan:
            return None
        
    def km_test_score_df(self):
        df = self.df
        vars = []
        seasons = []
        regions = []
        p_values = []
        KS_score = {}

        for variable in df.variables.unique():
                for season in df.seasons.unique():
                        for location in df.locations.unique():
                                filtered_df= df[(df["variables"] == variable) & (df["seasons"] == season) & (df["locations"]== location)]
                                ensemble1= filtered_df[(filtered_df["ensembles"]==df.ensembles.unique()[0])]
                                ensemble2= filtered_df[(filtered_df["ensembles"]==df.ensembles.unique()[1])]
                                vars.append(variable)
                                seasons.append(season+"_"+location)
                                regions.append(location)
                                
                                if np.isnan(ensemble1.pi_score.values).any():
                                    ks_score, pvalue = np.nan, np.nan
                                else:
                                    ks_score, pvalue = ks_2samp(ensemble1.pi_score.values, ensemble2.pi_score.values) 
                                p_values.append(pvalue)
                                
        data = {'variable': vars, "season": seasons,  'p_value': p_values}
        ks_score_df = pd.DataFrame(data)
        ks_score_df['category'] = ks_score_df['p_value'].apply(self.categorize)
        
        color_palette = {'p-values < 0.01': 'r',
                    '0.05 < p-values': 'g',
                    '0.05 > p-values > 0.01': 'y', None: 'w'}  # Adjust colors as needed

        ks_score_df['color_category'] = ks_score_df['category'].map(color_palette)
        self.ks_score_df = ks_score_df
        return




    def plot_pvalues(self):
        p_values = self.ks_score_df.pivot_table(index="variable", columns="season",
                                        values="p_value", aggfunc='mean')

        fig, ax = plt.subplots(figsize=(15,10)) 
        sns.heatmap(p_values, cbar= False, annot=False, linewidths=15.5)


        for i, row in enumerate(p_values.index):
            for j, col in enumerate(p_values.columns):
                color = self.ks_score_df[(self.ks_score_df['variable'] == row) & (self.ks_score_df['season'] == col)]['color_category'].values[0]
                ax.add_patch(plt.Rectangle((j, i), 0.9, 0.9, fill=True, color=color, edgecolor='white'))
                text = "{:.2f}".format(p_values.iloc[i, j])
                ax.text(j + 0.5, i + 0.5, text, ha='center', va='center', color='black',fontsize=14)

        plt.title(f"P values of Kolmogorov-Smirnov test ({self.ensemble1} vs {self.ensemble2})", fontsize=19, fontweight='bold', y=1.02)
        plt.xticks(fontsize=15,rotation=45)  # Adjust fontsize for x-axis ticks (columns)
        plt.yticks(fontsize=15) 

        ax.set_ylabel('')  # Set x-axis label with bold font
        ax.set_xlabel('') 

        filename = f"{self.ensemble1}_{self.ensemble2}_KS_test_p_values"
        create_folder(self.outputdir)
        create_folder(f"{self.outputdir}/pdf")
        create_folder(f"{self.outputdir}/yml")
        nested_dict = {}
        for variable, group in self.ks_score_df.groupby('variable'):
            nested_dict[variable] = {row['season']: row['p_value'] for index, row in group.iterrows()}
        dump_yaml(f"{self.outputdir}/yml/{filename}.yml", nested_dict)
        # with open(f"{self.output_dir}/yml/{filename}.yml", 'w') as file:
        #     yaml.dump(nested_dict, file)
        plt.savefig(f"{self.outputdir}/pdf/{filename}.pdf", bbox_inches='tight')
        

    def compute(self):
        self.combine_PI_dict()
        self.pi_score_df()
        self.km_test_score_df()
        
    def plot(self):
        self.compute()
        self.plot_pvalues()

if __name__ == '__main__':
    args = {
        "ensemble1" : "double_precision",
        "ensemble1_PI_dir" : "/pfs/lustrep3/scratch/project_465000454/kkeller/lab/replicability_mixed/PI_Scores/double_precision",
        
        "ensemble2" : "mixed_precision",
        "ensemble2_PI_dir" : "/pfs/lustrep3/scratch/project_465000454/kkeller/lab/replicability_mixed/PI_Scores/mixed_precision",
        
        "members" : ["fc1", "fc2", "fc3", "fc4", "fc5", "fc6", "fc8", "fc10"],
        "output_dir" : "./output"
    }
    ks_test_diag = ks_test(args)
    ks_test_diag.plot()
