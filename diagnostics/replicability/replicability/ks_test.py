# Kolmogorov-Smirnov Test

from aqua.util import load_yaml, get_arg, create_folder, dump_yaml
import pandas as pd
from scipy.stats import ks_2samp
import numpy as np
import glob
import matplotlib.pyplot as plt
import seaborn as sns
sns.set()
from aqua.logger import log_configure

class ks_test:
    """
    Class for performing Kolmogorov-Smirnov (KS) test between two ensembles.

    Args:
        ensemble1 (str): Name of the first ensemble.
        ensemble1_PI_dir (str): Directory containing the predictive intervals for ensemble1.
        ensemble2 (str): Name of the second ensemble.
        ensemble2_PI_dir (str): Directory containing the predictive intervals for ensemble2.
        members (list): List of members in each ensemble.
        outputdir (str): Directory to save the output files.

    Attributes:
        ensemble1 (str): Name of the first ensemble.
        ensemble1_PI_dir (str): Directory containing the predictive intervals for ensemble1.
        ensemble2 (str): Name of the second ensemble.
        ensemble2_PI_dir (str): Directory containing the predictive intervals for ensemble2.
        members (list): List of members in each ensemble.
        outputdir (str): Directory to save the output files.
        pi_score (dict): Dictionary containing predictive interval scores for each ensemble and member.
        df (DataFrame): DataFrame containing predictive interval scores for each ensemble, member, variable, season, and region.
        ks_score_df (DataFrame): DataFrame containing KS test scores and p-values for each variable, season, and region.
    """
    def __init__(self, *args):
        """
        Initializes the ks_test object.

        Args:
            *args (dict): Keyword arguments containing ensemble names, directories, members, and output directory.
        """
        args = args[0]
        self.ensemble1 = args.get("ensemble1")
        self.ensemble2 = args.get("ensemble2")
        
        self.ensemble1_PI_dir = args.get("ensemble1_PI_dir")
        self.ensemble2_PI_dir = args.get("ensemble2_PI_dir")
        
        self.members = args.get("members")
        self.outputdir = args.get("outputdir")
        
    
    def PI_dict(self, ensemble, ensemble_PI_dir, members,  loglevel="WARNING"):
        """
        Retrieves predictive interval scores for a given ensemble.

        Args:
            ensemble (str): Name of the ensemble.
            ensemble_PI_dir (str): Directory containing the predictive intervals for the ensemble.
            members (list): List of members in the ensemble.
            loglevel (str): Logging level (default is "WARNING").

        Returns:
            dict: Dictionary containing predictive interval scores for the ensemble and its members.
        """
        logger = log_configure(loglevel, 'PI_dict')
        
        pi_score = {}
        data_members= {}
        logger.DEBUG("Going to import yaml files")
        for member in members:
            pattern = f"{ensemble_PI_dir}/*{member}*yml"
            yaml_files = glob.glob(pattern)
            logger.debug("These are the yaml files availabel %s", yaml_files)
            logger.DEBUG("Going to import yaml of %s", member )
            loaded_yaml= load_yaml(yaml_files[0])
            logger.DEBUG("Done")
            data_members[member] = loaded_yaml
        pi_score[ensemble] = data_members
        return pi_score
    
    def combine_PI_dict(self, loglevel="WARNING"):
        """
        Combines predictive interval scores for both ensembles into a single dictionary.

        Args:
            loglevel (str): Logging level (default is "WARNING").
        """
        logger = log_configure(loglevel, 'combine_PI_dict')
        logger.DEBUG("Dictionary creation of %s from this directory: %s", self.ensemble1, self.ensemble1_PI_dir)
        ensemble1_PI_score = self.PI_dict(self.ensemble1, self.ensemble1_PI_dir, self.members)
        logger.DEBUG("Dictionary creation of %s from this directory: %s", self.ensemble2, self.ensemble2_PI_dir)
        ensemble2_PI_score = self.PI_dict(self.ensemble2, self.ensemble2_PI_dir, self.members)
        self.pi_score = {
            **ensemble1_PI_score,
            **ensemble2_PI_score,
        }
        return 
    
    def pi_score_df(self, loglevel="WARNING"):
        """
        Converts predictive interval scores into a DataFrame.

        Args:
            loglevel (str): Logging level (default is "WARNING").
        """
        logger = log_configure(loglevel, 'pi_score_df')
        pi_score_dict = self.pi_score
        ensembles = []
        members = []
        variables = []
        seasons = []
        regions = []
        values = []
        logger.DEBUG("DataFrame creation of PI Dictionary")
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
        """
        Categorizes p-values into different categories.

        Args:
            value (float): P-value.

        Returns:
            str: Category label.
        """
        if value < 0.01:
            return "p-values < 0.01"
        elif 0.01 <= value < 0.05:
            return "0.05 > p-values > 0.01"
        elif value >= 0.05:
            return "0.05 < p-values"
        elif value == np.nan:
            return None
        
    def km_test_score_df(self, loglevel="WARNING"):
        """
        Computes KS test scores and p-values for each variable, season, and region.

        Args:
            loglevel (str): Logging level (default is "WARNING").
        """
        logger = log_configure(loglevel, 'km_test_score_df')
        df = self.df
        vars = []
        seasons = []
        regions = []
        p_values = []
        KS_score = {}
        logger.DEBUG("Generating Kolmogorov test results")
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




    def plot_pvalues(self, loglevel="WARNING"):
        """
        Plots p-values of the KS test.

        Args:
            loglevel (str): Logging level (default is "WARNING").
        """
        logger = log_configure(loglevel, 'create_folder')
        logger.DEBUG("Plotting Kolmogorov test results!")
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
        filename = filename.replace(" ", "_")
        create_folder(self.outputdir)
        create_folder(f"{self.outputdir}/pdf")
        create_folder(f"{self.outputdir}/yml")
        nested_dict = {}
        for variable, group in self.ks_score_df.groupby('variable'):
            nested_dict[variable] = {row['season']: row['p_value'] for index, row in group.iterrows()}
        
        dump_yaml(f"{self.outputdir}/yml/{filename}.yml", nested_dict)
        plt.savefig(f"{self.outputdir}/pdf/{filename}.pdf", bbox_inches='tight')
        

    def compute(self):
        """
        Computes predictive interval scores and KS test scores.
        """
        logger.DEBUG("Computing Kolmogorov test p values!")
        self.combine_PI_dict()
        self.pi_score_df()
        self.km_test_score_df()
        
    def plot(self):
        """
        Plots the p-values of the KS test.
        """
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
