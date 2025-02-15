import xarray as xr
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from aqua.logger import log_configure

# Set default options for xarray
xr.set_options(keep_attrs=True)

class Radiation:

    def boxplot(self, datasets=None, model_names=None, variables=None, variable_names=None, loglevel='WARNING'):
        """
        Generate a boxplot showing the uncertainty of a global variable for different models.

        Args:
            datasets (list of xarray.Dataset): A list of xarray Datasets to be plotted.
            model_names (list of str): Names for the plotting, corresponding to the datasets.
            variables (list of str): List of variables to be plotted.
            variable_names (list of str): List of alternative names for the variables.
            loglevel (str): Logging level.

        Returns:
            tuple: Matplotlib figure, axis objects, Pandas DataFrame of the processed boxplot data, and xarray Dataset of the boxplot data.
        """

        logger = log_configure(log_level=loglevel, log_name='Boxplot')
        logger.info('Generating boxplot.')

        sns.set_palette("pastel")
        fontsize = 18

        # Initialize a dictionary to store data for the boxplot
        boxplot_data = {'Variables': [], 'Values': [], 'Datasets': []}

        label_map = {}
        if variable_names is not None and len(variable_names) == len(variables):
            for variable, name in zip(variables, variable_names):
                var_name = variable[1:] if variable.startswith('-') else variable
                label_map[variable] = name
        else:
            for variable in variables:
                label_map[variable] = variable

        for dataset, model_name in zip(datasets, model_names):
            for variable in variables:
                var_name = variable[1:] if variable.startswith('-') else variable  # Adjusted variable name
                gm = dataset.aqua.fldmean()

                values = gm[var_name].values.flatten()
                if variable.startswith('-'):
                    values = -values
                boxplot_data['Variables'].extend([label_map[variable]] * len(values))
                boxplot_data['Values'].extend(values)
                boxplot_data['Datasets'].extend([model_name] * len(values))

                units = dataset[var_name].attrs.get('units', 'Unknown')
                logger.info(f'Processed variable {var_name} for dataset {model_name} with units {units}.')

        logger.info(f'Producing boxplot for variables {variables}')
        df = pd.DataFrame(boxplot_data)
        fig, ax = plt.subplots(figsize=(12, 8))
        sns.boxplot(data=df, x='Variables', y='Values', hue='Datasets', ax=ax)
        
        ax.set_xlabel('Variables', fontsize=fontsize)
        ax.set_ylabel(f'Values [{units}]', fontsize=fontsize + 2) 
        ax.tick_params(axis='x', labelsize=fontsize - 4)  
        ax.tick_params(axis='y', labelsize=fontsize)  
        ax.legend(loc='upper right', fontsize=fontsize)
        ax.grid(True)

        # Convert the DataFrame to an xarray Dataset without a MultiIndex
        boxplot_dataset = xr.Dataset.from_dataframe(df)

        return fig, ax, boxplot_dataset
