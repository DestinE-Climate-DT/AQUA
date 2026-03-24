# Breakout Session Plan: AQUA Analysis 

# Table of Contents

1. [Introduction and Overview (1 min)](#1-introduction-and-overview-1-min)
   - [1.1 Briefly explain the session purpose and key activities](#11-briefly-explain-the-session-purpose-and-key-activities)
   - [1.2 The tools that will be used during the session](#12-the-tools-that-will-be-used-during-the-session)
   - [1.3 Preparational Steps for AQUAthon Git Branch ](#13-preparational-steps-for-aquathon-git-branch)
2. [AQUA Scientific Diagnostics (20 min)](#2-aqua-scientific-diagnostics-20-min)
   - [2.1 Available Diagnostics](#21-available-diagnostics)
   - [2.2 General Folder Contents](#22-general-folder-contents)
   - [2.3 Comprehensive Example](#23-comprehensive-example)
   - [2.4 CLI for a Each AQUA Diagnostic](#24-cli-for-a-each-aqua-diagnostic)
3. [AQUA Analyses Wrapper (5 min)](#3-aqua-analyses-wrapper-5-min)
   - [3.1 Show and discuss the aqua-analysis Wrapper](#31-show-and-discuss-the-aqua-analysis-wrapper)
   - [3.2 Demonstrate how to modify the YAML file for custom analyses](#32-demonstrate-how-to-modify-the-yaml-file-for-custom-analyses)
4. [Running AQUA Analyses (10 min)](#4-running-aqua-analyses-10-min)
   - [4.1 Running the AQUA Analyses CLI for a Specific Diagnostic](#41-running-the-aqua-analyses-cli-for-a-specific-diagnostic)
   - [4.2 Running AQUA Analyses for a Set of Diagnostics on a Specific Catalog Source](#42-running-aqua-analyses-for-a-set-of-diagnostics-on-a-specific-catalog-source)
   - [4.3 Submitting AQUA Analyses to the SLURM Queue for a Set of Catalog Sources](#43-submitting-aqua-analyses-to-the-slurm-queue-for-a-set-of-catalog-sources)
5. [AQUA Analysis Output Review (5 min)](#5-aqua-analysis-output-review-5-min)
   - [5.1 The output structure](#51-the-output-structure)
6. [Q&A Session (5 min)](#6-qa-session-5-min)


---
## Agenda Overview
This session will focus on **Sailing with AQUA:**, which includes running the aqua-analysis tool, modifying configuration files, submitting jobs, and interpreting the outputs.


---

## Session Structure

### 1. **Introduction and Overview (1 min)**
---
**Objective:** Introduce the session's objectives.

#### 1.1 Briefly explain the session purpose and key activities 
  
- To demonstrate examples of **diagnostics** using the AQUA package.
- To discuss **troubleshooting**, the current stage, and the upcoming **refactoring** of AQUA Analyses and diagnostics.
- To show how to **run** and **adjust** scientific **analyses** with AQUA.


#### 1.2 The tools that will be used during the session
  
  - **Notes**: Key takeaways and explanations will be provided in markdown notes.
  - **Python Scripts**: Scripts for running the diagnostics and analyses.
  - **Bash Scripts**: Shell scripts for automating tasks and submitting jobs.
  - **YAML Files**: Configuration files to set parameters for the diagnostics.
  - **Jupyter Notebooks**: Interactive notebooks for demonstrating and running analyses.



#### 1.3 Preparational Steps for AQUAthon Git Branch 

Follow these steps to update your local branch with remote changes while keeping your local modifications intact:

##### **Ensure you're on the correct branch:**

   ```bash
   git checkout aquathon
   ```

##### **Fetch the latest changes from the remote repository:**

   ```bash
   git fetch
   ```

##### **Merge the remote branch (`aquathon`) into your local branch:**

   ```bash
   git pull origin aquathon
   ```

---

### 2. **AQUA Scientific Diagnostics (20 min)**
---
**Objective:** Demonstrate how to use AQUA scientific diagnostics in a Jupyter notebook, showcasing key analytical workflows and outputs.

#### 2.1 Available Diagnostics

<details>
  <summary> <span style="color: green;">Frontier Diagnostics</span></summary>

These **diagnostics** offer new insights into **climate phenomena** that can't be studied with standard-resolution models. Their goal is to showcase the **scientific** and **technical capabilities** of **high-resolution data** from the Digital Twin, working directly with the full **temporal** and **spatial resolution** of climate models.

</details>

- [SSH variability](https://github.com/DestinE-Climate-DT/AQUA/tree/aquathon/diagnostics/ssh)
- [Tropical Cyclones detection, tracking, and zoom-in diagnostic](https://github.com/DestinE-Climate-DT/AQUA/tree/aquathon/diagnostics/tropical_cyclones)
- [Tropical rainfall diagnostic](https://github.com/DestinE-Climate-DT/AQUA/tree/aquathon/diagnostics/tropical_rainfall)

<details>
  <summary> <span style="color: green;">State-of-the-art Diagnostics</span></summary>

These **diagnostics** aim to **monitor** and **diagnose** model **drifts**, **imbalances**, and **biases**. Unlike the "frontier" diagnostics, they use aggregated **coarse-resolution data** from the **Low Resolution Archive (`LRA`)**.

</details>

- [Atmospheric Global Mean Biases Diagnostic](https://github.com/DestinE-Climate-DT/AQUA/tree/aquathon/diagnostics/atmglobalmean)
- [Performance Indices](https://github.com/DestinE-Climate-DT/AQUA/tree/aquathon/diagnostics/ecmean)
- [Global time series](https://github.com/DestinE-Climate-DT/AQUA/tree/aquathon/diagnostics/global_time_series)
- [Ocean3D](https://github.com/DestinE-Climate-DT/AQUA/tree/aquathon/diagnostics/ocean3d)
- [Radiation Budget Diagnostic](https://github.com/DestinE-Climate-DT/AQUA/tree/aquathon/diagnostics/radiation)
- [Sea ice extent](https://github.com/DestinE-Climate-DT/AQUA/tree/aquathon/diagnostics/seaice)
- [Teleconnections diagnostic](https://github.com/DestinE-Climate-DT/AQUA/tree/aquathon/diagnostics/teleconnections)

#### 2.2 General Folder Contents:

- **`cli/`**: 
  - Contains command-line interface (CLI) tools or Python scripts used to run the diagnostics.
  - **Example**: `nworkers` and `memory` update scripts for adjusting parallel processing settings.

- **`notebooks/`**:
  - Jupyter Notebooks used for demonstrating and testing the diagnostics.
  - **Example**: Contains examples, unit changes, or other updates in diagnostic workflows.

- **`README.md`**:
  - The README file provides an overview of the diagnostic package, instructions on usage, and important updates.
  - **Example**: Recent updates about YAML configurations or specific instructions on running the diagnostics.

- **Python Scripts (`*.py`)**:
  - Core Python scripts that define the logic of the diagnostics.
  - **Example**: `ssh_class.py` contains logic for SSH variability diagnostics, while `__init__.py` initializes the module and removes outdated hacks.

- **Extra files**:
  - Diagnostics may contain precomputed data, directories for storing results, additional scientific analysis outputs, or optional files like `pyproject.toml`.
  - **Example**: 
    - **`pyproject.toml`**: Defines the project’s dependencies, environment settings, and packaging configurations. Some diagnostics include this file for versioning and dependencies.
    - Data or result folders specific to each diagnostic.


<details>
  <summary>⚠️ <span style="color: red;">Disclaimer</span></summary>

> A major refactoring of diagnostics is underway, so be aware that this information may change. 
>  
> Each scientific diagnostic has been developed by different contributors, leading to variations in usage and structure. We are currently standardizing the diagnostics with clear requirements for implementation, documentation, and usage.  
>  
> Within the next month, there will be changes to how diagnostics are used.  
  
</details>

#### 2.3 Comprehensive Example

Access the  notebook with comprehensive example [here](https://github.com/DestinE-Climate-DT/AQUA/blob/aquathon/aquathon/breakout_1/aqua_analysis.ipynb)

If you would like to see a detailed example of how each diagnostic is used, please check the `$AQUA/diagnostics/diagnostic_name/notebooks` folder.

<details>
  <summary>📚 <span style="color: green;">Homework</span></summary>

1) Implement a time series plot for the tropical band (for example, from -20° to 20° latitude).

- Use the `Timeseries` class provided earlier.
- Define latitude boundaries from -20° to 20°.
- Include data from `1990-01-01` to `1999-12-01`.
- Set standard deviation start and end dates to match the period.

  
<details>
  <summary>💡 <strong><span style="color: orange;">Solution</span></strong></summary>
```python
# 0 s
ts_area = Timeseries(var='2t', models='IFS-NEMO', exps='historical-1990', sources='lra-r100-monthly',
                     startdate='1990-01-01', enddate='1999-12-01',
                     std_startdate='1990-01-01', std_enddate='1999-12-01', extend=False,
                     lat_limits=[-20, 20],
                     loglevel='INFO')
# 1 m, 10 s
ts_area.run()
```
</details>

2) For the same tropical band, compare the **annual mean** of model data with **CERES** reference data.

- Ensure the comparison is made on an annual basis.
- Include CERES data in the comparison as the reference dataset.


3) Generate an annual plot for Sea Surface Temperature (SST) using the `seasonal_bias` function.

- Use `seasonal_bias` to compare two datasets: one for the **IFS-NEMO historical** model and the other for **ERA5**.
- Set the variable to Sea Surface Temperature.
- Define the time periods:
  - **IFS-NEMO**: From `1990-01-01` to `2001-12-31`.
  - **ERA5**: From `1980-01-01` to `2010-12-31`.
- Set the value range between **-4** and **4** and include **9 levels**.
- Generate an **annual** plot.


<details>
  <summary>💡 <strong><span style="color: orange;">Solution</span></strong></summary>

```python
# 13 s
seasonal_bias(
    dataset1=data_ifs_nemo_historical,
    dataset2=data_era5,
    var_name='avg_tos',
    plev=None,
    model_label1='IFS-NEMO historical',
    model_label2='ERA5',
    start_date1='1990-01-01',
    end_date1='2001-12-31',
    start_date2='1980-01-01',
    end_date2='2010-12-31',
    vmin=-4,
    vmax=4,
    nlevels=9,
    seasons=False
)
```
</details>
</details>

#### 2.4 CLI for a Each AQUA Diagnostic

Each diagnostic has a Command Line Interface (CLI) for running analyses, which includes a Python script and a YAML file to configure the analysis. For example, in the **teleconnections** diagnostic, the Python script is located at [`AQUA/diagnostics/teleconnections/cli/cli_teleconnections.py`](https://github.com/DestinE-Climate-DT/AQUA/blob/aquathon/diagnostics/teleconnections/cli/cli_teleconnections.py), and the corresponding YAML file is at [`AQUA/diagnostics/teleconnections/cli/cli_config_atm.yaml`](https://github.com/DestinE-Climate-DT/AQUA/blob/aquathon/diagnostics/teleconnections/cli/cli_config_atm.yaml). This same structure applies to other diagnostics as well.

The CLI scripts include several important features to ensure reliable analysis:

- **Logging**: Tracks the execution of the analysis and records important events.
- **Error handling**: Identifies and manages errors during the process to prevent failures.
- **Metadata for PDF images**: Ensures that the output PDF images are documented with relevant metadata for better traceability.

During AQUA installation, the YAML files are copied to the `.aqua` folder. In your AQUA diagnostics setup, configuration files are organized under the `$HOME/.aqua/diagnostics` directory in a consistent hierarchical structure:

   - **`cli`**: Contains YAML configuration files specific to command-line interfaces for diagnostics.
   - **`config`**: Stores general configuration YAML files for each diagnostic.

<details>
  <summary>⚠️ <span style="color: red;">Disclaimer</span></summary>

  The configuration files stored under `$HOME/.aqua/diagnostics/` are only retained when the AQUA package is installed in non-development mode. If the package is installed in development mode, symbolic links (symlinks) may be used instead of actual files, pointing to the source files in the development environment.

</details>

---

### 3. **AQUA Analyses Wrapper (5 min)**
---
**Objective**: Demonstrate the aqua-analysis wrapper and how to modify the YAML configuration file.

#### 3.1 Show and Discuss the aqua-analysis Wrapper

The AQUA analyses wrapper is currently implemented as a bash script located at [`$AQUA/cli/aqua-analysis/aqua-analysis.sh`](https://github.com/DestinE-Climate-DT/AQUA/blob/aquathon/cli/aqua-analysis/aqua-analysis.sh).

- **Automates diagnostics**: Runs multiple diagnostics efficiently.
- **Parallel execution**: Speeds up analyses with multi-process support.
- **Flexible configuration**: Allows customization via command-line and YAML.


<details>
  <summary>⚠️ <span style="color: red;">Disclaimer</span></summary>
  
  The AQUA wrapper is currently being refactored and **will soon be replaced by a Python script** for improved functionality and flexibility.
  
</details>


#### 3.2 Demonstrate how to modify the YAML file for custom analyses

The YAML file used to configure the wrapper is located at [`$AQUA/cli/aqua-analysis/config.aqua-analysis.yaml`](https://github.com/DestinE-Climate-DT/AQUA/blob/aquathon/cli/aqua-analysis/config.aqua-analysis.yaml). If you want to customize the settings (different from the default) and run AQUA analyses, you will need to modify this YAML file accordingly.



<details>
  <summary><span style="color: green;">Click to expand YAML Template</span></summary>

```yaml
# Configuration file for aqua-analysis.sh

job:
    max_threads: {{ max_threads, int }}  # Max threads (0 for no limit)
    loglevel: {{ loglevel, str }}  # DEBUG, INFO, WARNING, ERROR, CRITICAL
    run_dummy: {{ run_dummy, bool }}  # Test run with dummy analysis
    outputdir: {{ /path/to/output, str }}  # Output directory path
    model: {{ model_name, str }}  # Model name for analysis
    exp: {{ experiment_name, str }}  # Experiment identifier
    source: {{ source_name, str }}  # Data source identifier

diagnostics:
  run: {{ diagnostics_to_analyze, list }}  # List of diagnostics to run
  diagnostic_name:
    nworkers: {{ nworkers, int }}  # Number of workers
    script_path: {{ script_path, str }}  # Path to diagnostic script
    extra: {{ extra_arguments, str }}  # Additional command-line arguments
    outname: {{ outname, str }}  # Name of output directory
```

#### **`nworkers` Key** 
<details>
  <summary><span style="color: green;">Click to expand</span></summary>

The `nworkers` key defines the number of parallel workers (or processes) used for running diagnostic analyses with Dask. More workers result in faster performance by distributing tasks across multiple CPU cores.

- **Tip**: Set `nworkers` based on the number of available cores on your machine (e.g., 4 or 8 workers for a multi-core system).

This ensures efficient utilization of your computing resources.

</details>

#### **`extra` Key**

<details>
  <summary><span style="color: green;">Click to expand</span></summary>

The `extra` key allows passing additional command-line arguments to the diagnostic script, providing flexibility for customization.

For example, you can:
- Specify a custom configuration file (`--config`).
- Enable reference data analysis (`--ref`).
- Set parameters such as regridding or frequency (`--regrid=r100 --freq=M`).

</details>

<details>
  <summary>⚠️ <span style="color: red;">Disclaimer</span></summary>

  Each diagnostic can have its own `extra` arguments tailored to specific needs, ensuring a flexible and detailed analysis process.

</details>

#### **`outname` Key**

<details>
  <summary><span style="color: green;">Click to expand</span></summary>

The `outname` key specifies the directory name where the output results for a diagnostic will be stored. By default, it uses the diagnostic name, but you can customize it as needed.

For example:
- Setting `outname: global_time_series` will store results in a directory named `global_time_series`.

This ensures that output files are organized in clearly labeled directories, making it easier to manage and access results.

</details>

</details>

---
### 4. **Running AQUA Analyses (10 min)**
---
**Objective:** Guide participants through the process of executing AQUA analyses using Python and Bash scripts, and submitting the jobs to the SLURM queue for parallel processing. 

#### 4.1 Running the AQUA Analyses CLI for a Specific Diagnostic

To execute the AQUA Analyses CLI for a specific diagnostic using command-line arguments, for example, the `tropical_rainfall` diagnostic:


```bash
 python diagnostics/tropical_rainfall/cli/cli_tropical_rainfall.py 
```

##### Required arguments for CLI:

- `-c`, `--config` (str): Specifies the YAML configuration file to be used. This file contains settings for the diagnostics.
- `-l`, `--loglevel` (str): Defines the logging level (e.g., `DEBUG`, `INFO`, `WARNING`). Default is `WARNING`.
- `-n`, `--nworkers` (int): Specifies the number of Dask distributed workers for parallel processing.
- `--model` (str): Overrides the model name from the configuration file if provided. This is optional.
- `--exp` (str): Overrides the experiment name from the configuration file if provided. This is optional.
- `--source` (str): Overrides the source name from the configuration file if provided. This is optional.
- `--outputdir` (str): Specifies the output directory for the analysis results. Overrides the configuration file if provided.
- `--dry`: Runs the analysis in dry mode, where no files are written (useful for debugging).
- `--ref`: Specifies whether the analysis should be performed against a reference dataset.

```bash
python diagnostics/tropical_rainfall/cli/cli_tropical_rainfall.py  --model IFS-NEMO --exp historical --source lra-r100-monthly \
 --config config.aqua-web.yaml --loglevel INFO 
```

<details>
  <summary>⚠️ <span style="color: red;">Disclaimer</span></summary>
  
  Each diagnostic has a configuration file. If the command-line arguments (such as `model`, `exp`, `source`, etc.) are not provided, the values from the configuration file will be used. However, if these arguments are supplied via the command line, they will override the values in the configuration file and will be considered the main ones for the analysis.
  
</details>


#### 4.2 Running AQUA Analyses for a Set of Diagnostics on a Specific Catalog Source

To run AQUA Analyses for multiple diagnostics using command-line arguments:

```bash
bash ./cli/aqua-analysis/aqua-analysis.sh
```

##### Required arguments for CLI (Bash Script `aqua-analysis.sh`):

- `-f`, `--config <FILE>`:  
  Specifies the YAML configuration file to be used. This file contains settings for the diagnostics. Default is `config.aqua-analysis.yaml`.

- `-m`, `--model <MODEL>`:  
  Specifies the model to be analyzed (both atmospheric and oceanic models). Overrides the model from the configuration file if provided.

- `-e`, `--exp <EXP>`:  
  Specifies the experiment to be run. Overrides the experiment from the configuration file if provided.

- `-s`, `--source <SOURCE>`:  
  Specifies the data source to be used. Overrides the source from the configuration file if provided.

- `-d`, `--outputdir <DIR>`:  
  Specifies the output directory for the analysis results. If not provided, the directory will be fetched from the configuration file.

- `-l`, `--loglevel <LEVEL>`:  
  Specifies the log level for the script. Accepted values are `INFO`, `DEBUG`, `ERROR`, `WARNING`, and `CRITICAL`. Default is `WARNING`.

- `-p`, `--parallel`:  
  Enables running diagnostics in parallel, allowing faster execution by utilizing multiple processes.

- `-t`, `--threads <N>`:  
  Specifies the maximum number of threads to be used for parallel execution. If not specified, all available threads will be used.

- `-c`, `--catalog <CATALOG>`:  
  Specifies the catalog for data sources. If provided, it will override the catalog specified in the configuration file.


```bash
bash ./cli/aqua-analysis/aqua-analysis.sh --model IFS-NEMO --exp historical --source lra-r100-monthly --catalog climatedt-phase1 \
 --outputdir /path/to/output  --threads 4 --loglevel INFO
```

<details>
  <summary>📚 <span style="color: green;">Homework</span></summary>

1) Run the **aqua-analysis** bash script on **Levante** or **Lumi** for **global mean time series** and **Sea Ice diagnostics**, saving the output to your new folder: `$HOME/homework`. Make sure to set the log level to **warning**.
  
2) On **Levante** or **Lumi** , run the **aqua-analysis** bash script for the **climatedt-phase1** catalog, with the following parameters:
   - Model: `IFS-NEMO`
   - Experiment: `ssp370`
   - Source: `lra-r100-monthly`
  
</details>


#### 4.3 Submitting AQUA Analyses to the SLURM Queue for a Set of Catalog Sources
  
Use the pre-prepared scripts in the `$AQUA/cli/aqua-web` folder to simplify the process.

Submit the wrapper with this command:

```bash
python cli/aqua-web/submit-aqua-web.py -p /users/jvonhar/aqua-web.experiment.list
```

<details>
  <summary>⚠️ <span style="color: red;">Disclaimer</span></summary>
  
  This script not only manages submission to SLURM but also handles the submission of results to [AQUA Web](https://aqua-web-climatedt.2.rahtiapp.fi/). It is used operationally, but you are welcome to explore, personalize, and adapt it to suit your own workflow or project needs. Feel free to get inspired and customize it as you see fit.
</details>


---
### 5. **AQUA Analysis Output Review (5 min)**
---
**Objective:** Review the output structure of aqua-analysis.

#### 5.1 The output structure

The output folder is organized in a clear hierarchical structure to help users easily navigate the analysis results. Below is a breakdown of the structure:

- **Catalog Level:**  
  Top-level directories represent the models or simulations included in the analysis.

- **Model Level:**  
  These directories specify the particular model used in the analysis.

- **Experiment Level:**  
  Each model directory contains subdirectories for different experiments or simulation periods.

- **Diagnostic Level:**  
  Inside each experiment folder, diagnostics are categorized by type (e.g., time series, seasonal cycles).

- **Output Files:**  
  Diagnostic folders contain various output files:
  - **Log files:** Track the progress and details of the analysis.
  - **PDF reports:** Provide visual summaries of the diagnostics.
  - **NetCDF files:** Contain detailed data for further analysis.

<details>
  <summary><span style="color: green;">Click to expand the example of the output structure</span></summary>

```plaintext
output/
└──climatedt-phase1/
└──── IFS-NEMO/
    └──── historical-1990/
        ├──── seasonal_cycles.log
        ├──── ecmean.log
        ├──── seasonal_cycles/
        │   ├──── pdf/
        │   │   └──── global_time_series_seasonalcycle_msshf_IFS-NEMO_historical-1990_ERA5.pdf
        │   └──── netcdf/
        │       └──── seasonal_cycles_msshf.nc
        ├──── ecmean/
        │   ├──── pdf/
        │   │   └──── PI4_EC23_historical-1990_ClimateDT_r1i1p1f1_1990_2002.pdf
        │   └──── yaml/
        │       └──── PI4_EC23_historical-1990_ClimateDT_r1i1p1f1_1990_2002.yml

```
</details>

---
### 6. **Q&A Session (5 min)**
---
**Objective:** Address questions and clarify any doubts regarding the session content.


---
## Further Reading and Events
- [AQUA Documentation](https://aqua-web-contbuild.2.rahtiapp.fi/documentation/index.html)
- [Hackathon Hamburg, Germany 11-15 November]()
