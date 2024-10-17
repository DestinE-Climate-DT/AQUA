# AQUAthon 2024 Practical session - Preparation and general info

[TOC]

## :exclamation: Important info

:::info
* Time: Thursday 12th September, 2.30 pm to 4.30pm CEST 
* Location: Zoom link: [link](https://didattica.polito.it/pls/portal30/sviluppo.bbb_corsi.waitRoom?id=59903&p_tipo=DOCENTE)
* Link to this document: [link](https://siili.rahtiapp.fi/SG2E5GTnT5mIDpB7v2bxKA#)
* [Slides theoretical session](https://docs.google.com/presentation/d/1TMExAamjBO9gMspaF3UL7lKsUpvgRMe-/edit?usp=sharing&ouid=103539641833901507286&rtpof=true&sd=true)
* Link to the wiki page: [link](https://wiki.eduuni.fi/pages/viewpage.action?spaceKey=cscRDIcollaboration&title=AQUAthon+September+2024)
* Follow the instuction in the Preparation for the plenary session Section
* Two parallel session will be available:
    * **1. Sailing with AQUA**: Running the aqua-analyis and exploit the full potentials of AQUA diagnostics
    * **2. Navigating the depth**: AQUA fixes and grids definitions, accessing high resolution data, streaming and performance optimization
* The session will be recorded in order to make it available offline. Anyway, the interactive moments (exercises, questions), will not be recorded. Feel free to ask questions!
:::

##  :clock230: Agenda

:::danger 
:fire: All times in CEST (pm)! :fire:
:::

**2:30-2:35** Welcome and brief overview of the workshop
**2:35-3:45** Plenary session
**3:45-4:30** Parallel sessions
**4:30-4:35** Closing remarks

## Live session pleanary modules

Here you can find the links to the different leve session plenary modules. In all the modules you will find a red area at the bottom to ask your questions

- [Module 1: AQUA console and Reader class](https://siili.rahtiapp.fi/BDUSgW6-Q2-cNra58Sswow#)
- [Module 2: Regridding with AQUA](https://siili.rahtiapp.fi/lZlFi14YRYyNFK7uYZ5UjA#)
- [Module 3: Other tools](https://siili.rahtiapp.fi/xRnmHRp_QI-rf2umyrOSBQ#)

---

##  :beers: Icebreaker

Leave an x on your answer in the following questions. :x: 
In which machine do you usually work?

- Lumi :x:
- Levante
- MN5
- Atos
- Others?

Have you already used AQUA?

- Yes
- A little bit :x:
- No

How would you like to use AQUA in your work (also outside this project)?

- ...

## :sailboat: Preparation for the plenary session

For the plenary session, we want to be sure that you are ready to use AQUA on one of the two supported machines, Lumi or Levante. The code can be accessed by an installation or by container. Detailed documentation can be found [here](https://aqua-web-climatedt.2.rahtiapp.fi/documentation/installation.html).

::: info
We suggest to use an installation of AQUA, but if you prefer to use the cointainer, instructions are provided as well in this section.
:::

:::danger
Lumi and Levante are the two machines that we will support in this tutorial. If you want to work on another machine please follow the documentation for the installation guide and be sure to have at least ERA5 data downloaded as in Lumi and Levante. We will not able to support you on a custom machine live.

You need to have access to the following projects:
- Levante: bb1153
- Lumi: project_465000454
:::

### :droplet: Clone the repositories

First we clone on our machine the AQUA code.
This is needed also for the container users, since we will make use of scripts that are stored in the AQUA repository.

```bash
git clone git@github.com:DestinE-Climate-DT/AQUA.git
```


or for `https` protocol:

```bash
git clone https://github.com/DestinE-Climate-DT/AQUA.git
```

:::info
If you don't have access to the [AQUA repository](https://github.com/DestinE-Climate-DT/AQUA) or to the [Climate-DT catalog repository](https://github.com/DestinE-Climate-DT/Climate-DT-catalog) please contact us at [p.davini@isac.cnr.it](mailto:p.davini@isac.cnr.it), [m.nurisso@isac.cnr.it](mailto:m.nurisso@isac.cnr.it) or in the AQUA [mattermost channel](https://mattermost.mpimet.mpg.de/destine/channels/aqua).
:::

We define for simplicity an `AQUA` environment variable pointing to the AQUA source code:

```bash
export AQUA=/path/to/AQUA
```


We then checkout on the AQUAthon specific branch, where the exercises are stored.

```bash
cd $AQUA
git checkout aquathon
```

Then we clone the catalog repository:

```bash
git clone git@github.com:DestinE-Climate-DT/Climate-DT-catalog.git
```

or for `https` protocol:

```bash
git clone https://github.com/DestinE-Climate-DT/Climate-DT-catalog.git
```

### Installation on Lumi

The detailed documentation on how to install on Lumi can be found [here](https://aqua-web-climatedt.2.rahtiapp.fi/documentation/installation.html#installation-on-lumi-hpc).
On Lumi it is **not** possible to use a conda manager and we're going to use a script specifically written for Lumi.

Navigate to the folder where`cli/lumi-install` is:

```bash
cd $AQUA/cli/lumi-install
```

Run the installation script:

```bash
./lumi-install.sh
```

The script will create a containerized version of a conda environment and a file `load_aqua.sh` in your `$HOME`. It will also, if you want, load this file automatically when starting Lumi, otherwise you need to manually load this file when using AQUA in your terminal, scripts or jobs.

:::info
If you have particular modules loaded while using Lumi, we suggest to turn them off during this session.
:::

### Installation on Levante

Make sure to install a conda manager. We suggest [mamba](https://mamba.readthedocs.io/en/latest/), but every solution works. Another option on Levante is to use the available python3 module [(DKRZ docs)](https://docs.dkrz.de/doc/levante/code-development/python.html#individual-python-environments). To use this run `module load python3` then `conda init` and copy the `.bashrc` conda content into `~.profile` so that it will also be available on login nodes. After that you should be able to use conda to create and activate the aqua environment.

Once the conda manager is installed, you can use the `environment.yaml` file in the main AQUA folder to install the required dependencies.

Open the AQUA folder:

```bash
cd $AQUA
```

and create the environment:

```bash
mamba env create -f environment.yml
```

Finally, to use the environment you will need to activate it:

```bash
mamba activate aqua
```

In order to use the FDB5 binaries and GSV on Levante (not available as modules) you will need to add to your `.bashrc` and `.bash_profile` these lines:

```bash
export LD_LIBRARY_PATH=/work/bb1153/b382075/aqua/local/lib:$LD_LIBRARY_PATH
export GRID_DEFINITION_PATH=/work/bb1153/b382321/grid_definitions
```

### Container

:::warning
We suggest to use one of the two suggested methods during the tutorial but a container is available on both Lumi and Levante.
:::

To launch a container you can follow the [documentation](https://aqua-web-climatedt.2.rahtiapp.fi/documentation/container.html), where it is explained in detail how to launch and use vscode with the AQUA container.

### Jupyter notebooks with AQUA

In order to not exploit the login node we suggest to connect a Jupyter notebook with an interactive job.
First we ask for an interactive node.

On Levante:

```bash
salloc --account=bb1153 --partition=compute --nodes=1 --time=02:30:00 --mem=128G
```

On Lumi:

```bash
salloc --account=project_465000454 --partition=debug --nodes=1 --time=00:30:00 --mem=128G
```

Then we run the following commands to launch the jupyter-lab session:

```bash
mamba activate aqua # on Levante
pip install jupyterlab # not part of the AQUA dependencies, only the first time
node=$(hostname -s)
port=$(shuf -i8000-9999 -n1)
jupyter-lab --no-browser --port=${port} --ip=${node}
```

We then copy the url that will be similar to `http://nodeurl:<port>/lab?token=random_token`

and then we can launch the notebook both on vscode or on our local browser.

#### VScode

On VScode you need to connect to your machine, open a new notebook and as kernel choose:

- `> Select Another Kernel`,
- `> Existing Jupyter Servers`,
- paste the URL, choose the name,
- accept the suggested kernel path.

#### Browser

On a browser you need to connect to the machine in a terminal with:

```bash
ssh -L <port>:nodeurl:<port> <machine-hostname>
```

e.g. `ssh -L 8839:nid007521:8839 lumi`.
Then you can copy and open the URL on your browser.

:::success
:fire: **Congratulations!** You're now set for the AQUAthon! See you soon for the plenary session!
:::

:rocket: [To the next module](https://siili.rahtiapp.fi/BDUSgW6-Q2-cNra58Sswow#)

### :interrobang: Issues 

Feel free to modify the following red paragraph with possible questions you have about the preparation procedure:

:::danger
- issue: ...
:::