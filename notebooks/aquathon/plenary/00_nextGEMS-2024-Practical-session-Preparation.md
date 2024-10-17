# nextGEMS 2024 Hazard Hackathon - Introduction to AQUA tool

[TOC]

## :exclamation: Important info

:::info
* Time: Tuesday 15th October, 9.30 AM CEST
* Duration: 1 hour, but we will be available in this room the entire morning
* Location: TBD
* Link to this document: [link](https://siili.rahtiapp.fi/V3yogQygS2aECB-XPKKRHQ#)
* [Slides Storms, Eddies and Science seminar](https://docs.google.com/presentation/d/1e__TJTn3j2anphe8SrQXjQq6LaHaHVq9/edit?usp=sharing&ouid=108099315407763926629&rtpof=true&sd=true)
:::

## :muscle:  Hands-on modules 

Today's introduction to AQUA has the purpose to give you the possibility to set up this new tool and check the access to the nextGEMS data in a hands-on session in which we want to have as much interaction with you as possible.

Notebooks and basic instruction are available, but we'd like to build this session based on your requirements.

Some material is available for these topics:

- AQUA console and Reader class
- Regridding with AQUA
- Other processing tools

---

##  :beers: Icebreaker

Leave an x on your answer in the following questions. :x: 
In which machine do you usually work?

- Lumi
- Levante
- MN5
- Atos
- Others?

Have you already used AQUA?

- Yes
- A little bit
- No

How would you like to use AQUA in your work?

- ...

Which feature would you really like to see in AQUA?

- ...

## :sailboat: Preparation

We want to be ready to use AQUA on Levante. The code can be accessed by an installation or by container. Detailed documentation can be found [here](http://wilma.to.isac.cnr.it/de/aqua/docs/) (user: aqua pass: aqua2024)

::: info
We suggest to use an installation of AQUA, since fixes and updates based on user requirement could be added to the code in real time during the hackathon. If you prefer to use the cointainer, instructions are provided as well in this section.
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

:::danger
If you don't have access to the [AQUA repository](https://github.com/DestinE-Climate-DT/AQUA) please contact us at [p.davini@isac.cnr.it](mailto:p.davini@isac.cnr.it), [m.nurisso@isac.cnr.it](mailto:m.nurisso@isac.cnr.it) or in Mattermost.
:::

We define for simplicity an `AQUA` environment variable pointing to the AQUA source code:

```bash
export AQUA=/path/to/AQUA
```

### Installation on Levante

Make sure to install a conda manager. We suggest [mamba](https://mamba.readthedocs.io/en/latest/), but every solution works.

Another option on Levante is to use the available python3 module [(DKRZ docs)](https://docs.dkrz.de/doc/levante/code-development/python.html#individual-python-environments). To use this run `module load python3` then `conda init` and copy the `.bashrc` conda content into `~.profile` so that it will also be available on login nodes. After that you should be able to use conda to create and activate the aqua environment.

Once you have a conda manager, you can use the `environment.yaml` file in the main AQUA folder to install the required dependencies.

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

:::info
What is it FDB?

>FDB (Fields DataBase) is a domain-specific object store developed at ECMWF for storing, indexing and retrieving GRIB data.

DestinE and the IFS-FESOM nextGEMS cycle4 run made use of it for the original output.
:::

### Container

:::warning
We suggest to use one of the two suggested methods during the tutorial but a container is available on  Levante.
:::

To launch a container you can follow the [documentation](http://wilma.to.isac.cnr.it/de/aqua/docs/container.html), where it is explained in detail how to launch and use vscode with the AQUA container.

### Jupyter notebooks with AQUA

In order to not exploit the login node we suggest to connect a Jupyter notebook with an interactive job.
First we ask for an interactive node.

On Levante:

```bash
salloc --account=bb1153 --partition=compute --nodes=1 --time=02:30:00 --mem=128G
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
:fire: **Congratulations!** You're now set!
:::

### :interrobang: Issues 

Feel free to modify the following red paragraph with possible questions you have about the preparation procedure:

:::danger
- issue: ...
:::