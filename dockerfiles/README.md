# Dockerfiles for AQUA

This directory contains files related to the creation of Docker containers
for AQUA. The instructions here are for building the containers from scratch.

If you just want to run a container with AQUA, check with others if there
is already a version of the AQUA container deployed in a shared folder in
your target HPC environment.

## Dockerfiles

The file `Dockerfile` is used to generate an image that contains:

- FDB with dependencies like metkit, eckit, and ecCodes;
- Micromamba;
- AQUA and dependencies installed in a conda environment
  using the `environment.yml` in this repository;
- JupyterLab to run notebooks.

The container was tested on offline login nodes on BSC MareNostrum 4, and on
different login nodes on CSC LUMI. In both HPC's, there were also tests with
Jupyter Lab containers using SSH tunnel, and also batch jobs submitted with
Slurm calling Singularity to run simple tests.

The final test was using FDB data that uses the DestinE schema, but with
fewer data entries.

A second file `Dockerfile.ubuntu` is used to generate a Docker image with 
a recent version of Ubuntu LTS and containing updated versions of FDB, ecCodes, metkit, eckit and ecbuild.  The AQUA docker image is then derived from this one.


### Prerequisites

These are the prerequisites for building the container images. For running,
the only requirements are an OCI compatible runner in the case of Docker, or
Singularity CE 3.7.3+ (MN4 uses 3.7.3, LUMI uses 3.11.4-1 ATOW).

You must have a computer with a Kernel whose architecture is compatible with
the target platform (i.e. ARM if your container will run on ARM, Intel if it
will run on Intel, etc.).

You must install the following software on your local computer:

- Docker
  - https://docs.docker.com/engine/install/ubuntu/
  - https://docs.docker.com/engine/install/linux-postinstall/ (so you don't need `sudo`)
- Singularity
  - https://singularity-docs.readthedocs.io/en/latest/#installation

Other tools you may require are:

- `git` to clone code;
- `scp` to transfer images to remote HPC's;
- `ssh` to connect to remote HPC's.

### Building

To build it, you can use a command similar to this one, adjusting
the `<CHANGEME_PARAMETER>` values:

```bash
$ cd AQUA/
# -t is the tag name, -f the file, last period the source path or URL.
# This will pull `kinow/fdb` and `mambaorg/micromamba` images from
# DockerHub and build them locally, the it will go through the multi-
# -stage build process, and each layer building the local image.
$ docker build -t <USER>/aqua:latest -f dockerfiles/Dockerfile .
```

That command may take anything between five and thirty minutes, depending
on networking and local computational resources. 

To confirm the image was created, you can run:

```bash
$ docker image ls <USER>/aqua
REPOSITORY   TAG       IMAGE ID       CREATED          SIZE
<USER>/aqua   latest    f6f97ee49b67   16 minutes ago   6.4GB
```

> NOTE: the image is quite large, but at least 5.61GB are coming
>       from the step that installs AQUA and other libs in the
>       conda environment. You can visualize that with
>       `docker history <USER>/aqua`:
>       `...`
>       `<missing>      17 minutes ago   RUN |3 MAMBA_USER=mamba MAMBA_USER_ID=1000 M…   5.61GB    buildkit.dockerfile.v0`

To build the Singularity container, you can start with the following command,
and adjust it if needed:

```bash
$ sudo singularity build <USER>-aqua-latest.sif docker-daemon://aqua:latest
```

The name of the `.sif` file above is just an example. Feel free to modify it,
just make sure to include information that will be helpful to distinguish it
from other files (i.e. `aqua.sif` is likely to cause confusions).

You can upload the `.sif` file to any HPC that supports Singularity,
like BSC MareNostrum 4, or CSC LUMI.

### Running

There are multiple ways to run and use this container with Docker.

#### JupyterLab with Docker

This one starts JupyterLab:

```bash
$ docker run --rm -p 8888:8888 <USER>/aqua:latest
```

You can then navigate to <http://localhost:8888/?token=docker>. If you
are asked for the token, it is hard-coded in the container image as
`docker`. If you prefer randomly-generated tokens, comment out that
part from your `Dockerfile` before building it.

Open a notebook and try something like

```python
import aqua
import pyfdb
import one_pass as opa
```

or

```bash
!fdb-schema | head -n 5
```

#### Python script with Docker

If you want to run a python script from the AQUA repository, you can do
so with a command like:

> NOTE: The command below should work since it runs the simplest test in the
>       `tests/test_basic.py` file. Other tests require downloading the test
>       data beforehand. But this is just to illustrate you can run any shell
>       or Python script with this syntax.

```bash
$ docker run --rm <USER>/aqua:latest pytest tests/test_basic.py::TestAqua::test_aqua_import
```

#### Bash with Docker

Finally, you can also drop into a bash shell within a newly created container
to run some local tests. Once you close the container, it is deleted (due to
the `--rm` flag), which prevents accidental experiments from re-using data
produced in previous tests (i.e. you will have an isolated environment with
every run):

```bash
$ docker run --rm -ti <USER>/aqua:latest /bin/bash
```

#### JupyterLab with Singularity

You can start JupyterLab using Singularity:

```bash
$ (local) ssh me@some-hpc
$ (hpc)   singularity run <USER>-aqua-latest.sif jupyter lab --ip 0.0.0.0 --port 18888
```

And then open up an SSH tunnel from your local computer to the SSH.

```bash
$ (local) ssh -L 8888:localhost:18888 me@some-hpc
```

Then navigate to <http://localhost:8888/?token=docker>.

>NOTE: We used `18888` at the HPC to avoid conflicts with others
>      using the same port number. You can use shuf or another
>      utility to get a random port. Just use the same in the ssh
>      command when creating the tunnel.

#### Singularity with Slurm

This is an example script to submit a Slurm job that executes the
Singularity container. First, make sure the container is in a location
that is accessible from the Slurm nodes (e.g. some network folder).

```bash
#!/bin/bash

#SBATCH --job-name="testing-aqua-singularity"
#SBATCH --cpus-per-task=1
#SBATCH --ntasks=1
#SBATCH --mem-per-cpu=1G
#SBATCH --time=00:05:00
#SBATCH --qos=debug
#SBATCH --output=./slurm-%j.out
#SBATCH --error=./slurm-%j.err

# If necessary, e.g. MN4
module load singularity
# Adjust the path
singularity run /gpfs/scratch/bruno/containers/aqua/aqua-some-version.sif python -c 'import aqua'
echo "OK!"
```

You can submit it with `sbatch slurm_singularity.cmd`, `srun`, etc.

### Cleaning up

Docker is quite hungry for disk space. It is advisable to keep an eye on the
number and size of images and containers.

To look at your containers, and delete stopped or unnecessary ones:

```bash
# This is a popular command, but old syntax, same as `docker container ls -a -s`,
# -a to show offline/all, and -s to show the size, not displayed by default.
$ docker ps -a -s
CONTAINER ID   IMAGE             COMMAND                  CREATED      STATUS                  PORTS     NAMES                  SIZE
b5f5ac0a10aa   aqua-fdb:latest   "/usr/local/bin/_ent…"   3 days ago   Exited (0) 3 days ago             condescending_bartik   0B (virtual 6.4GB)
$ docker rm b5f5ac0a10aa
$ docker container ls -a -s
CONTAINER ID   IMAGE     COMMAND   CREATED   STATUS    PORTS     NAMES     SIZE
```

And for images:

```bash
$ docker image ls
REPOSITORY                                     TAG             IMAGE ID       CREATED          SIZE
kinow/aqua                                     latest          f6f97ee49b67   41 minutes ago   6.4GB
aqua-fdb                                       latest          6df5d59d40ab   3 days ago       6.4GB
<none>                                         <none>          8deabb0538d9   3 days ago       5.58GB
<none>                                         <none>          e27583a80ded   3 days ago       5.57GB
<none>                                         <none>          418ef70f3fcd   3 days ago       746MB
<none>                                         <none>          88e1c438c144   3 days ago       5.56GB
<none>                                         <none>          8df9535b7e6c   3 days ago       5.56GB
<none>                                         <none>          6091197ecb59   3 days ago       5.56GB
kinow/fdb                                      5.11.17-jammy   3f544502f511   3 weeks ago      347MB
test-github-container                          latest          8fedf9a8737d   3 weeks ago      77.8MB
ghcr.io/kinow/test-github-container            latest          8fedf9a8737d   3 weeks ago      77.8MB
quay.io/brunokinoshita/test-github-container   latest          8fedf9a8737d   3 weeks ago      77.8MB
quay.io/kinow/test-github-container            latest          8fedf9a8737d   3 weeks ago      77.8MB
# If the image ID is referenced in multiple places, use the -f to force delete:
$ docker rmi 8fedf9a8737d -f
Untagged: test-github-container:latest
Untagged: ghcr.io/kinow/test-github-container:latest
Untagged: ghcr.io/kinow/test-github-container@sha256:11118ad68dc169dab22f8d4e184bdc84bc05268374f824a3a5fc3f47e4d348d7
Untagged: quay.io/brunokinoshita/test-github-container:latest
Untagged: quay.io/brunokinoshita/test-github-container@sha256:11118ad68dc169dab22f8d4e184bdc84bc05268374f824a3a5fc3f47e4d348d7
Deleted: sha256:8fedf9a8737d886735d27a7609e2d9b5d28c110a5b1a739bfbfdf9249f171b02
```

Finally, you can also wipe Docker resources selectively in groups, like:

```bash
$ docker image prune -f -a
$ docker volume prune -f -a
...
```

And if you just want to start over:

```bash
$ docker system prune -f -a
# A few minutes later...
Total reclaimed space: 85.04GB
```

Just be aware that pruning your whole system means the next
time you need to run a container or build an image, Docker
will start afresh downloading everything over again.

<!---
If we add a new images, like Dockerfile (just AQUA+Python),
and Dockerfile-fdb (AQUA+Python+FDB), and Dockerfile-fdb-jupyter
(AQUA+Python+FDB+JupyterLab) (e.g. for smaller images in the
workflow), then we can add a new section here with the file
name as title, and for building+running we just say something
like "You can follow the steps at the first section of this
document..."
-->
