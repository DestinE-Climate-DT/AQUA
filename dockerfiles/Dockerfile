# -----------------------------------------------------------------------------
# --- Micromamba / Conda (used later).
FROM mambaorg/micromamba:1.5.6 as micromamba

# -----------------------------------------------------------------------------
FROM ghcr.io/destine-climate-dt/ubuntu22.04-fdb-aqua-test:latest as fdb

# We switch to mamba user later, root for now just to create the user.
USER root

# --- x snip x ---
# --- This is from the official docs from the micromamba image.
# --- https://micromamba-docker.readthedocs.io/en/latest/advanced_usage.html#adding-micromamba-to-an-existing-docker-image
# if your image defaults to a non-root user, then you may want to make the
# next 3 ARG commands match the values in your image. You can get the values
# by running: docker run --rm -it my/image id -a
ARG MAMBA_USER=mamba
ARG MAMBA_USER_ID=1000
ARG MAMBA_USER_GID=1000
ENV MAMBA_USER=$MAMBA_USER
ENV MAMBA_ROOT_PREFIX="/opt/conda"
ENV MAMBA_EXE="/bin/micromamba"

COPY --from=micromamba "$MAMBA_EXE" "$MAMBA_EXE"
COPY --from=micromamba /usr/local/bin/_activate_current_env.sh /usr/local/bin/_activate_current_env.sh
COPY --from=micromamba /usr/local/bin/_dockerfile_shell.sh /usr/local/bin/_dockerfile_shell.sh
COPY --from=micromamba /usr/local/bin/_entrypoint.sh /usr/local/bin/_entrypoint.sh
COPY --from=micromamba /usr/local/bin/_dockerfile_initialize_user_accounts.sh /usr/local/bin/_dockerfile_initialize_user_accounts.sh
COPY --from=micromamba /usr/local/bin/_dockerfile_setup_root_prefix.sh /usr/local/bin/_dockerfile_setup_root_prefix.sh

RUN /usr/local/bin/_dockerfile_initialize_user_accounts.sh && \
    /usr/local/bin/_dockerfile_setup_root_prefix.sh

USER $MAMBA_USER
ENV ENV_NAME=base

SHELL ["/usr/local/bin/_dockerfile_shell.sh"]

# ENTRYPOINT ["/usr/local/bin/_entrypoint.sh"]
# Optional: if you want to customize the ENTRYPOINT and have a conda
# environment activated, then do this:
# ENTRYPOINT ["/usr/local/bin/_entrypoint.sh", "my_entrypoint_program"]
# --- x snip x ---

WORKDIR /app/

COPY --chown=$MAMBA_USER:$MAMBA_USER . AQUA

# git for the pip dependencies installed from source
# gcc for pytdigest (whells not being picked up?)
# jupyterlab to run notebooks
# Generate conda environment (env name does not matter,
# but easier to use base in the container to avoid having
# to activate or install kernels):
RUN micromamba install -y -n base -c conda-forge gcc git vim jupyterlab && \
    micromamba install -y -n base -f AQUA/environment.yml

EXPOSE 8888

WORKDIR /app/AQUA/

ENV PATH "$MAMBA_ROOT_PREFIX/bin:$PATH"
ARG MAMBA_DOCKERFILE_ACTIVATE=1

# NOTE: This works with Docker, but not with Singularity as
#       Singularity will bind the $HOME dir, effectively
#       removing the ~/.config folder that contains the
#       Kernel spec within the Docker squashed FS.
# Install kernel for AQUA
# RUN micromamba run -n base \
#    python -m ipykernel install --user --name aqua

# TODO: https://github.com/oloapinivad/AQUA/issues/76
#
# ...
#     from numba.np.ufunc.decorators import Vectorize, GUVectorize, vectorize, guvectorize
#  File "/opt/conda/lib/python3.9/site-packages/numba/np/ufunc/decorators.py", line 3, in <module>
#    from numba.np.ufunc import _internal
# SystemError: initialization of _internal failed without raising an exception
#
# Apparently this happens in the container on a blank
# environment? Solved with this comment,
# https://github.com/apcamargo/genomad/issues/7#issuecomment-1365256139,
# which is linked to this issue in numba
# https://github.com/numba/numba/issues/8615
# RUN micromamba remove --force --yes numpy && \
#     micromamba install -c conda-forge numpy=1.23.5 && \
#     micromamba clean --all --yes


# Bruno: If aqua has some entrypoint script (e.g. `aqua`), or another
#        useful command just replace it here.
# CMD ["python", "-c", "'import aqua'"]
# CMD ["jupyter", "lab", "--ip", "0.0.0.0", "--port", "8888"]

