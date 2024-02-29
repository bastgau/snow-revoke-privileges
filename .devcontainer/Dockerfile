# See here for image contents:
ARG VARIANT="3.11-bullseye"
FROM mcr.microsoft.com/vscode/devcontainers/python:0-${VARIANT}

ARG WORKSPACE_FOLDER="/workspaces/app"
ARG PACKAGE_NAME="snow_revoke_privileges"

WORKDIR $WORKSPACE_FOLDER

# Install program dependencies:

RUN mkdir /tmp/config_container

COPY /.devcontainer/install-deps.sh /tmp/config_container/install-deps.sh_

RUN tr -d '\015' < /tmp/config_container/install-deps.sh_>/tmp/config_container/install-deps.sh
RUN chmod +x /tmp/config_container/install-deps.sh
RUN /tmp/config_container/install-deps.sh

RUN rm -rvf /tmp/config_container

# Create virtual environment:

VOLUME $WORKSPACE_FOLDER/.venv

# Fix issue with pylint and other linters:

ENV PYTHONPATH=$WORKSPACE_FOLDER/src
ENV WORKDIR=$WORKSPACE_FOLDER
ENV PACKAGE_NAME=$PACKAGE_NAME
