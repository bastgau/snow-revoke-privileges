#!/bin/bash

RED="\e[31m"
GREEN="\e[32m"
BLUE="\e[34m"
YELLOW="\e[33m"
ENDCOLOR="\e[0m"

echo -e "\n${BLUE}################################"
echo -e "${BLUE}####       INSTALL.SH       ####${ENDCOLOR}"
echo -e "${BLUE}################################"

echo -e "\n${GREEN}> Configure virtual environment.${ENDCOLOR}\n"

sudo chgrp vscode $WORKDIR/.venv
sudo chown vscode $WORKDIR/.venv

git config --global --add safe.directory /workspaces/app
git config --global core.eol lf
git config --global core.autocrlf false

python3 -m venv $WORKDIR/.venv
PATH="$WORKDIR/.venv/bin:$PATH"

echo -e "Done.\n"

echo -e "${GREEN}> Update PIP tool.${ENDCOLOR}\n"
pip install --upgrade pip

echo -e "\n${GREEN}> Identify the packaging and dependency manager to install.${ENDCOLOR}\n"

PIP_MANAGER=false
POETRY_MANAGER=false

NEW_POETRY_INSTALL=false

FILE=$WORKDIR/requirements.txt

if [ -f "$FILE" ];
then
    echo -e "PIP configuration file was found (requirements.txt).\n"
    PIP_MANAGER=true
else

    FILE=$WORKDIR/pyproject.toml

    if [ -f "$FILE" ];
    then
        echo -e "POETRY configuration file was found (pyproject.toml).${ENDCOLOR}"
        POETRY_MANAGER=true
    fi

fi

if [ "$POETRY_MANAGER" = true ] && [ "$PIP_MANAGER" = true ];
then
    echo -e "${RED}> ERROR: You cannot define two packaging and dependency manager in the same time.${ENDCOLOR}\n"
    exit 1
fi

if [ "$POETRY_MANAGER" = false ] && [ "$PIP_MANAGER" = false ];
then

    echo -e "${YELLOW}No packaging and dependency manager was found.${ENDCOLOR}"
    echo -e "${YELLOW}Type 'PIP' or 'POETRY' if you want to install a packaging and dependency manager !${ENDCOLOR}"
    echo -e "${YELLOW}Another option will install no packaging and dependency manager.${ENDCOLOR}"
    echo -e "${YELLOW}Your selection :${ENDCOLOR}"

    read MANAGER
    echo -e "The following packaging and dependency manager will be installed : $MANAGER\n"

    if [ "${MANAGER^^}" = "POETRY" ]
    then
        POETRY_MANAGER=true
        NEW_POETRY_INSTALL=true
    fi

    if [ "${MANAGER^^}" = "PIP" ]
    then
        PIP_MANAGER=true
        touch $WORKDIR/requirements.txt
        touch $WORKDIR/requirements-dev.txt
    fi

fi

source $WORKDIR/.venv/bin/activate

if [ "$PIP_MANAGER" = true ];
then

    echo -e "${GREEN}> Install dependencies with PIP.${ENDCOLOR}\n"

    pip install -r $WORKDIR/requirements.txt
    pip install -r $WORKDIR/requirements-dev.txt
    pip install -r $WORKDIR/requirements-test.txt

fi

if [ "$POETRY_MANAGER" = true ];
then

    echo -e "${GREEN}> Install POETRY tool and install dependencies.${ENDCOLOR}\n"
    curl -sSL https://install.python-poetry.org | python3 -
    poetry completions bash >> ~/.bash_completion

    if [ "$POETRY_MANAGER" = true ];
    then
        poetry init
    fi

    poetry install

fi

chmod +x $WORKDIR/.devcontainer/check-post-install.sh
$WORKDIR/.devcontainer/check-post-install.sh
