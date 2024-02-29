#!/bin/bash

source $WORKDIR/.venv/bin/activate
sleep 0.5

RED="\e[31m"
GREEN="\e[32m"
BLUE="\e[34m"
YELLOW="\e[33m"
BOLD="\e[1m"

ENDCOLOR="\e[0m"

echo -e "\n${YELLOW}> Install build library.${ENDCOLOR}"
python3 -m pip install --upgrade build

echo -e "\n${YELLOW}> Build project $PACKAGE_NAME.${ENDCOLOR}"
python3 -m build
