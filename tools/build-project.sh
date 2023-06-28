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

echo -e "\n${YELLOW}> Install bump2version library.${ENDCOLOR}"
python3 -m pip install --upgrade bump2version

echo -e "\n${YELLOW}> Bump version.${ENDCOLOR}"

version=$(cat /workspaces/app/src/snow_revoke_privileges/version)
echo -e "${BLUE}Current version: $version${ENDCOLOR}"

bump2version --current-version $version $1 $PYTHONPATH/$PACKAGE_NAME/version --allow-dirty

version=$(cat /workspaces/app/src/snow_revoke_privileges/version)
echo -e "${BLUE}New version: $version${ENDCOLOR}"

echo -e "\n${YELLOW}> Build project $PACKAGE_NAME.${ENDCOLOR}"
python3 -m build
