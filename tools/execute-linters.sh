#!/bin/bash

source $WORKDIR/.venv/bin/activate
sleep 0.5

RED="\e[31m"
GREEN="\e[32m"
BLUE="\e[34m"
YELLOW="\e[33m"
BOLD="\e[1m"

ENDCOLOR="\e[0m"

echo -e "\n${YELLOW}> YAML Lint.${ENDCOLOR}"
yamllint $PYTHONPATH/snow_revoke_privileges/

if [ "$?" -eq 0 ]; then
    echo -e "${GREEN}${BOLD}Success: no issues found${ENDCOLOR}"
fi

echo -e "\n${YELLOW}> Pyright / Pylance.${ENDCOLOR}"
pyright $PYTHONPATH/snow_revoke_privileges/ -p /workspaces/app/tools/pyrightconfig.json

if [ "$?" -eq 0 ]; then
    echo -e "${GREEN}${BOLD}Success: no issues found${ENDCOLOR}"
fi

echo -e "\n${YELLOW}> Pylint.${ENDCOLOR}"
pylint $PYTHONPATH/snow_revoke_privileges/ --score=false --jobs=10

if [ "$?" -eq 0 ]; then
    echo -e "${GREEN}${BOLD}Success: no issues found${ENDCOLOR}"
fi

echo -e "\n${YELLOW}> Flake8.${ENDCOLOR}"
flake8 $PYTHONPATH/snow_revoke_privileges/

if [ "$?" -eq 0 ]; then
    echo -e "${GREEN}${BOLD}Success: no issues found${ENDCOLOR}"
fi

echo -e "\n${YELLOW}> Mypy.${ENDCOLOR}"
mypy $PYTHONPATH/snow_revoke_privileges/

echo -e "\n${YELLOW}> Yapf.${ENDCOLOR}"
yapf --diff $PYTHONPATH/snow_revoke_privileges/ --recursive

if [ "$?" -eq 0 ]; then
    echo -e "${GREEN}${BOLD}Success: no issues found${ENDCOLOR}"
fi

echo -e "\n${BLUE}All verifications are done.${ENDCOLOR}\n"
