#!/bin/bash

source $WORKDIR/.venv/bin/activate
sleep 0.5

RED="\e[31m"
GREEN="\e[32m"
BLUE="\e[34m"
YELLOW="\e[33m"
BOLD="\e[1m"

ENDCOLOR="\e[0m"

echo -e "\n${YELLOW}> Pyright / Pylance.${ENDCOLOR}"
pyright $PYTHONPATH/snow_revoke_privileges/ -p /workspaces/app/tools/pyrightconfig.json

if [ "$?" -eq 0 ]; then
    echo -e "${GREEN}${BOLD}Success: no issues found${ENDCOLOR}"
fi

echo -e "\n${YELLOW}> Pylint.${ENDCOLOR}"
pylint /workspaces/app/tests/ --score=false --jobs=10

if [ "$?" -eq 0 ]; then
    echo -e "${GREEN}${BOLD}Success: no issues found${ENDCOLOR}"
fi

echo -e "\n${YELLOW}> Flake8.${ENDCOLOR}"
flake8 /workspaces/app/tests/

if [ "$?" -eq 0 ]; then
    echo -e "${GREEN}${BOLD}Success: no issues found${ENDCOLOR}"
fi

echo -e "\n${YELLOW}> Mypy.${ENDCOLOR}"
mypy /workspaces/app/tests/

echo -e "\n${YELLOW}> Pylama.${ENDCOLOR}"
pylama /workspaces/app/tests/

if [ "$?" -eq 0 ]; then
    echo -e "${GREEN}${BOLD}Success: no issues found${ENDCOLOR}"
fi

echo -e "\n${YELLOW}> Yapf.${ENDCOLOR}"
yapf --diff /workspaces/app/tests/ --recursive

if [ "$?" -eq 0 ]; then
    echo -e "${GREEN}${BOLD}Success: no issues found${ENDCOLOR}"
fi

echo -e "\n${YELLOW}> Pytest.${ENDCOLOR}\n"
pytest ./tests/*/test_*.py ./tests/test_*.py -v -s -n auto

echo -e "\n${BLUE}All verifications are done.${ENDCOLOR}\n"
