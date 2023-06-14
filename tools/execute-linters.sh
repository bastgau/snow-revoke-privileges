#!/bin/bash

RED="\e[31m"
GREEN="\e[32m"
BLUE="\e[34m"
YELLOW="\e[33m"
BOLD="\e[1m"

ENDCOLOR="\e[0m"

echo -e "\n${YELLOW}> YAML Lint.${ENDCOLOR}"
yamllint /workspaces/app/snowflake_reset/

if [ "$?" -eq 0 ]
then
    echo -e "${GREEN}${BOLD}Success: no issues found${ENDCOLOR}"
fi

echo -e "\n${YELLOW}> Pyright / Pylance.${ENDCOLOR}"
result=$(pyright /workspaces/app/snowflake_reset/)

if [ "$?" -eq 0 ]
then
    echo -e "${GREEN}${BOLD}Success: $result${ENDCOLOR}"
else
    echo -e $result
fi

echo -e "\n${YELLOW}> Pylint.${ENDCOLOR}"
pylint /workspaces/app/snowflake_reset/ --score=false

if [ "$?" -eq 0 ]
then
    echo -e "${GREEN}${BOLD}Success: no issues found${ENDCOLOR}"
fi

echo -e "\n${YELLOW}> Flake8.${ENDCOLOR}"
flake8 /workspaces/app/snowflake_reset/

if [ "$?" -eq 0 ]
then
    echo -e "${GREEN}${BOLD}Success: no issues found${ENDCOLOR}"
fi

echo -e "\n${YELLOW}> Mypy.${ENDCOLOR}"
mypy /workspaces/app/snowflake_reset/

echo -e "\n${YELLOW}> Pylama.${ENDCOLOR}"
pylama /workspaces/app/snowflake_reset/

if [ "$?" -eq 0 ]
then
    echo -e "${GREEN}${BOLD}Success: no issues found${ENDCOLOR}"
fi

echo -e "\n${YELLOW}> Yapf.${ENDCOLOR}"
yapf --diff /workspaces/app/snowflake_reset/ --recursive

if [ "$?" -eq 0 ]
then
    echo -e "${GREEN}${BOLD}Success: no issues found${ENDCOLOR}"
fi

echo -e "\n${BLUE}All verifications are done.${ENDCOLOR}\n"
