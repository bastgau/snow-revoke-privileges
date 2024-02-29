[![maintainer](https://badgen.net/badge/maintainer/bastgau/orange?color=orange&icon=github)](https://gitHub.com/bastgau)
[![twitter](https://badgen.net/badge/twitter/_bastiengautier/?color=orange&icon=twitter)](https://www.twitter.com/_bastiengautier)
[![made-for-vscode](https://badgen.net/badge/Made%20for/VSCode/?color=blue&icon=visualstudio)](https://code.visualstudio.com/)
[![made-with-python](https://badgen.net/badge/Made%20with/Python/?color=blue&icon=pypi)](https://www.python.org/)
[![made-with-docker](https://badgen.net/badge/Made%20with/Docker/?color=blue&icon=docker)](https://www.docker.com/)
<br /><br />
[![quality-code](https://github.com/bastgau/snow-revoke-privileges/actions/workflows/main.yml/badge.svg)](https://github.com/bastgau/snow-revoke-privileges/actions/workflows/main.yml)

# snow-revoke-privileges

[![snowflake](https://github.com/bastgau/tools/blob/master/github-badge/logo-snowflake.svg)](https://www.snowflake.com/)

Script designed to simplify the management of permissions in your **Snowflake** databases.

With this tool, you can effortlessly revoke all permissions granted to existing objects, as well as future objects. Additionally, it modify object ownership by assigning them to another role (by default **SYSADMIN**).

## Usage

Please, check the instructions before executing the command line :

```
python -m snow_revoke_privileges
```

## Requirements

The project uses [pip](https://pypi.org/project/pip/) as package installer.

A configuration file named __config.yaml__ must be created using the same format as [config-example.yaml](https://github.com/bastgau/snow-revoke-privileges/blob/master/snow_revoke_privileges/config/config-example.yaml).

You have to use an account with the permissions to **REVOKE PRIVILEGE** and **GRANT OWNERSHIP**.

## VSCode extensions

The following extensions are recommanded:

- [AutoDocstring - Python Docstring Generator](https://marketplace.visualstudio.com/items?itemName=njpwerner.autodocstring)
- [Prettify JSON](https://marketplace.visualstudio.com/items?itemName=mohsen1.prettify-json)
- [Pylance](https://marketplace.visualstudio.com/items?itemName=ms-python.vscode-pylance)
- [Python](https://marketplace.visualstudio.com/items?itemName=ms-python.python)
- [Python Type Hint](https://marketplace.visualstudio.com/items?itemName=njqdev.vscode-python-typehint)
- [Todo Tree](https://marketplace.visualstudio.com/items?itemName=Gruntfuggly.todo-tree)
- [VSCode-icons](https://marketplace.visualstudio.com/items?itemName=vscode-icons-team.vscode-icons)

The file .vcode/settings.json was updated with my own configuration.

Enjoy!
