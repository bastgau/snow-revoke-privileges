"""reset_privilege.py"""

from typing import Any, Dict, List

import json
import re
import yaml

import pandas as pd

from snowflake.connector import SnowflakeConnection

from snowflake_reset.tools.my_dataframe import (
    concat_dataframe,
    concat_column,
    create_column,
    drop_columns,
    keep_columns,
    rename_column,
)

from snowflake_reset.tools.my_snowflake import (
    configure as snow_configure,
    fetch_pandas_all as snow_fetch_pandas_all,
    execute_multi_requests as snow_execute_multi_requests,
)


class ResetPrivilege:  # pylint: disable=unused-variable
    """
    The `ResetPrivilege` class contains methods for executing a series of Snowflake queries to prepare
    and filter a list of objects and privileges, and then executing a list of requests using a Snowflake
    connection.
    """

    def execute(self, working_databases: List[str], default_owner: str = "SYSADMIN", admin_role: str = "ACCOUNTADMIN", run_dry: bool = True) -> None:
        """
        This function executes a series of Snowflake queries based on provided parameters and either prints
        the resulting requests or executes them depending on the value of the "run_dry" parameter.

        Args:
          working_databases (List[str]): A list of database that the script will work on.
          default_owner (str): The user who will be assigned ownership. Defaults to SYSADMIN
          admin_role (str): The role used to execute administrative tasks. Defaults to ACCOUNTADMIN
          run_dry (bool): Flag to determines whether the script should execute the SQL queries or just print them out. Defaults to True
        """

        config: Dict[str, Any] = self.get_configuration()

        cnx: SnowflakeConnection = snow_configure(config["snowflake"]["credentials"], admin_role)

        all_objects: pd.DataFrame = pd.DataFrame([])

        snow_objects = self.prepare_all_objects(cnx)
        all_objects = concat_dataframe([all_objects, snow_objects])

        all_objects = self.filter_databases(all_objects, working_databases)

        all_privileges: pd.DataFrame = self.prepare_privileges(cnx, all_objects)
        all_requests: List[str] = self.prepare_requests(all_privileges, default_owner)

        if run_dry is True:
            print(all_requests)
        else:
            snow_execute_multi_requests(cnx, all_requests)

    def prepare_all_objects(self, cnx: SnowflakeConnection) -> pd.DataFrame:
        """
        The function prepares a pandas DataFrame of all database objects without arguments.

        Args:
            cnx (SnowflakeConnection): The parameter `cnx` is of type `SnowflakeConnection` and is used to
        establish a connection to a Snowflake database.

        Returns:
            a pandas DataFrame containing information about all the database objects (tables, views,
        sequences, etc.)
        """

        database_objects: List[str] = [
            "DATABASE",
            "SCHEMA",
            "EXTERNAL FUNCTION",
            "EXTERNAL TABLE",
            "FILE FORMAT",  # "MATERIALIZED VIEW",
            # "PIPE",
            "SEQUENCE",
            "STAGE",  # "STREAM",
            "TABLE",  # "TASK"
            "VIEW",
            "PROCEDURE",
            "USER FUNCTION",
        ]

        all_objects: pd.DataFrame = pd.DataFrame([])

        for database_object in database_objects:
            snow_objects: pd.DataFrame = self.prepare_object(cnx, database_object)
            if len(snow_objects) > 0:
                all_objects = concat_dataframe([all_objects, snow_objects])

        return all_objects

    def prepare_object(self, cnx: SnowflakeConnection, object_type: str) -> pd.DataFrame:
        """
        The function prepares a Pandas DataFrame of database objects without any arguments.

        Args:
            cnx (SnowflakeConnection): The parameter `cnx` is of type `SnowflakeConnection` and is used to establish a connection to a Snowflake database.
          object_type (str): a string representing the type of database object to retrieve (e.g."TABLE", "VIEW"").

        Returns:
          a pandas DataFrame containing information about the specified database object.
        """

        snow_objects: pd.DataFrame = snow_fetch_pandas_all(cnx, f"SHOW {object_type}S")

        if len(snow_objects) == 0:
            return snow_objects

        if object_type == "DATABASE":
            snow_objects = snow_objects.loc[snow_objects["kind"] == "STANDARD"]
            rename_column(snow_objects, {"name": "DATABASE_NAME"})
        elif object_type == "SCHEMA":
            rename_column(snow_objects, {"database_name": "DATABASE_NAME", "name": "SCHEMA_NAME"})
        elif object_type in ("PROCEDURE", "USER FUNCTION"):
            snow_objects = snow_objects.loc[(snow_objects.loc[:, "is_builtin"] != "Y")]  # type: ignore
            rename_column(snow_objects, {"arguments": "ARGUMENTS", "catalog_name": "DATABASE_NAME", "schema_name": "SCHEMA_NAME", "name": "OBJECT_NAME"})
        else:
            rename_column(snow_objects, {"database_name": "DATABASE_NAME", "schema_name": "SCHEMA_NAME", "name": "OBJECT_NAME"})

        snow_objects = self.prepare_columns(snow_objects, object_type)  # type: ignore
        snow_objects = snow_objects.loc[(~snow_objects.loc[:, "DATABASE_NAME"].isin(self.get_ignorable_databases())) & (snow_objects.loc[:, "SCHEMA_NAME"] != "INFORMATION_SCHEMA")]  # type: ignore

        return snow_objects

    def prepare_columns(self, snow_objects: pd.DataFrame, object_type: str) -> pd.DataFrame:
        """
        The function creates columns based on the provided arguments.

        Args:
            snow_objects (pd.DataFrame): A pandas DataFrame containing information about Snowflake objects.
            object_type (str): The type of Snowflake object to create a column for, such as "TABLE" or "VIEW".
            arguments (Optional[str]): This parameter is an optional string that represents the arguments to be passed to a procedure or an user function.
            object_name (Optional[str]): The name of the object.
            schema_name (Optional[str]): The name of the schema.
        """

        create_column(snow_objects, {"ARGUMENTS": None, "OBJECT_TYPE": object_type, "OBJECT_NAME": None, "SCHEMA_NAME": None})
        concat_column(snow_objects, "KEY_OBJECT", ["DATABASE_NAME", "SCHEMA_NAME", "OBJECT_NAME"], ".")
        snow_objects = keep_columns(snow_objects, self.get_expected_columns())

        return snow_objects

    def get_ignorable_databases(self) -> List[str]:
        """
        The function returns a list of ignorable databases that can be ignored.

        Returns:
            A list of strings containing the names of databases that can be ignored.
        """

        return ["SNOWFLAKE", "WORKSHEETS_APP"]

    def get_expected_columns(self) -> List[str]:
        """
        This function returns a list of column names that must be kept.

        Returns:
            a list of column names that must be kept.
        """

        return ["DATABASE_NAME", "SCHEMA_NAME", "OBJECT_NAME", "KEY_OBJECT", "ARGUMENTS", "OBJECT_TYPE"]

    def prepare_privileges(self, cnx: SnowflakeConnection, all_objects: pd.DataFrame) -> pd.DataFrame:
        """
        The function prepares and returns a pandas DataFrame containing all privileges for a given set of
        objects in a Snowflake database.

        Args:
            cnx (SnowflakeConnection): The parameter `cnx` is of type `SnowflakeConnection` and is used to
        establish a connection to a Snowflake database.
            all_objects (pd.DataFrame): A pandas DataFrame containing information about all objects in a
        Snowflake account, including their type and name.

        Returns:
            a pandas DataFrame containing all the privileges for the objects passed in the `all_objects`
        parameter.
        """

        all_privileges: pd.DataFrame = pd.DataFrame([])

        for index, current_object in all_objects.iterrows():  # type: ignore # pylint: disable=unused-variable

            object_type: str = str(current_object.get(key="OBJECT_TYPE"))
            object_name: str = str(current_object.get(key="KEY_OBJECT"))
            arguments: str = str(current_object.get(key="ARGUMENTS"))

            if arguments == "None":
                arguments = ""
            else:
                arguments = self.get_arguments(arguments)

            snow_privileges: pd.DataFrame = snow_fetch_pandas_all(cnx, f"SHOW GRANTS ON {object_type} {object_name} {arguments}")

            if len(snow_privileges) > 0:
                snow_privileges = drop_columns(snow_privileges, ["created_on", "granted_by", "grant_option", "name"])
                rename_column(snow_privileges, {"privilege": "PRIVILEGE", "granted_on": "GRANTED_ON", "grantee_name": "GRANTEE_NAME", "granted_to": "GRANTED_TO"})
                create_column(snow_privileges, {"KEY_OBJECT": object_name, "FUTURE": False, "OBJECT_TYPE": object_type, "ARGUMENTS": arguments})
                all_privileges = concat_dataframe([all_privileges, snow_privileges])

            if object_type in ("DATABASE", "SCHEMA"):

                snow_privileges = snow_fetch_pandas_all(cnx, f"SHOW FUTURE GRANTS IN {object_type} {object_name}")

                if len(snow_privileges) > 0:
                    snow_privileges = drop_columns(snow_privileges, ["created_on", "granted_by", "grant_option", "name"])
                    rename_column(snow_privileges, {"privilege": "PRIVILEGE", "grant_on": "GRANTED_ON", "grantee_name": "GRANTEE_NAME", "grant_to": "GRANTED_TO"})
                    create_column(snow_privileges, {"KEY_OBJECT": object_name, "FUTURE": True, "OBJECT_TYPE": object_type, "ARGUMENTS": arguments})
                    all_privileges = concat_dataframe([all_privileges, snow_privileges])

        return all_privileges

    def get_arguments(self, arguments: str) -> str:
        """
        The function extracts the procedure stored of function arguments from a string returned by the SHOW command.

        Args:
            arguments (str): The `arguments` parameter is a string that represents a complete argument string provided by a SHOW command.

        Returns:
            a string containing only the arguments.
        """

        regex = r"[^\(]+(\([^\)]*\)).*"
        matches = re.search(regex, arguments, re.DOTALL)

        if matches:
            return str(matches.groups(1)[0])

        return arguments

    def prepare_requests(self, all_privileges: pd.DataFrame, default_owner: str) -> List[str]:
        """
        This function prepares a list of SQL requests based on a given DataFrame of privileges and a default
        owner.

        Args:
            all_privileges (pd.DataFrame): A pandas DataFrame containing information about all the privileges
        that need to be granted or revoked.
            default_owner (str): A string representing the default owner to whom privileges will be granted if
        the current grantee is not the default owner.

        Returns:
            a list of SQL requests that either revoke or grant privileges on specific objects to specific
        users, based on the input parameters `all_privileges` and `default_owner`.
        """

        all_requests: List[str] = []

        for index, current_privilege in all_privileges.iterrows():  # type: ignore # pylint: disable=unused-variable

            request: str = ""

            privilege: str = str(current_privilege.get(key="PRIVILEGE"))
            object_type: str = str(current_privilege.get(key="OBJECT_TYPE"))
            key_object: str = str(current_privilege.get(key="KEY_OBJECT"))
            grantee_name: str = str(current_privilege.get(key="GRANTEE_NAME"))
            granted_on: str = str(current_privilege.get(key="GRANTED_ON")).replace("_", " ")
            granted_to: str = str(current_privilege.get(key="GRANTED_TO"))
            arguments: str = str(current_privilege.get(key="ARGUMENTS"))

            # SHOW GRANTS ON PROCEDURE PRFR_PRD_LND.PRFR_BUDGET_SCH.STPROC1(FLOAT)

            if current_privilege["FUTURE"] is False and privilege != "OWNERSHIP":
                request = f"REVOKE {privilege} ON {granted_on} {key_object}{arguments} FROM {granted_to} {grantee_name}"
                all_requests.insert(0, request)
            elif current_privilege["FUTURE"] is False and grantee_name != default_owner:
                request = f"GRANT {privilege} ON {granted_on} {key_object}{arguments} TO {granted_to} {default_owner}"
                all_requests.append(request)
            elif current_privilege["FUTURE"] is True:
                request = f"REVOKE {privilege} ON FUTURE {granted_on}S IN {object_type} {key_object} FROM {granted_to} {grantee_name}"
                all_requests.insert(0, request)

        return all_requests

    def get_configuration(self) -> Dict[str, Any]:
        """
        The function reads a YAML configuration file and returns its contents as a dictionary.

        Returns:
            a dictionary containing configuration information.
        """

        configuration_file: Any = "/workspaces/app/snowflake_reset/config.yaml"

        with open(configuration_file, "r", encoding="UTF-8") as file:
            config: Dict[str, Dict[str, Any]] = json.loads(json.dumps(yaml.safe_load(file)))

        return config

    def filter_databases(self, all_objects: pd.DataFrame, working_databases: List[str]) -> pd.DataFrame:
        """
        This function filters a pandas DataFrame based on a list of working databases.

        Args:
            all_objects (pd.DataFrame): a pandas DataFrame containing information about all databases
            working_databases (List[str]): A list of strings representing the names of the databases that
        should be included in the filtered DataFrame.

        Returns:
            a filtered pandas DataFrame based on the input parameters. The returned DataFrame will only
        contain rows where the "DATABASE_NAME" column matches one of the values in the "working_databases"
        list.
        """

        if len(working_databases) > 0:
            all_objects = all_objects.loc[all_objects["DATABASE_NAME"].isin(working_databases)]  # type: ignore

        return all_objects
