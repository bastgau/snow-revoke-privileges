"""reset_privilege.py"""

from typing import Any, Dict, List, Optional

import json
import os
import re
import yaml

import pandas as pd

from snowflake.connector import SnowflakeConnection

from snow_revoke_privileges.tools.my_dataframe import (
    concat_dataframe,
    concat_column,
    create_column,
    drop_columns,
    keep_columns,
    rename_column,
)

from snow_revoke_privileges.tools.my_snowflake import (
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

    # Configuration file.
    __config_file_path: Optional[str] = None
    snowflake_connection: Optional[SnowflakeConnection] = None

    # List of databases and schemas that can be ignored.
    system_databases: List[str] = ["SNOWFLAKE", "WORKSHEETS_APP"]
    system_schemas: List[str] = ["INFORMATION_SCHEMA"]

    # List of expected columns.
    expected_columns: List[str] = ["DATABASE_NAME", "SCHEMA_NAME", "OBJECT_NAME", "KEY_OBJECT", "ARGUMENTS", "OBJECT_TYPE"]

    @property
    def config_file_path(self) -> str:
        """
        The function sets the configuration file path.

        Args:
          file_path (str): File path of the configuration file that needs to be set.
        """

        if self.__config_file_path is None:
            return f"{os.path.dirname(__file__)}/config.yaml"

        return self.__config_file_path

    @config_file_path.setter
    def config_file_path(self, file_path: str) -> None:
        """
        The function sets the configuration file path.

        Args:
          file_path (str): File path of the configuration file that needs to be set.
        """

        if os.path.exists(file_path):
            self.__config_file_path = file_path
        else:
            raise RuntimeError(f"Configuration file is unreachable (path: {file_path})")

    def execute(self) -> None:
        """
        The function executes a series of Snowflake queries based on provided parameters and either prints
        the resulting requests or executes them depending on the value of the "run_dry" in settings.
        """

        self.initialize_database()

        settings: Dict[str, Any] = self.get_configuration("settings")

        all_objects: pd.DataFrame = pd.DataFrame([])

        snow_objects = self.prepare_all_objects()
        all_objects = concat_dataframe([all_objects, snow_objects])

        print(f"Total objects before filtering on database: {len(all_objects)}.")
        all_objects = self.filter_databases(
            all_objects,
            settings["databases"],
        )
        print(f"Total objects after filtering on database: {len(all_objects)}.")

        all_privileges: pd.DataFrame = self.prepare_privileges(all_objects)
        all_requests: List[str] = self.prepare_requests(all_privileges, settings["new_owner"])

        print(f"All modifications to perform after checking the ownerships: {len(all_requests)}.")

        if len(all_requests) > 0:

            if settings["run_dry"] is True:
                print(";\n".join(all_requests))
            else:
                self.request_update_privileges_and_ownerships(all_requests)

        else:
            print("Nothing to do. All objects Snowflake and ownerships are correct.")

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

        return ""

    def get_configuration(self, key: str) -> Dict[str, Any]:
        """
        This function reads the YAML configuration file and returns the value of a specified key.

        Args:
            key (str): Key of the configuration value that needs to be retrieved (snowflake or settings)

        Returns:
            a dictionary with the configuration values for the specified key.
        """

        with open(self.config_file_path, "r", encoding="UTF-8") as file:
            config: Dict[str, Dict[str, Any]] = json.loads(str(json.dumps(yaml.safe_load(file))))

        return config[key]

    def get_snowflake_connection(self) -> SnowflakeConnection:
        """
        The function returns a Snowflake connection if it exists, otherwise it raises a runtime error.

        Returns:
            a SnowflakeConnection object.
        """

        if self.snowflake_connection is None:
            raise RuntimeError("Snowflake connection should be already initialized.")

        return self.snowflake_connection

    def initialize_database(self) -> None:
        """
        The function initializes a Snowflake database connection using credentials and admin role specified
        in the configuration.
        """

        config: Dict[str, Any] = self.get_configuration("snowflake")

        snowflake_connection: SnowflakeConnection = snow_configure(
            config["credentials"],
            config["admin_role"],
        )

        self.snowflake_connection = snowflake_connection

    def prepare_all_objects(self) -> pd.DataFrame:
        """
        The function prepares a pandas DataFrame of all database objects without arguments.

        Returns:
            a pandas DataFrame containing information about all the database objects (tables, views,
        sequences, etc.)
        """

        # yapf: disable

        database_objects: List[str] = [
            "DATABASE",
            "SCHEMA",
            "EXTERNAL FUNCTION",
            "EXTERNAL TABLE",
            "FILE FORMAT",
            # "MATERIALIZED VIEW",
            # "PIPE",
            "SEQUENCE",
            "STAGE",
            # "STREAM",
            "TABLE",
            # "TASK"
            "VIEW",
            "PROCEDURE",
            "FUNCTION",
        ]

        # yapf: enable

        all_objects: pd.DataFrame = pd.DataFrame([])

        for database_object in database_objects:
            snow_objects: pd.DataFrame = self.prepare_object(database_object)
            if len(snow_objects) > 0:
                all_objects = concat_dataframe([all_objects, snow_objects])
            print(f"Prepare {database_object}: Done.")

        return all_objects

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
        concat_column(snow_objects, "KEY_OBJECT", ["DATABASE_NAME", "SCHEMA_NAME", "OBJECT_NAME"], ".", "\"")
        snow_objects = keep_columns(snow_objects, self.expected_columns)

        return snow_objects

    def prepare_object(self, object_type: str) -> pd.DataFrame:
        """
        The function prepares a Pandas DataFrame of database objects without any arguments.

        Args:
            object_type (str): a string representing the type of database object to retrieve (e.g."TABLE", "VIEW"").

        Returns:
            a pandas DataFrame containing information about the specified database object.
        """

        snowflake_connection: SnowflakeConnection = self.get_snowflake_connection()

        snow_objects: pd.DataFrame = snow_fetch_pandas_all(
            snowflake_connection,
            f"SHOW {object_type}S IN ACCOUNT",
        )

        if len(snow_objects) == 0:
            return snow_objects

        if object_type == "DATABASE":
            snow_objects = snow_objects.loc[snow_objects["kind"] == "STANDARD"]
            rename_column(snow_objects, {"name": "DATABASE_NAME"})
        elif object_type == "SCHEMA":
            rename_column(snow_objects, {"database_name": "DATABASE_NAME", "name": "SCHEMA_NAME"})
        elif object_type in ("PROCEDURE", "FUNCTION"):
            snow_objects = snow_objects.loc[(snow_objects.loc[:, "is_builtin"] == "N")]  # type: ignore
            rename_column(snow_objects, {"arguments": "ARGUMENTS", "catalog_name": "DATABASE_NAME", "schema_name": "SCHEMA_NAME", "name": "OBJECT_NAME"})
        else:
            rename_column(snow_objects, {"database_name": "DATABASE_NAME", "schema_name": "SCHEMA_NAME", "name": "OBJECT_NAME"})

        snow_objects = self.prepare_columns(snow_objects, object_type)  # type: ignore
        snow_objects = snow_objects.loc[(~snow_objects.loc[:, "DATABASE_NAME"].isin(self.system_databases)) & (~snow_objects.loc[:, "SCHEMA_NAME"].isin(self.system_schemas))]  # type: ignore

        print(f"Found: {len(snow_objects)} {object_type.lower()}(s).")

        return snow_objects

    def prepare_privileges(self, all_objects: pd.DataFrame) -> pd.DataFrame:
        """
        The function prepares and returns a pandas DataFrame containing all privileges for a given set of
        objects in a Snowflake database.

        Args:
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

            snow_privileges: pd.DataFrame = self.request_retrieve_grants(object_type, object_name, False, arguments)

            if len(snow_privileges) > 0:
                snow_privileges = drop_columns(snow_privileges, ["created_on", "granted_by", "grant_option", "name"])
                rename_column(snow_privileges, {"privilege": "PRIVILEGE", "granted_on": "GRANTED_ON", "grantee_name": "GRANTEE_NAME", "granted_to": "GRANTED_TO"})
                create_column(snow_privileges, {"KEY_OBJECT": object_name, "FUTURE": False, "OBJECT_TYPE": object_type, "ARGUMENTS": arguments})
                all_privileges = concat_dataframe([all_privileges, snow_privileges])

            if object_type in ("DATABASE", "SCHEMA"):

                snow_privileges = self.request_retrieve_grants(object_type, object_name, True)

                if len(snow_privileges) > 0:
                    snow_privileges = drop_columns(snow_privileges, ["created_on", "granted_by", "grant_option", "name"])
                    rename_column(snow_privileges, {"privilege": "PRIVILEGE", "grant_on": "GRANTED_ON", "grantee_name": "GRANTEE_NAME", "grant_to": "GRANTED_TO"})
                    create_column(snow_privileges, {"KEY_OBJECT": object_name, "FUTURE": True, "OBJECT_TYPE": object_type, "ARGUMENTS": arguments})
                    all_privileges = concat_dataframe([all_privileges, snow_privileges])

        return all_privileges

    def prepare_requests(self, all_privileges: pd.DataFrame, default_owner: str) -> List[str]:
        """
        The function prepares a list of SQL requests based on a given DataFrame of privileges and a default
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

            if current_privilege["FUTURE"] is False and privilege != "OWNERSHIP":
                request = f"REVOKE {privilege} ON {granted_on} {key_object}{arguments} FROM {granted_to} {grantee_name}"
                all_requests.insert(0, request)
            elif current_privilege["FUTURE"] is False and grantee_name != default_owner:
                request = f"GRANT {privilege} ON {granted_on} {key_object}{arguments} TO {granted_to} {default_owner} -- {grantee_name}"
                all_requests.append(request)
            elif current_privilege["FUTURE"] is True:
                request = f"REVOKE {privilege} ON FUTURE {granted_on}S IN {object_type} {key_object} FROM {granted_to} {grantee_name}"
                all_requests.insert(0, request)

        return all_requests

    def request_retrieve_grants(self, object_type: str, object_name: str, future: bool, arguments: Optional[str] = None) -> pd.DataFrame:
        """
        The function retrieves grants for a specified object.

        Args:
          object_type (str): Object type such as 'TABLE', 'VIEW', 'SCHEMA', etc.
          object_name (str): Object name of the object for which you want to retrieve grants.
          future (bool): Value indicating whether to retrieve future grants or not.
          arguments (str): Optional string parameter that can be used for the `SHOW GRANTS` related to PROCEDURE or FUNCTION

        Returns:
          a DataFrame containing the privileges (grants) for a specified object.
        """

        snowflake_connection: SnowflakeConnection = self.get_snowflake_connection()

        snow_privileges: pd.DataFrame

        if future is True:
            snow_privileges = snow_fetch_pandas_all(snowflake_connection, f"SHOW FUTURE GRANTS IN {object_type} {object_name}")
        else:
            snow_privileges = snow_fetch_pandas_all(snowflake_connection, f"SHOW GRANTS ON {object_type} {object_name} {arguments}")

        return snow_privileges

    def request_update_privileges_and_ownerships(self, requests: List[str]) -> None:
        """
        The function updates privileges and ownerships in a Snowflake database using a list of requests.

        Args:
            requests (List[str]): List of SQL queries to update privileges and ownerships.
        """

        snowflake_connection: SnowflakeConnection = self.get_snowflake_connection()
        snow_execute_multi_requests(snowflake_connection, requests)

    def filter_databases(self, all_objects: pd.DataFrame, working_databases: List[str]) -> pd.DataFrame:
        """
        The function filters a pandas DataFrame based on a list of working databases.

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

        return all_objects.reset_index(drop=True)
