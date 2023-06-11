"""reset_privilege.py"""

from typing import Any, Dict, List

import json
import yaml

import pandas as pd

from snowflake.connector import SnowflakeConnection

from snowflake_reset.tools.my_dataframe import (
    concat_dataframe,
    concat_column,
    create_column,
    drop_column,
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

    def execute(self, working_databases: List[str], default_owner: str = "SYSADMIN", admin_role: str = "ACCOUNTADMIN") -> None:
        """
        The function executes a series of Snowflake queries to prepare and filter a list of objects and
        privileges, and then executes a list of requests using a Snowflake connection.

        Args:
          working_databases (List[str]): A list of database names that the script will be executed on. Only
        objects and privileges related to these databases will be included in the output.
          default_owner (str): The default owner is a string parameter that specifies the default owner of
        the objects in Snowflake if no owner is specified explicitly. Defaults to SYSADMIN
          admin_role (str): The `admin_role` parameter is a string that represents the name of the Snowflake
        role that has administrative privileges in the Snowflake account. This role is used to connect to
        Snowflake and perform administrative tasks such as querying metadata, creating and dropping
        databases, schemas, and objects, and granting privileges to. Defaults to ACCOUNTADMIN
        """

        config: Dict[str, Any] = self.get_configuration()

        cnx: SnowflakeConnection = snow_configure(config["snowflake"]["credentials"], admin_role)

        all_objects: pd.DataFrame = pd.DataFrame()

        snow_objects = self.prepare_databases(cnx)
        all_objects = concat_dataframe([all_objects, snow_objects])

        snow_objects = self.prepare_schemas(cnx)
        all_objects = concat_dataframe([all_objects, snow_objects])

        snow_objects = self.prepare_objects(cnx)
        all_objects = concat_dataframe([all_objects, snow_objects])

        all_objects = self.filter_databases(all_objects, working_databases)

        all_privileges: pd.DataFrame = self.prepare_privileges(cnx, all_objects)
        all_requests: List[str] = self.prepare_requests(all_privileges, default_owner)

        snow_execute_multi_requests(cnx, all_requests)

    def prepare_databases(self, cnx: SnowflakeConnection) -> pd.DataFrame:
        """
        The function prepares a Pandas DataFrame containing information about all databases.

        Args:
          cnx (SnowflakeConnection): The parameter `cnx` is a SnowflakeConnection object, which represents a
        connection to a Snowflake database. It is used to execute SQL queries and fetch data from the
        database.

        Returns:
          a pandas DataFrame containing information about all the databases.
        """

        snow_objects: pd.DataFrame = snow_fetch_pandas_all(cnx, "SHOW DATABASES")
        snow_objects = snow_objects.loc[snow_objects["kind"] == "STANDARD"]
        snow_objects = drop_column(snow_objects, ["created_on", "is_default", "is_current", "comment", "options", "kind", "retention_time", "origin", "owner"])
        rename_column(snow_objects, {"name": "DATABASE_NAME"})
        create_column(snow_objects, {"SCHEMA_NAME": None, "OBJECT_NAME": None, "OBJECT_TYPE": "DATABASE"})
        concat_column(snow_objects, "KEY_OBJECT", ["DATABASE_NAME", "SCHEMA_NAME", "OBJECT_NAME"], ".")
        return snow_objects

    def prepare_schemas(self, cnx: SnowflakeConnection) -> pd.DataFrame:
        """
        The function prepares a Pandas DataFrame containing information about all schemas.

        Args:
          cnx (SnowflakeConnection): The parameter `cnx` is a SnowflakeConnection object, which is used to
        establish a connection to a Snowflake database.

        Returns:
          a pandas DataFrame containing information about all the schemas.
        """

        snow_objects: pd.DataFrame = snow_fetch_pandas_all(cnx, "SHOW SCHEMAS")
        snow_objects = drop_column(snow_objects, ["created_on", "is_default", "is_current", "comment", "options", "retention_time", "owner"])
        snow_objects = snow_objects.loc[(snow_objects.loc[:, "database_name"] != "SNOWFLAKE") & (snow_objects.loc[:, "name"] != "INFORMATION_SCHEMA")]
        rename_column(snow_objects, {"database_name": "DATABASE_NAME", "name": "SCHEMA_NAME"})
        create_column(snow_objects, {"OBJECT_NAME": None, "OBJECT_TYPE": "SCHEMA"})
        concat_column(snow_objects, "KEY_OBJECT", ["DATABASE_NAME", "SCHEMA_NAME", "OBJECT_NAME"], ".")
        return snow_objects

    def prepare_objects(self, cnx: SnowflakeConnection) -> pd.DataFrame:
        """
        The function prepares a Pandas DataFrame containing information about all objects (excluded database and
        schemas).

        Args:
          cnx (SnowflakeConnection): SnowflakeConnection object, which represents a connection to a
        Snowflake database.

        Returns:
          a pandas DataFrame containing information about all the objects (excluded databases and schemas).
        """

        snow_objects: pd.DataFrame = snow_fetch_pandas_all(cnx, "SHOW TABLES")
        snow_objects = drop_column(snow_objects, ["created_on", "comment", "cluster_by", "rows", "bytes", "owner", "retention_time", "automatic_clustering", "change_tracking", "is_external"])
        rename_column(snow_objects, {"database_name": "DATABASE_NAME", "schema_name": "SCHEMA_NAME", "name": "OBJECT_NAME", "kind": "OBJECT_TYPE"})
        concat_column(snow_objects, "KEY_OBJECT", ["DATABASE_NAME", "SCHEMA_NAME", "OBJECT_NAME"], ".")
        return snow_objects

    def prepare_privileges(self, cnx: SnowflakeConnection, all_objects: pd.DataFrame) -> pd.DataFrame:
        """
        The function prepares and returns a pandas DataFrame containing all privileges for a given set of
        objects in a Snowflake database.

        Args:
          cnx (SnowflakeConnection): The `cnx` parameter is a SnowflakeConnection object, which represents a
        connection to a Snowflake database.
          all_objects (pd.DataFrame): A pandas DataFrame containing information about all objects in a
        Snowflake account, including their type and name.

        Returns:
          a pandas DataFrame containing all the privileges for the objects passed in the `all_objects`
        parameter.
        """

        all_privileges: pd.DataFrame = pd.DataFrame()

        for index, current_object in all_objects.iterrows():  # type: ignore # pylint: disable=unused-variable

            object_type: str = str(current_object.get(key="OBJECT_TYPE"))
            object_name: str = str(current_object.get(key="KEY_OBJECT"))
            key_object: str = str(current_object.get(key="KEY_OBJECT"))

            snow_privileges = snow_fetch_pandas_all(cnx, f"SHOW GRANTS ON {object_type} {object_name}")
            snow_privileges = drop_column(snow_privileges, ["created_on", "granted_by", "grant_option"])
            rename_column(snow_privileges, {"name": "KEY_OBJECT", "privilege": "PRIVILEGE", "granted_on": "GRANTED_ON", "grantee_name": "GRANTEE_NAME", "granted_to": "GRANTED_TO"})
            create_column(snow_privileges, {"FUTURE": False, "OBJECT_TYPE": object_type})

            all_privileges = concat_dataframe([all_privileges, snow_privileges])

            if object_type in ("DATABASE", "SCHEMA"):

                snow_privileges = snow_fetch_pandas_all(cnx, f"SHOW FUTURE GRANTS IN {object_type} {object_name}")

                if len(snow_privileges) > 0:
                    snow_privileges = drop_column(snow_privileges, ["created_on", "granted_by", "grant_option", "name"])
                    rename_column(snow_privileges, {"privilege": "PRIVILEGE", "grant_on": "GRANTED_ON", "grantee_name": "GRANTEE_NAME", "grant_to": "GRANTED_TO"})
                    create_column(snow_privileges, {"KEY_OBJECT": key_object})
                    create_column(snow_privileges, {"FUTURE": True, "OBJECT_TYPE": object_type})

                    all_privileges = concat_dataframe([all_privileges, snow_privileges])

        return all_privileges

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

            if current_privilege["FUTURE"] is False and privilege != "OWNERSHIP":
                request = f"REVOKE {privilege} ON {granted_on} {key_object} FROM {granted_to} {grantee_name}"
                all_requests.insert(0, request)
            elif current_privilege["FUTURE"] is False and grantee_name != default_owner:
                request = f"GRANT {privilege} ON {granted_on} {key_object} TO {granted_to} {default_owner}"
                all_requests.append(request)
            elif current_privilege["FUTURE"] is True:
                request = f"REVOKE {privilege} ON FUTURE {granted_on}S IN {object_type} {key_object} FROM {granted_to} {grantee_name}"
                all_requests.insert(0, request)

        return all_requests

    def get_configuration(self) -> Dict[str, Any]:
        """
        The function reads a YAML configuration file and returns its contents as a dictionary.

        Returns:
          A dictionary containing configuration information.
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
