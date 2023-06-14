"""reset_privilege.py"""

from typing import Any, Dict, List, Optional

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
        role that has administrative privileges in the Snowflake account.
        """

        config: Dict[str, Any] = self.get_configuration()

        cnx: SnowflakeConnection = snow_configure(config["snowflake"]["credentials"], admin_role)

        all_objects: pd.DataFrame = pd.DataFrame([])

        snow_objects = self.prepare_databases(cnx)
        all_objects = concat_dataframe([all_objects, snow_objects])

        snow_objects = self.prepare_schemas(cnx)
        all_objects = concat_dataframe([all_objects, snow_objects])

        snow_objects = self.prepare_all_objects_without_arguments(cnx)
        all_objects = concat_dataframe([all_objects, snow_objects])

        snow_objects = self.prepare_all_objects_with_arguments(cnx)
        all_objects = concat_dataframe([all_objects, snow_objects])

        all_objects = self.filter_databases(all_objects, working_databases)

        all_privileges: pd.DataFrame = self.prepare_privileges(cnx, all_objects)
        all_requests: List[str] = self.prepare_requests(all_privileges, default_owner)

        snow_execute_multi_requests(cnx, all_requests)

    def prepare_databases(self, cnx: SnowflakeConnection) -> pd.DataFrame:
        """
        The function prepares a pandas DataFrame containing information about all databases.

        Args:
            cnx (SnowflakeConnection): The parameter `cnx` is of type `SnowflakeConnection` and is used to
        establish a connection to a Snowflake database.

        Returns:
            a pandas DataFrame containing information about all the databases.
        """

        snow_objects: pd.DataFrame = snow_fetch_pandas_all(cnx, "SHOW DATABASES")

        if len(snow_objects) > 0:
            snow_objects = snow_objects.loc[snow_objects["kind"] == "STANDARD"]
            rename_column(snow_objects, {"name": "DATABASE_NAME"})
            snow_objects = self.prepare_columns(snow_objects, "DATABASES") # type: ignore

        return snow_objects

    def prepare_schemas(self, cnx: SnowflakeConnection) -> pd.DataFrame:
        """
        The function prepares a pandas DataFrame containing information about all schemas.

        Args:
            cnx (SnowflakeConnection): The parameter `cnx` is of type `SnowflakeConnection` and is used to
        establish a connection to a Snowflake database.

        Returns:
            a pandas DataFrame containing information about all the schemas.
        """

        snow_objects: pd.DataFrame = snow_fetch_pandas_all(cnx, "SHOW SCHEMAS")

        if snow_objects is not None and len(snow_objects) > 0:
            snow_objects = snow_objects.loc[(snow_objects.loc[:, "database_name"] != "SNOWFLAKE") & (snow_objects.loc[:, "name"] != "INFORMATION_SCHEMA")]
            rename_column(snow_objects, {"database_name": "DATABASE_NAME", "name": "SCHEMA_NAME"})
            snow_objects = self.prepare_columns(snow_objects, "SCHEMAS")

        return snow_objects

    def prepare_all_objects_without_arguments(self, cnx: SnowflakeConnection) -> pd.DataFrame:
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
            # "EXTERNAL FUNCTION",
            "EXTERNAL TABLE",
            "FILE FORMAT",  # "MATERIALIZED VIEW",
            # "PIPE",
            "SEQUENCE",
            "STAGE",  # "STREAM",
            "TABLE",  # "TASK"
            "VIEW",
        ]

        all_objects: pd.DataFrame = pd.DataFrame([])

        for database_object in database_objects:
            snow_objects: pd.DataFrame = self.prepare_object_without_arguments(cnx, database_object)
            if len(snow_objects) > 0:
                all_objects = concat_dataframe([all_objects, snow_objects])

        return all_objects

    def prepare_object_without_arguments(self, cnx: SnowflakeConnection, database_object: str) -> pd.DataFrame:
        """
        The function prepares a Pandas DataFrame of database objects without any arguments.

        Args:
            cnx (SnowflakeConnection): The parameter `cnx` is of type `SnowflakeConnection` and is used to establish a connection to a Snowflake database.
          database_object (str): a string representing the type of database object to retrieve (e.g."TABLE", "VIEW"").

        Returns:
          a pandas DataFrame containing information about the specified database object.
        """

        snow_objects: pd.DataFrame = snow_fetch_pandas_all(cnx, f"SHOW {database_object}S")

        if len(snow_objects) == 0:
            return snow_objects

        snow_objects = snow_objects.loc[(~snow_objects.loc[:, "database_name"].isin(self.get_ignorable_databases())) & (snow_objects.loc[:, "schema_name"] != "INFORMATION_SCHEMA")]  # type: ignore

        if snow_objects is not None and len(snow_objects) > 0:
            rename_column(snow_objects, {"database_name": "DATABASE_NAME", "schema_name": "SCHEMA_NAME", "name": "OBJECT_NAME"})
            snow_objects = self.prepare_columns(snow_objects, database_object)

        return snow_objects

    def prepare_all_objects_with_arguments(self, cnx: SnowflakeConnection) -> pd.DataFrame:
        """
        The function prepares all database objects with arguments (procedure and functions).

        Args:
            cnx (SnowflakeConnection): The parameter `cnx` is of type `SnowflakeConnection` and is used to
        establish a connection to a Snowflake database.

        Returns:
            a pandas DataFrame that contains information about all the stored procedures and user-defined
        functions.
        """

        database_objects: List[str] = [
            "PROCEDURE",
            "USER FUNCTION",
        ]

        all_objects: pd.DataFrame = pd.DataFrame([])

        for database_object in database_objects:
            snow_objects: pd.DataFrame = self.prepare_object_without_arguments(cnx, database_object)
            if len(snow_objects) > 0:
                all_objects = concat_dataframe([all_objects, snow_objects])

        return all_objects

    def prepare_object_with_arguments(self, cnx: SnowflakeConnection, database_object: str) -> pd.DataFrame:
        """
        The function prepares a Pandas DataFrame of database objects with arguments.

        Args:
            cnx (SnowflakeConnection): The parameter `cnx` is of type `SnowflakeConnection` and is used to
        establish a connection to a Snowflake database.
          database_object (str): a string representing the type of database object to retrieve (e.g.
        "USER FUNCTION", "PROCEDURE").

        Returns:
          a pandas DataFrame containing information about the specified database object.
        """

        snow_objects: pd.DataFrame = snow_fetch_pandas_all(cnx, f"SHOW {database_object}S")

        if len(snow_objects) == 0:
            return snow_objects

        databases: List[str] = ["SNOWFLAKE", "WORKSHEETS_APP"]

        snow_objects = snow_objects.loc[(~snow_objects.loc[:, "catalog_name"].isin(databases)) & (snow_objects.loc[:, "schema_name"] != "INFORMATION_SCHEMA")]  # type: ignore
        snow_objects = snow_objects.loc[(snow_objects.loc[:, "is_builtin"] != "Y")]  # type: ignore

        if snow_objects is not None and len(snow_objects) > 0:
            rename_column(snow_objects, {"arguments": "ARGUMENTS", "catalog_name": "DATABASE_NAME", "schema_name": "SCHEMA_NAME", "name": "OBJECT_NAME"})
            snow_objects = self.prepare_columns(snow_objects, database_object)

        return snow_objects

    def prepare_columns(
        self,
        snow_objects: pd.DataFrame,
        object_type: str,
        arguments: Optional[str] = None,
        object_name: Optional[str] = None,
        schema_name: Optional[str] = None,
    ) -> None:
        """
        The function creates columns based on the provided arguments.

        Args:
            snow_objects (pd.DataFrame): A pandas DataFrame containing information about Snowflake objects.
            object_type (str): The type of Snowflake object to create a column for, such as "TABLE" or "VIEW".
            arguments (Optional[str]): This parameter is an optional string that represents the arguments to be passed to a procedure or an user function.
            object_name (Optional[str]): The name of the object.
            schema_name (Optional[str]): The name of the schema.
        """

        create_column(snow_objects, {"ARGUMENTS": arguments, "OBJECT_TYPE": object_type, "OBJECT_NAME": object_name, "SCHEMA_NAME": schema_name})
        concat_column(snow_objects, "KEY_OBJECT", ["DATABASE_NAME", "SCHEMA_NAME", "OBJECT_NAME"], ".")
        snow_objects = drop_column(snow_objects, self.get_droppable_columns())

    def get_ignorable_databases(self) -> List[str]:
        """
        The function returns a list of ignorable databases that can be ignored.

        Returns:
            A list of strings containing the names of databases that can be ignored.
        """

        return ["SNOWFLAKE", "WORKSHEETS_APP"]

    def get_droppable_columns(self) -> List[str]:
        """
        This function returns a list of column names that can be dropped.

        Returns:
            A list of column names that can be dropped from a table.
        """

        return [
            "is_default",
            "is_current",
            "options",
            "origin",
            "next_value",
            "interval",
            "text",
            "is_secure",
            "reserved",
            "is_materialized",
            "created_on",
            "comment",
            "cluster_by",
            "rows",
            "bytes",
            "owner",
            "retention_time",
            "automatic_clustering",
            "change_tracking",
            "is_external",
            "kind",
            "url",
            "has_credentials",
            "has_encryption_key",
            "region",
            "type",
            "cloud",
            "notification_channel",
            "storage_integration",
            "is_table_function",
            "valid_for_clustering",
            "is_builtin",
            "is_aggregate",
            "is_ansi",
            "min_num_arguments",
            "max_num_arguments",
            "description"
        ]

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

            # SHOW GRANTS ON PROCEDURE PRFR_PRD_LND.PRFR_BUDGET_SCH.STPROC1(FLOAT)

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
