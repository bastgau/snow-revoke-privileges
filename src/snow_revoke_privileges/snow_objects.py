"""..."""

from typing import Any, Dict, List
import pandas as pd

from snow_revoke_privileges.tools.my_snowflake import MySnowflake
from snow_revoke_privileges.tools.configuration import Configuration

from snow_revoke_privileges.tools.my_dataframe import (
    concat_dataframe,
    create_column,
    rename_column,
    keep_columns,
    concat_column,
)


class SnowObjects:  # pylint: disable=unused-variable
    """..."""

    all_objects: pd.DataFrame
    settings: Dict[str, Any] = {}
    snowflake_credentials: Dict[str, Any] = {}

    # List of databases and schemas that can be ignored.
    databases_to_ignore: List[str] = []
    schemas_to_ignore: List[str] = []

    # List of expected columns.
    expected_columns: List[str] = []

    def __init__(self) -> None:
        """..."""
        self.all_objects = pd.DataFrame([])
        self.__load_configuration()

    def __load_configuration(self) -> None:
        """..."""

        config: Configuration = Configuration()

        self.settings = config.get_user_configuration("settings")
        self.snowflake_credentials = config.get_user_configuration("snowflake_credentials")
        self.databases_to_ignore = config.get_application_configuration("databases_to_ignore")
        self.schemas_to_ignore = config.get_application_configuration("schemas_to_ignore")
        self.expected_columns = config.get_application_configuration("expected_columns")

    def filter(self) -> None:
        """..."""

        print(f"Total objects before filtering on database: {len(self.all_objects)}.")
        # On supprime de nos objets, tous les objets qui ne sont pas rattachés aux bases sur lesquelles nous travaillons.
        self.all_objects = self.all_objects.loc[self.all_objects["DATABASE_NAME"].isin(self.settings["databases"])]  # type: ignore
        self.all_objects = self.all_objects.reset_index(drop=True)
        print(f"Total objects after filtering on database: {len(self.all_objects)}.")

    def retrieve(self) -> None:
        """..."""

        database_objects = self.settings["objects"]

        for database_object in database_objects:
            self.retrieve_object(database_object)
            print(f"Prepare {database_object}: Done.")

    def get_dataframe(self) -> pd.DataFrame:
        """..."""
        return self.all_objects

    def retrieve_object(self, object_type: str) -> None:
        """
        The function prepares a Pandas DataFrame of database objects.

        Args:
            object_type (str): a string representing the type of database object to retrieve (e.g."TABLE", "VIEW"").

        Returns:
            a pandas DataFrame containing information about the specified database object.
        """

        snow_objects: pd.DataFrame = MySnowflake.fetch_pandas_all(f"SHOW {object_type}S IN ACCOUNT", )

        print(f"Found: {len(snow_objects)} {object_type.lower()}(s).")

        if len(snow_objects) == 0:
            return

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
        snow_objects = snow_objects.loc[(~snow_objects.loc[:, "DATABASE_NAME"].isin(self.databases_to_ignore)) & (~snow_objects.loc[:, "SCHEMA_NAME"].isin(self.schemas_to_ignore))]  # type: ignore

        self.append_objects(snow_objects)

    def prepare_columns(self, snow_objects: pd.DataFrame, object_type: str) -> pd.DataFrame:
        """
        The function creates columns based on the provided arguments.

        Args:
            snow_objects (pd.DataFrame): A pandas DataFrame containing information about Snowflake objects.
            object_type (str): The type of Snowflake object to create a column for, such as "TABLE" or "VIEW".
        """

        create_column(snow_objects, {"ARGUMENTS": None, "OBJECT_TYPE": object_type, "OBJECT_NAME": None, "SCHEMA_NAME": None})
        concat_column(snow_objects, "KEY_OBJECT", ["DATABASE_NAME", "SCHEMA_NAME", "OBJECT_NAME"], ".", "\"")
        snow_objects = keep_columns(snow_objects, self.expected_columns)

        return snow_objects

    def append_objects(self, snow_objects: pd.DataFrame) -> None:
        """..."""

        if len(snow_objects) > 0:
            self.all_objects = concat_dataframe([self.all_objects, snow_objects])
