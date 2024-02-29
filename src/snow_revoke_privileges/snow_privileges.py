"""..."""

from typing import Any, Dict, Optional
import pandas as pd

from snow_revoke_privileges.tools.my_snowflake import MySnowflake
from snow_revoke_privileges.tools.configuration import Configuration

from snow_revoke_privileges.tools.my_dataframe import (
    concat_dataframe,
    create_column,
    drop_columns,
    rename_column,
)


class SnowPrivileges:  # pylint: disable=unused-variable
    """..."""

    all_objects: pd.DataFrame
    all_privileges: pd.DataFrame

    settings: Dict[str, Any] = {}
    snowflake_credentials: Dict[str, Any] = {}

    def __init__(self, all_objects: pd.DataFrame) -> None:
        """..."""
        self.all_objects = all_objects
        self.all_privileges = pd.DataFrame([])
        self.__load_configuration()

    def prepare(self) -> None:
        """..."""

        self.__prepare_future_false()
        self.__prepare_future_true()

    def __prepare_future_false(self) -> None:
        """..."""

        current_object: Any

        for _, current_object in self.all_objects.iterrows():  # type: ignore # pylint: disable=unused-variable

            object_type: str = str(current_object.get(key="OBJECT_TYPE"))
            object_name: str = str(current_object.get(key="KEY_OBJECT"))
            arguments: str = str(current_object.get(key="ARGUMENTS"))

            arguments = MySnowflake.get_arguments(arguments)

            privileges: pd.DataFrame = self.__request_retrieve_grants(object_type, object_name, False, arguments)
            privileges["OWNERSHIP"] = privileges["privilege"] == "OWNERSHIP"

            index_to_drop: Any = privileges[(privileges["OWNERSHIP"] == True) & (privileges["grantee_name"] == self.settings["new_owner"])  # noqa: E712 # pylint: disable=singleton-comparison
                                            ].index  # pyright: ignore

            privileges = privileges.drop(index_to_drop)  # pyright: ignore

            if len(privileges) > 0:

                privileges = privileges.reset_index(drop=True)

                privileges = self.__prepare_dataframe(privileges, False, {"object_name": object_name, "object_type": object_type, "arguments": arguments})
                self.all_privileges = concat_dataframe([self.all_privileges, privileges])

    def __prepare_future_true(self) -> None:
        """..."""

        current_object: Any

        only_database_schema: pd.DataFrame = self.all_objects.loc[self.all_objects["OBJECT_TYPE"].isin(["DATABASE", "SCHEMA"])]  # type: ignore
        only_database_schema = only_database_schema.reset_index(drop=True)

        for _, current_object in only_database_schema.iterrows():  # type: ignore # pylint: disable=unused-variable

            object_type: str = str(current_object.get(key="OBJECT_TYPE"))
            object_name: str = str(current_object.get(key="KEY_OBJECT"))
            arguments: str = str(current_object.get(key="ARGUMENTS"))

            arguments = MySnowflake.get_arguments(arguments)

            privileges = self.__request_retrieve_grants(object_type, object_name, True)

            if len(privileges) > 0:
                privileges = self.__prepare_dataframe(privileges, True, {"object_name": object_name, "object_type": object_type, "arguments": arguments})
                self.all_privileges = concat_dataframe([self.all_privileges, privileges])

    def __prepare_dataframe(self, privileges: pd.DataFrame, future: bool, object_info: Dict[str, str]) -> pd.DataFrame:
        """..."""

        object_name: str = object_info["object_name"]
        object_type: str = object_info["object_type"]
        arguments: str = object_info["arguments"]

        privileges = drop_columns(privileges, ["created_on", "granted_by", "grant_option", "name", "granted_by_role_type"])
        rename_column(privileges, {"grantee_name": "GRANTEE_NAME"})
        create_column(privileges, {"KEY_OBJECT": object_name, "OBJECT_TYPE": object_type, "ARGUMENTS": arguments})

        if future is True:
            rename_column(privileges, {"grant_on": "GRANTED_ON", "grant_to": "GRANTED_TO"})
            create_column(privileges, {"FUTURE": True, "OWNERSHIP": False})
        else:
            privileges = drop_columns(privileges, ["privilege"])
            rename_column(privileges, {"granted_on": "GRANTED_ON", "granted_to": "GRANTED_TO"})
            create_column(privileges, {"FUTURE": False})

        privileges = privileges.drop_duplicates()

        return privileges

    def __request_retrieve_grants(self, object_type: str, object_name: str, future: bool, arguments: Optional[str] = "") -> pd.DataFrame:
        """..."""

        grants: pd.DataFrame

        if future is True:
            grants = MySnowflake.fetch_pandas_all(f"SHOW FUTURE GRANTS IN {object_type} {object_name}")
        else:
            grants = MySnowflake.fetch_pandas_all(f"SHOW GRANTS ON {object_type} {object_name} {arguments}")

        return grants

    def get_dataframe(self) -> pd.DataFrame:
        """..."""
        return self.all_privileges

    def __load_configuration(self) -> None:
        """..."""

        config: Configuration = Configuration()

        self.settings = config.get_user_configuration("settings")
        self.snowflake_credentials = config.get_user_configuration("snowflake_credentials")
        self.schemas_to_ignore = config.get_application_configuration("schemas_to_ignore")
        self.expected_columns = config.get_application_configuration("expected_columns")
        self.databases_to_ignore = config.get_application_configuration("databases_to_ignore")
