"""reset_privilege.py"""

from typing import Any, Dict, Optional

import pandas as pd

from snowflake.connector import SnowflakeConnection

from snow_revoke_privileges.tools.my_snowflake import MySnowflake

from snow_revoke_privileges.tools.configuration import Configuration

from snow_revoke_privileges.snow_privileges import SnowPrivileges
from snow_revoke_privileges.snow_objects import SnowObjects
from snow_revoke_privileges.snow_revoke_requests import SnowRevokeRequests
from snow_revoke_privileges.snow_new_grant_requests import SnowNewGrantRequest


class Application:  # pylint: disable=unused-variable
    """
    The `ResetPrivilege` class contains methods for executing a series of Snowflake queries to prepare
    and filter a list of objects and privileges, and then executing a list of requests using a Snowflake
    connection.
    """

    # Configuration file.
    snowflake_connection: Optional[SnowflakeConnection] = None

    settings: Dict[str, Any] = {}
    snowflake_credentials: Dict[str, Any] = {}

    def __init__(self) -> None:
        """..."""
        self.__load_configuration()

    def execute(self) -> None:
        """
        The function executes a series of Snowflake queries based on provided parameters and either prints
        the resulting requests or executes them depending on the value of the "run_dry" in settings.
        """

        # On initialize notre base de données.
        MySnowflake.initialize_database(self.snowflake_credentials)

        snow_objects: SnowObjects = SnowObjects()
        snow_objects.retrieve()
        snow_objects.filter()
        all_objects: pd.DataFrame = snow_objects.get_dataframe()

        snow_privilege: SnowPrivileges = SnowPrivileges(all_objects)
        snow_privilege.prepare()
        all_privileges: pd.DataFrame = snow_privilege.get_dataframe()

        snow_revoke_requests: SnowRevokeRequests = SnowRevokeRequests(all_privileges)
        snow_revoke_requests.prepare()
        snow_revoke_requests.execute()

        snow_new_grant_requests: SnowNewGrantRequest = SnowNewGrantRequest(all_objects)
        snow_new_grant_requests.prepare()
        snow_new_grant_requests.execute()

    def __load_configuration(self) -> None:
        """..."""

        config: Configuration = Configuration()
        self.settings = config.get_user_configuration("settings")
        self.snowflake_credentials = config.get_user_configuration("snowflake_credentials")
