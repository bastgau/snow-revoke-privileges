"""..."""

from pathlib import Path
from typing import Any, Dict, List, Tuple

import os
import pandas as pd

from snow_revoke_privileges.tools.my_snowflake import MySnowflake
from snow_revoke_privileges.tools.configuration import Configuration


class SnowRevokeRequests:  # pylint: disable=unused-variable
    """..."""

    settings: Dict[str, Any] = {}
    snowflake_credentials: Dict[str, Any] = {}

    all_privileges: pd.DataFrame

    ownership_requests: List[str] = []
    grant_requests: List[str] = []

    def __init__(self, all_privileges: pd.DataFrame) -> None:
        """..."""
        self.all_privileges = all_privileges
        self.__load_configuration()

    def __load_configuration(self) -> None:
        """..."""

        config: Configuration = Configuration()

        self.settings = config.get_user_configuration("settings")
        self.snowflake_credentials = config.get_user_configuration("snowflake_credentials")

    def execute(self) -> None:
        """"..."""

        self.__execute_part(self.grant_requests, "grants")
        self.__execute_part(self.ownership_requests, "ownerships")

    def __execute_part(self, requests: List[str], request_type: str) -> None:
        """"..."""

        filename: str = f"output-{request_type}.txt"

        if os.path.exists(filename):
            os.remove(filename)

        Path(filename).touch()

        if len(requests) == 0:
            return

        with open(f"output-{request_type}.txt", "w", encoding="utf-8") as file:

            file.write("-- Ready ...\n")
            file.write(";\n".join(requests))

            if self.settings["run_dry"] is False:
                MySnowflake.execute_multi_requests(requests)

            file.write("\n-- ... Done.")

    def prepare(self) -> None:
        """..."""
        self.__prepare_grants()
        self.__prepare_ownerships()

    def __prepare_grants(self) -> None:
        """..."""

        current_privilege: Any

        privileges: pd.DataFrame = self.all_privileges.loc[self.all_privileges["OWNERSHIP"] == False]  # noqa: E712 # pylint: disable=singleton-comparison

        for _, current_privilege in privileges.iterrows():  # type: ignore

            request: str = ""

            (object_type, key_object, grantee_name, granted_on, granted_to, arguments) = self.__get_privilege_attributes(current_privilege)

            if current_privilege["FUTURE"] is False:
                request = f"REVOKE ALL PRIVILEGES ON {granted_on} {key_object}{arguments} FROM {granted_to} {grantee_name}"
                self.grant_requests.append(request)
            elif current_privilege["FUTURE"] is True:
                request = f"REVOKE ALL PRIVILEGES ON FUTURE {granted_on}S IN {object_type} {key_object} FROM {granted_to} {grantee_name}"
                self.grant_requests.append(request)

    def __prepare_ownerships(self) -> None:
        """..."""

        current_privilege: Any

        privileges: pd.DataFrame = self.all_privileges.loc[self.all_privileges["OWNERSHIP"] == True]  # noqa: E712 # pylint: disable=singleton-comparison
        request: str = ""

        for _, current_privilege in privileges.iterrows():  # type: ignore

            (_, key_object, grantee_name, granted_on, granted_to, arguments) = self.__get_privilege_attributes(current_privilege)

            if granted_on in ["DATABASE", "SCHEMA"]:
                continue

            request = f"GRANT OWNERSHIP ON {granted_on} {key_object}{arguments} TO {granted_to} {self.settings['new_owner']} -- instead of {grantee_name}"
            self.ownership_requests.append(request)

    def __get_privilege_attributes(self, privilege: Any) -> Tuple[str, str, str, str, str, str]:
        """..."""

        object_type: str = str(privilege.get(key="OBJECT_TYPE"))
        key_object: str = str(privilege.get(key="KEY_OBJECT"))
        grantee_name: str = str(privilege.get(key="GRANTEE_NAME"))
        granted_on: str = str(privilege.get(key="GRANTED_ON")).replace("_", " ")
        granted_to: str = str(privilege.get(key="GRANTED_TO"))
        arguments: str = str(privilege.get(key="ARGUMENTS"))

        return (object_type, key_object, grantee_name, granted_on, granted_to, arguments)
