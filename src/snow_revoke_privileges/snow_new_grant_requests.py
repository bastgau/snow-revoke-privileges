"""..."""

from typing import List
import os

from pathlib import Path
import pandas as pd

from snow_revoke_privileges.tools.my_snowflake import MySnowflake
from snow_revoke_privileges.tools.configuration import Configuration


class SnowNewGrantRequest:  # pylint: disable=unused-variable
    """..."""

    all_objects: pd.DataFrame
    requests: List[str] = []

    def __init__(self, all_objects: pd.DataFrame) -> None:
        """..."""
        self.all_objects = all_objects
        self.__load_configuration()

    def __load_configuration(self) -> None:
        """..."""

        config: Configuration = Configuration()

        self.settings = config.get_user_configuration("settings")
        self.snowflake_credentials = config.get_user_configuration("snowflake_credentials")

    def prepare(self) -> None:
        """..."""

        all_objects: List[str] = self.settings["objects"]

        schemas: pd.DataFrame = self.all_objects.loc[self.all_objects["OBJECT_TYPE"] == "SCHEMA"]  # noqa: E712 # pylint: disable=singleton-comparison
        request: str = ""

        for _, schema in schemas.iterrows():  # type: ignore

            for current_object in all_objects:

                if current_object in ["EXTERNAL FUNCTION", "EXTERNAL TABLE"]:
                    continue

                if current_object in ["SCHEMA", "DATABASE"]:
                    request = f"GRANT USAGE ON {current_object.upper()} {schema['KEY_OBJECT']} TO ROLE {self.settings['new_owner']}"
                    self.requests.append(request)
                    continue

                request = f"GRANT ALL PRIVILEGES ON FUTURE {current_object.upper()}S IN SCHEMA {schema['KEY_OBJECT']} TO ROLE {self.settings['new_owner']}"
                self.requests.append(request)
                request = f"GRANT ALL PRIVILEGES ON ALL {current_object.upper()}S IN SCHEMA {schema['KEY_OBJECT']} TO ROLE {self.settings['new_owner']}"
                self.requests.append(request)

    def execute(self) -> None:
        """"..."""

        filename: str = "output-new-grants.txt"

        if os.path.exists(filename):
            os.remove(filename)

        Path(filename).touch()

        if len(self.requests) == 0:
            return

        with open(filename, "w", encoding="utf-8") as file:

            file.write("-- Ready ...\n")
            file.write(";\n".join(self.requests))

            if self.settings["run_dry"] is False:
                MySnowflake.execute_multi_requests(self.requests)

            file.write("\n-- ... Done.")
