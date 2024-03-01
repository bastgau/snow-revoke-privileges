"""..."""

from typing import List
import os
import logging

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

        databases: pd.DataFrame = self.all_objects.loc[self.all_objects["OBJECT_TYPE"] == "DATABASE"]  # noqa: E712 # pylint: disable=singleton-comparison
        request: str = ""

        for _, database in databases.iterrows():  # type: ignore
            request = f"GRANT USAGE ON DATABASE {database['KEY_OBJECT']} TO ROLE {self.settings['new_owner']}"
            self.requests.append(request)

        schemas: pd.DataFrame = self.all_objects.loc[self.all_objects["OBJECT_TYPE"] == "SCHEMA"]  # noqa: E712 # pylint: disable=singleton-comparison

        for _, schema in schemas.iterrows():  # type: ignore

            request = f"GRANT USAGE ON SCHEMA {schema['KEY_OBJECT']} TO ROLE {self.settings['new_owner']}"
            self.requests.append(request)

            for current_object in all_objects:

                if current_object in ["EXTERNAL FUNCTION", "EXTERNAL TABLE", "DATABASE", "SCHEMA"]:
                    continue

                request = f"GRANT ALL PRIVILEGES ON FUTURE {current_object.upper()}S IN SCHEMA {schema['KEY_OBJECT']} TO ROLE {self.settings['new_owner']}"
                self.requests.append(request)
                request = f"GRANT ALL PRIVILEGES ON ALL {current_object.upper()}S IN SCHEMA {schema['KEY_OBJECT']} TO ROLE {self.settings['new_owner']}"
                self.requests.append(request)

    def execute(self) -> None:
        """"..."""

        config: Configuration = Configuration()
        filename: str = config.get_output_path("output-grant.sql")

        if os.path.exists(filename):
            os.remove(filename)

        Path(filename).touch()

        if len(self.requests) == 0:
            logging.getLogger("app").info("All GRANT requests were now performed.")
            return

        with open(filename, "w", encoding="utf-8") as file:

            file.write("-- Ready ...\n")
            file.write(";\n".join(self.requests))

            if self.settings["run_dry"] is False:
                logging.getLogger("app").info("A total of %s GRANT requests will be performed.", len(self.requests))
                MySnowflake.execute_multi_requests(self.requests)
                logging.getLogger("app").info("All GRANT requests were now performed.")
            else:
                logging.getLogger("app").warning("No GRANT request will be performed as requested by the user (run_dry=True).")

            file.write("\n-- ... Done.")
