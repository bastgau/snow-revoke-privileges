"""..."""

import json
import os
from typing import Any, Dict
import yaml


class Configuration:  # pylint: disable=unused-variable
    """..."""

    def get_user_configuration(self, key: str) -> Any:
        """..."""
        return self.get_configuration(key, "config.yaml")

    def get_application_configuration(self, key: str) -> Any:
        """..."""
        return self.get_configuration(key, "application.yaml")

    def get_configuration(self, key: str, filename: str) -> Any:
        """
        This function reads the YAML configuration file and returns the value of a specified key.

        Args:
            key (str): Key of the configuration value that needs to be retrieved (snowflake or settings)

        Returns:
            a dictionary with the configuration values for the specified key.
        """

        config_file_path: str = f"{os.path.dirname(__file__)}{os.sep}..{os.sep}config{os.sep}{filename}"
        config_file_path = os.path.realpath(config_file_path)

        with open(config_file_path, "r", encoding="UTF-8") as file:
            config: Dict[str, Dict[str, Any]] = json.loads(str(json.dumps(yaml.safe_load(file))))

        return config[key]
