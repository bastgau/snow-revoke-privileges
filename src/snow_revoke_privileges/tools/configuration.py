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
        """..."""

        config_file_path: str = f"{os.path.dirname(__file__)}{os.sep}..{os.sep}config{os.sep}{filename}"
        config_file_path = os.path.realpath(config_file_path)

        with open(config_file_path, "r", encoding="UTF-8") as file:
            config: Dict[str, Dict[str, Any]] = json.loads(str(json.dumps(yaml.safe_load(file))))

        return config[key]

    def get_output_path(self, filename: str) -> Any:
        """..."""

        output_path: str = f"{os.path.dirname(__file__)}{os.sep}..{os.sep}output{os.sep}{filename}"
        output_path = os.path.realpath(output_path)

        return output_path
