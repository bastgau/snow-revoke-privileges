"""..."""

import os
from copy import deepcopy

from typing import Any, Dict, List

import pytest

import pandas as pd
from pandas.testing import assert_frame_equal

from snow_revoke_privileges.reset_privilege import ResetPrivilege

# def execute(self) -> None:
# def get_snowflake_connection(self) -> SnowflakeConnection:
# def initialize_database(self) -> None:
# def prepare_all_objects(self) -> pd.DataFrame:
# def prepare_columns(self, snow_objects: pd.DataFrame, object_type: str) -> pd.DataFrame:
# def prepare_object(self, object_type: str) -> pd.DataFrame:
# def prepare_privileges(self, all_objects: pd.DataFrame) -> pd.DataFrame:
# def prepare_requests(self, all_privileges: pd.DataFrame, default_owner: str) -> List[str]:
# def request_retrieve_grants(self, object_type: str, object_name: str, future: bool, arguments: Optional[str]) -> pd.DataFrame:
# def request_update_privileges_and_ownerships(self, requests: List[str]) -> None:


def test_filter_databases() -> None:  # pylint: disable=unused-variable
    """..."""

    datasets_path: str = os.path.dirname(__file__) + os.sep + "datasets" + os.sep + "test_reset_privilege"

    working_databases: List[str] = [
        "DEV_BRZ",
        "DEV_SLV",
        "DEV_GLD",
        "PRD_BRZ",
        "PRD_SLV",
        "PRD_GLD",
    ]

    my_dataframe: pd.DataFrame = pd.read_json(datasets_path + os.sep + "source_test_filter_databases.json")  # type: ignore
    expected_dataframe: pd.DataFrame = pd.read_json(datasets_path + os.sep + "expected_test_filter_databases.json")  # type: ignore

    reset_privilege: ResetPrivilege = ResetPrivilege()
    result_dataframe: pd.DataFrame = reset_privilege.filter_databases(my_dataframe, working_databases)

    assert_frame_equal(result_dataframe, expected_dataframe)

    working_databases = []

    my_dataframe = pd.read_json(datasets_path + os.sep + "source_test_filter_databases.json")  # type: ignore
    expected_dataframe = deepcopy(my_dataframe)

    reset_privilege = ResetPrivilege()
    result_dataframe = reset_privilege.filter_databases(my_dataframe, working_databases)

    assert_frame_equal(result_dataframe, expected_dataframe)


def test_get_configuration() -> None:  # pylint: disable=unused-variable
    """..."""

    reset_privilege: ResetPrivilege = ResetPrivilege()
    config_file_path: str = reset_privilege.config_file_path

    assert config_file_path.endswith("config.yaml") == True

    config_file_path = "config-missing.yaml"

    with pytest.raises(Exception) as exc_info:
        reset_privilege.config_file_path = config_file_path

    assert exc_info.type == RuntimeError
    assert str(exc_info.value) == "Configuration file is unreachable (path: config-missing.yaml)"

    config_file_path = os.path.dirname(__file__) + os.sep + "datasets" + os.sep + "test_reset_privilege" + os.sep + "config-test.yaml"

    reset_privilege = ResetPrivilege()
    reset_privilege.config_file_path = config_file_path

    expected_config_snowflake: Dict[str, Any] = {"credentials": {"account": "xxxxxx.west-europe.azure", "user": "xxxxxx", "password": "xxxxxx"}}
    config_snowflake: Dict[str, Any] = reset_privilege.get_configuration("snowflake")

    assert (expected_config_snowflake == config_snowflake) is True

    expected_config_settings: Dict[str, Any] = {"run_dry": True, "new_owner": "ACCOUNTADMIN", "databases": ["DEV_TLS", "DEV_LND", "DEV_BRZ"]}
    config_settings: Dict[str, Any] = reset_privilege.get_configuration("settings")

    assert (expected_config_settings == config_settings) is True

    other_settings: Dict[str, Any] = {}

    with pytest.raises(Exception) as exc_info:
        other_settings = reset_privilege.get_configuration("other_settings")

    del other_settings

    assert exc_info.type in (KeyError, RuntimeError)


def test_get_arguments() -> None:
    """..."""

    reset_privilege: ResetPrivilege = ResetPrivilege()

    argument: str = reset_privilege.get_arguments("MY_PROCEDURE1() RETURN VARCHAR")
    assert argument == "()"

    argument = reset_privilege.get_arguments("MY_PROCEDURE2(FLOAT) RETURN VARCHAR")
    assert argument == "(FLOAT)"

    argument = reset_privilege.get_arguments("MY_PROCEDURE3(FLOAT, VARCHAR) RETURN VARCHAR")
    assert argument == "(FLOAT, VARCHAR)"

    argument = reset_privilege.get_arguments("NOT_A_PROCEDURE")
    assert argument == ""
