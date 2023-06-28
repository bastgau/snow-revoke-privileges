"""..."""

from typing import Any, Dict, List, Optional
from copy import deepcopy

import pandas as pd
from pandas.testing import assert_frame_equal

from snow_revoke_privileges.tools.my_dataframe import (
    concat_dataframe,
    concat_column,
    create_column,
    create_dataframe,
    drop_columns,
    keep_columns,
    rename_column,
    set_column_names,
)


def empty_dataframe() -> pd.DataFrame:
    """..."""

    data: Dict[str, List[str]] = {}
    return pd.DataFrame(data=data)


def dataframe01() -> pd.DataFrame:
    """..."""

    data: Dict[str, List[str]] = {
        "ColA": ["AA1", "AA2", "AA3", "AA4", "AA5"],
        "ColB": ["BB1", "BB2", "BB3", "BB4", "BB5"],
        "ColC": ["CC1", "CC2", "CC3", "CC4", "CC5"],
    }
    return pd.DataFrame(data=data)


def dataframe02() -> pd.DataFrame:
    """..."""

    data: Dict[str, List[str]] = {
        "ColA": ["A1", "A2", "A3", "A4", "A5"],
        "ColB": ["B1", "B2", "B3", "B4", "B5"],
        "ColC": ["C1", "C2", "C3", "C4", "C5"],
    }
    return pd.DataFrame(data=data)


def concat_dataframe01_dataframe02() -> pd.DataFrame:
    """..."""

    data: Dict[str, List[str]] = {
        "ColA": ["A1", "A2", "A3", "A4", "A5", "AA1", "AA2", "AA3", "AA4", "AA5"],
        "ColB": ["B1", "B2", "B3", "B4", "B5", "BB1", "BB2", "BB3", "BB4", "BB5"],
        "ColC": ["C1", "C2", "C3", "C4", "C5", "CC1", "CC2", "CC3", "CC4", "CC5"],
    }
    return pd.DataFrame(data=data)


def test_concat_dataframe() -> None:  # pylint: disable=unused-variable
    """..."""

    my_empty_dataframe: pd.DataFrame = empty_dataframe()
    my_dataframe01: pd.DataFrame = dataframe01()
    my_dataframe02: pd.DataFrame = dataframe02()
    my_concat_dataframe01_dataframe02: pd.DataFrame = concat_dataframe01_dataframe02()

    result_dataframe: pd.DataFrame = concat_dataframe([my_dataframe02, my_dataframe01])
    assert_frame_equal(result_dataframe, my_concat_dataframe01_dataframe02)

    result_dataframe = concat_dataframe([my_empty_dataframe, my_dataframe01])
    assert_frame_equal(result_dataframe, my_dataframe01)


def dataframe01_with_concatenated_ab_with_comma() -> pd.DataFrame:
    """..."""

    data: Dict[str, List[str]] = {
        "ColA": ["AA1", "AA2", "AA3", "AA4", "AA5"],
        "ColB": ["BB1", "BB2", "BB3", "BB4", "BB5"],
        "ColC": ["CC1", "CC2", "CC3", "CC4", "CC5"],
        "ColD": ["#AA1#,#BB1#", "#AA2#,#BB2#", "#AA3#,#BB3#", "#AA4#,#BB4#", "#AA5#,#BB5#"],
    }
    return pd.DataFrame(data=data)


def dataframe02_with_concatenated_ab_with_default() -> pd.DataFrame:
    """..."""

    data: Dict[str, List[str]] = {
        "ColA": ["A1", "A2", "A3", "A4", "A5"],
        "ColB": ["B1", "B2", "B3", "B4", "B5"],
        "ColC": ["C1", "C2", "C3", "C4", "C5"],
        "ColD": ["A1.B1", "A2.B2", "A3.B3", "A4.B4", "A5.B5"],
    }
    return pd.DataFrame(data=data)


def test_concat_column() -> None:  # pylint: disable=unused-variable
    """..."""

    column_dst: str = "ColD"
    columns_src: List[str] = ["ColA", "ColB"]
    separator: str = ","
    enclosure: str = "#"

    my_dataframe: pd.DataFrame = deepcopy(dataframe01())
    expected_dataframe: pd.DataFrame = dataframe01_with_concatenated_ab_with_comma()
    concat_column(my_dataframe, column_dst, columns_src, separator, enclosure)
    assert_frame_equal(my_dataframe, expected_dataframe)

    my_dataframe = deepcopy(dataframe02())
    expected_dataframe = dataframe02_with_concatenated_ab_with_default()
    concat_column(my_dataframe, column_dst, columns_src)
    assert_frame_equal(my_dataframe, expected_dataframe)


def dataframe01_with_two_new_column() -> pd.DataFrame:
    """..."""

    data: Dict[str, List[Optional[str]]] = {
        "ColA": ["AA1", "AA2", "AA3", "AA4", "AA5"],
        "ColB": ["BB1", "BB2", "BB3", "BB4", "BB5"],
        "ColC": ["CC1", "CC2", "CC3", "CC4", "CC5"],
        "ColE": ["EEE", "EEE", "EEE", "EEE", "EEE"],
        "ColF": [None, None, None, None, None],
    }
    return pd.DataFrame(data=data)


def test_create_column() -> None:  # pylint: disable=unused-variable
    """..."""

    my_dataframe: pd.DataFrame = deepcopy(dataframe01())
    expected_dataframe: pd.DataFrame = dataframe01_with_two_new_column()
    columns: Dict[str, Any] = {"ColE": "EEE", "ColF": None}
    create_column(my_dataframe, columns)
    assert_frame_equal(my_dataframe, expected_dataframe)

    my_dataframe = deepcopy(dataframe01())
    expected_dataframe = deepcopy(dataframe01())
    columns = {}
    create_column(my_dataframe, columns)
    assert_frame_equal(my_dataframe, expected_dataframe)


def data_to_create_dataframe01() -> List[Dict[str, Any]]:
    """..."""

    data: List[Dict[str, Any]] = [
        {"a": "AA1", "b": "BB1", "c": "CC1"},
        {"a": "AA2", "b": "BB2", "c": "CC2"},
        {"a": "AA3", "b": "BB3", "c": "CC3"},
        {"a": "AA4", "b": "BB4", "c": "CC4"},
        {"a": "AA5", "b": "BB5", "c": "CC5"},
    ]
    return data


def column_to_create_dataframe01() -> List[str]:
    """..."""

    return ["ColA", "ColB", "ColC"]


def test_create_dataframe() -> None:  # pylint: disable=unused-variable
    """..."""

    data: List[Dict[str, Any]] = data_to_create_dataframe01()
    column_names: List[str] = column_to_create_dataframe01()
    my_dataframe: pd.DataFrame = create_dataframe(data, column_names)
    expected_dataframe: pd.DataFrame = dataframe01()
    assert_frame_equal(my_dataframe, expected_dataframe)


def dataframe01_with_new_column_name() -> pd.DataFrame:
    """..."""

    data: Dict[str, List[str]] = {
        "AAA": ["AA1", "AA2", "AA3", "AA4", "AA5"],
        "ColB": ["BB1", "BB2", "BB3", "BB4", "BB5"],
        "CCC": ["CC1", "CC2", "CC3", "CC4", "CC5"],
    }
    return pd.DataFrame(data=data)


def test_rename_column() -> None:  # pylint: disable=unused-variable
    """..."""

    my_dataframe: pd.DataFrame = deepcopy(dataframe01())
    new_column_names: Dict[str, str] = {
        "ColA": "AAA",
        "ColC": "CCC",
    }
    rename_column(my_dataframe, new_column_names)
    expected_dataframe: pd.DataFrame = dataframe01_with_new_column_name()
    assert_frame_equal(my_dataframe, expected_dataframe)


def test_drop_columns() -> None:  # pylint: disable=unused-variable
    """..."""

    my_dataframe: pd.DataFrame = deepcopy(dataframe01_with_two_new_column())
    my_dataframe = drop_columns(my_dataframe, ["ColE", "ColF"])
    expected_dataframe: pd.DataFrame = dataframe01()
    assert_frame_equal(my_dataframe, expected_dataframe)

    my_dataframe = deepcopy(dataframe01_with_two_new_column())
    my_dataframe = drop_columns(my_dataframe, ["Col1", "Col2"])
    expected_dataframe = deepcopy(dataframe01_with_two_new_column())
    assert_frame_equal(my_dataframe, expected_dataframe)

    my_dataframe = deepcopy(dataframe01_with_two_new_column())
    my_dataframe = drop_columns(my_dataframe, [])
    expected_dataframe = deepcopy(dataframe01_with_two_new_column())
    assert_frame_equal(my_dataframe, expected_dataframe)


def test_keep_columns() -> None:  # pylint: disable=unused-variable
    """..."""

    my_dataframe: pd.DataFrame = deepcopy(dataframe01_with_two_new_column())
    my_dataframe = keep_columns(my_dataframe, ["ColB", "ColA", "ColC"])
    expected_dataframe: pd.DataFrame = dataframe01()
    assert_frame_equal(my_dataframe, expected_dataframe)

    my_dataframe = deepcopy(dataframe01_with_two_new_column())
    my_dataframe = keep_columns(my_dataframe, ["ColA", "ColC", "ColF", "ColB", "ColE"])
    expected_dataframe = deepcopy(dataframe01_with_two_new_column())
    assert_frame_equal(my_dataframe, expected_dataframe)


def test_set_column_names() -> None:  # pylint: disable=unused-variable
    """..."""

    my_dataframe: pd.DataFrame = deepcopy(dataframe01_with_new_column_name())
    set_column_names(my_dataframe, ["ColA", "ColB", "ColC"])
    expected_dataframe: pd.DataFrame = dataframe01()
    assert_frame_equal(my_dataframe, expected_dataframe)
