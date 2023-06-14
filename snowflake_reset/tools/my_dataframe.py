"""tools/my_dataframe.py"""

from typing import Any, Dict, List

import pandas as pd


def concat_dataframe(list_dataframes: List[pd.DataFrame]) -> pd.DataFrame:  # pylint: disable=unused-variable
    """
    The function takes a list of pandas dataframes and concatenates them into a single dataframe.

    Args:
      list_dataframes (List[pd.DataFrame]): A list of pandas DataFrames that will be concatenated into a
    single DataFrame.

    Returns:
      The function `concat_dataframe` is returning a concatenated pandas DataFrame, which is created by
    concatenating the list of pandas DataFrames passed as an argument to the function.
    """

    return pd.concat(list_dataframes)  # type: ignore


def concat_column(current_dataframe: pd.DataFrame, column_dst: str, columns_src: List[str], separator: str = ".") -> None:  # pylint: disable=unused-variable
    """
    The function concatenates multiple columns in a pandas DataFrame into a single column using a
    specified separator.

    Args:
      current_dataframe (pd.DataFrame): A pandas DataFrame that contains the columns to be concatenated.
      column_dst (str): The name of the new column that will be created in the dataframe to store the
    concatenated values from the source columns.
      columns_src (List[str]): A list of column names in the current_dataframe that you want to
    concatenate.
      separator (str): The separator parameter is a string that is used to join the values of the
    columns in columns_src. By default, it is set to ".". Defaults to .
    """

    current_dataframe[column_dst] = current_dataframe[columns_src].apply(lambda x: f"{separator}".join(x.dropna()), axis=1)  # type: ignore


def create_column(current_dataframe: pd.DataFrame, columns: Dict[str, Any]) -> None:  # pylint: disable=unused-variable
    """
    The function adds new columns to a given dataframe with specified column names and values.

    Args:
      current_dataframe (pd.DataFrame): A pandas DataFrame that represents the current state of the
    data.
      columns (Dict[str, Any]): The `columns` parameter is a dictionary where the keys are the names of
    the columns to be created in the `current_dataframe` and the values are the values to be assigned to
    each cell in the column.
    """

    for column_name, value in columns.items():
        if column_name not in current_dataframe:
            current_dataframe[column_name] = value


def set_column_names(current_dataframe: pd.DataFrame, field_names: List[str]) -> None:  # pylint: disable=unused-variable
    """
    The function sets the column names of a pandas DataFrame to a given list of field names.

    Args:
      current_dataframe (pd.DataFrame): A pandas DataFrame that we want to modify by setting its column
    names.
      field_names (List[str]): A list of strings representing the desired column names for the given
    pandas DataFrame.
    """

    if len(current_dataframe) > 0:
        current_dataframe.columns = pd.Index(field_names)


def keep_columns(current_dataframe: pd.DataFrame, expected_columns: List[str]) -> pd.DataFrame:  # pylint: disable=unused-variable
    """
    The function drops all non specified columns from a pandas DataFrame.

    Args:
      current_dataframe (pd.DataFrame): A pandas DataFrame that contains the data to be filtered.
      expected_columns (List[str]): A list of columns that must be kept in the input dataframe.

    Returns:
      a pandas DataFrame without the dropped columns.
    """

    columns: List[str] = current_dataframe.columns.values.tolist()  # type: ignore
    columns = [column for column in columns if column not in expected_columns and column in current_dataframe]

    if len(columns) > 0:
        return current_dataframe.drop(columns, axis=1)

    return current_dataframe


def drop_columns(current_dataframe: pd.DataFrame, columns: List[str]) -> pd.DataFrame:  # pylint: disable=unused-variable
    """
    The function drops specified columns from a pandas DataFrame if they exist.

    Args:
      current_dataframe (pd.DataFrame): A pandas DataFrame that contains the data to be filtered.
      columns (List[str]): A list of columns to be dropped in the input dataframe.

    Returns:
      a pandas DataFrame without the dropped columns.
    """

    columns = [column for column in columns if column in current_dataframe]

    if len(columns) > 0:
        return current_dataframe.drop(columns, axis=1)

    return current_dataframe


def rename_column(current_dataframe: pd.DataFrame, columns: Dict[str, str]) -> None:  # pylint: disable=unused-variable
    """
    The function renames columns in a pandas DataFrame based on a dictionary of old and new column
    names.

    Args:
      current_dataframe (pd.DataFrame): The current dataframe that needs to be modified.
      columns (Dict[str, str]): The `columns` parameter is a dictionary where the keys represent the
    current column names in the `current_dataframe` and the values represent the new column names that
    we want to assign to those columns.
    """

    current_dataframe.rename(columns=columns, inplace=True)


def create_dataframe(data: List[Any], column_names: List[str]) -> pd.DataFrame:  # pylint: disable=unused-variable
    """
    The function creates a pandas DataFrame from a list of data and sets the column names.

    Args:
    data (List[Any]): The data parameter is a list of any data type. It represents the data that will
    be used to create the pandas DataFrame.
    column_names (List[str]): A list of strings representing the column names for the dataframe.

    Returns:
    a pandas DataFrame with the data provided and column names set according to the column_names
    parameter.
    """

    current_dataframe: pd.DataFrame = pd.DataFrame(data)
    set_column_names(current_dataframe, column_names)
    return current_dataframe
