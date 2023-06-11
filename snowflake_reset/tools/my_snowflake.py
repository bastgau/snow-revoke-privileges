"""tools/my_snowflake.py"""

from typing import Any, Dict, List

import pandas as pd

import snowflake.connector as sc
from snowflake.connector import SnowflakeConnection

from snowflake_reset.tools.my_dataframe import create_dataframe


def configure(config: Dict[str, Any], role: str) -> SnowflakeConnection:  # pylint: disable=unused-variable
    """
    The function configures a Snowflake connection with a specified role.

    Args:
    config (Dict[str, Any]): A dictionary containing the configuration parameters for connecting to a
    Snowflake database. This could include parameters such as the account name, user name and a password.
    role (str): The `role` parameter is a string that represents the Snowflake role that the
    connection should use. When a user or role connects to Snowflake, they assume the privileges of the role they
    specify.

    Returns:
    a SnowflakeConnection object.
    """

    cnx: SnowflakeConnection = sc.connect(**config)  # type: ignore
    cur = cnx.cursor(sc.DictCursor)
    cur.execute(f"USE ROLE {role};")
    return cnx


def fetch_pandas_all(cnx: SnowflakeConnection, request: str) -> pd.DataFrame:  # pylint: disable=unused-variable
    """
    The function fetches data from a Snowflake database using a provided SQL query and returns it as a
    Pandas DataFrame.

    Args:
      cnx (SnowflakeConnection): The parameter `cnx` is a SnowflakeConnection object, which represents a
    connection to a Snowflake database. It is used to execute SQL queries and fetch results from the
    database.
      request (str): The SQL query to be executed on the Snowflake database.

    Returns:
      a pandas DataFrame created from the results of a SQL query executed on a Snowflake database
    connection.
    """

    cur = cnx.cursor(sc.DictCursor)

    try:
        cur.execute(request)
        all_rows: List[Dict[Any, Any]] = cur.fetchall()  # type: ignore
        field_names: List[str] = [i[0] for i in cur.description]
    finally:
        cur.close()

    return create_dataframe(all_rows, field_names)


def execute_single_request(cnx: SnowflakeConnection, request: str) -> None:  # pylint: disable=unused-variable
    """
    The function executes a single SQL request using a Snowflake connection and a cursor.

    Args:
      cnx (SnowflakeConnection): The parameter `cnx` is of type `SnowflakeConnection`, which is a
    connection object used to connect to a Snowflake database. It is likely created using the
    `snowflake.connector.connect()` method.
      request (str): The `request` parameter is a string that contains a SQL query to be executed on a
    Snowflake database. The function `execute_single_request` takes this query as input and executes it
    using the provided `cnx` connection object. The result of the query execution is not returned by
    this function.
    """

    cur = cnx.cursor(sc.DictCursor)
    cur.execute(request)


def execute_multi_requests(cnx: SnowflakeConnection, requests: List[str]) -> None:  # pylint: disable=unused-variable
    """
    The function executes multiple SQL requests using a Snowflake connection object.

    Args:
      cnx (SnowflakeConnection): The parameter "cnx" is of type SnowflakeConnection, which is likely a
    connection object to a Snowflake database.
      requests (List[str]): A list of SQL queries to be executed on a Snowflake database connection.
    """

    for request in requests:
        execute_single_request(cnx, request)
