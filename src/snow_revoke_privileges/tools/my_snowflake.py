"""tools/my_snowflake.py"""

import logging
import re
from typing import Any, Dict, List, Optional

from multiprocessing import Pool

from progress.bar import Bar  # pyright: ignore

import pandas as pd

import snowflake.connector as sc
from snowflake.connector import SnowflakeConnection

from snow_revoke_privileges.tools.my_dataframe import create_dataframe


class MySnowflake:
    """..."""

    snow_cnn: SnowflakeConnection

    @staticmethod
    def initialize_database(config: Dict[str, Any]) -> None:
        """
        The function initializes a Snowflake database connection using credentials and admin role specified
        in the configuration.
        """

        cnx: SnowflakeConnection = sc.connect(**config)  # type: ignore
        cur = cnx.cursor(sc.DictCursor)
        cur.execute(f"USE ROLE {config['role']};")

        logging.getLogger("app").debug("Connection with Snowflake: OK")

        MySnowflake.snow_cnn = cnx

    @staticmethod
    def fetch_pandas_all(request: str) -> pd.DataFrame:  # pylint: disable=unused-variable
        """
        The function fetches data from a Snowflake database using a provided SQL query and returns it as a
        pandas DataFrame.

        Args:
        cnx (SnowflakeConnection): The parameter `cnx` is a SnowflakeConnection object, which represents a
        connection to a Snowflake database. It is used to execute SQL queries and fetch results from the
        database.
        request (str): The SQL query to be executed on the Snowflake database.

        Returns:
        a pandas DataFrame created from the results of a SQL query executed on a Snowflake database
        connection.
        """

        cur = MySnowflake.snow_cnn.cursor(sc.DictCursor)

        try:
            cur.execute(request)
            all_rows: List[Dict[Any, Any]] = cur.fetchall()  # type: ignore
            field_names: List[str] = [i[0] for i in cur.description]
        finally:
            cur.close()

        return create_dataframe(all_rows, field_names)

    @staticmethod
    def execute_single_request(request: str) -> None:  # pylint: disable=unused-variable
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

        try:
            cur = MySnowflake.snow_cnn.cursor(sc.DictCursor)
            cur.execute(request)
        except Exception as err:  # pylint: disable=broad-exception-caught
            logging.getLogger("app").fatal("SQL request : '%s' has failed (%s).", request, type(err))

    @staticmethod
    def execute_multi_requests(requests: List[str]) -> None:  # pylint: disable=unused-variable
        """
        The function executes multiple SQL requests using a Snowflake connection object.

        Args:
        cnx (SnowflakeConnection): The parameter "cnx" is of type SnowflakeConnection, which is likely a
        connection object to a Snowflake database.
        requests (List[str]): A list of SQL queries to be executed on a Snowflake database connection.
        """

        with Bar("Executing request in Snowflake", max=len(requests)) as progress:

            with Pool(processes=8) as pool:
                for _ in pool.imap_unordered(MySnowflake.execute_single_request, requests):  # pyright: ignore
                    progress.next()

    @staticmethod
    def get_arguments(arguments: Optional[str]) -> str:
        """
        The function extracts the procedure stored of function arguments from a string returned by the SHOW command.

        Args:
            arguments (str): The `arguments` parameter is a string that represents a complete argument string provided by a SHOW command.

        Returns:
            a string containing only the arguments.
        """

        if arguments is None or arguments == "None":
            return ""

        regex = r"[^\(]+(\([^\)]*\)).*"
        matches = re.search(regex, arguments, re.DOTALL)

        if matches:
            return str(matches.groups(1)[0])

        return ""
