import os
import psycopg2
from dotenv import dotenv_values


class DbConnection:
    """Class responsible for all implementations of database conections"""
    def __init__(self):
        ...

    def connection(self):
        """Method responsible for create the database connection"""

        conn = psycopg2.connect(
            host=dotenv_values(".env")["HOST"],
            port=dotenv_values(".env")["PORT"],
            database=dotenv_values(".env")["DATABASE"],
            user=dotenv_values(".env")["USER"],
            password=dotenv_values(".env")["PASSWORD"]
        )
        conn.autocommit = True

        return conn

    def run_query(self, query, get_all=False):
        """Method responsible for execute the query

        :param query: The received query to execute
        :type query: str
        :return: result
        :rtype: list
        """
        connection = self.connection()

        if not connection:
            raise Exception("No connection with database")

        cursor = connection.cursor()
        cursor.execute(query)

        if get_all:
            return cursor.fetchall()

        return
        

    
