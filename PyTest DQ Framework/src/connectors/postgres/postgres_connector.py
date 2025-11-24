import psycopg2
import pandas as pd
from typing import Optional


class PostgresConnectorContextManager:
    """
    A context manager for managing PostgreSQL database connections.
    
    This class provides a convenient way to establish, use, and close
    PostgreSQL database connections using the context manager protocol.
    It also includes a method to execute SQL queries and return results
    as pandas DataFrames.
    
    Attributes:
        db_host (str): The hostname or IP address of the PostgreSQL server.
        db_name (str): The name of the database to connect to.
        db_port (int): The port number for the database connection.
        db_user (str): The username for authentication.
        db_password (str): The password for authentication.
        connection: The active database connection object.
        cursor: The active database cursor object.
    """
    
    def __init__(self, db_host: str, db_name: str, db_port: int, db_user: str, db_password: str):
        """
        Initialize the PostgreSQL connector with connection parameters.
        
        Args:
            db_host (str): The hostname or IP address of the PostgreSQL server.
            db_name (str): The name of the database to connect to.
            db_port (int): The port number for the database connection.
            db_user (str): The username for authentication.
            db_password (str): The password for authentication.
        """
        self.db_host = db_host
        self.db_name = db_name
        self.db_port = db_port
        self.db_user = db_user
        self.db_password = db_password
        self.connection = None
        self.cursor = None

    def __enter__(self):
        """
        Establish the database connection when entering the context.
        
        Returns:
            PostgresConnectorContextManager: The instance itself with an active connection.
            
        Raises:
            psycopg2.Error: If connection to the database fails.
        """
        try:
            self.connection = psycopg2.connect(
                host=self.db_host,
                database=self.db_name,
                port=self.db_port,
                user=self.db_user,
                password=self.db_password
            )
            self.cursor = self.connection.cursor()
            return self
        except psycopg2.Error as e:
            raise Exception(f"Failed to connect to database: {e}")

    def __exit__(self, exc_type, exc_value, exc_tb):
        """
        Close the database connection and cursor when exiting the context.
        
        Args:
            exc_type: Exception type if an exception was raised.
            exc_value: Exception value if an exception was raised.
            exc_tb: Exception traceback if an exception was raised.
        """
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def get_data_sql(self, query: str) -> pd.DataFrame:
        """
        Execute a SQL query and return the results as a pandas DataFrame.
        
        Args:
            query (str): The SQL query to execute.
            
        Returns:
            pd.DataFrame: A DataFrame containing the query results.
            
        Raises:
            Exception: If the query execution fails or connection is not established.
        """
        if not self.connection:
            raise Exception("Database connection is not established. Use this method within a context manager.")
        
        try:
            df = pd.read_sql_query(query, self.connection)
            return df
        except Exception as e:
            raise Exception(f"Failed to execute query: {e}")


