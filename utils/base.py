from dotenv import dotenv_values
import pyodbc
from datetime import datetime
import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
from io import BytesIO
from loguru import logger
import sys

class base():

    def __init__(self, logger_name=None, to_file=False) -> None:

        # get all config
        self.config = dotenv_values()

        # overwrite by children
        self.logger = LoggerManager.get_logger(logger_name, to_file=to_file, level="TRACE", mode='w') if logger_name else None

        self.conn = None

        self.conn_name = None

    def create_pyodbc_connection(self, connection_string):
        '''
        return a pyodbc connection 
        '''
        try:
            connection = pyodbc.connect(connection_string)
            if self.logger:
                self.logger.info(f"Successfully connected to the {self.conn_name} through pyodbc.")
            return connection
        except pyodbc.Error as e:
            if self.logger:
                self.logger.error(f"Failed to connect to {self.conn_name}: {e}")
            raise
        
    def execute_a_query(self, query, param = None):
        '''
        execute a query throught odbc connection
        '''
        with self.conn.cursor() as cursor:
            try:
                cursor.execute(query, param)
                if cursor.description:
                    rows = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]
                    if self.logger:
                        self.logger.info(f'Successfully execute query {query[:200]}')
                    return rows, columns
                else:
                    self.conn.commit()
                    return None
            except pyodbc.Error as e:
                if self.logger:
                    self.logger.error(f"SQL query execution failed for query: {query[:200]}... | Error: {e}")  # Log first 100 chars of query for brevity
                raise e
    
    def get_API_report(self, URL):
        response = requests.get(URL, auth=HTTPBasicAuth(self.config['API_USERNAME'], self.config['API_PASSWORD']), verify=False)

        # Checking if the request was successful
        if response.status_code == 200:
            # Use BytesIO to convert the byte content to a file-like object
            excel_data = BytesIO(response.content)
            
            # Read the Excel data into a pandas DataFrame
            df = pd.read_excel(excel_data, engine='openpyxl')

            if self.logger:
                self.logger.info(f"Successfully get data from report URL {URL}.")
            return df
            
        else:
            if self.logger:
                self.logger.error(f"Request failed with status code {response.status_code}: {response.text}")
         
    def _get_today(self):
        """
        Returns today's date in 'yyyy-mm-dd' format.
        """
        return datetime.now().strftime('%Y-%m-%d')
    
    def _get_sql(self, file):
        with open(file, 'r') as file:
            query = file.read()
        return query


class LoggerManager:
    _loggers = {}

    @classmethod
    def get_logger(cls, name="test", to_file=False, level="TRACE", mode='w'):
        if name not in cls._loggers:
            # Configure your logger
            log_config = {"handlers": [{"sink": sys.stdout, "format": "{time} {level} {message}", "level": level}]}
            if to_file:
                log_config["handlers"].append({"sink": f"{name}", "format": "{time} {level} {message}", "level": level, "mode": mode})
            
            logger.configure(**log_config)
            cls._loggers[name] = logger
        return cls._loggers[name]
