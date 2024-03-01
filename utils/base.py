from dotenv import dotenv_values
import pyodbc
from datetime import datetime
import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
from io import BytesIO

class base():

    def __init__(self) -> None:

        # get all config
        self.config = dotenv_values()

        # overwrite by children
        self.logger = None

        self.conn = None

    def create_pyodbc_connection(self, connection_string):
        '''
        return a pyodbc connection 
        '''
        try:
            connection = pyodbc.connect(connection_string)
            if self.logger:
                self.logger.info("Successfully connected to the SQL Server using Windows Authentication.")
            return connection
        except pyodbc.Error as e:
            if self.logger:
                self.logger.error("Failed to connect to SQL Server: %s", e)
            raise
        
    def execute_a_query(self, query):
        '''
        execute a query throught odbc connection
        '''
        with self.conn.cursor() as cursor:
            try:
                cursor.execute(query)
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

