from utils import *
from utils.connection import connection
import os
from datetime import datetime
import pandas as pd
# from sqlalchemy import create_engine
# from sqlalchemy.exc import SQLAlchemyError
# from sqlalchemy.engine import Engine

class sqlserver(connection):

    def __init__(self, logger_name=None) -> None:
        super().__init__(logger_name=logger_name, conn_name = 'SQL_SERVER')
        self.engine = self.create_sqlalchemy_engine(self.config['SQLSERVER_CONN'])
        self.conn = self.create_connection_sqlserver()
        

    def get_dataframe(self, table_name, schema = None):
        if not schema:
            schema = self.config['SQL_SERVER_SCHEMA']

        q = f"""
            select * from {schema}.{table_name}
        """
        rows, columns = self.execute_a_query(q)
        return pd.DataFrame(rows, columns=columns)

    def if_table_exists(self, table_name, schema = None):
        """
        Check if a table exists in the given schema.
        """
        if schema:
            check_query = f"""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{schema}' 
            AND TABLE_NAME = '{table_name}'
            """
        else:
            check_query = f"""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{self.config['SQL_SERVER_SCHEMA']}' 
            AND TABLE_NAME = '{table_name}'
            """
        count, _ = self.execute_a_query(check_query)
        return count[0][0] > 0
             
    def create_a_table(self, table_name, column_def, schema = None):
        '''
        create a new table in sql server based on column schema
            table_name: str, column's name
            column_def: str, e.g. BRANCHKEY int,RESTOCKEDBYUSERNAME nvarchar(64),INITIALINVENTORY smallint
        '''
        if not schema:
            schema = self.config['SQL_SERVER_SCHEMA']

        # Check if table exists and then drop
        sql_drop_table = f"""
            IF OBJECT_ID('{schema}.{table_name}', 'U') IS NOT NULL
            DROP TABLE {schema}.{table_name}
        """
        self.execute_a_query( sql_drop_table)
        

        # Check if table exists and then drop
        sql_create_table = f"""
            CREATE TABLE {schema}.{table_name} ({column_def})
        """
        self.execute_a_query( sql_create_table)

        self.logger.info(f"Successfully create a new table {table_name} in SQL Server.")

    def bulk_insert(self, table_name, csv_file, schema = None):
        if not schema:
            schema = self.config['SQL_SERVER_SCHEMA']

        absolute_path = os.path.abspath(csv_file)

        sql_bulk_insert = f"""
        BULK INSERT {schema}.{table_name}
        FROM '{absolute_path}'
        WITH (
            FORMAT = 'CSV',
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            FIRSTROW = 2
        );
        """
        self.execute_a_query( sql_bulk_insert)

        self.logger.info(f"Successfully bulk insert {table_name} with data {csv_file} to SQL Server.")

    def dataframe_to_sqlserver(self, df, table_name, schema = None, column_types= None, chunksize=10000, if_exists='replace'):
        if not schema:
            schema = self.config['SQL_SERVER_SCHEMA']
        try:
            df.to_sql(table_name, schema=schema, con=self.engine, if_exists=if_exists, index=False, dtype=column_types)
            self.logger.info(f"Successfully update table {table_name} in SQL Server.")
            return True
        except Exception as e:
            self.logger.error(f"Faield update table {table_name} as {e}") 

    def delete_table_records(self, table_name, schema= None, filter_string=None):
        '''
        delete records based on filter
        '''
        if not schema:
            schema = self.config['SQL_SERVER_SCHEMA']
        
        if filter_string:
            query = f'''
            DELETE FROM {schema}.{table_name} WHERE {filter_string}
            '''
        else:
            query = f'''
            DELETE FROM {schema}.{table_name}
            '''
        self.execute_a_query( query)

    # calendar execution
    def get_current_fiscal_info(self):
        '''
        fy, fp, fw = get_current_fiscal_year_period()
        '''
        # Get today's date
        today = datetime.now().strftime('%Y-%m-%d')

        # SQL query
        sql_query = f"""
        SELECT FiscalYear, [Fiscal Period], [Fiscal Week]
        FROM LFL.Ref_FY_Calendar_New
        WHERE date = '{today}'
        """

        data, _ = self.execute_a_query( sql_query)

        return data[0][0], data[0][1], data[0][2]

    def find_start_end_date_of_a_fiscal_week(self, fy, fw):
        '''
        start_date, end_date
        '''
        sql_query = "SELECT MIN(date) FROM LFL.Ref_FY_Calendar_New WHERE FiscalYear = {} and [Fiscal Week] = {}".format(fy, fw)
        data, _ = self.execute_a_query( sql_query)
        start_date = data[0][0]

        sql_query = "SELECT MAX(date) FROM LFL.Ref_FY_Calendar_New WHERE FiscalYear = {} and [Fiscal Week] = {}".format(fy, fw)
        data, _ = self.execute_a_query( sql_query)
        end_date = data[0][0]
        return start_date,end_date

    def get_max_week_number_of_a_year(self, fy):
        sql_query = f"select max([Fiscal Week]) from LFL.Ref_FY_Calendar_New where FiscalYear = {fy}"
        data, _ = self.execute_a_query(sql_query)
        return data[0][0]

    def get_column_names(self, table_name, schema=None):
        if not schema:
            schema = self.config['SQL_SERVER_SCHEMA']

        query = f'''
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{table_name}'
                AND TABLE_SCHEMA = '{schema}';
                '''
        data, _ = self.execute_a_query(query)

        # Convert list of tuples to list of strings
        return [item[0] for item in data]
    
def test():
    # get connection 
    ss = sqlserver()
    a = ss.get_column_names('business_unit_ref', 'LFL')
    print(a)
