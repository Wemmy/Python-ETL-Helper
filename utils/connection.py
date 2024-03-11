from utils.base import base
import snowflake.connector
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
import sys

class connection(base):
    
    def __init__(self, logger_name=None, conn_name = None) -> None:
        super().__init__(logger_name=logger_name, to_file=True if logger_name else False)
        self.conn_name = conn_name

    def create_sqlalchemy_engine(self, conn_string) -> Engine:
    #     '''
    #     return an sqlalchemy enginer for loading pandas dataframe to sql server
    #     '''
    #     if database == 'sql_server':
    #         conn_string = self.config['SQLSERVER_CONN']
    #     if database == 'snowflake':
    #         conn_string = self.config['SNOWFLAKE_CONN']
    #     if database == 'snowflake':
    #         conn_string = self.config['REDSHIFT_CONN']
    #     if database == 'postgres':
    #         conn_string = self.config['POSTGRES_CON']
    #     else:
    #         raise Exception(f"Invalid database: {database}")
        if self.conn_name == 'SQL_SERVER':
            kargs = {'fast_executemany': True, 'future':True, 'isolation_level':'AUTOCOMMIT'}
        if self.conn_name == 'POSTGRES':
            kargs = None
        try:
            # self.conn_string = f"DRIVER={self.config['SQL_DRIVER']};SERVER={self.config['SQL_SERVER']};DATABASE={self.config['SQL_SERVER_DATABASE']};Trusted_Connection=yes"
            engine = create_engine(conn_string, **kargs) if kargs else create_engine(conn_string)
            if self.logger:
                self.logger.info(f"Successfully create sqlalchemy engine with {self.conn_name}")
            return engine
        except SQLAlchemyError as e:
            # Handle specific SQLAlchemy errors
            if self.logger:
                self.logger.error(f"An error occurred while creating the database engine: {e}")
            sys.exit(1)
        except Exception as e:
            # Handle any other exceptions
            if self.logger:
                self.logger.error(f"An unexpected error occurred: {e}")
            sys.exit(1)

    def create_connection_sqlserver(self):
        '''
        return pyodbc connection with sql server
        '''
        connection_string = f"DRIVER={self.config['SQL_DRIVER']};SERVER={self.config['SQL_SERVER']};DATABASE={self.config['SQL_SERVER_DATABASE']};Trusted_Connection=yes"
        return super().create_pyodbc_connection(connection_string)
    
    def create_connection_postgres(self):
        '''
        return pyodbc connection with postgres
        '''
        connection_string = f"DRIVER={self.config['postgres_DRIVER']};SERVER={self.config['postgres_HOST']};DATABASE={self.config['postgres_DBNAME']};UID={self.config['postgres_USER']};PWD={self.config['postgres_PASSWORD']}"
        return super().create_pyodbc_connection(connection_string)
    
    def create_connection_snowflake(self):
        '''
        return pyodbc connection with snowflake
        '''
        try:
            conn = snowflake.connector.connect(
                user=self.config['USERNAME'],
                password=self.config['PASSWORD'],
                account=self.config['ACCOUNT'],
                warehouse=self.config['WAREHOUSE'],
                database=self.config['DATABASE'],
                schema=self.config['SCHEMA']
            )
            if self.logger:
                self.logger.info("Successfully connected to Snowflake.")
            return conn
        except snowflake.connector.errors.ProgrammingError as e:
            if self.logger:
                self.logger.error(f"Failed to connect to Snowflake: {e}")

    def create_connection_redshift(self):
        '''
        create connection with redshift
        '''
        pass
