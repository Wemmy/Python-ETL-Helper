from utils.base import base
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import snowflake.connector
from utils.utils_snowflake import SnowflakeCredentials
from sqlalchemy.engine import Engine

class connection(base):
    

    def __init__(self) -> None:
        super().__init__()

    def create_sqlalchemy_engine(self, database = 'sql_server') -> Engine:
        '''
        return an sqlalchemy enginer for loading pandas dataframe to sql server
        '''
        if database == 'sql_server':
            conn_string = self.config['SQLSERVER_CONN']
        if database == 'snowflake':
            conn_string = self.config['SNOWFLAKE_CONN']
        if database == 'snowflake':
            conn_string = self.config['REDSHIFT_CONN']
        else:
            raise Exception(f"Invalid database: {database}")

        try:
            # self.conn_string = f"DRIVER={self.config['SQL_DRIVER']};SERVER={self.config['SQL_SERVER']};DATABASE={self.config['SQL_SERVER_DATABASE']};Trusted_Connection=yes"
            engine = create_engine(conn_string, fast_executemany=True, future=True, isolation_level="AUTOCOMMIT")
            if self.logger:
                self.logger.info("Successfully create sqlalchemy engine")
            return engine
        except SQLAlchemyError as e:
            # Handle specific SQLAlchemy errors
            if self.logger:
                self.logger.error(f"An error occurred while creating the database engine: {e}")
        except Exception as e:
            # Handle any other exceptions
            if self.logger:
                self.logger.error(f"An unexpected error occurred: {e}")


    def create_connection_sqlserver(self):
        '''
        return pyodbc connection with sql server
        '''
        connection_string = f"DRIVER={self.config['SQL_DRIVER']};SERVER={self.config['SQL_SERVER']};DATABASE={self.config['SQL_SERVER_DATABASE']};Trusted_Connection=yes"
        return super().create_pyodbc_connection(connection_string)
    
    
    def create_connection_snowflake(self):
        '''
        return pyodbc connection with sql server
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
