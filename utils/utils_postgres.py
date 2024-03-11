from utils import *
from utils.connection import connection
import os
from datetime import datetime

class postgres(connection):

    def __init__(self,  logger_name=None) -> None:
        super().__init__(logger_name=logger_name, conn_name = 'POSTGRES')
        self.engine = self.create_sqlalchemy_engine(self.config['POSTGRES_CON'])
        self.conn = self.create_connection_postgres()

    def dataframe_to_postgres(self, df, table_name, schema = None, column_types= None, chunksize=10000, if_exists='replace'):
        if not schema:
            schema = self.config['postgres_SCHEMA']
        try:
            df.to_sql(table_name, schema=schema, con=self.engine, if_exists=if_exists, index=False, dtype=column_types)
            self.logger.info(f"Successfully update table {table_name} to postgres.")
            return True
        except Exception as e:
            self.logger.error(f"Faield update table {table_name} as {e}")
    
def test():
    # get connection 
    ps = postgres()
    
    # read data from excel
    
    print(1)
