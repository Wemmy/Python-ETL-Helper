from utils.task import task
import pandas as pd
import os, csv
from sqlalchemy.engine.reflection import Inspector


def main():
    # create a task
    t = task('sql_to_postgres_copy', required_conn = ['sql_server', 'postgres'])

    table_names = [
        'vendingmicromarketvisititemfact_v'
    ]
    batch_size = 1000000
    for tb_name in table_names:
        source_table_name, target_table_name = tb_name, tb_name
        source_shema, target_shema = 'canteen', 'canteen'
        
        # Read from SQL Server and write to PostgreSQL in batches
        t.logger.info(f"Start Transferring from {source_shema}.{source_table_name} to {target_shema}.{target_table_name} rows")
        csv_file_path = os.path.join('data', f'{source_table_name}.csv')

        # write to csv
        with t.ss.engine.connect() as connection:
            chunk_iter = pd.read_sql_query(f'select * from {source_shema}.{source_table_name}', connection, chunksize=batch_size)
            first_chunk = True
            for chunk_dataframe in chunk_iter:
                # Check and create table based on the first chunk
                if first_chunk:
                    inspector = Inspector.from_engine(t.ps.engine)
                    if not inspector.has_table(table_name =target_table_name, schema =target_shema):
                        t.logger.info(f"Table {target_shema}.{target_table_name} does not exist. Creating...")
                        # Use DataFrame to_sql method to create table from DataFrame schema
                        chunk_dataframe.head(0).to_sql(name=target_table_name, schema=target_shema, con=t.ps.engine, index=False)
                    else:
                        t.logger.info(f"Table {target_shema}.{target_table_name} already exists.")
                chunk_dataframe.to_csv(csv_file_path, mode='a', header=first_chunk, index=False, quoting=csv.QUOTE_NONNUMERIC)
                first_chunk = False
    
        # Upload CSV to PostgreSQL
        with t.ps.engine.connect() as connection:
            # Access the raw psycopg2 connection
            raw_conn = connection.connection
            # Example: Using copy_expert to copy data from a file to a table
            with raw_conn.cursor() as cur:                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
                with open(csv_file_path, 'r') as file:
                    # Skip the header
                    next(file)
                    cur.copy_expert(f"COPY {target_shema}.{target_table_name} FROM STDIN WITH CSV QUOTE '\"' ", file)
            # Commit the transaction
            raw_conn.commit()
            t.logger.info(f"Data uploaded to {target_table_name} successfully.")

        # Clean up: remove the CSV file after upload
        os.remove(csv_file_path)
        
if __name__ == "__main__":
    main()
