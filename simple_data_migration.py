from utils.task import task
import pandas as pd

def main():
    # create a task
    t = task('sql_to_postgres', required_conn = ['sql_server', 'postgres'])

    # table_name = [

    # ]
    '''use sqlalchemy for large chunk'''
    source_table_name, target_table_name = 'ytd_revenue', 'ytd_revenue'
    source_shema, target_shema = 'lfl', 'lfl'
    batch_size = 10000

    # Read from SQL Server and write to PostgreSQL in batches
    offset = 0
    while True:
        # Read a batch of rows
        df = pd.read_sql_query(
            f'''
            select * from {source_shema}.{source_table_name} order by 1 offset {offset} rows fetch next {batch_size} rows only
            ''',
            t.ss.engine
        )

        # If the dataframe is empty, break the loop
        if df.empty:
            break
        
        # Check and create schema if it doesn't exist
        t.ps.execute_a_query(f"CREATE SCHEMA IF NOT EXISTS {target_shema};")

        # Write the data to PostgreSQL
        df.to_sql(
            name=target_table_name,
            con=t.ps.engine,
            if_exists='append',
            schema=target_shema,  # Adjust schema as necessary
            index=False,
            method='multi'
        )

        # Update offset
        offset += batch_size
        t.logger.info(f"Transferred {offset} rows")

    # data quality check 
    if t.quality_check('count', 
                       query_source=f'select count(*) from {source_shema}.{source_table_name}', 
                       query_target=f'select count(*) from {target_shema}.{target_table_name}'):
        t.logger.info(f"succefully migrate data")
    else:
        t.logger.error(f"failed migrate data")

    '''use pyodbc for small amount of data'''
    # source_table_name, target_table_name = 'ytd_revenue', 'ytd_revenue'
    # rows, columns =  t.ss.execute_a_query('select * from lfl.{source_table_name}')
    # for row in rows:
    #     t.ps.execute_a_query(f'INSERT INTO {target_table_name} ({", ".join(columns)}) VALUES ({", ".join(["?" for _ in columns])})', row)

if __name__ == "__main__":
    main()