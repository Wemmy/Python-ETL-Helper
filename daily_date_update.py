from utils.task import task
import pandas as pd

def main():
    t = task('daily_update')

    # today's datekey
    today_datekey = t.sf.get_datekey_of_date(t._get_today())

    # get all data that is newly changed or updated since yesterday from snowflake
    query = t._get_sql('sql/daily_query.sql')
    query = query.format(datakey = today_datekey)
    rows_sf, columns_sf = t.sf.execute_a_query(t.sf.connection, query)
    
    # get columns from the target in sql server
    _, columns_ss = t.ss.get_column_names('table1')

    # align data with columns_ss
    data_dicts = [{columns_sf[i]: value for i, value in enumerate(row)} for row in rows_sf]
    rows = [[row_dict[col] for col in columns_ss] for row_dict in data_dicts]

    # append new data to existing table
    data= pd.DataFrame(data=rows, columns=columns_ss)
    t.ss.dataframe_to_sqlserver(data, 'table1', if_exists='append')

    # data quality check
    # check if the new data has duplicated UID
    query_sq = t._get_sql('sql/query_get_max_number_of_uid.sql')
    if not t.quality_check('uid', query_sql=query_sq):
        t.logger.error('quality check failed')
    

if __name__ == '__main__':
    main()