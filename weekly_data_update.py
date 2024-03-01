from utils.task import task
from utils.utils_sqlserver import test as t_ss
from utils.utils_snowflake import test as t_sf
import pandas as pd

def main():
    # initiate a task
    t = task('Weekly_Data_Update')

    # get current fiscal year and fiscal week
    fy, fw = t.ss.get_current_fiscal_info()

    # get the start and end data of the last complete week
    startdate, enddate = t.ss.find_start_end_date_of_a_fiscal_week(fy, fw-1)
    t.logger.info('start updating fiscal year: {}, fiscal Week: {}, start date: {}, end date: {}',fy, fw-1, startdate, enddate)

    # get API data
    data1 = t.get_API_report(f'API_URL&filter0={startdate}&filter0={enddate}')
    data2 = t.get_API_report(f'API_URL&filter0={startdate}&filter0={enddate}')

    # remove duplicated data of same timeframe
    filter_string = f"column1 between \'{startdate}\' and \'{enddate}\'"
    t.ss.delete_table_records('table1', filter_string= filter_string)
    filter_string = f"column2 between \'{startdate}\' and \'{enddate}\'"
    t.ss.delete_table_records('table2', filter_string= filter_string)

    # insert new data
    t.ss.dataframe_to_sqlserver(data1, 'table1', if_exists='append')
    t.ss.dataframe_to_sqlserver(data2, 'table2', if_exists='append')

    # get Snowflake Data
    query = t._get_sql('sql/snowflake_data.sql')
    query = query.format(startdatekey = startdate, enddatekey=enddate)
    rows, columns = t.sf.execute_a_query(query)
    data3= pd.DataFrame(data=rows, columns=columns)

    # clear room for data3
    filter_string = f"column3 between \'{startdate}\' and \'{enddate}\'"
    t.ss.delete_table_records('table3', filter_string= filter_string)

    # insert new data
    t.ss.dataframe_to_sqlserver(data3, 'table3', if_exists='append')

    # data quality check
    # compare the number of records in both sqlserver and snowflake
    num_sf = t.sf.execute_a_query(t.sf.connection, 'select count(*) from table3')
    num_ss = t.ss.execute_a_query(t.sf.connection, 'select count(*) from table3')
    if num_ss == num_sf:
        t.logger.info('data migrated successfully')
    else:
        t.logger.info('data migrated Failed')


if __name__ == "__main__":
    main()
    # t_ss()