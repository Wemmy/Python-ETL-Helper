
# Python ETL Process Helper

## Introduction

This Python ETL Process Helper is designed to streamline data migration and integrity checks for personal and small-scale projects. It provides a user-friendly interface to connect with multiple data sources, including Snowflake, SQL Server, API services, and Redshift, making it an ideal tool for local data integration applications. Its extendable architecture allows for easy addition of new data sources, ensuring your project can grow and adapt over time.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Examples](#examples)
    - [Use Case 1: Weekly Data Update from Snowflake to SQL Server](#use-case-1-weekly-data-update-from-snowflake-to-sql-server)
    - [Use Case 2: Daily Data Update](#use-case-2-daily-data-update)
- [Troubleshooting](#troubleshooting)
- [Contributors](#contributors)
- [License](#license)

## Features

- **Connection Handling**: Create connections with various data sources like Snowflake, SQL Server, API services, and Redshift at a lower level, providing both query-level and abstract-level loading processes.
- **Logging Integration**: Integrates with a global instance of Loguru for comprehensive logging capabilities.
- **Email Notifications**: Sends out email notifications for critical events or errors during the data migration process.
- **Data Quality Checks**: Implements data quality checks to ensure the integrity of the migrated data.

## Installation

```cmd
pip install requirements.txt
```

## Examples

### Use Case 1: Weekly Data Update from Snowflake to SQL Server

This example outlines a typical process for a weekly data update from Snowflake to SQL Server. It includes fetching current fiscal information, deleting old records within a specific timeframe, and inserting new data into SQL Server tables. It also demonstrates how to perform data quality checks by comparing record counts between the source and destination.

```python
def main():
    # initiate a task
    t = task('Weekly_Data_Update')

    # get current fiscal year and fiscal week
    fy, fw = t.ss.get_current_fiscal_info()

    # get the start and end date of the last complete week
    startdate, enddate = t.ss.find_start_end_date_of_a_fiscal_week(fy, fw-1)
    t.logger.info('start updating fiscal year: {}, fiscal Week: {}, start date: {}, end date: {}', fy, fw-1, startdate, enddate)

    # get API data
    data1 = t.get_API_report(f'API_URL&filter0={startdate}&filter0={enddate}')
    data2 = t.get_API_report(f'API_URL&filter0={startdate}&filter0={enddate}')

    # remove duplicated data of the same timeframe
    filter_string = f"column1 between '{startdate}' and '{enddate}'"
    t.ss.delete_table_records('table1', filter_string=filter_string)
    filter_string = f"column2 between '{startdate}' and '{enddate}'"
    t.ss.delete_table_records('table2', filter_string=filter_string)

    # insert new data
    t.ss.dataframe_to_sqlserver(data1, 'table1', if_exists='append')
    t.ss.dataframe_to_sqlserver(data2, 'table2', if_exists='append')

    # get Snowflake Data
    query = t._get_sql('sql/snowflake_data.sql')
    query = query.format(startdatekey=startdate, enddatekey=enddate)
    rows, columns = t.sf.execute_a_query(query)
    data3 = pd.DataFrame(data=rows, columns=columns)

    # clear room for data3
    filter_string = f"column3 between '{startdate}' and '{enddate}'"
    t.ss.delete_table_records('table3', filter_string=filter_string)

    # insert new data
    t.ss.dataframe_to_sqlserver(data3, 'table3', if_exists='append')

    # data quality check
    # compare the number of records in both SQL Server and Snowflake
    num_sf = t.sf.execute_a_query('select count(*) from table3')
    num_ss = t.ss.execute_a_query('select count(*) from table3')
    if num_ss == num_sf:
        t.logger.info('Data migrated successfully')
    else:
        t.logger.info('Data migration failed')

```

### Use Case 2: Daily Data Update

The following example demonstrates a daily update process from Snowflake to an SQL Server database table. It involves fetching data that has changed or been updated since the previous day, aligning it with the target table's schema in SQL Server, and appending the new data. Additionally, it includes a data quality check to ensure there are no duplicate entries based on a unique identifier.

```python
def main():
    t = task('daily_update')

    # today's datekey
    today_datekey = t.sf.get_datekey_of_date(t._get_today())

    # get all data that is newly changed or updated since yesterday from Snowflake
    query = t._get_sql('sql/daily_query.sql')
    query = query.format(datakey=today_datekey)
    rows_sf, columns_sf = t.sf.execute_a_query(query)
    
    # get columns from the target in SQL Server
    _, columns_ss = t.ss.get_column_names('table1')

    # align data with columns_ss
    data_dicts = [{columns_sf[i]: value for i, value in enumerate(row)} for row in rows_sf]
    rows = [[row_dict[col] for col in columns_ss] for row_dict in data_dicts]

    # append new data to existing table
    data = pd.DataFrame(data=rows, columns=columns_ss)
    t.ss.dataframe_to_sqlserver(data, 'table1', if_exists='append')

    # data quality check
    # check if the new data has duplicated UID
    query_sq = t._get_sql('sql/query_get_max_number_of_uid.sql')
    if not t.quality_check('uid', query_sql=query_sq):
        t.logger.error('Quality check failed')

```
