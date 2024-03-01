
from utils.connection import connection


class snowflake(connection):

    def __init__(self) -> None:
        super().__init__()
        self.conn = self.create_connection_snowflake()
    
    def get_max_min_value_of_cloumn(self, table_name, column_name):
        '''
        TO GET THE MAX DATE IN A TABLE
        INPUT: Table and date column
        output: date
        '''

        query_max = f'''
        select max(datevalue) from {table_name} f left join TIME_DIMENSION.DIMDATE_V dd on f.{column_name} = dd.datekey
        '''

        query_min = f'''
        select min(datevalue) from {table_name} f left join TIME_DIMENSION.DIMDATE_V dd on f.{column_name} = dd.datekey
        '''
        results,_ = self.execute_a_query(query_max)
        max = results[0][0]
        results,_ = self.execute_a_query(query_min)
        min = results[0][0]
        return max, min
    
    def get_datekey_of_date(self, date):
        query = f'''
        select datekey from TIME_DIMENSION.DIMDATE_V dd where datevalue = '{date}'
        '''
        results,_ = self.execute_a_query(query)
        return results[0][0]

    def get_date_of_datekey(self, datekey):
        query = f'''
        select datevalue from TIME_DIMENSION.DIMDATE_V dd where datekey = {datekey}
        '''
        results,_ = self.execute_a_query(query)
        return results[0][0]

    def get_start_datekey_of_past_n_weeks(self, n):
        query = self._get_sql('sql/get_week_sequence_of_a_datekey.sql')
        query = query.format(datekey = self.get_datekey(self._get_today()))
        results,_ = self.execute_a_query(query)
        max_week_seq =  results[0][0] 

        query = self._get_sql('sql/get_min_datekey_of_a_weeksequence.sql')
        query = query.format(WEEK_SEQUENCE = max_week_seq-n)
        results,_ = self.execute_a_query(query)
        return results[0][0] 


    
def test():
    sf = snowflake()
    print(sf.get_date_of_datekey(sf.get_min_datekey_of_past_n_weeks(1)))


        
