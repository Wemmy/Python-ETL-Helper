from utils.utils_sqlserver import sqlserver
from utils.utils_snowflake import snowflake
from utils.base import base
import os
from loguru import logger


class task(base):
      
    def __init__(self, task_name):
        
        self.task_name = task_name

        # init log
        self.log_dir = 'log'
        if not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir)
        
        # sink the log file to text file
        self.log_file = os.path.join(self.log_dir,f"{self.task_name}_{self._get_date_key()}.txt")
        self.logger.add(f"{self.log_file}", level="INFO", format="{time} {level} {message}", mode = 'w')
        self.logger = logger
        logger.info("initiate a global logger")

        self.sf = snowflake()
        self.ss = sqlserver()
        # self.rs = redshift()

    def quality_check(self, quality_type, query_sql=None, query_sf=None):
        if quality_type == 'count':
            '''
            count of rows match
            '''
            if self.sf.execute_a_query(query_sf)[0][0] == self.ss.execute_a_query(query_sql)[0][0]:
                return True
            else:
                return False
            
        if quality_type == 'uid':
            '''
            check duplicated UID
            '''
            if query_sql:
                if self.ss.execute_a_query(query_sql)[0][0] > 1:
                    return False

            if query_sf:
                if self.sf.execute_a_query(query_sf)[0][0] > 1:
                    return False
            return True

    


