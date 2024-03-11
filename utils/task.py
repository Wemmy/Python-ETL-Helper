from utils.utils_sqlserver import sqlserver
from utils.utils_snowflake import snowflake
from utils.utils_postgres import postgres
from utils.base import base
import os
from loguru import logger as loguru_logger


class task(base):
      
    def __init__(self, task_name, required_conn:list = None):

        self.log_dir = 'log'
        if not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir)

        # init log
        log_name = os.path.join(self.log_dir, f"{task_name}_{self._get_today()}.txt")
        super().__init__(logger_name=log_name, to_file =True)
        self.logger.info("initiated a global logger")

        if 'snowflake' in required_conn:
            self.sf = snowflake(logger_name=log_name)
        if 'sql_server' in required_conn:
            self.ss = sqlserver(logger_name=log_name)
        if 'postgres' in required_conn:
            self.ps = postgres(logger_name=log_name)
        # self.rs = redshift()

    def quality_check(self, quality_type, query_source=None, query_target=None):
        if quality_type == 'count':
            '''
            count of rows match
            '''
            if self.sf.execute_a_query(query_source)[0][0] == self.ss.execute_a_query(query_target)[0][0]:
                return True
            else:
                return False
            
        if quality_type == 'uid':
            '''
            check duplicated UID
            '''
            if query_source:
                if self.ss.execute_a_query(query_source)[0][0] > 1:
                    return False

            if query_target:
                if self.sf.execute_a_query(query_target)[0][0] > 1:
                    return False
            return True

    


