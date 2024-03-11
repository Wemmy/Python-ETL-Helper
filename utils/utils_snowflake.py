
from utils.connection import connection
import os
from dataclasses import dataclass
from utils.dataclasses import iterate_as_dict
from utils.utils_aws import AWSUtils


class snowflake(connection):

    def __init__(self, logger_name=None) -> None:
        super().__init__(logger_name=logger_name, conn_name = 'SNOWFLAKE')
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



@iterate_as_dict
@dataclass(frozen=True)
class SnowflakeCredentials:
    username: str
    password: str
    account: str
    port: int
    database: str
    warehouse: str
    role: str
    driver: str = "snowflake"

    @property
    def url(self):
        """Constructs the database url for the credentials instance."""
        return f"{self.driver}://{self.username}:{self.password}@{self.account}/{self.database}?warehouse={self.warehouse}&role={self.role}"

    @staticmethod
    def from_env_vars(prefix: str) -> "SnowflakeCredentials":
        """Constructs a SnowflakeCredentials instance from env vars.
        
        To use credentials from your environment variables, ensure your credentials all have
        the same prefix, and pass that prefix along through the "prefix" arg.

        For example, you can pass prefix="MY_PREFIX" and the following env vars will be used:
            - MY_PREFIX_USER
            - MY_PREFIX_ENV_SECRET_PASSWORD
            - MY_PREFIX_ACCOUNT
            - MY_PREFIX_PORT
            - MY_PREFIX_DB_NAME
            - MY_PREFIX_WAREHOUSE
            - MY_PREFIX_ROLE
        """

        return SnowflakeCredentials(
            username=os.environ[f"{prefix}_USER"],
            password=os.environ[f"{prefix}_ENV_SECRET_PASSWORD"],
            account=os.environ[f"{prefix}_ACCOUNT"],
            port=os.environ[f"{prefix}_PORT"],
            database=os.environ[f"{prefix}_DB_NAME"],
            warehouse=os.environ[f"{prefix}_WAREHOUSE"],
            role=os.environ[f"{prefix}_ROLE"],
        )

    @staticmethod
    def from_aws_secrets(secret_name: str, region_name: str):
        """Constructs a SnowflakeCredentials instance from the supplied aws secret.
        
        To use credentials from an aws secret, the secret must have the following keys:
            - username
            - password
            - account
            - port
            - db_name
            - warehouse
            - role
        """

        secrets = AWSUtils.get_secret(secret_name, region_name)

        return SnowflakeCredentials(
            username=secrets["username"],
            password=secrets["password"],
            account=secrets["account"],
            port=secrets["port"],
            database=secrets["db_name"],
            warehouse=secrets["warehouse"],
            role=secrets["role"],
        )
    
def test():
    sf = snowflake()
    print(sf.get_date_of_datekey(sf.get_min_datekey_of_past_n_weeks(1)))


        
