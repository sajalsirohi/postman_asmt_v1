import os
import urllib
from datetime import datetime as dt

import pandas as pd
from sqlalchemy import create_engine, Table, Column, String, MetaData, DateTime
from sqlalchemy.exc import *

meta = MetaData()
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


def process_data(data):
    """
    Process the data, drop the duplicates on `sku` column
    :param data: pandas.DataFrame
    :return:
    """
    data['chg_dttm'] = dt.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    return data.drop_duplicates(subset="sku",
                                keep='first')


class SQL:
    """
    An SQL utility class to connect to a Database and do CRUD operations
    """
    def __init__(self, host_name, database_name, username, password, **options):
        """
        :param [OPTIONAL] driver: Name of the pyodbc driver
        :param database_name: Name of the database to connect to
        :param host_name: Server / Host name of the DB, localhost for local instances
        :param username: Admin / User username for the DB
        :param password: Admin / User Password for the DB
        """
        self.host_name         = host_name
        self.database_name     = database_name
        self.username          = username
        self.password          = password
        self.driver            = options.get('driver', "ODBC Driver 17 for SQL Server")
        self.connection_string = self._create_connection_string()
        self.conn              = None
        self.engine            = create_engine(f"mssql+pyodbc:///?odbc_connect="
                                               f"{urllib.parse.quote_plus(self.connection_string)}",
                                               fast_executemany=True)

    def _create_connection_string(self):
        """
        Returns pyodbc URL based on the given input

        :return: String
        """
        start_curly = "{"
        end_curly   = "}"
        return f"Driver={start_curly}{self.driver}{end_curly};"\
               f"UID={self.username};" \
               f"PWD={self.password};" \
               f"Server={self.host_name};" \
               f"database={self.database_name}"

    def check_connection(self, **options):
        """
        Check the connection to your DB, and sets the connection object to class variable self.conn
        :param: connection_string: pyodbc string for the connection
        :param: [OPTIONAL] skip_table_creation: If you do not want to check connection by creating a sample table and dropping it
        pass this flag as true if you are sure you queries will run on the server.
        :return: Boolean
        """
        try:
            print("Trying to connect...")
            self.conn   = self.engine.connect()

            if not options.get('skip_table_creation'):
                print("Trying to create a temp table...")
                self.conn.execute("IF OBJECT_ID('dbo.CUSTOMER_Test_Python', 'U') IS NOT NULL "
                                  "DROP TABLE dbo.CUSTOMER_Test_Python; ")
                self.conn.execute('create table CUSTOMER_Test_Python'
                                  '("CUST_ID" INTEGER not null,'
                                  ' "NAME" VARCHAR(50) not null,'
                                  ' primary key ("CUST_ID"))'
                                  )
                print("Table created successfully, now inserting some dummy data")
                self.conn.execute("insert into CUSTOMER_Test_Python values (?, ?)", (1, 'John'))
                result = self.conn.execute("select * from CUSTOMER_Test_Python")
                print(f"Data fetched from the temporary table : {[row for row in result][0]}")
                print("Dropping table...")
                self.conn.execute('DROP table CUSTOMER_Test_Python')
            print("Connection is live and running..")
        except Exception as err:
            print('Error connecting to DB')
            raise Exception(err.args)

    def save_to_sql(self, data, table_name, if_exists="append"):
        """
        Save the data (pandas dataframe) to the SQL table, defined by table_name
        :param data: pandas dataframe that needs to be stored
        :param table_name: name of the table
        :param if_exists: replace, append, fail. same as
         https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html
        :return:
        """
        assert isinstance(data, pd.DataFrame), f"Data should be a pandas DataFrame, received type : {type(data)}"
        self.conn.execute(f"drop table if exists {table_name}")
        data.to_sql(name=table_name, con=self.conn, if_exists=if_exists, index=False)

    def get_from_sql(self, table_name, create_table_if_not_present=True):
        """
        Get data from SQL, for the given table name
        :param create_table_if_not_present: Create the table if it is not present
        :param table_name: Name of the table
        :return:
        """
        try:
            return pd.read_sql(f"SELECT * from {table_name}", self.conn)
        except ProgrammingError as e:
            print(f"The following table : {table_name} is not present. Exception -> {e.args}\n"
                  f"Data is going to be inserted for the first time")
            if create_table_if_not_present:
                print(f"Creating the table : {table_name} with [SKU] as primary key")
                data = Table(
                    table_name, meta,
                    Column('name', String),
                    Column('sku', String(50), primary_key=True),
                    Column('description', String),
                    Column('chg_dttm', DateTime)
                )
                meta.create_all(self.engine)
            return pd.DataFrame([], columns=['name', 'sku', 'description', 'chg_dttm'])
        except Exception as e:
            raise e

    def execute_raw_query(self, query):
        """
        Execute raw query
        :param query:
        :return:
        """
        return pd.read_sql(query, self.conn)

    def latest_processed_time(self, table_name="data", convert_to_datetime=True):
        """
        Get the latest time the data was processed
        :return:
        """
        from datetime import datetime as dt
        import datetime
        val = list(self.execute_raw_query(f"SELECT MAX(CHG_DTTM) as CHG_DTTM FROM {table_name}")['CHG_DTTM'])[0]
        if convert_to_datetime:
            return dt.strptime(val, "%Y-%m-%d %H:%M:%S").replace(tzinfo=datetime.timezone.utc)
        else:
            return val

class MaintainTime:
    """
    A util class to use as a stop watch
    """
    def __init__(self):
        self.starting_time = None

    def start(self):
        """
        Set the current time
        :return:
        """
        self.starting_time = dt.now()

    def stop(self, operation = None):
        """
        Calculate the time difference between the start time and time which is now
        :return:
        """
        diff = dt.now() - self.starting_time
        if not operation:
            print(f"Time elapsed : {diff}")
        else:
            print(f"Time elapsed for operation - '{operation}' : {diff}")


def setup():
    """
    Create neccessary directories if they do not exist
    :return:
    """
    for dir_name in ['data', 'processed_data']:
        path = os.path.join(ROOT_DIR, dir_name)
        if not os.path.exists(path):
            os.mkdir(path)