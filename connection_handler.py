from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
import pandas as pd
import logging
from data_cleaning import var_to_string

'''
The connection handler connects to the company's MS SQL Server and provides a number of methods
that allow users to read and write data.
'''
class ConnectionHandler:
    def __init__(self, server, db):
        self.server = server
        self.db = db
        
        #driver = "SQL Server"
        driver = "ODBC Driver 17 for SQL Server"
        # conn_str = f'mssql://@{self.server}/{self.db}?trusted_connection=yes&driver={driver}' # Commented out for CSV development

        try:
        #Create the engine to connect to the database
            # self.engine_ = create_engine(conn_str) # Commented out for CSV development
            # self.db_connection = self.engine_.connect() # Commented out for CSV development
            self.engine_ = None # Added for CSV development
            self.db_connection = None # Added for CSV development
            pass # Added for CSV development
        except Exception as e:
            logging.error('Error at %s', 'database connection', exc_info=e)
        
    def fetch_data(self, query):
        return pd.read_sql(query, con=self.db_connection)

    # NOT USED
    def fetch_data_file(self, query):
        file = open(query,'r').read()
        return pd.read_sql(file, con=self.db_connection)
    
    def insert_data(self, df, tablename, aSchema):
        aSchema = var_to_string(aSchema)
        tablename = var_to_string(tablename)
        df.to_sql(tablename, if_exists = 'append', index = False, con = self.db_connection, schema=aSchema, chunksize = 5000)

    def insert_data_w_eng(self, df, tablename, aSchema):
        aSchema = var_to_string(aSchema)
        tablename = var_to_string(tablename)
        df.to_sql(tablename, self.engine_, if_exists = 'append', index = False, schema=aSchema, chunksize = 5000)
    
    def replace_data(self, df, tablename, TSchema, dict_):
        # requires mapping of datatypes to columns to avoid maxvarchar with dict_, USE AS NEEDED
        TSchema = var_to_string(TSchema)
        tablename = var_to_string(tablename)
        df.to_sql(tablename, self.engine_, if_exists = 'replace', index = False, schema=TSchema,dtype=dict_, chunksize = 5000)

    def truncate_replica_table(self, table_name, schema):
        try:
            Session = sessionmaker(bind=self.engine_)
            session = Session()
            query = str(f'TRUNCATE TABLE {schema}.{table_name}').replace("'","")
            session.execute(text(query))
            session.commit()
            session.close()
        except Exception as e:
            logging.error('Error at %s', 'count not truncate table', exc_info=e)

    def execute_query(self, query):
        # self.db_connection.execute(query) # Commented out for CSV development
        # self.db_connection.commit() # Commented out for CSV development
        pass # Added for CSV development

    def execute_query_file(self, query):
        try:
            file = open(query,'r').read()
            # self.db_connection.execute(file) # Commented out for CSV development
            # self.db_connection.commit() # Commented out for CSV development
            pass # Added for CSV development
        except Exception as e:
            logging.error('Error at %s', 'execute query file', exc_info=e)

    def __del__(self):
        try:
            if self.db_connection: # Added check for CSV development
                self.db_connection.close()
        except Exception as e:
            logging.error('Error at %s', 'close connection', exc_info=e)

    def conn(self):
        return self.db_connection
    
    def engine(self):
        return self.engine_

