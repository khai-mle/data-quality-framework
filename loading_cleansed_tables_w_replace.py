import pandas as pd
from file_path import FileDirectory as fd
from connection_handler import ConnectionHandler
import logging
from data_validation import table_prep
from table_mapping import tables, ods_tables
from data_cleaning import get_sqlal_dict
import sys

print("Loading Table(s)\n")
pd.options.mode.copy_on_write = True 
pd.set_option('future.no_silent_downcasting', True)


# Now using system arguments and the hacky code below doesn't use argparser by sysarg
# Time was the factor in developing the arguments in this way
# python <script>.py sql_files_queries MDM_Contact__Active.sql MDM_Customer__Active.sql

argument_counts = len(sys.argv)

# No argument runs the entired directory
if argument_counts == 1:
    sql_directory = "sql_files_replica"
    sql_files_dir_list = fd(sql_directory).list_dir()
# runs everything outside of the full directory
elif argument_counts == 2:
    sql_directory = sys.argv[1]
    sql_files_dir_list = fd(sql_directory).list_dir()
# runs the files called out in each argument
# make sure they are spelled correctly or it will error
# didn't add checks yet. To be added.
elif len(sys.argv) > 2:
    sql_directory = sys.argv[1]
    # SET FILE LIST
    files_to_run = []
    # GET DIRECTORY
    # GET FILES FROM EACH ARGUMENT
    for arg in sys.argv[2:]:
        files_to_run.append(arg)
    # SET DIR LIST TO FILES TO RUN
    # THEY SHOULD BE SPELLED EXACTLY
    sql_files_dir_list = files_to_run

# Directories in case of debugging comment out arguments
#sql_directory = "sql_files_queries"
#sql_files_dir_list = fd(sql_directory).list_dir()

# GET the ODS query table attributes
sql_mapping = "sql_mapping"
sql_mapping_name = 'query_table_attributes.sql'

# Connect to the EDA lake 
try:
    ch = ConnectionHandler('STL00-BISQL02','eda_lake')
    conn = ch.conn()
except Exception as e:
    logging.error('Error at %s', 'creating dataframe', exc_info=e)


with conn:

    print(f'fetching sql attributes\n')
    mapping_file = fd(sql_mapping,sql_mapping_name).file_path()
    mapping = ch.fetch_data_file(mapping_file)


    for file in sql_files_dir_list:

        # SET OUTPUT FILE NAMES BASED ON QUERY .SQL FILE
        output_file_name = file[0:(len(file)-4)]

        # GET DF OF SQL TABLES
        ods_table = ods_tables[output_file_name]

        # SET SQL FILE TO QUERY
        file_fp = fd(sql_directory,file).file_path()

        # RUN REPORTS SECTION
        try:
            # RUN QUERY AND SET TO DATAFRAME
            print(f'fetching data: {output_file_name}')
            table = ch.fetch_data_file(file_fp)
            
            # EXTRACT COLUMN VALUES THAT ARE TEXT
            print(f'extracting ods column metadata: {ods_table}')
            var_columns = \
                mapping[\
                    (mapping['TABLE_NAME'] == ods_table) & \
                    (mapping['DATA_TYPE'].isin(['nvarchar','varchar','nchar','char']))]\
                    ['COLUMN_NAME'].tolist()

            table_columns = mapping[(mapping['TABLE_NAME'] == ods_table)]
            dataTypeDict = dict(zip(table_columns['COLUMN_NAME'], table_columns['SA_COLUMN_FORMAT1']))


            # REMOVE CHARACTERS THAT BREAK REPORTING
            print(f'preping table: {output_file_name}')
            table = table_prep(table).prep_sql_table(var_columns)
            all_columns = table.columns.to_list()

            full_dict = get_sqlal_dict(dataTypeDict, all_columns)
            
        except Exception as e:
            logging.error('Error at %s', 'creating dataframe', exc_info=e)
            continue
        
        print(f'Number of Rows/Columns to Load: {table.shape}')
        print(f'\n')
        try:
            # GRAB DB SCHEMA FOR TABLE LOAD INTO TSR_CLEANUP
            schema = tables[output_file_name]['db_schema']
            # GRAB TABLE FOR TABLE LOAD INTO TSR_CLEANUP
            db_table = tables[output_file_name]['db_table']
            # CONNECT TO TSR_CLEANUP
            ch2 = ConnectionHandler('SLO-EDA-DEVSQL1','TSR_Cleanup')
            conn2 = ch2.conn()
            # INSERT DATA 
            print(f'loading {db_table}...')
            ch2.replace_data(table, db_table, schema, full_dict)

        except Exception as e:
            logging.error('Error at %s', f'writing table with df Rows/Columns: {table.shape}', exc_info=e)
        print(f'finished loading\n')    
        # GET INFORMATION ABOUT THE DATAFRAME THERE WERE ISSUES HAD TO USE A BUFFER

print('\n')
print("data loading has finished")
