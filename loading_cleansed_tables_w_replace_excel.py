import pandas as pd
from file_path import FileDirectory as fd
from connection_handler import ConnectionHandler
import logging
from data_validation import table_prep
from table_mapping import tables, ods_tables
from data_cleaning import get_sqlal_dict
import sys
from excel_db_mapping import excel_mapping
from column_mapping import hubspot

print("Loading Table(s)\n")

# Directories in case of debugging comment out arguments
directory = "excel_files"
files_dir_list = fd(directory).list_dir()


for file in files_dir_list:

    # SET OUTPUT FILE NAME
    output_file_name = file[0:(len(file)-5)]

    # GET TABLE NAME
    ods_table = ods_tables[file]

    # GET 
    table_attributes = tables[ods_table]

    excel_sheet = table_attributes['excel_sheet']

    # SET SQL FILE TO QUERY
    file_fp = fd(directory,file).file_path()

    dataTypeDict = excel_mapping[ods_table]

    mapping_varchars = {k:v for k,v in dataTypeDict.items() if "String" in v}
    var_columns = [abbr for abbr in mapping_varchars.keys()]


    # RUN REPORTS SECTION
    try:
        # RUN QUERY AND SET TO DATAFRAME
        print(f'fetching data: {output_file_name}')
        df = pd.read_excel(file_fp)
        
        # REMOVE CHARACTERS THAT BREAK REPORTING
        print(f'preping table: {output_file_name}')
        table = table_prep(df).prep_sql_table(var_columns)

        all_columns = table.columns.to_list()

        full_dict = get_sqlal_dict(dataTypeDict, all_columns)
        
    except Exception as e:
        logging.error('Error at %s', 'creating dataframe', exc_info=e)
        continue
    
    print(f'Number of Rows/Columns to Load: {table.shape}')
    print(f'\n')
  
    try:
        # GRAB DB SCHEMA FOR TABLE LOAD INTO TSR_CLEANUP
        schema = tables[ods_table]['db_schema']
        # GRAB TABLE FOR TABLE LOAD INTO TSR_CLEANUP
        db_table = tables[ods_table]['db_table']
        # CONNECT TO TSR_CLEANUP
        ch = ConnectionHandler('SLO-EDA-DEVSQL1','TSR_Cleanup')
        conn = ch.conn()
        # INSERT DATA 
        print(f'loading {db_table}...')
        with conn:
            ch.replace_data(table, db_table, schema, full_dict)

    except Exception as e:
        logging.error('Error at %s', f'writing table with df Rows/Columns: {table.shape}', exc_info=e)
    print(f'finished loading\n')    
    # GET INFORMATION ABOUT THE DATAFRAME THERE WERE ISSUES HAD TO USE A BUFFER

print('\n')
print("data loading has finished")
