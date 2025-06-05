import sys
import pandas as pd
from file_path import FileDirectory as fd
from connection_handler import ConnectionHandler
from data_validation import Phone, Email, Website, UserName, Outliers
import numpy as np
import logging
from table_mapping import tables
from datetime import datetime
from data_cleaning import find_fuzzy_matches


# Now using system arguments and the hacky code below doesn't use argparser by sysarg
# Time was the factor in developing the arguments in this way
# python <script> sql_files_queries MDM_Contact__Active.sql MDM_Customer__Active.sql
'''
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
'''
pd.options.mode.copy_on_write = True 

truncate_verification_table = True

# Directories in case of debugging comment out arguments
sql_directory = "sql_files_queries"
sql_files_dir_list = fd(sql_directory).list_dir()


output_directory = "output_files"


ch = ConnectionHandler('SLO-EDA-DEVSQL1','TSR_Cleanup')
conn = ch.conn()

pd.set_option('future.no_silent_downcasting', True)

with conn:

    for file in sql_files_dir_list:

        # SET OUTPUT FILE NAMES BASED ON QUERY .SQL FILE
        output_file_name = file[0:(len(file)-4)]

        # SET SQL FILE TO QUERY
        file_fp = fd(sql_directory,file).file_path()

        # RUN QUERY AND SET TO DATAFRAME
        # RUN REPORTS SECTION
        try:
            # RUN QUERY AND SET TO DATAFRAME
            print(f'fethcing data: {output_file_name}')
            table = ch.fetch_data_file(file_fp)

        except Exception as e:
            logging.error('Error at %s', 'creating dataframe', exc_info=e)
            continue


        # ALL REPORTS REQUIRE AN ID TO REFERENCE AND WITHOUT IT, WE DON'T RUN REPORTS            
        primary_key = tables[output_file_name]['db_primary_key']
        reference_fields = tables[output_file_name]['reference_fields']
        db_table = tables[output_file_name]['db_table']
        db_schema = tables[output_file_name]['db_schema']
        key_test = len(primary_key)

        if key_test == 1 and primary_key != None:

            if reference_fields != None:
               validation_table_fields = primary_key + reference_fields
               base_fields = primary_key + reference_fields
            else:
                validation_table_fields = primary_key
                base_fields = primary_key

            duplicates = tables[output_file_name]['duplicates']

            # SCANS LISTS OF FIELDS FOR EXACT DUPLICATES
            # FUTURE WILL HAVE FUZZY MATCH REPORT RAN IN THIS SECTION
            # ADDRESS DUPLICATES WILL BE HANDLED WITH CLEANSED ADDRESSES IN ADDRESS SECTION
            if duplicates != [None]:
                fuzzy = find_fuzzy_matches(table,output_file_name,duplicates,output_directory,db_table,85)
                #m_fuzzy = multi_fuzzy_matches(table,output_file_name,duplicates,output_directory,db_table,85)
