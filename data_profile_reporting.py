import pandas as pd
from ydata_profiling import ProfileReport
from file_path import FileDirectory as fd
from connection_handler import ConnectionHandler
import logging
from data_validation import table_prep
from data_cleaning import frequency_reports, describe, db_freq_report, describe_to_db
from table_mapping import tables
import sys


# SET TO TRUE IF YOU WANT TO RUN PROFILE REPORTS
run_profile_report = True
run_prep_table = False
run_describe = True
run_frequency = True
w_frequency_db = True
w_describe_db = True

print("running data profiling reports")
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
#sql_directory = "sql_files_replica"
#sql_files_dir_list = fd(sql_directory).list_dir()

output_dir = "output_files"
html_output_dir = "html_output_files"

sql_mapping = "sql_mapping"
sql_mapping_name = 'query_table_attributes_replica.sql'


# NOT COMPLETE
try:
    ch = ConnectionHandler('SLO-EDA-DEVSQL1','TSR_Cleanup')
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
        output_html_name = fd(html_output_dir, f"{output_file_name}.html").file_path()
        output_csv_name = fd(output_dir, f"{output_file_name}.csv").file_path()
        output_descr_name = fd(output_dir, f"Describe_{output_file_name}.csv").file_path()
        output_t_descr_name = fd(output_dir, f"Transposed_Describe_{output_file_name}.csv").file_path()
        output_freq_name = fd(output_dir, f"Frequency_Counts_{output_file_name}.txt").file_path()


        # SET SQL FILE TO QUERY
        file_fp = fd(sql_directory,file).file_path()

        # RUN REPORTS SECTION
        try:
            # RUN QUERY AND SET TO DATAFRAME
            print(f'fethcing data: {output_file_name}')
            table = ch.fetch_data_file(file_fp)

            # EXTRACT COLUMN VALUES THAT ARE TEXT
            print(f'extracting ods column metadata: {output_file_name}')
            var_columns = \
                mapping[\
                    (mapping['TABLE_NAME'] == output_file_name) & \
                    (mapping['DATA_TYPE'].isin(['nvarchar','varchar','nchar','char']))]\
                    ['COLUMN_NAME'].tolist()

            if run_prep_table:
                print(f'preping table: {output_file_name}')
                table = table_prep(table).prep_sql_table(var_columns)

        except Exception as e:
            logging.error('Error at %s', 'creating dataframe', exc_info=e)
            continue
    
        # GET INFORMATION ABOUT THE DATAFRAME THERE WERE ISSUES HAD TO USE A BUFFER
        try:
            #id_ = tables[output_file_name]['db_primary_key']
            db_table = tables[output_file_name]['db_table']
            schema = tables[output_file_name]['db_schema']
            # GET DESCRIBE INFORMATION ABOUT DATAFRAME TABLE
            # RUN SEPARATE FROM PREP TABLE TO PRESERVE NULL READS
            if w_describe_db:
                print(f'gathering describe data')
                db_desc = describe_to_db(table, db_table, output_t_descr_name)
                print(f'inserting {output_file_name} into Describe Report')
                ch.insert_data_w_eng(db_desc, 'Describe_Report', 'db_report')

            if run_describe:
                print(f'describing data: {output_file_name}')
                describe(table, output_descr_name)

            if w_frequency_db:
                print(f'creating frequency report for db')
                db_freq = db_freq_report(table, output_file_name, db_table)
                print(f'inserting {output_file_name} into frequecy table')
                ch.insert_data_w_eng(db_freq, 'Table_Frequency', 'db_report')

            # GET FREQUENCY REPORTS FROM DATAFRAME
            if run_frequency:
                print(f'frequency report generation: {output_file_name}')
                frequency_reports(table,output_freq_name)
            
            # GET PROFILE REPORT
            # HAD TO REMOVE SCATTER AND CORRELATIONS DUE TO DATA SIZE AND DATA INCONSISTENCIES
            if run_profile_report:
                print(f'generating profile report: {output_file_name}')
                profile = ProfileReport(
                    table, 
                    correlations=None,
                    interactions={'continuous': False},
                    title=f"{output_file_name.replace("_"," ")} Report w/o interaction and correlations",
                )
                profile.to_file(output_html_name)

            print('\n')
            print(f"Finished processing profile reports: {output_file_name}")
        except Exception as e:
            logging.error('Error at %s', 'creating reports', exc_info=e)
            continue


print('\n')
print("data profile reporting is complete")