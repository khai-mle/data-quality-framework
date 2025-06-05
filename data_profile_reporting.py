import pandas as pd
from ydata_profiling import ProfileReport
from file_path import FileDirectory as fd
# from connection_handler import ConnectionHandler # Commented out for CSV
import logging
from data_validation import table_prep
from data_cleaning import frequency_reports, describe, db_freq_report, describe_to_db
from table_mapping import tables
import sys
import os # Added for CSV support


# SET TO TRUE IF YOU WANT TO RUN PROFILE REPORTS
run_profile_report = True
run_prep_table = False # This relies on DB metadata, keep False for CSVs
run_describe = True
run_frequency = True
# w_frequency_db = False # Disabled for CSV support
# w_describe_db = False # Disabled for CSV support

print("running data profiling reports")
pd.options.mode.copy_on_write = True 
# pd.set_option('future.no_silent_downcasting', True) # Commented out to prevent potential version errors

# --- New CSV Argument Handling ---
argument_counts = len(sys.argv)
csv_files_to_run = []

if argument_counts == 1:
    print("Please provide CSV file paths as arguments.")
    sys.exit(1)
elif argument_counts > 1:
    for arg in sys.argv[1:]:
        if not arg.lower().endswith('.csv'):
            print(f"Warning: File '{arg}' does not end with .csv. Skipping.")
            continue
        if not os.path.exists(arg):
            print(f"Warning: File '{arg}' not found. Skipping.")
            continue
        csv_files_to_run.append(arg)

if not csv_files_to_run:
    print("No valid CSV files found to process.")
    sys.exit(1)


output_dir = "output_files"
html_output_dir = "html_output_files"

# Create output directories if they don't exist
if not os.path.exists(output_dir):
    os.makedirs(output_dir)
if not os.path.exists(html_output_dir):
    os.makedirs(html_output_dir)


# --- DB Connection and mapping commented out ---
# sql_mapping = "sql_mapping"
# sql_mapping_name = 'query_table_attributes_replica.sql'
#
# try:
#     ch = ConnectionHandler('SLO-EDA-DEVSQL1','TSR_Cleanup')
#     conn = ch.conn()
# except Exception as e:
#     logging.error('Error at %s', 'creating dataframe', exc_info=e)
#
# with conn:
#
#     print(f'fetching sql attributes\n')
#     mapping_file = fd(sql_mapping,sql_mapping_name).file_path()
#     mapping = ch.fetch_data_file(mapping_file)

for csv_file_path in csv_files_to_run:

    # SET OUTPUT FILE NAMES BASED ON CSV FILE
    output_file_name = os.path.splitext(os.path.basename(csv_file_path))[0]
    output_html_name = fd(html_output_dir, f"{output_file_name}.html").file_path()
    output_csv_name = fd(output_dir, f"{output_file_name}.csv").file_path()
    output_descr_name = fd(output_dir, f"Describe_{output_file_name}.csv").file_path()
    output_t_descr_name = fd(output_dir, f"Transposed_Describe_{output_file_name}.csv").file_path()
    output_freq_name = fd(output_dir, f"Frequency_Counts_{output_file_name}.txt").file_path()


    # RUN REPORTS SECTION
    try:
        # READ CSV AND SET TO DATAFRAME
        print(f'Reading data from: {csv_file_path}')
        table = pd.read_csv(csv_file_path)
        print(f"Successfully read {len(table)} rows from {csv_file_path}")
        
        # --- DB METADATA LOGIC COMMENTED OUT ---
        # var_columns = \
        #     mapping[\
        #         (mapping['TABLE_NAME'] == output_file_name) & \
        #         (mapping['DATA_TYPE'].isin(['nvarchar','varchar','nchar','char']))]\
        #         ['COLUMN_NAME'].tolist()
        #
        # if run_prep_table: # This depends on var_columns from DB metadata
        #     print(f'preping table: {output_file_name}')
        #     table = table_prep(table).prep_sql_table(var_columns)

    except Exception as e:
        logging.error(f"Error creating dataframe from {csv_file_path}", exc_info=e)
        continue

    # GET INFORMATION ABOUT THE DATAFRAME
    try:
        # --- DB-related table lookup commented out ---
        # db_table = tables[output_file_name]['db_table']
        # schema = tables[output_file_name]['db_schema']
        
        # --- DB WRITE-BACKS COMMENTED OUT ---
        # if w_describe_db:
        #     print(f'gathering describe data')
        #     db_desc = describe_to_db(table, db_table, output_t_descr_name)
        #     print(f'inserting {output_file_name} into Describe Report')
        #     ch.insert_data_w_eng(db_desc, 'Describe_Report', 'db_report')

        if run_describe:
            print(f'describing data: {output_file_name}')
            describe(table, output_descr_name)

        # if w_frequency_db:
        #     print(f'creating frequency report for db')
        #     db_freq = db_freq_report(table, output_file_name, db_table)
        #     print(f'inserting {output_file_name} into frequecy table')
        #     ch.insert_data_w_eng(db_freq, 'Table_Frequency', 'db_report')

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
                title=f"{output_file_name.replace('_',' ')} Report w/o interaction and correlations",
                minimal=True # Added for performance with large CSVs
            )
            profile.to_file(output_html_name)

        print('\n')
        print(f"Finished processing profile reports: {output_file_name}")
    except Exception as e:
        logging.error('Error creating reports for %s', output_file_name, exc_info=e)
        continue


print('\n')
print("data profile reporting is complete")