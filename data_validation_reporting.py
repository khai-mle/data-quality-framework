import sys
import pandas as pd
from file_path import FileDirectory as fd
# from connection_handler import ConnectionHandler # Commented out for CSV development
from data_validation import Phone, Email, Website, UserName, Outliers
import numpy as np
import logging
from table_mapping import tables # Ensure this mapping supports your CSV file names as keys
from datetime import datetime
from data_cleaning import \
    find_nulls, find_duplicates, address_cleaning, \
    frequency_report_by_column, phone_cleaning, var_to_string, \
    get_sqlal_dict, db_freq_report_by_column
import os # Added for CSV development

# Now using system arguments and the hacky code below doesn't use argparser by sysarg
# python <script> Logistics_DQPilot_CONTACT.csv Logistics_DQPilotACCOUNT.csv

argument_counts = len(sys.argv)
csv_files_to_run = [] # Renamed for clarity

# No argument runs the entired directory - This behavior will be disabled for CSV, expect file paths
if argument_counts == 1:
    print("Please provide CSV file paths as arguments.")
    sys.exit(1) # Exit if no CSV files are provided
# runs everything outside of the full directory - This behavior will be disabled for CSV
elif argument_counts > 1: # Expect CSV file paths directly
    # GET FILES FROM EACH ARGUMENT
    for arg in sys.argv[1:]: # All arguments are considered CSV file paths
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

# sql_directory = sys.argv[1] # Commented out for CSV development
# sql_files_dir_list = files_to_run # Commented out for CSV development

pd.options.mode.copy_on_write = True 

# truncate_verification_table = False # Related to DB, commented out

output_directory = "output_files"
if not os.path.exists(output_directory):
    os.makedirs(output_directory)


# sql_mapping = "sql_mapping" # Commented out for CSV development
# sql_mapping_name = 'query_table_attributes_replica.sql' # Commented out for CSV development

# ch = ConnectionHandler('SLO-EDA-DEVSQL1','TSR_Cleanup') # Commented out for CSV development
# conn = ch.conn() # Commented out for CSV development

pd.set_option('future.no_silent_downcasting', True)

# with conn: # Commented out for CSV development
    # if truncate_verification_table: # Commented out for CSV development
    #     print("Truncating fixed tables: validation frequency & exact duplicate") # Commented out for CSV development
    #     ch.truncate_replica_table('Validation_Frequency', 'db_report') # Commented out for CSV development
    #     ch.truncate_replica_table('Exact_Duplicate', 'db_report') # Commented out for CSV development

for csv_file_path in csv_files_to_run: # Loop through CSV files

    # SET OUTPUT FILE NAMES BASED ON CSV FILE
    output_file_name = os.path.splitext(os.path.basename(csv_file_path))[0] # E.g., "Logistics_DQPilot_CONTACT"

    # GRAB REPLICA FIELDS - This part is database-specific and complex to replicate for CSVs without more info.
    # We will proceed without a specific dataTypeDict from DB metadata.
    # Pandas will infer types from CSV. Validation functions might need adjustments if they rely heavily on specific SQLAlchemy types.
    print(f"Processing CSV file: {csv_file_path}")
    # mapping_file = fd(sql_mapping,sql_mapping_name).file_path() # Commented out
    # mapping = ch.fetch_data_file(mapping_file) # Commented out

    # table_columns = mapping[(mapping['TABLE_NAME'] == output_file_name)] # Commented out
    # dataTypeDict = dict(zip(table_columns['COLUMN_NAME'], table_columns['SA_COLUMN_FORMAT1'])) # Commented out
    dataTypeDict = {} # Initialize empty, as we are not fetching DB types. Some functions might use this.

    # SET SQL FILE TO QUERY # Commented out
    # file_fp = fd(sql_directory,file).file_path() # Commented out

    # RUN QUERY AND SET TO DATAFRAME
    # RUN REPORTS SECTION
    try:
        # RUN QUERY AND SET TO DATAFRAME
        print(f"Reading data from: {output_file_name}.csv")
        # table = ch.fetch_data_file(file_fp) # Commented out
        table = pd.read_csv(csv_file_path)
        print(f"Successfully read {len(table)} rows from {csv_file_path}")

    except Exception as e:
        logging.error(f"Error creating dataframe from {csv_file_path}", exc_info=e)
        continue


    # ALL REPORTS REQUIRE AN ID TO REFERENCE AND WITHOUT IT, WE DON'T RUN REPORTS
    # Ensure 'output_file_name' (e.g., "Logistics_DQPilot_CONTACT") is a key in your 'tables' dict from table_mapping.py
    if output_file_name not in tables:
        logging.warning(f"No configuration found for '{output_file_name}' in table_mapping.py. Skipping detailed validations for this file.")
        # Optionally, run generic profiling or basic checks here if desired
        # For now, we just skip if no specific mapping config exists
        print(f"Skipping detailed validation for {output_file_name} due to missing configuration in table_mapping.py.")
        continue
            
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

        def_country_field = tables[output_file_name]['default_country_field']
        def_country_code = tables[output_file_name]['default_country_code']
        hasAddress = tables[output_file_name]['address']['hasAddress']
        phone = tables[output_file_name]['phone']
        email = tables[output_file_name]['email']
        url = tables[output_file_name]['url']
        name = tables[output_file_name]['name']
        z_num_outliers = tables[output_file_name]['z_num_outliers']
        iq_num_outliers = tables[output_file_name]['iq_num_outliers']
        date_outliers = tables[output_file_name]['date_outliers']
        duplicates = tables[output_file_name]['duplicates']
        is_null = tables[output_file_name]['isNull']
        exception_from_row_validation = tables[output_file_name]['exception_from_row_validation']
        
        validity_fields = []
        outlier_fields = []
        # SCANS LISTS OF FIELDS FOR EXACT DUPLICATES
        # FUTURE WILL HAVE FUZZY MATCH REPORT RAN IN THIS SECTION
        # ADDRESS DUPLICATES WILL BE HANDLED WITH CLEANSED ADDRESSES IN ADDRESS SECTION
        if duplicates != [None]:
            print(f"finding exact duplicates for {output_file_name}")
            dups_df = find_duplicates(table,output_file_name,duplicates,output_directory,primary_key,db_table) # db_table might be irrelevant for CSV output
            # print(f"loading exact duplicates to db") # Commented out
            # ch.insert_data_w_eng(dups, 'Exact_Duplicate', 'db_report') # Commented out
            if not dups_df.empty:
                dup_output_path = os.path.join(output_directory, f"exact_duplicates_{output_file_name}.csv")
                dups_df.to_csv(dup_output_path, index=False)
                print(f"Saved exact duplicates report to {dup_output_path}")


        # is_null IS USED TO FIND THE NULL VALUES OF REQUIRED FIELDS
        # IT IS COMBINED WITH ALL FIELDS TO GET A SENSE OF SCALE FOR WHAT FIELDS ARE COMPROMISED
        if is_null != [None]:
            print(f"finding null required fields for {output_file_name}")
            # table_name_db = f'null_required_fields_{output_file_name}' # For DB table name
            r_fields = base_fields + is_null
            # null_dict = get_sqlal_dict(dataTypeDict, r_fields) # dataTypeDict is empty, this might not work as intended or be needed for CSV
            nulls_df = find_nulls(table,output_file_name,output_directory,r_fields)
            # ch.replace_data(nulls, table_name_db, 'db_report', null_dict) # Commented out
            if not nulls_df.empty:
                nulls_output_path = os.path.join(output_directory, f"null_required_fields_{output_file_name}.csv")
                nulls_df.to_csv(nulls_output_path, index=False)
                print(f"Saved null required fields report to {nulls_output_path}")
        
        #SET VALIDATION ROW
        valid_row = 'row_is_valid'
        table[valid_row] = True

        # SET VALIDATION VALUES
        valid_row_dict = {valid_row:'sa.Boolean'}
        validation_row_list = ['row_is_valid']
        dataTypeDict = dataTypeDict | valid_row_dict
        validation_table_fields = validation_table_fields + validation_row_list

        # ADDRESS will use the address logic to see if addresses are valid
        if hasAddress:
            print(f"performing address validation")
            address_fields = []
            addresses = tables[output_file_name]['address']['addresses']
            keys = sorted([key for key in addresses.keys()])

            for add in keys:
                # THE FOLLOWING IF STATEMENTS ARE A WORK AROUND FOR THE ISSUE
                # CONCERNING LAMBDAS AND USING LIST VARIABLES LIKE *ListName TO
                # INSTANTIATE ROW LEVEL ANALYSIS. WE ARE CREATING NULL DUMMY COLUMNS
                # WHEN COLUMN DOESN'T EXIST TO KEEP CODE LIGHT AND NOT EVERY PERMUTATION
                # THE OLD SOLUTION CREATED LISTS WITH None TO BE A PLACEHOLDER.
                address = tables[output_file_name]['address']['addresses'][add]
                addr_root_name = address['address_root_name']

                fullAddress = address['full_address']
                if fullAddress != None:
                    address_fields.append(fullAddress)
                else:
                    fullAddress = f"{addr_root_name}_DFullAddress"
                    table[fullAddress] = None
                    address_fields.append(fullAddress)
                    full_address_dict = {fullAddress:'sa.String(1000)'}
                    dataTypeDict = dataTypeDict | full_address_dict

                address1 = address['address1']
                if address1 != None:
                    address_fields.append(address1)

                address2 = address['address2']
                if address2 != None:
                    address_fields.append(address2)
                else:
                    address2 = f"{addr_root_name}_DAddress2"
                    table[address2] = None
                    address_fields.append(address2)
                    address2_dict = {address2:'sa.String(300)'}
                    dataTypeDict = dataTypeDict | address2_dict

                address3 = address['address3']
                if address3 != None:
                    address_fields.append(address3)
                else:
                    table[f"{addr_root_name}_DAddress3"] = None
                    address3 = f"{addr_root_name}_DAddress3"
                    address_fields.append(address3)
                    address3_dict = {address3:'sa.String(300)'}
                    dataTypeDict = dataTypeDict | address3_dict

                city = address['city']
                if city != None:
                    address_fields.append(city)

                state = address['state']
                if state != None:
                    address_fields.append(state)

                postal_code = address['postal_code']
                if postal_code != None:
                    address_fields.append(postal_code)

                country = address['country']
                if country != None:
                    address_fields.append(country)
                elif 'DCountry' in table.columns:
                    pass
                else:
                    country = 'DCountry'
                    table[country] = 'US'
                    address_fields.append(country)
                    country_dict = {country:'sa.String(100)'}
                    dataTypeDict = dataTypeDict | country_dict

                validation_table_fields = validation_table_fields + address_fields
                address_fields = validation_table_fields

                r_validate = f'{addr_root_name}_addr_is_valid' 
                r_message = f'{addr_root_name}_addr_error_messg'                    
                r_street = f'{addr_root_name}_street'
                r_suite = f'{addr_root_name}_suite'
                r_city = f'{addr_root_name}_city'
                r_state = f'{addr_root_name}_state'
                r_zip = f'{addr_root_name}_postal_code' 
                r_country = f'{addr_root_name}_country'
                r_full_address = f'{addr_root_name}_full_address'

                validity_fields.append(r_validate)

                address_fields_list = [
                    r_validate,
                    r_message,
                    r_street,
                    r_suite,
                    r_city,
                    r_state,
                    r_zip,
                    r_country,
                    r_full_address
                ]

                address_fields = address_fields + address_fields_list
                validation_table_fields = validation_table_fields + address_fields_list

                address_dtypes = {
                    r_validate:'sa.Boolean',
                    r_message:'sa.String(1000)',
                    r_street:'sa.String(300)',
                    r_suite:'sa.String(300)',
                    r_city:'sa.String(100)',
                    r_state:'sa.String(100)',
                    r_zip:'sa.String(100)',
                    r_country:'sa.String(100)',
                    r_full_address:'sa.String(1500)'
                }

                dataTypeDict = dataTypeDict | address_dtypes

                table[\
                    [\
                        r_validate, 
                        r_message, 
                        r_street, 
                        r_suite, 
                        r_city, 
                        r_state, 
                        r_zip, 
                        r_country,
                        r_full_address]] = table.apply(\
                            lambda row: \
                                address_cleaning(\
                                    row[fullAddress], 
                                    row[address1], 
                                    row[address2], 
                                    row[address3], 
                                    row[city], 
                                    row[state], 
                                    row[postal_code], 
                                    row[country]), \
                                        axis=1, \
                                        result_type = "expand"
                                        )
                
                # SET DUPLICATE ADDRESSES
                address_duplicate_list = [r_full_address]
                dup_full_address_fields = primary_key + address_duplicate_list
                # dup_full_address_dict = get_sqlal_dict(dataTypeDict, dup_full_address_fields) # Commented out as dataTypeDict is minimal for CSV
                addr_dups_df = find_duplicates(table,output_file_name,address_duplicate_list,output_directory,primary_key,db_table) # db_table may be irrelevant
                # dup_address_table_name = f'duplicate_{output_file_name.lower()}_{addr_root_name.lower()}_addresses' # Commented out
                # ch.replace_data(addr_dups, dup_address_table_name, 'db_report', dup_full_address_dict) # Commented out
                if not addr_dups_df.empty:
                    addr_dups_output_path = os.path.join(output_directory, f"duplicate_addresses_{output_file_name}_{addr_root_name}.csv")
                    addr_dups_df.to_csv(addr_dups_output_path, index=False)
                    print(f"Saved duplicate addresses report to {addr_dups_output_path}")


                # SET ROW VALID TO FALSE IF VALIDATION IS FALSE
                if add not in exception_from_row_validation:
                    table[valid_row] = table.apply(lambda row: False if row[r_validate] == False else row[valid_row], axis = 1)
                # SET MESSAGE TO STRING SO IT CAN LOAD INTO THE DATABASE
                table[r_message] = [','.join(map(str, l)) if isinstance(l, list) else l for l in table[r_message]] # Ensure conversion if list
                # SET ADDRESS DATAFRAME
                address_df_cols = [col for col in address_fields if col in table.columns] # Ensure all columns exist
                address_df = table[address_df_cols]
                # APPLY MESSAGE TO ADDRESS DATAFRAME
                # address_df[r_message] # This line does nothing
                # SET ADDRESS TABLE NAME
                address_table_name = f'{output_file_name}_{addr_root_name}'
                # SET FILEPATH NAME AND WRITE TO CSV
                addr_vald_path = fd(output_directory,f"Addr_Vald_{address_table_name}.csv").file_path()
                address_df.to_csv(addr_vald_path, index=False, sep = "|")
                print(f"Saved address validation detail to {addr_vald_path}")
                # SET FREQUENCY REPORT
                addr_freq_path = fd(output_directory,f"Addr_Freq_Count_{address_table_name}.csv").file_path()
                freq_col = [r_validate,r_message]
                
                # Assuming frequency_report_by_column is modified to save to CSV or returns df
                addr_freq_df = frequency_report_by_column(address_df, addr_freq_path, freq_col) # Pass path
                if addr_freq_df is not None and not addr_freq_df.empty: # If it returns a df that was saved
                     print(f"Saved address frequency count to {addr_freq_path}")

                # freq_addr_col = db_freq_report_by_column(address_df, output_file_name, db_table, freq_col) # Commented out
                # ch.insert_data_w_eng(freq_addr_col, 'Validation_Frequency', 'db_report') # Commented out
                

        if phone != [None]:
            print(f"performing phone validation")
            phone_all = base_fields[:] # Create a copy
            phone_all.extend([p for p in phone if p in table.columns]) # Add only existing phone columns
            
            validation_table_fields = validation_table_fields + [p for p in phone if p in table.columns]


            country_col_name = def_country_field
            if country_col_name and country_col_name in table.columns:
                phone_country_col = country_col_name
                if country_col_name not in phone_all: phone_all.append(country_col_name)
                if country_col_name not in validation_table_fields: validation_table_fields.append(country_col_name)
            else:
                country_col_name = 'DCountry' # Default if not specified or not exists
                table[country_col_name] = def_country_code if def_country_code else 'US' # Use default code or 'US'
                phone_country_col = country_col_name
                if country_col_name not in phone_all: phone_all.append(country_col_name)
                if country_col_name not in validation_table_fields: validation_table_fields.append(country_col_name)
                country_dict = {country_col_name:'sa.String(100)'} # Still for dataTypeDict, though it's minimally used
                dataTypeDict = dataTypeDict | country_dict


            phone_valid_col = []
            # GOT THROUGH PHONE FIELDS IN DATAFRAME AND PERFORM CHECKS AND FORMATS
            for col in [p for p in phone if p in table.columns]: # Iterate only existing phone columns
                valid = f'{col}_is_valid'
                format_col = f'{col}_formatted' # Renamed to avoid conflict with 'format' keyword
                ext =  f'{col}_ext_formatted' 

                validity_fields.append(valid)

                phone_dtypes = {
                    valid:'sa.Boolean',
                    format_col:'sa.String(100)',
                    ext:'sa.String(100)'
                }

                dataTypeDict = dataTypeDict | phone_dtypes

                phone_list = [valid,format_col,ext]

                phone_valid_col.append(valid)

                validation_table_fields = validation_table_fields + phone_list
                if col in phone_all : phone_all = phone_all + phone_list


                table[[valid, format_col, ext]] = \
                    table.apply(lambda row: phone_cleaning(row[col],row[phone_country_col]), \
                    axis=1, result_type = "expand")

                # SET ROW VALID TO FALSE IF VALIDATION IS FALSE
                if col not in exception_from_row_validation:
                    table[valid_row] = table.apply(lambda row: False if row[valid] == False else row[valid_row], axis = 1)                 
            
            phone_df_cols = [col for col in phone_all if col in table.columns]
            phone_df = table[phone_df_cols] 

            phone_validation_path = fd(output_directory,f"Phone_validation_{output_file_name}.csv").file_path()       
            phone_df.to_csv(phone_validation_path, index=False, sep = "|")
            print(f"Saved phone validation detail to {phone_validation_path}")

            phone_freq_path = fd(output_directory,f"Phone_Freq_Count_{output_file_name}.csv").file_path()

            phone_freq_df = frequency_report_by_column(phone_df, phone_freq_path, phone_valid_col)
            if phone_freq_df is not None and not phone_freq_df.empty:
                print(f"Saved phone frequency count to {phone_freq_path}")

            # freq_phone_col = db_freq_report_by_column(phone_df, output_file_name, db_table, phone_valid_col) # Commented out
            # ch.insert_data_w_eng(freq_phone_col, 'Validation_Frequency', 'db_report') # Commented out

        
        if email != [None]:
            print(f"performing email validation")
            email_all = base_fields[:] # Create a copy
            email_all.extend([e_col for e_col in email if e_col in table.columns])
            validation_table_fields = validation_table_fields + [e_col for e_col in email if e_col in table.columns]
            email_valid_col = []
            
            for col in [e_col for e_col in email if e_col in table.columns]:

                normalized = f'{col}_normalized'   
                check = f'{col}_check'

                validity_fields.append(check)

                email_dtypes = {
                    check:'sa.Boolean',
                    normalized:'sa.String(100)'
                }

                dataTypeDict = dataTypeDict | email_dtypes

                email_list = [normalized,check]

                email_valid_col.append(check)

                validation_table_fields = validation_table_fields + email_list
                if col in email_all: email_all = email_all + email_list

                table[normalized] = \
                    table.apply(\
                    lambda row: \
                    Email(row[col]).email_valid(), \
                    axis=1)
                table[check] = \
                    table.apply(\
                    lambda row: \
                    Email(row[col]).email_check(), \
                    axis=1)
                
                # SET ROW VALID TO FALSE IF VALIDATION IS FALSE
                if col not in exception_from_row_validation:
                    table[valid_row] = table.apply(lambda row: False if row[check] == False else row[valid_row], axis = 1)

            email_df_cols = [col for col in email_all if col in table.columns]
            email_df = table[email_df_cols]
            email_validation_path = fd(output_directory,f"Email_validation_{output_file_name}.csv").file_path()       
            email_df.to_csv(email_validation_path, index=False, sep = "|")
            print(f"Saved email validation detail to {email_validation_path}")

            email_freq_path = fd(output_directory,f"Email_Freq_Count_{output_file_name}.csv").file_path()
            email_freq_df = frequency_report_by_column(email_df, email_freq_path, email_valid_col)
            if email_freq_df is not None and not email_freq_df.empty:
                print(f"Saved email frequency count to {email_freq_path}")


            # freq_email_col = db_freq_report_by_column(email_df, output_file_name, db_table, email_valid_col) # Commented out
            # ch.insert_data_w_eng(freq_email_col, 'Validation_Frequency', 'db_report') # Commented out
        
        if url != [None]:
            print(f"performing url validation")
            url_all = base_fields[:] # Create a copy
            url_all.extend([u_col for u_col in url if u_col in table.columns])
            validation_table_fields = validation_table_fields + [u_col for u_col in url if u_col in table.columns]
            url_valid_col = []
            
            for col in [u_col for u_col in url if u_col in table.columns]:

                url_valid = f'{col}_is_valid'  

                validity_fields.append(url_valid)

                url_dtypes = {
                    url_valid:'sa.Boolean'
                }

                dataTypeDict = dataTypeDict | url_dtypes

                url_list = [url_valid]

                url_valid_col.append(url_valid)

                validation_table_fields = validation_table_fields + url_list
                if col in url_all: url_all = url_all + url_list

                table[url_valid] = \
                    table.apply(\
                    lambda row: \
                    Website(row[col]).website_check(), \
                    axis=1)

                # SET ROW VALID TO FALSE IF VALIDATION IS FALSE
                if col not in exception_from_row_validation:
                    table[valid_row] = table.apply(lambda row: False if row[url_valid] == False else row[valid_row], axis = 1)

            url_df_cols = [col for col in url_all if col in table.columns]
            url_df = table[url_df_cols]
            url_validation_path = fd(output_directory,f"URL_validation_{output_file_name}.csv").file_path()       
            url_df.to_csv(url_validation_path, index=False, sep = "|")
            print(f"Saved URL validation detail to {url_validation_path}")

            url_freq_path = fd(output_directory,f"URL_Freq_Count_{output_file_name}.csv").file_path()
            url_freq_df = frequency_report_by_column(url_df, url_freq_path, url_valid_col)
            if url_freq_df is not None and not url_freq_df.empty:
                print(f"Saved URL frequency count to {url_freq_path}")

            # freq_url_col = db_freq_report_by_column(url_df, output_file_name, db_table, url_valid_col) # Commented out
            # ch.insert_data_w_eng(freq_url_col, 'Validation_Frequency', 'db_report') # Commented out
 

        if name != [None]:
            print(f"performing proper name validation")
            name_all = base_fields[:] # Create a copy
            name_all.extend([n_col for n_col in name if n_col in table.columns])
            validation_table_fields = validation_table_fields + [n_col for n_col in name if n_col in table.columns]
            name_valid_col = []
            
            for col in [n_col for n_col in name if n_col in table.columns]:

                name_valid = f'{col}_check' 

                validity_fields.append(name_valid)

                name_dtypes = {
                    name_valid:'sa.Boolean'
                }

                dataTypeDict = dataTypeDict | name_dtypes

                name_list = [name_valid]

                name_valid_col.append(name_valid)

                validation_table_fields = validation_table_fields + name_list
                if col in name_all: name_all = name_all + name_list

                table[name_valid] = \
                    table.apply(\
                    lambda row: \
                    UserName(row[col]).name_check(), \
                    axis=1)

                # SET ROW VALID TO FALSE IF VALIDATION IS FALSE
                if col not in exception_from_row_validation:
                    table[valid_row] = table.apply(lambda row: False if row[name_valid] == False else row[valid_row], axis = 1)

            name_df_cols = [col for col in name_all if col in table.columns]
            name_df = table[name_df_cols]
            name_validation_path = fd(output_directory,f"Name_validation_{output_file_name}.csv").file_path()       
            name_df.to_csv(name_validation_path, index=False, sep = "|")
            print(f"Saved name validation detail to {name_validation_path}")

            name_freq_path = fd(output_directory,f"NAME_Freq_Count_{output_file_name}.csv").file_path()
            name_freq_df = frequency_report_by_column(name_df, name_freq_path, name_valid_col)
            if name_freq_df is not None and not name_freq_df.empty:
                 print(f"Saved name frequency count to {name_freq_path}")

            # name_freq_col = db_freq_report_by_column(name_df, output_file_name, db_table, name_valid_col) # Commented out
            # ch.insert_data_w_eng(name_freq_col, 'Validation_Frequency', 'db_report') # Commented out


        if z_num_outliers != [None]:
            print(f"performing z outlier analysis")
            z_num_outliers_all = base_fields[:] # Create a copy
            z_num_outliers_all.extend([z_col for z_col in z_num_outliers if z_col in table.columns])
            validation_table_fields = validation_table_fields + [z_col for z_col in z_num_outliers if z_col in table.columns]
            z_valid_col = []
            
            for col in [z_col for z_col in z_num_outliers if z_col in table.columns]:

                z_valid = f'{col}_isOutlier'
                z_score_col = f'{col}_z_score' # Renamed

                outlier_fields.append(z_valid)

                z_dtypes = {
                    z_valid:'sa.Boolean'
                }

                dataTypeDict = dataTypeDict | z_dtypes

                z_list = [z_valid]

                z_valid_col.append(z_valid)

                validation_table_fields = validation_table_fields + z_list
                if col in z_num_outliers_all: z_num_outliers_all = z_num_outliers_all + z_list

                table[z_score_col] = Outliers(table[col]).z_score()
                table[z_valid] = table.apply(lambda row: Outliers(row[z_score_col]).z_score_outlier(), axis=1)
                table = table.drop([z_score_col], axis=1)
            
                # SET ROW VALID TO FALSE IF VALIDATION IS TRUE (OUTLIERS ARE TRUE)
                if col not in exception_from_row_validation:
                    table[valid_row] = table.apply(lambda row: False if row[z_valid] == True else row[valid_row], axis = 1)

            z_df_cols = [col for col in z_num_outliers_all if col in table.columns]
            z_df = table[z_df_cols]
            z_validation_path = fd(output_directory,f"Num_Outlier_validation_{output_file_name}.csv").file_path()       
            z_df.to_csv(z_validation_path, index=False, sep = "|")
            print(f"Saved Z-score outlier detail to {z_validation_path}")


            z_freq_path = fd(output_directory,f"Num_Outlier_Freq_Count_{output_file_name}.csv").file_path()
            z_freq_df = frequency_report_by_column(z_df, z_freq_path, z_valid_col)
            if z_freq_df is not None and not z_freq_df.empty:
                print(f"Saved Z-score outlier frequency to {z_freq_path}")


            # z_freq_col = db_freq_report_by_column(z_df, output_file_name, db_table, z_valid_col) # Commented out
            # ch.insert_data_w_eng(z_freq_col, 'Validation_Frequency', 'db_report') # Commented out

        
        if iq_num_outliers != [None]:
            print(f"performing interquartile range analysis")
            iq_num_outliers_all = base_fields[:] # Create a copy
            iq_num_outliers_all.extend([iq_col for iq_col in iq_num_outliers if iq_col in table.columns])
            validation_table_fields = validation_table_fields + [iq_col for iq_col in iq_num_outliers if iq_col in table.columns]
            iq_valid_col = []
            
            for col in [iq_col for iq_col in iq_num_outliers if iq_col in table.columns]:

                iq_valid = f'{col}_iq_outlier'

                outlier_fields.append(iq_valid)

                iq_dtypes = {
                    iq_valid:'sa.Boolean'
                }

                dataTypeDict = dataTypeDict | iq_dtypes

                iq_list = [iq_valid]

                iq_valid_col.append(iq_valid)

                validation_table_fields = validation_table_fields + iq_list
                if col in iq_num_outliers_all: iq_num_outliers_all = iq_num_outliers_all + iq_list

                table[col].fillna(0, inplace=True) # Added inplace=True
                first_quartile  = table[col].quantile(0.25)
                third_quartile  = table[col].quantile(0.75)
                interquartile_range = third_quartile - first_quartile
                iq_cut_off = interquartile_range * 1.5
                iq_upper = third_quartile + iq_cut_off
                iq_lower = first_quartile - iq_cut_off 
                table[iq_valid] = table.apply(lambda row: (row[col] < iq_lower) | (row[col] > iq_upper) if(np.all(pd.notnull(row[col]))) else False, axis=1) # Ensure False if null

                # SET ROW VALID TO FALSE IF VALIDATION IS TRUE (OUTLIERS ARE TRUE)
                if col not in exception_from_row_validation:
                    table[valid_row] = table.apply(lambda row: False if row[iq_valid] == True else row[valid_row], axis = 1)

            iq_df_cols = [col for col in iq_num_outliers_all if col in table.columns]
            iq_df = table[iq_df_cols]
            iq_validation_path = fd(output_directory,f"IQ_Outlier_validation_{output_file_name}.csv").file_path()       
            iq_df.to_csv(iq_validation_path, index=False, sep = "|")
            print(f"Saved IQR outlier detail to {iq_validation_path}")


            iq_freq_path = fd(output_directory,f"IQ_Outlier_Freq_Count_{output_file_name}.csv").file_path()
            iq_freq_df = frequency_report_by_column(iq_df, iq_freq_path, iq_valid_col)
            if iq_freq_df is not None and not iq_freq_df.empty:
                print(f"Saved IQR outlier frequency to {iq_freq_path}")

            # iq_freq_col = db_freq_report_by_column(iq_df, output_file_name, db_table, iq_freq_col) # Commented out
            # ch.insert_data_w_eng(iq_freq_col, 'Validation_Frequency', 'db_report') # Commented out


        print(f"creating data type dictionary") # This dictionary is minimally used now
        final_dict = {}

        # Ensure all keys in dataTypeDict are actually in table columns or created validation_table_fields
        safe_validation_table_fields = [fld for fld in validation_table_fields if fld in table.columns or fld in dataTypeDict]

        for key,value in dataTypeDict.items():
            if key in safe_validation_table_fields: # Use the safer list
                final_dict[key] = value  
                  
        index_map = {v: i for i, v in enumerate(safe_validation_table_fields)}
        # sorted_final_dict = dict(sorted(final_dict.items(), key=lambda pair: index_map[pair[0]])) # Sorting might fail if a key from final_dict is not in index_map
        # sa_sorted_final_dict = get_sqlal_dict(sorted_final_dict, safe_validation_table_fields) # Commented out, DB specific

        # report_df_cols = [col for col in safe_validation_table_fields if col in table.columns] # Ensure all columns exist in table for report_df
        # report_df = table[report_df_cols]
        # validation_table_name = f'Validation_{output_file_name}' # For DB

        print(f"Preparing full validation report for {output_file_name}")
        # ch.replace_data(report_df, validation_table_name, 'db_report', sa_sorted_final_dict) # Commented out

        # GET ROW FREQUENCY
        
        vrow_freq_path = fd(output_directory,f"Valid_Row_Freq_Count_{output_file_name}.csv").file_path()
        # Ensure validation_row_list columns exist in table
        valid_row_list_cols_in_table = [col for col in validation_row_list if col in table.columns]
        if valid_row_list_cols_in_table:
            vrow_freq_df = frequency_report_by_column(table, vrow_freq_path, valid_row_list_cols_in_table) # Use table, not report_df
            if vrow_freq_df is not None and not vrow_freq_df.empty:
                print(f"Saved valid row frequency to {vrow_freq_path}")


        # freq_valrow_col = db_freq_report_by_column(report_df, output_file_name, db_table, validation_row_list) # Commented out
        # ch.insert_data_w_eng(freq_valrow_col, 'Validation_Frequency', 'db_report') # Commented out

        # This section prepares for writing validation summary to db_report.Validation_Table
        # We will comment out the DB write and instead save the table with validation flags to a CSV.
        if not table.empty:
            validation_summary_path = os.path.join(output_directory, f"validation_summary_{output_file_name}.csv")
            table.to_csv(validation_summary_path, index=False)
            print(f"Saved validation summary with flags to {validation_summary_path}")

        # print(f"loading record level validation to db_report.Validation_Table for {output_file_name}") # Commented out
        # db_table_name = var_to_string(output_file_name) # Commented out
        # validation_table_sql_types = get_sqlal_dict(dataTypeDict,validation_table_fields) # Commented out
        # ch.replace_data(table[validation_table_fields], db_table_name, 'db_validation',validation_table_sql_types) # Commented out

        #FREQUENCY REPORT IS LAST
        print(f"Starting frequency report for {output_file_name}")
        # db_freq_report_by_column(table,conn,output_file_name, validation_table_fields) # Commented out db version
        
        # Generating frequency reports as CSVs instead
        # Use safe_validation_table_fields or a more targeted list for frequency reports
        cols_for_freq_report = [fld for fld in safe_validation_table_fields if fld in table.columns]
        for col in cols_for_freq_report: 
            if col in table.columns:
                try:
                    # The frequency_report_by_column function seems to take a path for CSV output in its modified version
                    freq_output_path = os.path.join(output_directory, f"frequency_{output_file_name}_{col}.csv")
                    freq_df_report = frequency_report_by_column(table, freq_output_path, [col]) # Pass path and col as a list
                    if freq_df_report is not None and not freq_df_report.empty:
                         print(f"Saved frequency report for column '{col}' to {freq_output_path}")
                except Exception as freq_e:
                    logging.error(f"Error generating frequency report for column {col} in {output_file_name}: {freq_e}")
            else:
                logging.warning(f"Column {col} specified for frequency report not found in {output_file_name}")


        print(f"Finished processing {output_file_name}.csv\n")
    else:
        print(f"Primary key not found for {output_file_name} in table_mapping.py, cannot run reports for this file.")
        logging.warning(f"Primary key not found for {output_file_name} in table_mapping.py, cannot run reports for this file.")

print("CSV file processing complete.")
