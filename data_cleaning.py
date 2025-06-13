from data_validation import Address, Country, Zip, State, Title, Phone
import numpy as np
import re
from file_path import FileDirectory as fd
import pandas as pd
from dictionaries import country_code, street_type_list, secondary
import sqlalchemy as sa
from fuzzy_scores import compare_score, get_score

    
def address_cleaning(fullAddress, address1, address2, address3, city, state, zip, country_code):

    ############ TEST IF FULL ADDRESS CAN BE PARSED ###############
    if pd.isnull(fullAddress):
        if pd.isnull(address3) and pd.notnull(address2):
            fulladdress_assembled = f'{address1} {address2} {city}, {state} {zip} {country_code}'
        elif pd.notnull(address3):
            fulladdress_assembled = f'{address1} {address2} {address3} {city}, {state} {zip} {country_code}'
        else:
            fulladdress_assembled = f'{address1} {city}, {state} {zip} {country_code}'
        address_dict = Address(fulladdress_assembled, country = country_code).address_parse_full()
    else:        
        address_dict = Address(fullAddress, country = country_code).address_parse_full()
    

    #### CHECK FOR SUITES IN ADDRESS ####
    # GIVEN PROFILE I DECIDED AGAINST REGEX
    # IN CASE OF APARTMENT OR NUMBER ISSUES 
    #^(\d+) ?([A-Za-z](?= ))? (.*?) ([^ ]+?) ?((?<= )APT)? ?((?<= )\d*)?$
    try:
        address_check = address1.lower()

        #suite_pattern =r'[Ss][Uu][Ii][Tt][Ee]\ |[Ss][Tt][Ee]\.?'
        #regex space matching is not working
        #suite_test = re.search(suite_pattern, address_check)

        suite_list = ['suite','ste']
        suite_check = any(map(address_check.__contains__, suite_list))

        # CHECK FOR PO BOX USED REGEX DUE TO PERMUTATIONS
        pattern = r"(?:[Pp]\.?\ ?[Oo]\.?\ [Bb][Oo][Xx]\ \d+|\s[Pp]\.?\ ?[Oo]\.?\ [Bb][Oo][Xx]\ \d+)"
        po_box_test = re.search(pattern, address1)
        po_box_test_2 = re.search(pattern, address2)

        if po_box_test:
            address_test = address_check.replace(' ','').lower()
            address1 = f'PO Box {address_test.split("box",1)[1]}'

        # CHECK IF NUMBERED ADDRESS IS IN ADDRESS    
        address_num_check = any(map(address1.__contains__, '#'))
        comma_check = any(map(address1.__contains__, ','))
    except:
        suite_check = False
        po_box_test = False
        address_num_check = False
        comma_check = False

    
    #### SET BAD LIST FOR WHEN ADDRESS OBJECTS FAIL TESTS ####
    bad_data = []
    
    if address_dict == None:
        # Parser can't handle building number convention '#'
        # created check and moves everything after number to suite
        if address_num_check:
            street = f'{address1.split("#",1)[0]}'
            street = street.strip()
            p_suite = f'#{address1.split("#",1)[1]}'
            if address2 in [np.nan, None]:
                suite = p_suite
            else:
                suite = f'{address2} {p_suite}'
        elif comma_check:
            street = f'{address1.split(",",1)[0]}'
            street = street.strip()
            p_suite = f'#{address1.split(",",1)[1]}'
            if address2 in [np.nan, None]:
                suite = p_suite
            else:
                suite = f'{p_suite} {address2}'            
        else:
            street = address1
            suite = address2
        city = city
        state = state
        postal_code = zip
        country = country_code
    else:
        street = f'{address_dict["street_number"]} {address_dict["street_name"]} {address_dict["street_type"]}'
        suite = address_dict['occupancy'] or address_dict['building_id'] 
        city = address_dict['city']
        state = address_dict['region1']
        postal_code = address_dict['postal_code']
        country = address_dict['country_id'] or country_code

    # Run Country Checks
    if street == None:
        bad_data.append('Missing address')
    try:
        check_address = re.sub(r'[^A-Za-z \'-]+', '', street).strip().lower()
        lower_street_list = list(map(lambda x: x.lower(), street_type_list))
        lower_suite_list = list(map(lambda x: x.lower(), secondary))

        street_value_counts = sum(1 for word in check_address.split() if word in lower_street_list)
        suite_value_counts = sum(1 for word in check_address.split() if word in lower_suite_list)
    except:
        street_value_counts = 1
        suite_value_counts = 1

    if street_value_counts > 2  or suite_value_counts >= 1:
        bad_data.append('possible parsing issue')

    try:
        country_check = Country(country).country_check()
    except:
        country = None
    
    if country == None:
        bad_data.append('Missing Country')

    if country_check == False and pd.notnull(country):
        country_format = Country(country).country_format()
        isCountry = Country(country_format).country_check()
        if isCountry == True:
            bad_data.append('Country Reformatted to Code')
            country = country_format
        else:
            bad_data.append('Invalid Country')
            country = None
    elif country_check == False and pd.notnull(state):
        try:
            country = State(state, country).country_by_state()
            bad_data.append('Country Reformatted by State')
        except:
            country = None
    
    
    # Run Zip Checks
    try:
        zip_check = Zip(postal_code, country).zip_code_check()
    except:
        zip_check = None

    if pd.isnull(postal_code) or postal_code == None or zip_check == False:
        bad_data.append('Bad Postal Code')
    
    if zip_check == True:
        check_city = Zip(postal_code, country).us_city_by_zip()
        check_state = Zip(postal_code, country).us_state_by_zip()
    else:
        check_city = None
        check_state = None
    
    # Run State Checks
    try:
        state_check = State(state, country).state_check()
    except:
        state = None

    if pd.isnull(state) and pd.isnull(zip):
        bad_data.append('Missing State / Zip')

    elif pd.isnull(state) and zip != None:
        bad_data.append('Reformatting State Using Zip')
        state = check_state

    if state_check == False and country != None:
        state_format = State(state, country).state_name_to_code()
        isState = State(state_format, country).state_check()
        if isState == True:
            bad_data.append('State Reformatted')
            state = state_format
        else:
            bad_data.append('Invalid State')
            country = None      

    try:
        city_check = Address(None,None,city,None).city_check()
    except:
        city_check = False
    
    if pd.isnull(city) and pd.isnull(zip):
        bad_data.append('Missing City')

    if city_check == False and city != None:
        bad_data.append('City is not alpha')

    elif city == None and check_city != None:
        city = check_city
        bad_data.append('Reformatted City with Zip')

    elif city != None and state != None and postal_code != None and check_city != None:
        city_text = str(city).lower()
        check_city_text = str(check_city).lower()
        if check_city_text != city_text:
            city = check_city
            bad_data.append('City spelling reformatted')
    
    if isinstance(city, str) and len(city) > 3:
        city = Title(city).to_title().strip()

    if address_dict:
        s_street = str(street) if pd.notnull(street) else ""
        s_suite = str(suite) if pd.notnull(suite) else ""
        s_city = str(city) if pd.notnull(city) else ""
        s_state = str(state) if pd.notnull(state) else ""
        s_postal_code = str(postal_code) if pd.notnull(postal_code) else ""
        s_country = str(country_code) if pd.notnull(country_code) else ""
        s_address1 = str(address1) if pd.notnull(address1) else ""

        if pd.notnull(suite):
            address_reassembled = f'{s_street} {s_suite} {s_city}, {s_state} {s_postal_code} {s_country}'
        else:
            address_reassembled = f'{s_address1} {s_city}, {s_state} {s_postal_code} {s_country}'
    
    if None not in (street, city, state, postal_code):
        s_street = str(street) if pd.notnull(street) else ""
        s_city = str(city) if pd.notnull(city) else ""
        s_state = str(state) if pd.notnull(state) else ""
        s_postal_code = str(postal_code)[:5] if pd.notnull(postal_code) and isinstance(postal_code, str) else (str(postal_code)[:5] if pd.notnull(postal_code) else "")

        formatted_full_address = f'{s_street} {s_city} {s_state} {s_postal_code}'
        formatted_full_address = re.sub(r'[^A-Za-z0-9 ]+', '', formatted_full_address).strip().lower()
    else: 
        formatted_full_address = None
        
    # TEST WHEN ADDRESS PARSER FAILED WITH DATA THAT HAS BEEN REVIEWED
    try:
        test_new_address = Address(address_reassembled, country = country).address_parse_full()
        #full_address = test_new_address
    except:
        test_new_address = None
    
    # PROVIDES MESSAGE ON BAD DATA
    if test_new_address == None and suite_check and pd.notnull(address2):
        # SOME ADDRESSES HAVE TWO SUITES OR MULTIPLE DESIGNATIONS WHICH ERROR
        bad_data.append('Check if suite is valid')
    elif test_new_address == None and po_box_test == None and address_num_check == None:
        # BLANKET REASON FOR PARSING ERROR
            bad_data.append('Not capable of parsing')
    elif test_new_address == None and po_box_test != None:
        # PARSER WILL ERROR WITH PO BOXES
            bad_data.append('PO Box reformat')
    elif test_new_address == None and po_box_test_2 != None:
        # PARSER WILL ERROR WITH PO BOXES
            bad_data.append('Check PO Box in Suite')
    elif test_new_address == None and address_num_check == True:
        # PARSER WILL ERROR ON BUILDING #
            bad_data.append('Building number reformat')

    # SET FLAG WHEN DATA HAS BEEN MODIFIED OR UNPARSED
    if len(bad_data) == 0:
        validate = True
    else:
        validate = False

    # SETS OBJECTS TO BE CREATED AS FIELD IN DATAFRAME USING THE APPLY METHOD IN THE DATAFRAME
    #return validate, bad_data, street, suite, city, state, postal_code, country
    return validate, bad_data, street, suite, city, state, postal_code, country,formatted_full_address


def find_duplicates(df, file_name, columns, output_directory, primary_key, table):
    # USE dictionaries.py TO ESTABLISH WHAT FIELDS NEED DEDUPLICATION ANALYSIS
    # THIS IS TAXING AND SHOULD HAVE REQUIREMENTS
    df2 = pd.DataFrame(columns=['table_primary_key','field_value','table_column','table_name']) 
    for c in columns:
        df_columns = primary_key + [c]
        # EXTRACT DUPLICATED FROM ALL REQUIRED FIELDS
        duplicates = fd(output_directory,f"Exact_Duplicates_{file_name}_{c.upper()}.csv").file_path()
        dups = df[df.duplicated(c, keep=False) == True]
        dups = dups.sort_values(by=[c])
        dups = dups[df_columns]
        dups = dups[(dups[c].notnull())]
        dups.to_csv(duplicates, index=False)

        dups_db = dups
        dups_db['table_column'] = str(c)
        dups_db['table_name'] = var_to_string(table)
        dups_db.columns = ['table_primary_key','field_value','table_column','table_name']
        dups_df = pd.DataFrame(dups_db)
        df2 = pd.concat([df2, dups_df], ignore_index=True, axis = 0)

    return df2


def find_nulls(df, output_file_name, output_directory, columns):
    # IF NONE, ANALYZES ALL FIELDS, BUT WILL GET LARGE REPORTS
    file_name = fd(output_directory,f"Null_Values_{output_file_name}.csv").file_path()
    if columns == None:
        null_fields = df
    else:
    # CHECK ONLY REQUIRED FIELDS FROM dictionaries.py FILE
        null_fields = df[columns]
    # SUM NULLS ACROSS ALL FIELDS
    nulls = null_fields[null_fields.isnull().any(axis=1)]
    # PRINT FILE
    nulls.to_csv(file_name, index=False)
    return nulls


def frequency_reports(df, file_name):
    with open(file_name, 'w', encoding="utf-8") as f:
        for col in df.select_dtypes(include=object):
            # GET THE COUNTS AND PERCENTAGES OF EACH VALUE IN A COLUMN
            counts = df[col].value_counts(dropna=False)
            percent = df[col].value_counts(normalize=True, dropna=False).mul(100).round(2).astype(str) + '%'
            # RESET DATAFRAME AND INDEXS FOR COUNTS AND PERCENTAGES
            df_value_counts = pd.DataFrame(counts)
            df_value_counts = df_value_counts.reset_index()

            df_value_percent = pd.DataFrame(percent)
            df_value_percent = df_value_percent.reset_index()

            # COMBINE DATAFRAMES TOGETHER
            report = pd.concat([df_value_counts, df_value_percent['proportion']], axis=1)

            # TEST ROW SIZE OF DATAFRAME AFTER COUNTS TO REDUCE SIZE OF REPORTS
            shape = int(report.shape[0])
            # ALLOW FOR 100 ROWS OR LESS OF COUNTS
            if shape > 50:
                report = report.head(50)
            else:
                report
            # CREATE COLUMN HEADERS BASED ON COLUMN NAME
            report.columns = [f'{col.lower()}_categories', f'{col.lower()}_counts', f'{col.lower()}_percent']
            # SET THE PROPERTIES OF THE THE REPORT TO ENSURE THE TEXT ALIGN IN A SANE WAY
            report = report.style.hide(axis='index').set_properties(**{'text-align': 'left'}).to_string(delimiter='|')
            # THIS IS KEPT INCASE THERE ARE MORE REQUIREMENTS, CAN BE REMOVED ONCE MATURE
            final_report = report

            # PRINT REPORT TO FILE
            f.write(f"Category Analysis: {col}")
            f.write("\n")
            f.write("-----------------")
            f.write("\n")
            f.write(final_report)
            f.write("\n\n")


def db_freq_report(df, file_name, table_name): 
    
    final_report = pd.DataFrame()
    for col in df.select_dtypes(include=object):
        # GET THE COUNTS AND PERCENTAGES OF EACH VALUE IN A COLUMN
        counts = df[col].value_counts(dropna=False)
        percent = df[col].value_counts(normalize=True, dropna=False)
        # RESET DATAFRAME AND INDEXS FOR COUNTS AND PERCENTAGES
        df_value_counts = pd.DataFrame(counts)
        df_value_counts = df_value_counts.reset_index()

        df_value_percent = pd.DataFrame(percent)
        df_value_percent = df_value_percent.reset_index()

        # COMBINE DATAFRAMES TOGETHER
        report = pd.concat([df_value_counts, df_value_percent['proportion']], axis=1)

        # TEST ROW SIZE OF DATAFRAME AFTER COUNTS TO REDUCE SIZE OF REPORTS
        shape = int(report.shape[0])
        # ALLOW FOR 100 ROWS OR LESS OF COUNTS
        if shape > 50:
            report = report.head(50)
        else:
            report

        report.columns = ['table_values', 'value_counts', 'value_percent']
        # CREATE COLUMN HEADERS BASED ON COLUMN NAME
        report['table_column'] = col
        report['table_name'] = var_to_string(table_name)

        final_report = pd.concat([final_report, report], axis=0, ignore_index=True)

    return final_report

def frequency_report_by_column(df, file_name, columns):
    with open(file_name, 'w', encoding="utf-8") as f:
        for col in df[columns]:
            # GET THE COUNTS AND PERCENTAGES OF EACH VALUE IN A COLUMN
            counts = df[col].value_counts(dropna=False)
            percent = df[col].value_counts(normalize=True, dropna=False).mul(100).round(2).astype(str) + '%'
            # RESET DATAFRAME AND INDEXS FOR COUNTS AND PERCENTAGES
            df_value_counts = pd.DataFrame(counts)
            df_value_counts = df_value_counts.reset_index()

            df_value_percent = pd.DataFrame(percent)
            df_value_percent = df_value_percent.reset_index()

            # COMBINE DATAFRAMES TOGETHER
            report = pd.concat([df_value_counts, df_value_percent['proportion']], axis=1)

            # TEST ROW SIZE OF DATAFRAME AFTER COUNTS TO REDUCE SIZE OF REPORTS
            shape = int(report.shape[0])
            # ALLOW FOR 100 ROWS OR LESS OF COUNTS
            if shape > 50:
                report = report.head(50)
            else:
                report
            # CREATE COLUMN HEADERS BASED ON COLUMN NAME
            report.columns = [f'{col.lower()}_categories', f'{col.lower()}_counts', f'{col.lower()}_percent']
            # SET THE PROPERTIES OF THE THE REPORT TO ENSURE THE TEXT ALIGN IN A SANE WAY
            report = report.style.hide(axis='index').set_properties(**{'text-align': 'left'}).to_string(delimiter='|')
            # THIS IS KEPT INCASE THERE ARE MORE REQUIREMENTS, CAN BE REMOVED ONCE MATURE
            final_report = report

            # PRINT REPORT TO FILE
            f.write(f"Category Analysis: {col}")
            f.write("\n")
            f.write("-----------------")
            f.write("\n")
            f.write(final_report)
            f.write("\n\n")

def describe(df, filename):
    describe = df.describe(include='all')
    describe.to_csv(filename,mode='w',index_label = 'Categories')

    # ADDITIONAL ROWS
    table_dtypes = df.dtypes
    table_nulls = df.isna().sum()
    shape = int(df.shape[0])
    columns = int(df.shape[1])
    #total = df.sum()

    # DELIMITER
    delimiter = ","

    # ADD DATA TYPES
    dtypes_list = ['data_types']
    for d in table_dtypes:
        dtypes_list.append(str(d))

    dtype_list = [str(element) for element in dtypes_list]
    dtype_string = delimiter.join(dtype_list)
    
    # ADD NULL COUNTS
    nulls_list = [f'\nnull_counts']
    for n in table_nulls:
        nulls_list.append(str(n))
    null_list = [str(element) for element in nulls_list]
    null_string = delimiter.join(null_list)

    # ADD NULL COUNTS
    counts_list = [f'\ntotal_rows']
    for n in range(columns):
        counts_list.append(str(shape))
    counts_list = [str(element) for element in counts_list]
    count_string = delimiter.join(counts_list)

    
    with open(filename, 'a') as f:
        f.write(dtype_string)
        f.write(null_string)
        f.write(count_string)
        f.close()

def describe_to_db(df, table, filename):

    describe = df.describe(include='all')
    describe_df = pd.DataFrame(describe).reset_index().T
    # ADDITIONAL ROWS
    table_dtypes = df.copy()
    table_dtypes = pd.DataFrame(table_dtypes.dtypes).copy()
    table_nulls = df.copy()
    table_nulls = pd.DataFrame(table_nulls.isna().sum()).reset_index().copy()
    shape = int(df.shape[0])

    d_report = pd.concat([describe_df, table_dtypes], axis=1, ignore_index=True).reset_index()

    d_report = d_report.drop(0).reset_index(drop=True)
    d_report = d_report.copy()
    n_report = pd.concat([d_report, table_nulls[0]], axis=1, ignore_index=True)
    n_report['total_rows'] = shape
    n_report['table_name'] = var_to_string(table)
    try:
        n_report.columns = ['column_name','counts','unique_values','top_values','freq','mean','min','first_iq','middle_iq','third_iq','max','std','data_types','nulls','total_rows','table_name']
        report = n_report.reindex(['table_name','column_name','data_types','counts','unique_values','top_values','freq','mean','min_value','first_iq','middle_iq','third_iq','max_value','std','nulls','total_rows'], axis = 1)
    except:
        n_report.columns = ['column','counts','unique_values','top_values','freq','data_types','nulls','total_rows','table_name']
        report = n_report.reindex(['table_name','column','data_types','counts','unique','top','freq','nulls','total_rows'], axis = 1)
    db_report = report.astype(str)
    #db_report = report.replace({np.nan: None})
    report.to_csv(filename, index=False)
    return db_report
     
    #return report


def var_to_string(variable):
    chars_to_remove = ["'", "[","]"]
    value = str(variable).strip()
    value = ''.join(x for x in value if not x in chars_to_remove)
    return value


def country_check(country):
    try:
        test = country_code[country]
        test = True
    except:
        test = False
    return test

def country_format(country):
    test = country_check(country)
    if test == True:
        country_name = country
    elif test == False and country != None:
        try:
            c_upper = country.upper()
        except:
            c_upper = None
        c_names = sorted([name for name in country_code.values()])
        country_list = {v: k for k, v in country_code.items()}
        try:
            if c_upper in c_names:
                country_name = country_list[c_upper]
            else:
                country_name = None
        except:
            country_name = None
    return country_name

def phone_cleaning(phone, country):
    country = Country(country).country_format()
    
    if country == 'US':
        try:
            has_plus = phone[:1]
            phone_no_space = phone.replace(" ","")
            phone_2 = phone_no_space[:2]
            phone_format = Phone(phone,country).format_phone()
            phone_is_valid = Phone(phone_format,country).phone_is_valid()
        except:
            phone_format = phone
            phone_is_valid = False
            has_plus = None
    else:
        phone_format = phone
        phone_is_valid = False
        has_plus = None
        
    if has_plus == "+" and phone_2 != "+1":
        phone_is_valid = False

    if phone_is_valid:
        extension = Phone(phone,country).extension()
    else:
        extension = None
    return phone_is_valid, phone_format, extension

def get_sqlal_dict(sql_dict, fields):
    dict_ = dict((k, sql_dict[k]) for k in fields)
    for key in dict_:
        dict_[key] = eval(dict_[key])
    return dict_

def db_freq_report_by_column(df, file_name, table_name, columns): 
    
    final_report = pd.DataFrame()
    for col in columns:
        # GET THE COUNTS AND PERCENTAGES OF EACH VALUE IN A COLUMN
        counts = df[col].value_counts(dropna=False)
        percent = df[col].value_counts(normalize=True, dropna=False)
        # RESET DATAFRAME AND INDEXS FOR COUNTS AND PERCENTAGES
        df_value_counts = pd.DataFrame(counts)
        df_value_counts = df_value_counts.reset_index()

        df_value_percent = pd.DataFrame(percent)
        df_value_percent = df_value_percent.reset_index()

        # COMBINE DATAFRAMES TOGETHER
        report = pd.concat([df_value_counts, df_value_percent['proportion']], axis=1)

        # TEST ROW SIZE OF DATAFRAME AFTER COUNTS TO REDUCE SIZE OF REPORTS
        shape = int(report.shape[0])
        # ALLOW FOR 100 ROWS OR LESS OF COUNTS
        if shape > 50:
            report = report.head(50)
        else:
            report

        report.columns = ['table_values', 'value_counts', 'value_percent']
        # CREATE COLUMN HEADERS BASED ON COLUMN NAME
        report['table_column'] = col
        report['table_name'] = var_to_string(table_name)

        final_report = pd.concat([final_report, report], axis=0, ignore_index=True)

    return final_report

def find_fuzzy_matches(df, file_name, columns, output_directory, table, threshold):
    # USE dictionaries.py TO ESTABLISH WHAT FIELDS NEED DEDUPLICATION ANALYSIS
    # THIS IS TAXING AND SHOULD HAVE REQUIREMENTS
    final_report = pd.DataFrame()
    for c in columns:
        csv_name = f'Fuzzy_matching_{file_name}_{c}'
        csv_file = fd(output_directory,f"{csv_name}.csv").file_path()
        unique_values = df[c].unique().tolist()
        scores = compare_score(unique_values, threshold)
        scores.to_csv(csv_file, index=False)

    return scores

def multi_fuzzy_matches(df, file_name, columns, output_directory, table, threshold):
    # USE dictionaries.py TO ESTABLISH WHAT FIELDS NEED DEDUPLICATION ANALYSIS
    # THIS IS TAXING AND SHOULD HAVE REQUIREMENTS
    final_report = pd.DataFrame()
    for c in columns:
        csv_name = f'Fuzzy_matching_{file_name}_{c}'
        csv_file_sort = fd(output_directory,f"{csv_name}_sort_ratio.csv").file_path()
        csv_file_set = fd(output_directory,f"{csv_name}_set_ratio.csv").file_path()
        unique_values = df[c].unique().tolist()

        sort_scores = get_score(unique_values, 'fuzz.token_sort_ratio', threshold)
        sort_scores.to_csv(csv_file_sort, index=False)

        set_scores = get_score(unique_values, 'fuzz.token_set_ratio', threshold)
        set_scores.to_csv(csv_file_set, index=False)

    #return scores