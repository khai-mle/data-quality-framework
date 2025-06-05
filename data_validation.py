from pyzipcode import ZipCodeDatabase
import re
import phonenumbers
from email_validator import validate_email
from dictionaries import country_code, can_abbr_names, state_abbr_to_name
from urllib.parse import urlparse
import pyap
from scipy import stats
from datetime import date
import numpy as np
import logging
import pandas as pd

class Zip:
    def __init__(self, zipcode = None, country = None):
        self.country = country
        self.zipcode = zipcode
    
    def us_zip_code(self):
        zcdb = ZipCodeDatabase()
        try:
            zip_5 = self.zipcode[:5]
            zquery = zcdb[zip_5]
            test = zquery.zip
            if len(test) > 1:
                test = True
        except:
            test = False
        return test

    def us_state_by_zip(self):
        zcdb = ZipCodeDatabase()
        try:
            zip_5 = self.zipcode[:5]
            zquery = zcdb[zip_5]
            test = zquery.zip
            if len(test) > 1:
                state = zquery.state
        except:
            state = None
        return state

    def us_city_by_zip(self):
        zcdb = ZipCodeDatabase()
        try:
            zip_5 = self.zipcode[:5]
            zquery = zcdb[zip_5]
            test = zquery.zip
            if len(test) > 1:
                city = zquery.place
        except:
            city = None
        return city

    def us_dict_by_zip(self):
        zcdb = ZipCodeDatabase()
        try:
            zip_5 = self.zipcode[:5]
            zquery = zcdb[zip_5]
            test = zquery.zip
            if len(test) > 1:
                city = zquery.__dict__
        except:
            city = None
        return city

    def zip_lookup(self, place, state):
        self.place = place
        self.state = state
        zips = []
        zcdb = ZipCodeDatabase()
        zipcode = zcdb.find_zip(place=self.place,state=self.state)
        length = len(zipcode)
        for i in range(length):
            zips.append(zipcode[i].zip)
        return zips
    
    def zip_code_regex(self):
        dict_ = {\
            "US": r"^\d{5}(-\d{4})?$",\
            "AU": r'(\d{4})', \
            "BR": r"\d{5}-?\d{3}", \
            "DE": r'(\d{5})',\
            "CA": r"^[A-Z]\d[A-Z] \d[A-Z]\d$",\
            "FR": r"^\d{5}$", \
            "JP": r"^\d{3}-\d{4}$", \
            "GB": r"^[A-Z]{1,2}\d[A-Z\d]? \d[A-Z]{2}$",\
            "IN": r"^[1-9]{1}[0-9]{2}\\s{0,1}[0-9]{3}$"
            }
        try:
            pattern = dict_[self.country]
            match = re.match(pattern, self.zipcode)
            if match: 
                test = True
            else: 
                test = False
        except:
            test = False

        return test
    
    def zip_code_check(self):
        try:
            if self.country == "US":
                test = Zip(self.zipcode, self.country).us_zip_code()
            else:
                test = Zip(self.zipcode, self.country).zip_code_regex()
        except:
            test = False
        return test

    def zip_format(self):
        try:
            if self.country == "US":
                if Zip(self.zipcode, self.country).us_zip_code() == True:
                    test = self.zipcode[:5]
            else:
                if Zip(self.zipcode, self.country).zip_code_regex() == True:
                    test = self.zipcode
        except:
            test = False
        return test


class Phone:
    def __init__(self, phone, country):
        self.phone = phone
        self.country = country
    
    def clean_phone_us(self):
        if self.country == 'US':
            tel = self.phone
            tel = tel.removeprefix("+")
            tel = tel.removeprefix("1")     # remove leading +1 or 1
            tel = re.sub("[ ()-]", '', tel) # remove space, (), -
            tel = tel.strip()
        else:
            tel = self.phone
        return tel
    
    def format_phone(self):
        if self.country == 'US':
            tel = Phone.clean_phone_us(self)
            assert(len(tel[:10]) == 10)
            tel = f"({tel[:3]}) {tel[3:6]}-{tel[6:10]}"
        else:
            tel = self.phone
        return tel

    def extension(self):
        tel = Phone.clean_phone_us(self)
        num_char = len(tel)
        if self.country == 'US' and num_char > 10:
            ext = tel[10:num_char].strip()
            ext = re.sub(r'[^\w\s]', '', ext)
            ext = re.sub(r'[^\d]+', '', ext).strip()
            ext = f'Ext {ext}'
            #ext.translate(string.punctuation)
        else:
            ext = None
        return ext
    
    def phone_is_valid(self):
        try:
            Phone_Number = phonenumbers.parse(self.phone, self.country)
            valid = phonenumbers.is_valid_number(Phone_Number)
            possible = phonenumbers.is_possible_number(Phone_Number)
            test = valid and possible 
        except:
            test = False
        return test

class Email:
    def __init__(self, email):
        self.email = email

    def email_check(self):
        regex = r'\b[A-Za-z0-9.\'_%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,20}\b'
        # pass the regular expression
        # and the string into the fullmatch() method
        try:
            if(re.fullmatch(regex, self.email)):
                email_test = True
            else:
                email_test = False
        except:
            email_test = False
        return email_test
    
    def email_valid(self):
        # Check that the email address is valid. Turn on check_deliverability
        # for first-time validations like on account creation pages (but not
        # login pages).
        try:
            emailinfo = validate_email(self.email, check_deliverability=False).normalized
        # After this point, use only the normalized form of the email address,
        # especially before going to a database query.
        # use https://pypi.org/project/email-validator/ for more details
            email = emailinfo
        except:
            email = None
        return email
        

class Address:
    def __init__(self, address, company = None, city = None, country = None):
        self.address = address
        self.company = company
        self.city = city
        self.country = country

    def address_parse_field(self):
        try:
            index1 = self.address.index(self.company)
            index2 = self.address.index(self.city)
            parsed_address = self.address[index1 + len(self.company) + 1: index2]
            company_address = parsed_address.strip()
        except:
            company_address = None
        return company_address
    
    def city_check(self):
        chars_to_remove = ["'", "-"," ","\t"]
        name = self.city.strip()
        name = ''.join(x for x in name if not x in chars_to_remove)
        if name.isalpha():
            test = True
        else:
            test = False
        return test
    
    def address_parse_full(self):
        try:
            addresses = pyap.parse(self.address, country = self.country)
            parsed = addresses[0]
            dict_ = parsed.data_as_dict
        except:
            dict_ = None
        return dict_

class Country: 
    def __init__(self,  country):
        self.country = country

    def country_check(self):
        country = self.country
        try:
            test = country_code[country]
            test = True
        except:
            test = False
        return test
    
    def country_format(self):
        test = Country(self.country).country_check()
        if test == True:
            country_name = self.country
        else:
            try:
                c_upper = self.country.upper()
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

class State: 
    def __init__(self, state, country):
        self.state = state
        self.country = country

    def state_check(self):
        try:
            if self.country == 'US':
                test = state_abbr_to_name[self.state]
                test = True
            elif self.country == 'CA':
                test = can_abbr_names[self.state]
                test = True
            elif self.country not in ['US','CA']:
                test = True
            else:
                test = False
        except:
            test = False
        return test
    
    def state_name_to_code(self):
        try:
            if self.country == 'US':
                state = Title(self.state).to_title().replace('Of','of')
                s_names = sorted([name for name in state_abbr_to_name.values()])
                state_list = {v: k for k, v in state_abbr_to_name.items()}
            elif self.country == 'CA':
                state = Title(self.state).to_title().replace('And','and')
                s_names = sorted([name for name in can_abbr_names.values()])
                state_list = {v: k for k, v in can_abbr_names.items()}
            if state in s_names:
                test = state_list[self.state]
            else:
                test = None
        except:
            test = None
        return test
    
    def country_by_state(self):
        province_list = sorted([abbr for abbr in can_abbr_names.keys()])
        state_list = sorted([abbr for abbr in state_abbr_to_name.keys()])
        if self.state in state_list:
            country = 'US'
        elif self.state in province_list:
            country = 'CA'
        return country

class Title:
    def __init__(self,  string):
        self.string = string
        
    def to_title(self):
        try:
            regex = re.compile("[a-z]+('[a-z]+)?", re.I)
            title = regex.sub(lambda grp: grp.group(0)[0].upper() + grp.group(0)[1:].lower(),self.string)
        except:
            title = self.string
        return title

class Website:
    def __init__(self,  website):
        self.website = website

    def website_check(self):
        result = urlparse(self.website)
        if result.scheme and result.netloc:
            web = True  
        else:
            web = False
        return web

class UserName:
    def __init__(self,  userName):
        self.userName = userName

    def name_check(self):
        chars_to_remove = ["'", "-"," ","\t"]
        try:
            name = self.userName.strip()
            name = ''.join(x for x in name if not x in chars_to_remove)
            if name.isalpha():
                test = True
            else:
                test = False
        except:
                test = False
        return test
    
    def name_format(self):
        name = self.userName
        if UserName(name).name_check() == True:
            name = name
        else:
            name.replace('\t',' ')
            name = re.sub(r'[^A-Za-z \'-]+', '', name).strip()
        
        return name

class Outliers:
    def __init__(self,  column):
        self.column = column
    
    def z_score(self):
        self.column.fillna(0, inplace=True)
        return np.abs(stats.zscore(self.column))

    def z_score_outlier(self):
        result = False
        if self.column < -3 or self.column > 3:
            result = False
        return result
    
    def column_outlier(self, upper, lower):
        result = False
        if self.column < lower or self.column > upper:
            result = True
        return result

    def quartile_outlier(self):

        first_quartile  = float(self.column.quantile(0.25))
        third_quartile  = float(self.column.quantile(0.75))
        interquartile_range = third_quartile - first_quartile
        iq_cut_off = interquartile_range * 1.5
        iq_upper = third_quartile + iq_cut_off
        iq_lower = first_quartile - iq_cut_off 
        
        return stats

class OutliersStats:
    def __init__(self,  df):
        self.df = df

    def all_numerical_stats_to_dict(self):
        df = self.df
        stats = {}
        for col in df.select_dtypes(include='number'):
            calcs_dict = {}
            mean = float(df[col].mean())
            median = float(df[col].median())
            std = float(df[col].std())
            variance = float(df[col].var())
            first_quartile  = float(df[col].quantile(0.25))
            third_quartile  = float(df[col].quantile(0.75))
            interquartile_range = third_quartile - first_quartile
            iq_cut_off = interquartile_range * 1.5
            std_cut_off = std * 3
            calcs_dict['mean'] = mean
            calcs_dict['median'] = median
            calcs_dict['std'] = std
            calcs_dict['variance'] = variance
            calcs_dict['first_quartile']  = first_quartile
            calcs_dict['third_quartile']  = third_quartile
            calcs_dict['interquartile_range'] = interquartile_range
            calcs_dict['iq_upper'] = third_quartile + iq_cut_off
            calcs_dict['iq_lower'] = first_quartile - iq_cut_off
            calcs_dict['upper'] = mean + std_cut_off
            calcs_dict['lower'] = mean - std_cut_off
            
            stats[col] = calcs_dict
        return stats

    def date_quartile_stats_to_dict(self):
        df = self.df
        stats = {}
        for col in df.select_dtypes(include='datetime64'):
            calcs_dict = {}
            first_quartile  = float(df[col].quantile(0.25))
            third_quartile  = float(df[col].quantile(0.75))
            interquartile_range = third_quartile - first_quartile
            iq_cut_off = interquartile_range * 1.5
            calcs_dict['first_quartile']  = first_quartile
            calcs_dict['third_quartile']  = third_quartile
            calcs_dict['interquartile_range'] = interquartile_range
            calcs_dict['iq_upper'] = third_quartile + iq_cut_off
            calcs_dict['iq_lower'] = first_quartile - iq_cut_off 
            stats[col] = calcs_dict
        return stats

    def mean_stats_to_dict(self):
        df = self.df
        stats = {}
        for col in df.select_dtypes(include='number'):
            calcs_dict = {}
            mean = float(df[col].mean())
            std = float(df[col].std())
            std_cut_off = std * 3
            calcs_dict['mean'] = mean
            calcs_dict['std'] = std
            calcs_dict['upper'] = mean + std_cut_off
            calcs_dict['lower'] = mean - std_cut_off
            
            stats[col] = calcs_dict
        return stats

    def quartile_stats_to_dict(self):
        df = self.df
        stats = {}
        for col in df.select_dtypes(include='number'):
            calcs_dict = {}
            first_quartile  = float(df[col].quantile(0.25))
            third_quartile  = float(df[col].quantile(0.75))
            interquartile_range = third_quartile - first_quartile
            iq_cut_off = interquartile_range * 1.5
            calcs_dict['first_quartile']  = first_quartile
            calcs_dict['third_quartile']  = third_quartile
            calcs_dict['interquartile_range'] = third_quartile - first_quartile
            calcs_dict['iq_upper'] = third_quartile + iq_cut_off
            calcs_dict['iq_lower'] = first_quartile - iq_cut_off 
            stats[col] = calcs_dict
        return stats

class table_prep:
    def __init__(self,  df):
        self.df = df

    # THIS PREVENTS NULL ISSUE OF VOIDING NULL COUNTS WITH PREP TABLE
    def prep_file(self):
        try:
            #CONVERT ESCAPTING CHARACTERS TO SPACE
            df = self.df.replace(r'[\r\n\t\f]',' ', regex = True)
            #WILL KEEP LISTED CHARACTERS
                #[A-zÀ-ú] // accepts lowercase and uppercase characters
                #[A-zÀ-ÿ] // as above, but including letters with an umlaut (includes [ ] ^ \ × ÷)
                #[A-Za-zÀ-ÿ] // as above but not including [ ] ^ \
                #[A-Za-zÀ-ÖØ-öø-ÿ] // as above, but not including [ ] ^ \ × ÷
            df = df.replace(r'[^A-Za-z0-9 -.&,@()_%$#!*;:?></~`{}|\\=+\'\"\[\]\^]+', '', regex = True)
        except Exception as e:
            logging.error('Error at %s', 'most likely datatype issue with lambda', exc_info=e)

        return df
    
    # FOR TESTING PURPOSES ONLY
    def prep_sql_table(self, var_list):

        try:
            df = self.df

            #CONVERT ESCAPTING CHARACTERS TO SPACE
            df = df.replace(r'[\r\n\t\f]',' ', regex = True)
            #WILL KEEP LISTED CHARACTERS
                #[A-zÀ-ú] // accepts lowercase and uppercase characters
                #[A-zÀ-ÿ] // as above, but including letters with an umlaut (includes [ ] ^ \ × ÷)
                #[A-Za-zÀ-ÿ] // as above but not including [ ] ^ \
                #[A-Za-zÀ-ÖØ-öø-ÿ] // as above, but not including [ ] ^ \ × ÷
            df = df.replace(r'[^A-Za-z0-9 -.&,@()_%$#!*;:?></~`{}|\\=+\'\"\[\]\^]+', '', regex = True)
            for col in var_list:
                #STRING TO TEXT MAPPED BASED ON VARCHAR
                #REMOVE WHITESPACE AROUND ALL COLUMN AND CONVERT EMPTY
                df[col] = df[col].apply(lambda x: None if x == "" or pd.isna(x) else x)
                df[col] = df[col].apply(lambda x: x.strip() if x != None or pd.notna(x) else x)
        except Exception as e:
            logging.error('Error at %s', 'most likely datatype issue with lambda', exc_info=e)

        return df

    #THIS CAUSED A NULL COUNTS ISSUE AND SHOULD ONLY BE USED WITH TABLES WITHOUT NULL COLUMNS
    #THIS IS KEPT FOR LEGACY PURPOSES
    def prep_table(self):
        try:
            df = self.df.fillna(value=np.NaN)
            #REMOVE WHITESPACE AROUND ALL COLUMN TEXT
            df = df.apply(lambda x: x.astype(str).str.strip() if x.dtype == "object" else x)  
            #CONVERT INFINITE NUMERICAL VALUES TO NULL
            df = df.replace(np.inf, np.NaN)
            #CONVERT BLANK FIELDS TO NULL
            df = df.infer_objects(copy=False).replace(r'^\s*$', np.NaN, regex=True)
            #CONVERT ESCAPTING CHARACTERS TO SPACE
            df = df.replace(r'[\r\n\t\f]',' ', regex = True)
            #WILL KEEP LISTED CHARACTERS
                #[A-zÀ-ú] // accepts lowercase and uppercase characters
                #[A-zÀ-ÿ] // as above, but including letters with an umlaut (includes [ ] ^ \ × ÷)
                #[A-Za-zÀ-ÿ] // as above but not including [ ] ^ \
                #[A-Za-zÀ-ÖØ-öø-ÿ] // as above, but not including [ ] ^ \ × ÷
            df = df.replace(r'[^A-Za-z0-9 -.&,@()_%$#!*;:?></~`{}|\\=+\'\"\[\]\^]+', '', regex = True)
        except Exception as e:
            logging.error('Error at %s', 'most likely datatype issue with lambda', exc_info=e)

        return df
    