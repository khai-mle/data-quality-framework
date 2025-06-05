import unittest
from data_validation import Zip, Phone, Email, Address, Country,State, Website, UserName, Title

'''
TestDataValidation unit tests call all classes from data_validation and ensure they are working
as desi
'''
    #VARIABLES FOR TESTING
Country1 = 'US'
Country2 = 'CA'
Country3 = 'GB'
Country4 = 'United States'
Phone1 = '1 978 877-3200, ext. 456'
Phone2 = '(000) 000-0000, ext. 456'
Phone3 = '(226) 791-9215'
Website1 = 'https://www.google.com/'
UserName1 = "Micky Mouse-O'Maley's"
UserName2 = "Mickey Mouse"
UserName3 = "Mickey Mouse 4"
fullAddress1 = \
    '''
    '12641 The Company Name
    8283 N Hayden Rd, Ste 145 
    Scottsdale AZ 85258 United States'
    '''
fullAddress2 = \
    '''
    "Duck Man 4701 West Research Drive, 
    Suite 120 Suite 300 Sioux Falls SD 57107-1312"
    '''
fullAddress3 = \
    "20 Fleeceline Road, Toronto, Ontario M8V 2K3"
zipcode1 = '01824'
zipcode2 = '8124'
zipcode3 = 'L6S 0A0'
city1 = 'Chelmsford'
state1 = 'MA'
state2 = 'Massachusetts'
email1 = 'mickey.mouse@gmail.com'
email2 = 'mickey.mousegmail.com'

class TestDataValidation(unittest.TestCase):

    #PHONE TESTS
    def test_phone_is_removing_spaces(self):
        self.assertEqual(\
            Phone(Phone1, Country1).clean_phone_us(), \
            '9788773200,ext.456', \
            "Tests if it removed prefix and spaces: 9788773288,ext.456"\
        )

    def test_us_phone_is_reformated(self):
        self.assertEqual(\
            Phone(Phone1, Country1).format_phone(), \
            '(978) 877-3200', \
            "Tests if US phone is reformatted: (978) 877-3200"\
        )

    def test_us_phone_ext_is_reformated(self):
        self.assertEqual(\
            Phone(Phone1, Country1).extension(), \
            'ext456', \
            "Tests if US phone extension is reformatted: ext456"\
        )

    def test_us_phone_is_valid(self):
        self.assertEqual(\
            Phone(Phone1, Country1).phone_is_valid(), \
            True, \
            "Tests if US phone is valid: returns boolean True"\
        )

    def test_us_phone_is_not_valid(self):
        self.assertEqual(\
            Phone(Phone2, Country1).phone_is_valid(), \
            False, \
            "Tests if US phone is not valid: returns boolean False"\
        )

    def test_ca_phone_is_reformated(self):
        self.assertEqual(\
            Phone(Phone3, Country2).format_phone(), \
            '(226) 791-9215', \
            "Tests if CA phone is not reformatted: 1-416-979-996"\
        )

    def test_ca_phone_is_valid(self):
        self.assertEqual(\
            Phone(Phone3, Country2).phone_is_valid(), \
            True, \
            "Tests if US phone is valid: returns boolean True"\
        )

    # WEBSITE TESTS
    def test_known_web_site(self):
        self.assertEqual(\
            Website(Website1).website_check(), \
            True, \
            "Tests if Website is valid: returns boolean True"\
        ) 

    # USERNAME TESTS
    def test_user_name_success(self):
        self.assertEqual(\
            UserName(UserName1).name_check(), \
            True, \
            "Tests if username is valid: returns boolean True"\
        )  

    def test_user_name_fail(self):
        self.assertEqual(\
            UserName(UserName3).name_check(), \
            False, \
            "Tests if username is valid: returns boolean True"\
        )    

    def test_user_name_format(self):
        self.assertEqual(\
            UserName(UserName1).name_format(), \
            'Mickey Mouse', \
            "Tests if User Name is valid: returns name without tab"\
        )  

    def test_user_name_format(self):
        self.assertEqual(\
            UserName(UserName2).name_format(), \
            'Mickey Mouse', \
            "Tests if User Name is valid: returns name without tab"\
        )    

    def test_user_name_format_invalid(self):
        self.assertEqual(\
            UserName(UserName3).name_format(), \
            'Mickey Mouse', \
            "Tests if User Name is valid: returns name without tab"\
        ) 
        
    # TITLE
    def test_title(self):
        self.assertEqual(\
            Title('FunkY TiTLE').to_title(), \
            'Funky Title', \
            "Tests if Title returns: Funky Title"\
        )  

    # ADDRESS TESTS
    def test_address_pyap_validator(self):
        dict_ = Address(fullAddress1, country = Country1).address_parse_full()
        test = dict_['postal_code']
        self.assertEqual(\
            test, \
            '85258', \
            "Tests if pyap validator returns dictionary: zip code 85258"\
        )

    def test_address_pyap_validator2(self):
        test = Address(fullAddress2, country = Country1).address_parse_full()
        self.assertEqual(\
            test, \
            None, \
            "Tests if pyap validator bad address returns: None"\
        ) 

    def test_address_pyap_validator3(self):
        dict_ = Address(fullAddress3, country=Country2).address_parse_full()
        test = dict_['postal_code']
        self.assertEqual(\
            test, \
            'M8V 2K3', \
            "Tests if pyap validator good CA address returns postal code: M8V 2K3"\
        ) 

    # COUNTRY FIELD TESTS
    def test_country_check_true(self):
        self.assertEqual(\
            Country(Country1).country_check(), \
            True, \
            "Tests if country check returns: True"\
        )

    def test_country_check_false(self):
        self.assertEqual(\
            Country(Country4).country_check(), \
            False, \
            "Tests if country check returns: False"\
        )

    def test_country_format(self):
        self.assertEqual(\
            Country(Country4).country_format(), \
            'US', \
            "Tests if country format returns US from United States: US"\
        ) 

    # POSTAL / ZIP CODE TESTS
    def test_us_zip_code_valid(self):
        self.assertEqual(\
            Zip(zipcode1, Country1).us_zip_code(), \
            True, \
            "Tests if zipcode is valid for us address returns: True"\
        )

    def test_us_zip_code_invalid(self):
        self.assertEqual(\
            Zip(zipcode2, Country1).us_zip_code(), \
            False, \
            "Tests if zipcode is invalid for us bad address returns: False"\
        )

    def test_us_return_state(self):
        self.assertEqual(\
            Zip(zipcode1, Country1).us_state_by_zip(), \
            "MA", \
            "Tests if state shows up for zipcode1: MA"\
        ) 

    def test_us_city_by_zip(self):
        self.assertEqual(\
            Zip(zipcode1, Country1).us_city_by_zip(), \
            "Chelmsford", \
            "Tests if city shows up for zipcode1: Chelmsford"\
        )

    def test_us_dict_by_zip(self):
        dict_ = Zip(zipcode1, Country1).us_dict_by_zip()
        test = dict_['zip']
        self.assertEqual(\
            test, \
            '01824', \
            "Tests if dictionary of zip info shows up for zipcode1: shows dictionary"\
        )

    def test_zip_lookup(self):
        dict_ = Zip(None, None).zip_lookup(city1, state1)
        list_ = '01824'
        self.assertEqual(\
            dict_, \
            [list_], \
            "Tests if lookup of city1, state1 returns zip code: 01824"\
        )

    def test_zip_code_regex(self):
        self.assertEqual(\
            Zip(zipcode3,Country2).zip_code_regex(), \
            True, \
            "Tests if lookup of CA postal code returns: True"\
        )

    def test_zip_code_check(self):
        self.assertEqual(\
            Zip(zipcode3,Country2).zip_code_check(), \
            True, \
            "Tests if lookup of CA postal code returns: True"\
        )

    def test_zip_format(self):
        self.assertEqual(\
            Zip(zipcode3,Country2).zip_format(), \
            'L6S 0A0', \
            "Tests if lookup of CA postal code returns: L6S 0A0"\
        ) 

    # EMAIL TESTS
    def test_email_check(self):
        self.assertEqual(\
            Email(email1).email_check(), \
            True, \
            "Tests if email1 meets regex and returns: True"\
        )

    def test_email_check(self):
        self.assertEqual(\
            Email(email1).email_valid(), \
            'mickey.mouse@gmail.com', \
            "Tests if email1 is valid and returns formatted email: mickey.mouse@gmail.com"\
        )

    # STATE TESTS
    def test_state_check(self):
        self.assertEqual(\
            State(state1, Country1).state_check(), \
            True, \
            "Tests if us state code is valid: True"\
        )

    def test_state_name_to_code(self):
        self.assertEqual(\
            State(state2, Country1).state_name_to_code(), \
            'MA', \
            "Tests if us state code is found from state name: MA"\
        )

if __name__ == '__main__':
    unittest.main()

