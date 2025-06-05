import unittest
from data_validation import Address, Country, Zip, State, Title
import numpy as np
import re
from data_cleaning import address_cleaning

'''
TestDataValidation unit tests for address cleaning to ensure they are working
as designed
'''

id_1 = '1'
Country1 = 'US'
address1_1 = '8283 N Hayden Rd, Ste 145'
address2_1 = np.NaN
address3_1 = np.NaN
city1 = 'Scottsdale'
state1 = 'AZ'
zip1 = '85258'
fullAddress1 = \
    '''
    '12641 The Company Name
    8283 N Hayden Rd, Ste 145 
    Scottsdale AZ 85258 United States'
    '''

id_2 = '2'
Country2 = 'US'
address1_2 = '4701 West Research Drive, Suite 120'
address2_2 = 'Suite 300'
address3_2 = np.NaN
city2 = 'Sioux Falls'
state2 = 'SD'
zip2 = '57107-1312'
fullAddress2 = \
    '''
    "Duck Man 4701 West Research Drive, 
    Suite 120 Suite 300 Sioux Falls SD 57107-1312"
    '''

id_3 = '3'
Country3 = 'Canada'
address1_3 = '20 Fleeceline Road'
address2_3 = np.NaN
address3_3 = np.NaN
city3 = 'Toronto'
state3 = 'Ontario'
zip3 = 'M8V 2K3'
fullAddress3 = \
    "20 Fleeceline Road, Toronto, Ontario M8V 2K3"

id_4 = '4'
Country4 = 'US'
address1_4 = '250 Williams St Nw #3000'
address2_4 = np.NaN
address3_4 = np.NaN
city4 = 'Atlanta'
state4 = 'GA'
zip4 = '30303-1042'
fullAddress4 = \
    "James Hutchinson 250 Williams St Nw #3000 Atlanta GA 30303-1042"

id_5 = '5'
Country5 = 'US'
address1_5 = 'P.O. Box 110404'
address2_5 = np.NaN
address3_5 = np.NaN
city5 = 'Durham'
state5 = 'NC'
zip5 = '27709'
fullAddress5 = \
    'Tom Livingston P.O. Box 110404 Durham NC 27709'

#TEST ADDRESS SCENARIOS    
class TestDataValidation(unittest.TestCase):

    def test_address1(self):
        test = address_cleaning(fullAddress1, address1_1, address2_1, address1_3, city1, state1, zip1, Country1)
        test = str(test[1])
        self.assertEqual(
            test,
            '[]',
            "Tests typical address has no error messages"
        ) 

    def test_address2(self):
        test = address_cleaning(fullAddress2, address1_2, address2_2, address2_2, city2, state2, zip2, Country2)
        test = str(test[1])
        self.assertEqual(
            test,
            "['Check if suite is valid']",
            "Tests when typical address doesn't parse due to multiple suites"
        )       

    def test_address3(self):
        test = address_cleaning(fullAddress3, address1_3, address2_3, address3_3, city3, state3, zip3, Country3)
        test = str(test[1])
        self.assertEqual(
            test,
            "['Country Reformatted to Code', 'State Reformatted']",
            "Tests when country and state is spelled out and not in their codes"
        )  

    def test_address4(self):
        test = address_cleaning(fullAddress4, address1_4, address2_4, address3_4, city4, state4, zip4, Country4)
        test = str(test[1])
        self.assertEqual(
            test,
            "['Building number reformat']",
            "Tests when address has building number only"
        ) 

    def test_address5(self):
        test = address_cleaning(fullAddress5, address1_5, address2_5, address3_5, city5, state5, zip5, Country5)
        test = str(test[1])
        self.assertEqual(
            test,
            "['PO Box reformat']",
            "Tests when PO Box is in an address"
        ) 

if __name__ == '__main__':
    unittest.main()
