from column_mapping \
import \
mapping_template, gp_CustomerAddresses,gp_CustomerMaster,\
tpdb_contact_email,tpdb_contact_phone,tpdb_contact_address, \
tpdb_contact_account_link, tpdb_contact,tpdb_company_status, \
tpdb_company_source, tpdb_company_accting_mapping, \
tpdb_company, snow_contact, snow_company, snow_account, ns_customers, \
ns_contacts, tpdb_company_source_sys_type, customer_cols, mdm_customer__active, \
mdm_contact__active, mdm_customer__active_view, mdm_contact__active_view, \
non_closedwon_address_phone_email_view, non_closedwon_addresses_view,\
hubspot, logistics_dqpilot_contact, logistics_dqpilot_account

import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Debug imports
logging.debug("Checking imported configurations:")
logging.debug(f"logistics_dqpilot_account configuration: {logistics_dqpilot_account}")

tables = {
    "template":mapping_template,
    "customer":customer_cols,
    "GreatPlains_Customer_Addresses":gp_CustomerAddresses,
    "GreatPlains_Customer_Master": gp_CustomerMaster,
    "NetSuite2_Contacts":ns_contacts,
    "NetSuite2_Customers": ns_customers,
    "Snow_Company": snow_company,
    "Snow_Contact": snow_contact,
    "Snow_Account": snow_account,
    "TPDB_Company_Accounting_Mapping": tpdb_company_accting_mapping,
    "TPDB_Company_Source_System_Type": tpdb_company_source_sys_type,
    "TPDB_Company_Source": tpdb_company_source,
    "TPDB_Company_Status": tpdb_company_status,
    "TPDB_Company": tpdb_company,
    "TPDB_Contact_Account_Link": tpdb_contact_account_link,
    "TPDB_Contact_Address": tpdb_contact_address,
    "TPDB_Contact_Email": tpdb_contact_email,
    "TPDB_Contact_Phone": tpdb_contact_phone,
    "TPDB_Contact": tpdb_contact,
    "MDM_Contact__Active": mdm_contact__active,
    "MDM_Customer__Active":mdm_customer__active,
    "MDM_Contact__Active_View": mdm_contact__active_view,
    "MDM_Customer__Active_View":mdm_customer__active_view,
    "Non_ClosedWon_Address_Phone_and_Email_View":non_closedwon_address_phone_email_view,
    "Non_Customer_Addresses_View": non_closedwon_addresses_view,
    "Hubspot":hubspot,
    "Logistics_DQPilot_CONTACT": logistics_dqpilot_contact,
    "LogisticsDQPilotACCOUNT": logistics_dqpilot_account
    }

# Debug tables
logging.debug("Tables after configuration:")
for table_name, config in tables.items():
    logging.debug(f"  - {table_name}")

ods_tables = {
    "template":mapping_template,
    "customer":customer_cols,
    "GreatPlains_Customer_Addresses":"tbl__gpcache__CustomerAddresses",
    "GreatPlains_Customer_Master": "tbl__gpcache__CustomerMaster",
    "NetSuite2_Contacts":"tbl__NS2__CONTACT",
    "NetSuite2_Customers": "tbl__NS2__CUSTOMER",
    "Snow_Company": "tbl__Company",
    "Snow_Contact": "tbl__Contact2",
    "Snow_Account": "tbl__Account",
    "TPDB_Company_Accounting_Mapping": "tbl__dcim__CompanyAccountingMapping",
    "TPDB_Company_Source_System_Type": "tbl__dcim__CompanySourceSystemType",
    "TPDB_Company_Source": "tbl__dcim__CompanySource",
    "TPDB_Company_Status": "tbl__dcim__CompanyStatus",
    "TPDB_Company": "tbl__dcim__Company",
    "TPDB_Contact_Account_Link": "tbl__dcim__ContactAccountLink",
    "TPDB_Contact_Address": "tbl__dcim__ContactAddress",
    "TPDB_Contact_Email": "tbl__dcim__ContactEmail",
    "TPDB_Contact_Phone": "tbl__dcim__ContactPhone",
    "TPDB_Contact": "tbl__dcim__Contact",
    "MDM_Contact__Active":"tbl__TSR_Cleanup_Feeder__MDM_Contact__Active",
    "MDM_Customer__Active":"tbl__TSR_Cleanup_Feeder__MDM_Customer__Active",
    "MDM_Contact__Active_View":"vw__TSR_Cleanup_Feeder__MDM_Contact__Active",
    "MDM_Customer__Active_View":"vw__TSR_Cleanup_Feeder__MDM_Customer__Active",
    "Non_ClosedWon_Address_Phone_and_Email_View":"vw__TSR_Cleanup_Feeder__Non_ClosedWon_Address_Phone_and_Email",
    "Non_Customer_Addresses_View": "vw__TSR_Cleanup_Feeder__Non_Customer_Addresses",
    "hubspot-crm-exports-all-companies-2024-06-11.xlsx":"Hubspot",
    "Logistics_DQPilot_CONTACT.csv":"Logistics_DQPilot_CONTACT",
    "LogisticsDQPilotACCOUNT.csv":"LogisticsDQPilotACCOUNT"
    }