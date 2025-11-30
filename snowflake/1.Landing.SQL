/* Creating Database, schema and Table where the raw data comes from source system through
 Kinesis and Firehose */

CREATE OR REPLACE DATABASE RETAIL_DEMO_DB;

CREATE OR REPLACE SCHEMA BRONZE;

CREATE OR REPLACE TABLE RETAIL_DEMO_DB.BRONZE.RETAIL_RAW_POS(
    raw_data VARIANT,
    ingest_time TIMESTAMP_LTZ 
);


/* Creating a Stream object on top of the RETAIL_RAW_POS Table, to capture the streaming data 
other than old data */

CREATE OR REPLACE STREAM RETAIL_DEMO_DB.BRONZE.RETAIL_RAW_POS_STREAM
ON TABLE RETAIL_DEMO_DB.BRONZE.RETAIL_RAW_POS
APPEND_ONLY = TRUE;


/* Creating and Granting access to the newly created user for getting the raw json data 
from Kinesis and granting accountadming role */

CREATE OR REPLACE USER devuser1 type = 'service';

GRANT ROLE accountadmin to USER devuser1;



-- Updating the RSA PUBLIC KEY attribute (a property) of the user

ALTER USER devuser1 SET RSA_PUBLIC_KEY = 
'MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA4OUXhCEMsjoUqW8DUxUK
vwQFcILEsdDP04qlt4Cqk5hc69jm/ON6M71PzUwGUpWNCvx7lZ+J0Nj5Tgzc+6dl
GBWRiFkoelXsSi328Adbd7cwgepqx1BIrqlsTMANJBVWQiX6SV1BdDjEt9VUgGyg
h6LoUZHrIPSWYk4AnHbq9Q2Zci3xsbCMVsDRHMoMe77wwUZvMqZrtjCsqGDkI/X9
MNPCFXHdesRUa0kKQLWk0mnu5tJbp3ipF8uySxhIxJMrDJI8fcqmMPrdZS6ylntN
x0LL7QHOQEdCtFFDvZCXId8TyQ+unR9JRM8KUz7ftgtQZbB6XtRFxp/XFliuh+IY
rQIDAQAB';



-- check the flow of data 

SELECT * FROM RETAIL_DEMO_DB.BRONZE.RETAIL_RAW_POS;




