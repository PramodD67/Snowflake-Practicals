/*  4 STAGES:
1. TABLE STAGE @%TABLE_NAME
2. USER STAGE @~TABLE_NAME  
3. NAMED INTERNAL STAGE  
4. NAMED EXTERNAL STAGE
*/

LIST @~; //USER STAGE
LIST @%SAMPLE_TBL; //TABLE STAGE

//TO LIST NAMED STAGES:
SHOW STAGES;

//Create internal stage:
Create stage stg_int_customer_simple;

create file format ff_csv
type='csv';

create file format ff_json
type='json';
//Assoc File Formats for internal/external Stages. --By default it takes csv while loading the data from stage to table.


Create stage STG_INT_WITH_FF_CSV
file_format =ff_csv;

Create stage STG_INT_WITH_FF_JSON
file_format=ff_json;

--s3://mys3bucketpramod/Customer_folder/csv/

--create external stage with s3 bucket
create stage STG_EXT_S3
url='s3://mys3bucketpramod/Customer_folder/csv/';

show stages;



CREATE or replace STORAGE INTEGRATION my_s3_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE 
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::277707120370:role/Role_Snowflake_access2'
STORAGE_ALLOWED_LOCATIONS = ('s3://mys3bucketpramod/Customer_folder/csv/');
------------
create or replace stage stg_ext_cust_simple 
copy_options = (size_limit=null)
url='s3://mys3bucketpramod/Customer_folder/csv/'
FILE_FORMAT = 'ff_csv'
STORAGE_INTEGRATION = my_s3_integration;
---------------------
--Lets query the data from External Stage i.e Amazon S3 bucket:
drop stage stg_ext_cust_simple;

select $1
from @stg_ext_cust_simple/Customer_simple.csv
(File_format=>'ff_csv_cust');
-----------------
list @stg_ext_cust_simple;
DESC INTEGRATION my_s3_integration;
DESC stage stg_ext_cust_simple;

create table tbl_cust_simple (custid number,name varchar,amount float,items varchar);

drop stage stg_ext_cust_simple;
------------------------------
show stages;

LIST @~; --USER STAGE -- 0 ROWS
LIST @%SAMPLE_TBL; -- TABLE STAGE -- 0 ROWS
LIST @%tbl_cust_simple; 
LIST @%TBL_CUSTOMERSCSV; -- TABLE STAGE -- 0 ROWS
-----------

-- LETS Load the data into these stages using SnowCLI or upload the files in the snowflake web ui and then
--Lets query data from table stage without loading data into target tbl.
--This works only if your file has semi str data (eg. Json, Parque)

--Create a Table and assign a FF
create table tbl_intstg_to_tbl (
CustomerID varchar,
Age number,
Gender Varchar,
stage_file_format=(Type=csv);

--Load the data from table stage to Target tbl:
Copy into SAMPLE_TBL 
from @%Sample_tbl/TBL_stg/Customers.csv;

select * from sample_tbl;

--Load the data from user stage to Target tbl:
Copy into tbl_customer_csv_ff 
from @~/USER_STG/CUST_CSV/customer_000.csv.gz
ON_ERROR = 'CONTINUE';

--Load the data from internal stage to Target tbl:
Copy into tbl_customer_csv_ff 
from @~/USER_STG/CUST_CSV/customer_000.csv.gz
ON_ERROR = 'CONTINUE';
