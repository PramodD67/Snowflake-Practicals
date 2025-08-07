--1. Lets Create FILE FORMAT:
create file format ff_csv
type='csv';

--2. Lets Create STORAGE INTEGREATION If not created earlier:
CREATE or replace STORAGE INTEGRATION my_s3_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE 
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::277707120370:role/Role_Snowflake_access2'
STORAGE_ALLOWED_LOCATIONS = ('s3://mys3bucketpramod/Customer_folder/csv/');

--3. Lets create an EXTERNAL STAGE with AWS S3:
create or replace stage stg_ext_cust_simple 
url='s3://mys3bucketpramod/Customer_folder/csv/'
FILE_FORMAT = 'ff_csv'
STORAGE_INTEGRATION = my_s3_integration

--Lets create a TABLE and LOAD the data into Table Stage using SNOWSQL:
create table tbl_cust_simple (custid number,name varchar,amount float,items varchar);

--We have loaded cust_simple.csv file into tbl_cust_simple table stage using snowsql and STG_INT_CUSTOMER_SIMPLE internal stage using snowsql put file command.

list @%tbl_cust_simple; --Table Stage
list @STG_INT_CUSTOMER_SIMPLE; -- Internal Stage

--Lets create a FILE FORMAT that has field optionally enclosed parameter: to handle commas, quotes, or special characters inside fields.
--FIELD_OPTIONALLY_ENCLOSED_BY = '"'
--This tells Snowflake:
--“Some fields may be enclosed in double quotes, and if they are, treat the content inside ----as a single field — even if it contains commas, newlines, etc.”

CREATE OR REPLACE FILE FORMAT ff_csv_cust
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 0
  COMPRESSION = 'NONE';
  --------------------

--Lets query the data from TABLE stage:
select $1,$2,$3,$4
from @%tbl_cust_simple/Customer_simple.csv
(FILE_FORMAT => 'FF_CSV');

--Lets query the data from INTERNAL Stage:
--( While querying the data from any stages, SF provides 2 more additional attributes (columns) such as metadata$filename, metadata$file_row_number to fetch the filename (if multiple files are loaded then useful) and row number. We need to include these columns explicitly if we need) :
select $1,$2,$3,$4, metadata$filename, metadata$file_row_number
from @stg_int_customer_simple/CUSTOMER_SIMPLE.CSV
(File_format=> 'FF_CSV');

--Lets query the data from USER stage:
list @~/CUSTSIMPLE;

select $1,$2
from @~/CUSTSIMPLE/CUSTOMER_SIMPLE.CSV
(File_format=>'FF_CSV');

--Lets query the data from External Stage i.e Amazon S3 bucket:
select $1 as custid,$2 as Cust_Name,$3 as Amount,$4 as Items,  metadata$filename, metadata$file_row_number
from @stg_ext_cust_simple
(File_format=> 'ff_csv_cust');
-----------------------------
DESC FILE FORMAT ff_csv_cust;
DESC INTEGRATION my_s3_integration;
list @stg_ext_cust_simple;
DESC stage stg_ext_cust_simple;

--Lets create an EXTERNAL TABLE and query the data from stage: ( Ext Table adds "Value" Coumn which contains JSON data of all columns)
create or replace external table tbl_ext_cust( 
custid varchar as ( value:c1::varchar),
name varchar as (value:c2::varchar),
amount varchar as (value:c3::varchar),
items varchar as (value:c4::varchar))
with location = @stg_ext_cust_simple
File_format=(format_name='ff_csv_cust');

SELECT * FROM tbl_ext_cust;
-------------------------------


