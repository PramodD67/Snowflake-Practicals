-- Refer 'Working with Stages' worksheet for better understanding before going through this data loading worksheet. 

-- We have already loaded the files into INTERNAL, EXTERNAL, USER AND TABLE STAGES using SNOWSQL. Now lets load data from stage to target tables.

--Lets create a file format:
create file format ff_csv_ch09
type='csv' RECORD_DELIMITER = '\\n';
--Lets create a Internal_Stage and load the file into it:

create stage STG_INT_Dataloading_Ch09; -- ( We are not associating any file format in the stage, it will default takes CSV)

desc stage STG_INT_Dataloading_Ch09;

--Lets upload the file into INTERNAL STAGE (STG_INT_Dataloading_Ch09) using snowsight upload option instead of SNOWSQL.

--Lets Query the Data in the Internal Stage:
select $1 as ID, $2 as Name, $3 as Amount,$4 as Items
from @stg_int_dataloading_ch09;

--Lets Create a Table :
create or replace table TBL_dataloading_ch09 (custid number,name varchar,amount float,items varchar)
stage_file_format=(type=csv); 

create or replace table tbl3_cust (custid number,name varchar,amount float,items varchar); --used in upcoming loading

select * from TBL_dataloading_ch09;

--Lets LOAD the data from INTERNAL STAGE to Target table :
copy into TBL_dataloading_ch09 
from @stg_int_dataloading_ch09
FILE_FORMAT = (TYPE = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '\"'); --loaded
------------------------------------
list @%tbl_cust_simple;

--Lets LOAD the data from TABLE STAGE to Target table :
copy into tbl_cust_simple 
from @%tbl_cust_simple
FILE_FORMAT = (TYPE = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '\"')
FORCE = TRUE; --loaded

SELECT * FROM tbl_cust_simple; --1k rows
-----------------------------

--Lets create a table with one column and try to load data into it.
create table tbl2_cust( data variant );

select * from tbl2_cust;

--Lets LOAD the data from Internal Stage to Target table :
copy into tbl2_cust 
from @stg_int_dataloading_ch09
FILE_FORMAT = (TYPE = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '\"') ; --ERROR 

--ERROR: Number of columns in file (4) does not match that of the corresponding table (1). ( You should have exact number of columns in target table inorder to load the data into it. Since stage file has 4 columns its not loaded into target table)

--Lets alter the table:
Alter table tbl2_cust add column cust_id varchar, name varchar, amount varchar, items varchar;
select * from tbl2_cust;
alter table tbl2_cust drop column data;

--Lets try to load data from internal stage to tbl2_cust:
copy into tbl2_cust from @stg_int_dataloading_ch09
file_format=(type='csv' field_optionally_enclosed_by='\"'); -- loaded 1k rows

--If there are more than one files in the stage then use pattern for filtering the files or use below method.
copy into tbl3_cust from @stg_int_dataloading_ch09
files=('Customer_simple.csv') --you should give exact file name as it is case sensitive. (if you have multiple files place them here by comma)
file_format=(type='csv' field_optionally_enclosed_by='\"'); -- loaded 1k rows

--Lets use pattern for filtering files:
copy into tbl3_cust from @stg_int_dataloading_ch09
pattern= 'Customer_s[^[0-9]{1,3}$$].csv' --use pattern format
file_format=(type='csv' field_optionally_enclosed_by='\"'); -- loaded 1k rows

list @~;

--lets query user stage:
select $1,$2,$3,$4
from @~;

--Lets try to load data from USER STAGE to Target Table:
copy into tbl2_cust from @~
file_format=(type='csv' field_optionally_enclosed_by='\"'); --loaded

select * from tbl2_cust;

--Lets try to load data from EXTERNAL STAGE to Target Table in snowflake:
copy into tbl3_cust from @stg_ext_cust_simple
file_format=(type='csv' field_optionally_enclosed_by='\"'); --Loaded 1k rows

select * from tbl3_cust;

--Lets validate data before loading from USER STAGE to Target Table: This doesnot actually load the data into table, It's just validate.
copy into tbl2_cust from @~
file_format=(type='csv' field_optionally_enclosed_by='\"')
validation_mode= 'RETURN_ERRORS'; 


--YOU CAN ALSO PERFORM PARTITIONS IN THE EXTERNAL TABLE. Internal tables are auto clustered by snowflake so partition is not needed for internal tables.

copy into tbl2_cust from @~
file_format=(type='csv' field_optionally_enclosed_by='\"')
validation_mode= 'RETURN_10_ROWS'; 

