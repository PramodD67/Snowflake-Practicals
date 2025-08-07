--Lets load data from table stage to a table:

--Table stage is a unnamed stage, so it cannot be modified. so it is hard to associate File Format with it. Easy to work with CSV but hard to work with semi-str data.

--lets create customer table and associate a file format with it:

create table tbl_customer_parquet_ff (
my_data variant
)
stage_file_format =(Type=parquet);


list @%tbl_customer;

select * from @%tbl_customer
(File_format=>'ff_csv');

select 
$1:Customer_Id::varchar,
$2:First_Name::varchar,
$3:Last_Name::varchar,
$4:Company::varchar,
$5:City::varchar,
$6:Country::varchar,
$7:Phone_1::varchar,
$8:Phone_2::varchar,
$9:Email::varchar,
$10:Subscription_Date::varchar,
$11:Website::varchar

from @%TBL_CUSTOMER; --error
--------------------------

--You cannot load data from one table stage to another target table. You should load the data from same table stage to same table.


copy into tbl_cust2 from @%tbl_customer/tbl_stg/customers-100.csv.gz; --Error: Access to the stage area of a table (TBL_CUSTOMER) with the schema of another table (TBL_CUST2) is not allowed.


--Lets load the data from table stage to same table:

select * from tbl_customer; --27

copy into tbl_customer from @%tbl_customer/tbl_stg/customers-100.csv.gz 
ON_ERROR = 'CONTINUE'; -- The data and columns in the tables and the table stage are different so load failed. so lets remove the file from the stage.

remove @%tbl_customer/tbl_stg/customers-100.csv.gz ; 

list @%tbl_customer; -- 0 rows

------------
select * from tbl_customerscsv; --valid table with valid columns --100 rows
list @%tbl_customerscsv; --file found

--lets load the file to a table:
copy into tbl_customerscsv from @%tbl_customerscsv 
on_error='continue'; --PARTIALLY_LOADED --Numeric value 'Index' is not recognized --66 rows loaded.

select * from tbl_customerscsv; --166 rows

list @stg_internal_customers;

create pipe my_pipe_ch10
as copy into tbl_customerscsv from @stg_internal_customers;

--lets check the status of the pipe:

select system$pipe_status('my_pipe_ch10');

--lets resume the pipe:
alter pipe my_pipe_ch10 refresh;

--To check the pipe load history:
select * from table(validate_pipe_load(pipe_name=>'my_pipe_ch10',
start_time=>dateadd(hour,-1,current_timestamp())));

--To pause the pipe:
alter pipe my_pipe_ch10 set pipe_execution_paused=true;

--To resume the pipe:
alter pipe my_pipe_ch10 set pipe_execution_paused=false;
alter pipe my_pipe_ch10 refresh;

--We can also specify particular files to load for a pipe:

alter pipe my_pipe_ch10 refresh prefix='/customer_10*';




