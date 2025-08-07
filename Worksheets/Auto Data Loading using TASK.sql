--Lets load the data from INTERNAL STAGES To Target Tables in Snowflake:


--Step1: Create INTERNAL STAGE and upload a file.
--Step2: Create a TABLE.
--Step3: Create a STREAM to detect CDC.
--Step4: Create a TASK to load the files from Stage to table.

--Lets Create a TABLE:
create or replace TABLE tbl5_cust_simple (custid number,name varchar,amount float,items varchar);

--Lets Create an INTERNAL STAGE:
create or replace STAGE stg_int_cust_simple2
FILE_FORMAT = (TYPE = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '\"');

list @stg_int_cust_simple2;

--Lets UPLOAD the files into internal stage using snowsight. --DONE

--Lets Query the Stage:
select $1,$2,$3,$4
from @stg_int_cust_simple2;

--Lets Create a STREAM:
create or replace stream Str_cust_simple2 on table tbl5_cust_simple;

select * from tbl5_cust_simple; --0 ROWS

select * from Str_cust_simple2; -- 0 ROWS

--Lets Create a TASK that loads data every min from Internal Stage to Table:
create or replace task task_int_load_cust_simple
WAREHOUSE=COMPUTE_WH
schedule = '1 minute'
AS copy into tbl5_cust_simple from @stg_int_cust_simple2; --when created task will be in suspended state, we need to resume it.


desc task task_int_load_cust_simple; -- Check the state, It is started state so suspend it.

alter task task_int_load_cust_simple suspend; -- Suspend Task after load completed.

select * from table(information_schema.task_history()) where name='TASK_INT_LOAD_CUST_SIMPLE' order by scheduled_time; --Task History

--Lets query the data:
list @stg_int_cust_simple2;

select $1,$2,$3,$4
from @stg_int_cust_simple2; --1k rows

select * from tbl5_cust_simple; --2k rows

select * from Str_cust_simple2; -- 1k rows

desc task task_int_load_cust_simple; --Suspended
----------------------------------
