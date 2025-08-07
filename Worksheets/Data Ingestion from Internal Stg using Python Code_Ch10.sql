--Lets Create a TABLE:
create or replace TABLE tbl_cust_simple_ch10 (custid number,name varchar,amount float,items varchar);

--Lets create a file format:
create file format ff_csv_ch10
type='csv' RECORD_DELIMITER = '\\n';

--Lets Create an INTERNAL STAGE:
create or replace STAGE stg_int_cust_simple_ch10
FILE_FORMAT = (TYPE = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '\"');


--Upload the file into Internal Stage using snowsight --DONE

--Load the data into target table
copy into tbl_cust_simple_ch10 from @stg_int_cust_simple_ch10;

select $1,$2,$3,$4 from @stg_int_cust_simple_ch10;

---Create a Pipe:
create or replace pipe Pipe_int_cust_simple_ch10
AS
copy into tbl_cust_simple_ch10 from @stg_int_cust_simple_ch10;

--Desc
Desc pipe Pipe_int_cust_simple_ch10;

alter pipe Pipe_int_cust_simple_ch10 refresh; --Resume the pipe

select system$pipe_status('Pipe_int_cust_simple_ch10'); --To know the pipe status (Running or Suspended)

--IMP NOTE: PIPE WILL NOT AUTOMATICALLY LOAD THE DATA WHEN NEW FILE IS ADDED IN THE STAGE UNLESS THE NOTIFICATION IS AVAILABLE IN THE SNOWFLAKE QUEUE.

--Lets create PYTHON CODE to trigger notification to trigger the notifcation to snowflake queue.


