
--Lets create a PIPE object:
create or replace pipe Pipe_ext_cust_simple
AS
copy into tbl4_cust_simple from @STG_EXT_CUST_SIMPLE;

list @STG_EXT_CUST_SIMPLE;

create or replace pipe Pipe_int_cust_simple
AS
copy into tbl5_cust_simple from @stg_int_cust_simple2;

Desc pipe Pipe_int_cust_simple;

select system$pipe_status('Pipe_int_cust_simple'); --In Running state when created

--Lets PAUSE the pipe:
alter pipe Pipe_cust_simple 
set pipe_execution_paused=true;

--Lets RESUME the pipe:
alter pipe pipe_cust_simple
set pipe_execution_paused=false;

select system$pipe_status('Pipe_int_cust_simple');

alter pipe Pipe_int_cust_simple refresh;
