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


