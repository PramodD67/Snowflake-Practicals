create or replace file format csv_ff
type='csv'
error_on_column_count_mismatch=false
parse_header=true
field_optionally_enclosed_by='"'
field_delimiter=',';

create file format def_ff;

desc file format csv_ff;

create stage mystg
file_format=(format_name=csv_ff);

desc stage mystg;

--Load file into stage and query
select $1,$2,$3,$4,$5,$6 from @mystg/Customer_history.csv
(file_format => 'def_ff');


create or replace TABLE tar_tbl
using template(
select array_agg(object_construct(*)) from table(
infer_schema(
location=>'@mystg/Customer_history.csv',
file_format=>'csv_ff'
)
));

select array_agg(object_construct(*)) from table(
infer_schema(
location=>'@mystg/Customer_history.csv',
file_format=>'csv_ff'
));

Alter table tar_tbl set enable_schema_evolution=true;

show tables like 'tar_tbl';

truncate table tar_tbl;

copy into tar_tbl 
from @mystg
file_format='csv_ff'
match_by_column_name=case_insensitive;

table tar_tbl;






