create or replace file format ff_zom_csv
type='csv'
skip_header=1;

create or replace stage stg_ext_zom
url='s3://pramoddbt-bucket/Zomato_data/'
storage_integration=mys3
file_format=(Format_name='ff_zom_csv');

list @stg_ext_zom;

select 
$1 as restaurant_name, 
$2 as online_order,
$3 as book_table,
$4 as rate,
$5 as votes,
$6 as approx_cost,
$7 as listed_in
from @stg_ext_zom;

---------------

select * from table(infer_schema (
location=>'@stg_ext_zom',
file_format=>'ff_zom_csv'
));

-- create or replace table sdl_zom_tbl using template(
-- select array_agg(object_construct(*)) from table ( infer_schema (
-- location=>'@stg_ext_zom',
-- file_format=>'ff_zom_csv'
-- ))
-- );

create or replace table sdl_zom_tbl( name varchar,	online_order varchar,	book_table	varchar , rate varchar 	,votes integer,	approx_cost integer, listed_in varchar
);

desc table sdl_zom_tbl;

alter table sdl_zom_tbl rename column "listed_in(type)" to listed_in  ;

create or replace pipe zom_sdl_pipe
auto_ingest=true -- this is must for auto ingestion
as 
copy into sdl_zom_tbl 
from @stg_ext_zom
file_format='ff_zom_csv';

desc pipe zom_sdl_pipe;
show pipes;
alter pipe zom_sdl_pipe set pipe_execution_paused=false;

table sdl_zom_tbl;

select system$pipe_status('zom_sdl_pipe');


create or replace stream str_zom
on table sdl_zom_tbl;


select * from str_zom;

truncate table sdl_zom_tbl;

create or replace table bdl_zom_tbl( name varchar,	online_order varchar,	book_table	varchar , rate varchar 	,votes integer,	approx_cost integer, listed_in varchar
);

create or replace task sdl_to_bdl_tsk
warehouse='compute_wh'
schedule='1 minute'
when system$stream_has_data('str_zom')
as
merge into BDL_ZOM_TBL t
using (select * from str_zom ) s 
on t.name=s.name

when matched  then
update set t.online_order =s.online_order ,	t.book_table=s.book_table , t.rate=s.rate 	,t.votes=s.votes,	t.approx_cost=s.approx_cost
, t.listed_in=s.listed_in


when not matched  then
insert ( name ,	online_order ,	book_table	 , rate  	,votes ,	approx_cost , listed_in ) 
values(s.name,s.online_order
 ,	s.book_table , s.rate 	,s.votes,	s.approx_cost,s.listed_in);


table sdl_zom_tbl;
table bdl_zom_tbl;
desc task sdl_to_bdl_tsk;

select * from table(information_schema.task_history(
task_name=>'sdl_to_bdl_tsk'
)) order by SCHEDULED_TIME desc;

alter task sdl_to_bdl_tsk suspend;
alter task sdl_to_bdl_tsk set schedule='1 minute'; 
----------------------------------------------------------
