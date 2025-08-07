create or replace table BDL_tbl  (
	CUSTOMER_ID VARCHAR(16777216),
	SALUTATION VARCHAR(16777216),
	FIRST_NAME VARCHAR(16777216),
	LAST_NAME VARCHAR(16777216),
	BIRTH_DAY NUMBER(2,0),
	BIRTH_MONTH NUMBER(2,0),
	BIRTH_YEAR NUMBER(4,0),
	BIRTH_COUNTRY VARCHAR(16777216),
	EMAIL_ADDRESS VARCHAR(16777216),
	SCHEMA_EV_COL_GENDER VARCHAR(16777216),
    start_date date,
    end_date date,
    is_current boolean
);
select * from bdl_tbl;

create or replace stream sdl_str on table tar_tbl;

select * from sdl_str;

truncate table tar_tbl;
truncate table bdl_tbl;

--SCD TYPE 2 Implementation:

--IMPORTANT NOTE: YOU MUST INCLUDE THE COLUMN (Which you want to track the SCD type 2 changes. eg in the below case we are tracking Email changes) IN THE MERGE CONDITION ALONG WITH PRIMARY KEY CONDTION 
Merge into bdl_tbl t
using sdl_str s
on s.customer_id=t.customer_id and s.EMAIL_ADDRESS=t.EMAIL_ADDRESS

when matched and s.metadata$action='DELETE'  then
UPDATE SET end_date=CURRENT_DATE, is_current=FALSE

When not matched and s.metadata$action='INSERT' then
INSERT (CUSTOMER_ID,
SALUTATION,
FIRST_NAME,
LAST_NAME,
BIRTH_DAY,
BIRTH_MONTH,
BIRTH_YEAR,
BIRTH_COUNTRY,
EMAIL_ADDRESS,
SCHEMA_EV_COL_GENDER,
start_date ,
end_date ,
is_current)
VALUES (
S.CUSTOMER_ID,
S.SALUTATION,
S.FIRST_NAME,
S.LAST_NAME,
S.BIRTH_DAY,
S.BIRTH_MONTH,
S.BIRTH_YEAR,
S.BIRTH_COUNTRY,
S.EMAIL_ADDRESS,
S.SCHEMA_EV_COL_GENDER,
CURRENT_DATE,
NULL,
TRUE
);

update  tar_tbl set EMAIL_ADDRESS='098@gail.com' where first_name='Christopher';

select customer_id, count(*) from sdl_str
group by customer_id having count(*)>1;


select * from bdl_tbl t join sdl_str s
on s.customer_id=t.customer_id;

select * from sdl_str;
table bdl_tbl;
