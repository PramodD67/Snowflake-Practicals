create view v_employees as select * from employees limit 3;
select * from v_employees;

create secure view sv_employees as select * from employees limit 3;

create force view force_v as select * from futuretable;


create materialized view MV_Customertbl as select * from tbl_customer; --Error because MV are supported only on Enterprise and above editions and not in standard editions and also it is not supported during free trail
