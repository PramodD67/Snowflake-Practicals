create or replace row access policy sal_access_policy
as (name varchar) returns boolean ->
case when current_role() IN ('ROLE_MANAGER','ACCOUNTADMIN') and current_user()='PRAMOD67' AND name in ('Pramod D','Kiran S','Sneha R','Arjun M')  then true
else false
end;


select current_user();

alter table employees add row access policy sal_access_policy on (name) ;
alter table employees DROP row access policy sal_access_policy ;

select * from employees;

------------------------

