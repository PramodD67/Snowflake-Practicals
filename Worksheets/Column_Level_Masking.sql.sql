create or replace masking policy salary_mask_policy
as (salary number) returns number ->
case 
when current_role()='ROLE_MANAGER' then Salary
else 00000
end;

create or replace masking policy empid_masking_policy
as (employee_id number) returns number->
case when current_role()='ROLE_MANAGER' then employee_id
else null
end;

alter table employees
modify column salary
set masking policy salary_mask_policy;

alter table employees
modify column employee_id
set masking policy empid_masking_policy;

select * from employees;

use role role_manager;
use role role_employee;

select current_role();