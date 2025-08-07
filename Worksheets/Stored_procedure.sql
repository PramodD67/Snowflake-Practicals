select * from employees;
-----------------
create procedure removecustomers() 
 returns string
 language javascript
 strict
 execute as owner
 as
 $$
 var rs=snowflake.execute({sqlText:'Delete from EMPLOYEES where employee_number=1004' });
 return 'Inactive customers removed'
 $$;
-----------
 call removecustomers();
 ------------
 desc procedure removecustomers();
 ----------
 show procedures;
 ----------

 create procedure sp_nothing()
 returns float
 language javascript
 as 
 'var my_age=10;
 return 5*my_age ;';

 call sp_nothing();