show tables;

-- Create managers table
CREATE or replace TABLE managers (
    manager_id INT PRIMARY KEY,
    name VARCHAR(100),
    department VARCHAR(100)
);

-- Create employees table with salary column
CREATE or replace TABLE employees (
    employee_id INT PRIMARY KEY,
    name VARCHAR(100),
    job_title VARCHAR(100),
    salary DECIMAL(10, 2),
    manager_id INT,
    FOREIGN KEY (manager_id) REFERENCES managers(manager_id)
);

-- Insert sample data into managers table
INSERT INTO managers (manager_id, name, department) VALUES
(1, 'Shankar K.B', 'Data Engineering'),
(2, 'Anita Rao', 'Software Development'),
(3, 'Ravi Kumar', 'Product Management'),
(4, 'Meena Joshi', 'Quality Assurance');

-- Insert sample data into employees table with salary
INSERT INTO employees (employee_id, name, job_title, salary, manager_id) VALUES
(101, 'Pramod D', 'Data Engineer', 95000.00, 1),
(102, 'Kiran S', 'Data Analyst', 72000.00, 1),
(103, 'Sneha R', 'Backend Developer', 88000.00, 2),
(104, 'Arjun M', 'Frontend Developer', 86000.00, 2),
(105, 'Divya T', 'QA Engineer', 70000.00, 4),
(106, 'Rohit B', 'Product Designer', 91000.00, 3),
(107, 'Neha G', 'Scrum Master', 93000.00, 3),
(108, 'Vikram L', 'DevOps Engineer', 94000.00, 2),
(109, 'Aishwarya N', 'Data Scientist', 98000.00, 1),
(110, 'Manoj P', 'Software Tester', 69000.00, 4),
(111, 'Ritika S', 'UI/UX Designer', 85000.00, 3),
(112, 'Sandeep K', 'Cloud Engineer', 97000.00, 2),
(113, 'Pooja V', 'Business Analyst', 89000.00, 3),
(114, 'Tarun J', 'ML Engineer', 99000.00, 1),
(115, 'Mehul D', 'Security Analyst', 88000.00, 2),
(116, 'Anjali C', 'Technical Writer', 75000.00, 3),
(117, 'Nikhil R', 'QA Lead', 80000.00, 4),
(118, 'Shruti M', 'Full Stack Developer', 96000.00, 2),
(119, 'Harsha Y', 'Data Architect', 102000.00, 1),
(120, 'Rajesh H', 'Release Manager', 87000.00, 4);


create role role_manager;
create role role_employee;

create or replace  row access policy sal_row_policy
as (employee_id integer) returns boolean ->
case 
WHEN current_role()='role_manager' and employee_id=101  THEN TRUE
ELSE FALSE
end;

select current_role();

alter table employees 
add row access policy sal_row_policy on (employee_id);

show users;
show roles;

use role role_employee;
use role accountadmin;

create user user_employees;
create user user_managers;

grant role role_manager to user user_managers;
grant role role_employee to user user_employees;

grant role role_manager to user pramod67;
grant role role_employee to user pramod67;
grant usage on schema dbt_pd to role role_manager;
grant select on table employees to role  role_manager; 
grant usage on schema dbt_pd to role role_employee;
grant select on table employees to role  role_employee; 

select * from employees;

use role accountadmin;


ALTER USER user_employees SET DEFAULT_ROLE = role_employee;


use role role_employee;
use role role_manager;
select * from employees;
use role accountadmin;

desc table employees;
drop row access policy sal_row_policy;