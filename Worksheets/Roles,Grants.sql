--Types of Roles: 7
--AccountAdmin,SecurityAdmin, UserAdmin, SystemAdmin, Public, OrgAdmin, Sandbox_role

show roles;
select current_role();

use role sysadmin;

create user PramodD
password ='Test@123'
Must_change_password=False; //Insufficient privileges bcz only useradmin,securityadmin and accountadmin roles can create users

use role useradmin;

create user PramodD
password ='Test@123'
Must_change_password=False; //created //bydeefalut public role is assigned


//to see grants available to public role:
show grants to role public;

//To see user:
desc user PramodD;

//To see grants avail.. to user:
show grants to user PramodD; -- to list all roles assigned to this user
show grants on user PramodD; --to see ownership of this role

//Add more roles to the user:
Grant role SECURITYADMIN to user PramodD; //Error bcz Only securityadmin and accountadmin can grant roles

use role securityadmin;
Grant role SECURITYADMIN to user PramodD; //granted
Grant role useradmin to user PramodD;

show grants to user pramodd;

show users;
show roles;

---------------------
--Create custom roles:

select current_role(); --security admin 
//Roles should be created by SecurityAdmin. 
//Roles can be created by Accountadmin, SecurtiyAdmin and UserAdmin.
use role securityadmin;

create role projectmanager_role;
grant role projectmanager_role to role securityadmin;

create role Dev_team_role;
grant role dev_team_role to role projectmanager_role; -- PM role inherits all privilages of dev_team

create role Testing_team_role;
grant role Testing_team_role to role projectmanager_role; -- PM can get all privilages of test_team


// When a role is granted to other role, The role in the hierarchy can switch the context and perform operations of the granted role

--Lets assign the users
create user PM_PramodD;
desc user PM_pramodd;

create user DEV_Sagar;
create user Test_Brunda;

--lets assign the roles to the above users:

grant role PROJECTMANAGER_ROLE to user PM_pramodd;
grant role dev_team_role to user dev_sagar;
grant role testing_team_role to user test_brunda;

//We have assigned the roles to the users but these roles doesnt have any privilages, so we have to grant privilages:

select current_role();
use role sysadmin; -- Only accountadmin and Sysadmin roles can give below grants:


grant create warehouse on account to role projectmanager_role; 
grant create database on account to role projectmanager_role;

grant usage on warehouse compute_wh to role projectmanager_role; 

grant usage on database db_mydb to role projectmanager_role;
--revoke usage on database db_mydb  from role projectmanager_role;
grant usage on schema sc_myschema to role projectmanager_role;

--Lets give individual privilage to role :
grant create schema on database db_mydb  to role dev_team_role;
grant create table on schema sc_myschema  to role dev_team_role;

grant select on table sample_tbl  to role dev_team_role;
grant insert on table sample_tbl  to role dev_team_role;

--lets give individual privilege on all tables to a role:
grant select on all tables in schema sc_myschema  to role dev_team_role;
grant update on all tables in schema sc_myschema  to role dev_team_role;
grant delete on all tables in schema sc_myschema  to role dev_team_role;
grant insert on all tables in schema sc_myschema  to role dev_team_role;

---------------
--lets give all privilege on all tables to a role:

grant all privileges on schema sc_myschema to role projectmanager_role;
grant all privileges on all tables in schema sc_myschema to role projectmanager_role;
-------------

--Lets give usage privilage to a role:
grant usage on warehouse compute_wh to role dev_team_role; 
grant usage on database db_mydb to role dev_team_role;
grant usage on schema sc_myschema to role dev_team_role;

grant select on all tables in schema sc_myschema to role dev_team_role;
grant select on all  in schema sc_myschema to role dev_team_role;

----------------------------------

//There is no one single command to affect all the objects under the database, but you can run these set of SQLs per object:


GRANT ALL ON ALL schemas in database db_mydb TO ROLE projectmanager_role; 
GRANT ALL ON ALL TABLES IN SCHEMA sc_myschema TO ROLE projectmanager_role; 

--similarly for future grants:
grant all on future schemas in database db_mydb TO ROLE projectmanager_role;

grant all on future tables in schema sc_myschema to ROLE projectmanager_role;
-----------------------------------------------
//To grant all the objects in the schema to a role:


GRANT ALL ON ALL TABLES IN SCHEMA sc_myschema TO ROLE projectmanager_role;
GRANT ALL ON ALL VIEWS IN SCHEMA sc_myschema TO ROLE projectmanager_role;
GRANT ALL ON ALL MATERIALIZED VIEWS IN SCHEMA sc_myschema TO ROLE projectmanager_role;
GRANT ALL ON ALL STAGES IN SCHEMA sc_myschema TO ROLE projectmanager_role;

GRANT delete ON  stg_010  TO ROLE projectmanager_role;


GRANT ALL ON ALL TASKS IN SCHEMA sc_myschema TO ROLE projectmanager_role;
GRANT ALL ON ALL STREAMS IN SCHEMA sc_myschema TO ROLE projectmanager_role;
GRANT ALL ON ALL PROCEDURES IN SCHEMA sc_myschema TO ROLE projectmanager_role;
GRANT ALL ON ALL FUNCTIONS IN SCHEMA sc_myschema TO ROLE projectmanager_role;
GRANT ALL ON ALL sequences in SCHEMA sc_myschema TO ROLE projectmanager_role;
GRANT ALL ON ALL file formats in SCHEMA sc_myschema TO ROLE projectmanager_role;
GRANT ALL privileges on file format ff_csv TO ROLE projectmanager_role;
