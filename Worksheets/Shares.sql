
-- create the share in provider account
CREATE SHARE my_share;
GRANT USAGE ON DATABASE db_mydb TO SHARE my_share;
GRANT USAGE ON SCHEMA DB_MYDB.SC_MYSCHEMA TO SHARE my_share;
GRANT SELECT ON ALL TABLES IN SCHEMA DB_MYDB.SC_MYSCHEMA TO SHARE my_share;
grant all on schema DB_MYDB.SC_MYSCHEMA to share my_share;



GRANT ALL ON ALL TABLES IN SCHEMA sc_myschema TO SHARE my_share;
GRANT ALL ON ALL VIEWS IN SCHEMA sc_myschema TO SHARE my_share; --error
GRANT ALL ON ALL MATERIALIZED VIEWS IN SCHEMA sc_myschema TO SHARE my_share;
GRANT ALL ON ALL STAGES IN SCHEMA sc_myschema TO SHARE my_share; 
GRANT ALL ON ALL TASKS IN SCHEMA sc_myschema TO SHARE my_share;
GRANT ALL ON ALL STREAMS IN SCHEMA sc_myschema TO SHARE my_share;
GRANT ALL ON ALL PROCEDURES IN SCHEMA sc_myschema TO SHARE my_share; --error
GRANT ALL ON ALL FUNCTIONS IN SCHEMA sc_myschema TO SHARE my_share; --error
GRANT ALL ON ALL sequences in SCHEMA sc_myschema TO SHARE my_share;
GRANT ALL ON ALL file formats in SCHEMA sc_myschema TO SHARE my_share;
GRANT ALL privileges on file format ff_csv TO SHARE my_share;

SELECT CURRENT_ROLE();
SELECT CURRENT_DATABASE();
SELECT CURRENT_WAREHOUSE();
select current_account();
select current_region();

show shares;

--create db in consumer account 
create database db_mydb from share MSYDLEK.EQ49095.my_share; -- run in consumer account
------------------------------------------------------------------------------------------------------------------------------------------------------------


CREATE SHARE my_share;
GRANT USAGE ON DATABASE db_mydb TO SHARE my_share;
GRANT USAGE ON SCHEMA DB_MYDB.SC_MYSCHEMA TO SHARE my_share;
grant all on all schemas TO SHARE my_share;

ALTER SHARE MY_SHARE ADD ACCOUNTS=MFVHLDU.LV97747;


select current_region();

GRANT SELECT ON ALL TABLES IN SCHEMA DB_MYDB.SC_MYSCHEMA TO SHARE my_share;
grant all on schema DB_MYDB.SC_MYSCHEMA to share my_share;

GRANT ALL ON ALL TABLES IN SCHEMA sc_myschema TO SHARE my_share;
GRANT ALL ON ALL VIEWS IN SCHEMA sc_myschema TO SHARE my_share; --error
GRANT ALL ON ALL MATERIALIZED VIEWS IN SCHEMA sc_myschema TO SHARE my_share;
GRANT ALL ON ALL STAGES IN SCHEMA sc_myschema TO SHARE my_share; 
GRANT ALL ON ALL TASKS IN SCHEMA sc_myschema TO SHARE my_share;
GRANT ALL ON ALL STREAMS IN SCHEMA sc_myschema TO SHARE my_share;
GRANT ALL ON ALL PROCEDURES IN SCHEMA sc_myschema TO SHARE my_share; --error
GRANT ALL ON ALL FUNCTIONS IN SCHEMA sc_myschema TO SHARE my_share; --error
GRANT ALL ON ALL sequences in SCHEMA sc_myschema TO SHARE my_share;
GRANT ALL ON ALL file formats in SCHEMA sc_myschema TO SHARE my_share;
GRANT ALL privileges on file format ff_csv TO SHARE my_share;







