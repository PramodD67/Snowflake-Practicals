Different Types of Tables in Snowflake:

1. Permanent Table.
2. Transient Table.
3. Temporary Table.
4. External Table.
5. Dynamic Table : https://docs.snowflake.com/en/user-guide/dynamic-tables-intro  ;
---------------------------------------------------


create table tbl_permanent ( Name varchar, Age number, Height float, dob date );

insert into tbl_permanent ( Name , Age , Height , dob  ) values ('Pramod' , '26' ,5.11, '06/09/2001');
------------------
create temporary table tbl_temporary ( Name varchar, Age number, Height float, dob date );
------------------
create transient table tbl_Transient ( Name varchar, Age number, Height float, dob date );
--------------------
CREATE OR REPLACE DYNAMIC TABLE Tbl_dynamic
  TARGET_LAG = '20 minutes'
  WAREHOUSE = compute_wh
  REFRESH_MODE = auto
  INITIALIZE = on_create
  AS
    SELECT Name, age FROM tbl_permanent ;
---------------

select * from tbl_permanent;
    
select * from tbl_dynamic;
    
-------------------

Temporary Tables:
Snowflake supports creating temporary tables for storing non-permanent, transitory data (e.g. ETL data, session-specific data). Temporary tables only exist within the session in which they were created and persist only for the remainder of the session. As such, they are not visible to other users or sessions. Once the session ends, data stored in the table is purged completely from the system and, therefore, is not recoverable, either by the user who created the table or Snowflake.

Transient Tables :
Snowflake supports creating transient tables that persist until explicitly dropped and are available to all users with the appropriate privileges. Transient tables are similar to permanent tables with the key difference that they do not have a Fail-safe period. As a result, transient tables are specifically designed for transitory data that needs to be maintained beyond each session (in contrast to temporary tables), but does not need the same level of data protection and recovery provided by permanent tables.


External Table:
An external table is a Snowflake feature that allows you to query data stored in an external stage as if the data were inside a table in Snowflake. The external stage is not part of Snowflake, so Snowflake does not store or manage the stage. To harden your security posture, you can configure the external stage for outbound private connectivity in order to access the external table using private connectivity.

External tables let you store (within Snowflake) certain file-level metadata, including filenames, version identifiers, and related properties. External tables can access data stored in any format that the COPY INTO <table> command supports except XML.

External tables are read-only. You cannot perform data manipulation language (DML) operations on them. However, you can use external tables for query and join operations. You can also create views against external tables.
