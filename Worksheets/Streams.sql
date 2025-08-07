CREATE TABLE  "TBL_CUSTOMER" 
   (	"CUST_CODE" VARCHAR2(6) NOT NULL PRIMARY KEY, 
	"CUST_NAME" VARCHAR2(40) NOT NULL, 
	"CUST_CITY" CHAR(35), 
	"WORKING_AREA" VARCHAR2(35) NOT NULL, 
	"CUST_COUNTRY" VARCHAR2(20) NOT NULL, 
	"GRADE" NUMBER, 
	"OPENING_AMT" NUMBER(12,2) NOT NULL, 
	"RECEIVE_AMT" NUMBER(12,2) NOT NULL, 
	"PAYMENT_AMT" NUMBER(12,2) NOT NULL, 
	"OUTSTANDING_AMT" NUMBER(12,2) NOT NULL, 
	"PHONE_NO" VARCHAR2(17) NOT NULL, 
	"AGENT_CODE" CHAR(6) NOT NULL 
);   

INSERT INTO TBL_CUSTOMER VALUES ('C00013', 'Holmes', 'London', 'London', 'UK', '2', '6000.00', '5000.00', '7000.00', '4000.00', 'BBBBBBB', 'A003');
INSERT INTO TBL_CUSTOMER VALUES ('C00001', 'Micheal', 'New York', 'New York', 'USA', '2', '3000.00', '5000.00', '2000.00', '6000.00', 'CCCCCCC', 'A008');
INSERT INTO TBL_CUSTOMER VALUES ('C00020', 'Albert', 'New York', 'New York', 'USA', '3', '5000.00', '7000.00', '6000.00', '6000.00', 'BBBBSBB', 'A008');
INSERT INTO TBL_CUSTOMER VALUES ('C00025', 'Ravindran', 'Bangalore', 'Bangalore', 'India', '2', '5000.00', '7000.00', '4000.00', '8000.00', 'AVAVAVA', 'A011');
INSERT INTO TBL_CUSTOMER VALUES ('C00024', 'Cook', 'London', 'London', 'UK', '2', '4000.00', '9000.00', '7000.00', '6000.00', 'FSDDSDF', 'A006');
INSERT INTO TBL_CUSTOMER VALUES ('C00015', 'Stuart', 'London', 'London', 'UK', '1', '6000.00', '8000.00', '3000.00', '11000.00', 'GFSGERS', 'A003');
INSERT INTO TBL_CUSTOMER VALUES ('C00002', 'Bolt', 'New York', 'New York', 'USA', '3', '5000.00', '7000.00', '9000.00', '3000.00', 'DDNRDRH', 'A008');
INSERT INTO TBL_CUSTOMER VALUES ('C00018', 'Fleming', 'Brisban', 'Brisban', 'Australia', '2', '7000.00', '7000.00', '9000.00', '5000.00', 'NHBGVFC', 'A005');
INSERT INTO TBL_CUSTOMER VALUES ('C00021', 'Jacks', 'Brisban', 'Brisban', 'Australia', '1', '7000.00', '7000.00', '7000.00', '7000.00', 'WERTGDF', 'A005');
INSERT INTO TBL_CUSTOMER VALUES ('C00019', 'Yearannaidu', 'Chennai', 'Chennai', 'India', '1', '8000.00', '7000.00', '7000.00', '8000.00', 'ZZZZBFV', 'A010');
INSERT INTO TBL_CUSTOMER VALUES ('C00005', 'Sasikant', 'Mumbai', 'Mumbai', 'India', '1', '7000.00', '11000.00', '7000.00', '11000.00', '147-25896312', 'A002');
INSERT INTO TBL_CUSTOMER VALUES ('C00007', 'Ramanathan', 'Chennai', 'Chennai', 'India', '1', '7000.00', '11000.00', '9000.00', '9000.00', 'GHRDWSD', 'A010');
INSERT INTO TBL_CUSTOMER VALUES ('C00022', 'Avinash', 'Mumbai', 'Mumbai', 'India', '2', '7000.00', '11000.00', '9000.00', '9000.00', '113-12345678','A002');
INSERT INTO TBL_CUSTOMER VALUES ('C00004', 'Winston', 'Brisban', 'Brisban', 'Australia', '1', '5000.00', '8000.00', '7000.00', '6000.00', 'AAAAAAA', 'A005');
INSERT INTO TBL_CUSTOMER VALUES ('C00023', 'Karl', 'London', 'London', 'UK', '0', '4000.00', '6000.00', '7000.00', '3000.00', 'AAAABAA', 'A006');
INSERT INTO TBL_CUSTOMER VALUES ('C00006', 'Shilton', 'Torento', 'Torento', 'Canada', '1', '10000.00', '7000.00', '6000.00', '11000.00', 'DDDDDDD', 'A004');
INSERT INTO TBL_CUSTOMER VALUES ('C00010', 'Charles', 'Hampshair', 'Hampshair', 'UK', '3', '6000.00', '4000.00', '5000.00', '5000.00', 'MMMMMMM', 'A009');
INSERT INTO TBL_CUSTOMER VALUES ('C00017', 'Srinivas', 'Bangalore', 'Bangalore', 'India', '2', '8000.00', '4000.00', '3000.00', '9000.00', 'AAAAAAB', 'A007');
INSERT INTO TBL_CUSTOMER VALUES ('C00012', 'Steven', 'San Jose', 'San Jose', 'USA', '1', '5000.00', '7000.00', '9000.00', '3000.00', 'KRFYGJK', 'A012');
INSERT INTO TBL_CUSTOMER VALUES ('C00008', 'Karolina', 'Torento', 'Torento', 'Canada', '1', '7000.00', '7000.00', '9000.00', '5000.00', 'HJKORED', 'A004');
INSERT INTO TBL_CUSTOMER VALUES ('C00003', 'Martin', 'Torento', 'Torento', 'Canada', '2', '8000.00', '7000.00', '7000.00', '8000.00', 'MJYURFD', 'A004');
INSERT INTO TBL_CUSTOMER VALUES ('C00009', 'Ramesh', 'Mumbai', 'Mumbai', 'India', '3', '8000.00', '7000.00', '3000.00', '12000.00', 'Phone No', 'A002');
INSERT INTO TBL_CUSTOMER VALUES ('C00014', 'Rangarappa', 'Bangalore', 'Bangalore', 'India', '2', '8000.00', '11000.00', '7000.00', '12000.00', 'AAAATGF', 'A001');
INSERT INTO TBL_CUSTOMER VALUES ('C00016', 'Venkatpati', 'Bangalore', 'Bangalore', 'India', '2', '8000.00', '11000.00', '7000.00', '12000.00', 'JRTVFDD', 'A007');
INSERT INTO TBL_CUSTOMER VALUES ('C00011', 'Sundariya', 'Chennai', 'Chennai', 'India', '3', '7000.00', '11000.00', '7000.00', '11000.00', 'PPHGRTS', 'A010');


SELECT * FROM TBL_CUSTOMER;

create stream str_customer
on table TBL_CUSTOMER;

--Lets create Append only stream:
create stream str_apponly_customer
on table TBL_CUSTOMER 
append_only=True;

SELECT * FROM STR_CUSTOMER; -- 0 ROWS

INSERT INTO TBL_CUSTOMER VALUES ('C00025', 'Surya', 'Bangalore', 'Bangalore', 'India', '4', '800.00', '1100.00', '700.00', '1200.00', 'SDFSVD', 'A007');

SELECT * FROM STR_CUSTOMER; -- 1 ROW

INSERT INTO TBL_CUSTOMER VALUES ('C00026', 'Raghya', 'Bangalore', 'Bangalore', 'India', '4', '8800.00', '11800.00', '7000.00', '12000.00', 'SDFSVD', 'A007');

SELECT * FROM STR_CUSTOMER; -- 2 rows

update TBL_CUSTOMER set PHONE_NO='747283276' where CUST_CODE='C00026';

SELECT * FROM STR_CUSTOMER; -- 2 rows because it will update the same record unless the offset gets changed ( Version will get change only when stream data is consumed, when stream data is consumed all the data will get empty in the stream. when the existing data is updated in the source table then 2 records will get inserted into stream table)

SELECT * FROM STR_apponly_CUSTOMER; -- 1 row

delete from TBL_CUSTOMER  where CUST_CODE='C00026';

SELECT * FROM STR_CUSTOMER; -- 1 rows
SELECT * FROM STR_apponly_CUSTOMER; -- 1 row


create table tbl_cust2 as select * from TBL_CUSTOMER;

--Load the data into table from the stream ( Consuming Data from stream )
--Method1: Insert into table from stream
insert into tbl_cust2 select  CUST_CODE,CUST_NAME,CUST_CITY,WORKING_AREA,CUST_COUNTRY,GRADE,OPENING_AMT,	RECEIVE_AMT,PAYMENT_AMT,OUTSTANDING_AMT,PHONE_NO,AGENT_CODE,
from STR_CUSTOMER
where metadata$isupdate= false and metadata$action='Insert';

--Method2: loading data to user stage from stream
COPY INTO @~/cust_data/cust FROM (SELECT * FROM STR_CUSTOMER);

select * from STR_CUSTOMER; --0 rows, since all data is consumed by a table in method1 or user stage in method2

list @~/cust_data;

--Lets perform the update again on the source table and check the stream, 

update TBL_CUSTOMER set PHONE_NO='7472832734' where CUST_CODE='C00026';

SELECT * FROM STR_CUSTOMER;  -- 2 rows as older record is shown as deleted and newer record shows as inserted with bith isupdate flag as true. 

//Why older record is marked as delete? Because it was already consumed by the table from the stream I guess :)

--Stream has no Timetravel and Failsafe feature because it has its own data rentention period which is 14 days by default abd it is not dependent on table's timetravel period.( Even if TT is 0 days for source table, it doesnt impact stream's retention period).
--once the stream is dropped, it cannot be undropped again using time travel unlike in the tables and other objects.
--We can create stream on Transient table.

--We can create stream on Temp table however, since temp tables exist only within session, any associated stream will also dissapears once session ends
create temporary table temp_table as select * from tbl_customer;

create stream str_on_temp_table on table temp_table; --created but both gets dropped once session ends.


--Note: Streams on External Table must have INSERT_ONLY set to True. Append_only works for only snowflake's internal tables not for external tables.


--Note:
--Recreating  stream on any table or view makes the stream become stale.
--Renaming source object doesnt cause stream go for stale or break.
--If source obj is dropped and new obj is created with same name then any stream linked to the original obj are not linked to new obj.


--Streams can be created on views (Both standard views and secure views) but not on materialized views.

create view V_Customertbl as select * from tbl_customer;

create stream str_on_viewcust on view V_Customertbl;

--Views with group by,qualify,limit,distinct and sub queries not in from clause there are all not supported.

create view V_Customertbl_lim10 as select * from tbl_customer limit 10;

create stream str_on_viewcust_lim10 on view V_Customertbl_lim10; --Error: Change tracking is not supported on queries with LIMIT.

create materialized view MV_Customertbl as select * from tbl_customer;--Error because MV are supported only on Enterprise and above editions and not in standard editions and also it is not supported during free trail
