CREATE or replace TABLE  "TBL_CUSTOMER2" 
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

SELECT * FROM TBL_CUSTOMER2;

create or replace stream str_customer2
on table TBL_CUSTOMER2;

--Lets create Append only stream:
create or replace stream str_append_customer2
on table TBL_CUSTOMER2 
append_only=True;

SELECT * FROM STR_CUSTOMER2; -- 0 ROWS
SELECT * FROM str_append_customer2; -- 0 ROWS

--Insert 10 records
INSERT INTO TBL_CUSTOMER2 VALUES ('C00013', 'Holmes', 'London', 'London', 'UK', '2', '6000.00', '5000.00', '7000.00', '4000.00', 'BBBBBBB', 'A003');
INSERT INTO TBL_CUSTOMER2 VALUES ('C00001', 'Micheal', 'New York', 'New York', 'USA', '2', '3000.00', '5000.00', '2000.00', '6000.00', 'CCCCCCC', 'A008');
INSERT INTO TBL_CUSTOMER2 VALUES ('C00020', 'Albert', 'New York', 'New York', 'USA', '3', '5000.00', '7000.00', '6000.00', '6000.00', 'BBBBSBB', 'A008');
INSERT INTO TBL_CUSTOMER2 VALUES ('C00025', 'Ravindran', 'Bangalore', 'Bangalore', 'India', '2', '5000.00', '7000.00', '4000.00', '8000.00', 'AVAVAVA', 'A011');
INSERT INTO TBL_CUSTOMER2 VALUES ('C00024', 'Cook', 'London', 'London', 'UK', '2', '4000.00', '9000.00', '7000.00', '6000.00', 'FSDDSDF', 'A006');
INSERT INTO TBL_CUSTOMER2 VALUES ('C00015', 'Stuart', 'London', 'London', 'UK', '1', '6000.00', '8000.00', '3000.00', '11000.00', 'GFSGERS', 'A003');
INSERT INTO TBL_CUSTOMER2 VALUES ('C00002', 'Bolt', 'New York', 'New York', 'USA', '3', '5000.00', '7000.00', '9000.00', '3000.00', 'DDNRDRH', 'A008');
INSERT INTO TBL_CUSTOMER2 VALUES ('C00018', 'Fleming', 'Brisban', 'Brisban', 'Australia', '2', '7000.00', '7000.00', '9000.00', '5000.00', 'NHBGVFC', 'A005');
INSERT INTO TBL_CUSTOMER2 VALUES ('C00021', 'Jacks', 'Brisban', 'Brisban', 'Australia', '1', '7000.00', '7000.00', '7000.00', '7000.00', 'WERTGDF', 'A005');
INSERT INTO TBL_CUSTOMER2 VALUES ('C00019', 'Yearannaidu', 'Chennai', 'Chennai', 'India', '1', '8000.00', '7000.00', '7000.00', '8000.00', 'ZZZZBFV', 'A010');


--Delete 5 records from table
delete from tbl_customer2 where cust_code in ('C00018','C00024','C00025','C00020','C00019');

--Now check the count of the records in stream:

SELECT * FROM STR_CUSTOMER2; -- 5 ROWS
SELECT * FROM str_append_customer2; -- 10 ROWS

--Lets consume the stream:

CREATE or replace TABLE  TBL_CUSTOMER_target
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


--Note: Copy into command doesn't works for streams directly. like: copy into target_tbl from stream;

copy into TBL_CUSTOMER_target from (select * from str_customer2); --doesnt work

insert into TBL_CUSTOMER_target select * from str_customer2; -- number of columns mismatch

insert into TBL_CUSTOMER_target select "CUST_CODE", "CUST_NAME" , "CUST_CITY", "WORKING_AREA" , "CUST_COUNTRY" , "GRADE" NUMBER,"OPENING_AMT" , "RECEIVE_AMT" , "PAYMENT_AMT","OUTSTANDING_AMT","PHONE_NO" ,"AGENT_CODE" from str_customer2; --Consumed


TABLE tbl_customer2;
SELECT * FROM STR_CUSTOMER2; -- 0 ROWS

SELECT * FROM TBL_CUSTOMER_target; -- 5 ROWS

delete from TBL_CUSTOMER2 where cust_name='Bolt';

SELECT * FROM STR_CUSTOMER2; -- 1 rows
TABLE tbl_customer2; --4

--Delete One more record
delete from TBL_CUSTOMER2 where cust_name='Jacks';

SELECT * FROM STR_CUSTOMER2; -- 2 rows
TABLE tbl_customer2; --3

update TBL_CUSTOMER2 set PHONE_NO='747283277' where CUST_CODE='C00015';

SELECT * FROM STR_CUSTOMER2; --4 -- Because it is consumed data

--Now insert one record:
INSERT INTO TBL_CUSTOMER2 VALUES ('C00044', 'Rajesh', 'Bangalore', 'Chennai', 'India', '1', '8000.00', '7000.00', '7000.00', '8000.00', 'ZZZZBFV', 'A010');

TABLE tbl_customer2; --4
SELECT * FROM STR_CUSTOMER2; --5

update TBL_CUSTOMER2 set PHONE_NO='564667676' where CUST_CODE='C00044';

TABLE tbl_customer2; --4
SELECT * FROM STR_CUSTOMER2; --5 -- Since the newly inserted record is not consumed from stream, it will not insert 2 records into stream, instead it will update the existing record. Once this record is consumed from stream then any changes made on this record will insert 2 records into stream.

--Delete same  record
delete from TBL_CUSTOMER2 where cust_name='Rajesh';

--Now insert again :
INSERT INTO TBL_CUSTOMER2 VALUES ('C00044', 'Rajesh', 'Bangalore', 'Chennai', 'India', '1', '8000.00', '7000.00', '7000.00', '8000.00', 'ZZZZBFV', 'A010');

TABLE tbl_customer2; --4
SELECT * FROM STR_CUSTOMER2; --5 -- Unless n untill stream data is consumed there will be no difference, whatever changes we make in source table, same reflects in stream.

--Now lets consume stream
insert into TBL_CUSTOMER_target select "CUST_CODE", "CUST_NAME" , "CUST_CITY", "WORKING_AREA" , "CUST_COUNTRY" , "GRADE" NUMBER,"OPENING_AMT" , "RECEIVE_AMT" , "PAYMENT_AMT","OUTSTANDING_AMT","PHONE_NO" ,"AGENT_CODE" from str_customer2; --Consumed

TABLE tbl_customer2; --4 
SELECT * FROM STR_CUSTOMER2; --0

--Delete same  record
delete from TBL_CUSTOMER2 where cust_name='Rajesh';


--Now insert again :
INSERT INTO TBL_CUSTOMER2 VALUES ('C00044', 'Rajesh', 'Bangalore', 'Chennai', 'India', '1', '8000.00', '7000.00', '7000.00', '8000.00', 'ZZZZBFV', 'A010');


update TBL_CUSTOMER2 set PHONE_NO='121212' where CUST_CODE='C00015';

TABLE tbl_customer2; --4 
SELECT * FROM TBL_CUSTOMER_target; --0






