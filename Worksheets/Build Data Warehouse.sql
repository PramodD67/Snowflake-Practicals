create database MY_ETL_DB ;

USE DATABASE MY_ETL_DB;

create schema my_schema;

create schema STAGING_LAYER;
create schema HISTORY_LAYER;
create schema BUSINESS_LAYER;
--------------------------------
use schema staging_layer;

CREATE or replace STORAGE INTEGRATION MY_ETL_S3_INTEGRATION
TYPE=EXTERNAL_STAGE
STORAGE_PROVIDER=S3
ENABLED=TRUE
STORAGE_AWS_ROLE_ARN='arn:aws:iam::277707120370:role/Role_Etl'
STORAGE_ALLOWED_LOCATIONS=('s3://mybucketforetl2025/customers/delta /');
----------------------------------

DESC STORAGE INTEGRATION MY_ETL_S3_INTEGRATION; 

create or replace file format csv_ff
TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 0
COMPRESSION = 'NONE'
null_if=('\\N');


create or replace stage stg_ext_sdl
url='s3://mybucketforetl2025/customers/delta /'
STORAGE_INTEGRATION = MY_ETL_S3_INTEGRATION;

DESC stage stg_ext_sdl;

create or replace TABLE tbl_cust_sdl (custid number,name varchar,amount float,items varchar);

--lets load history data into table, so before that we will upload the history file to 
--INTERNAL STAGE and load the data into table.

create or replace stage stg_int_sdl
file_format='csv_ff';

--I have uploaded the file into internal stage.
--lets load the data into table from internal stage

copy into tbl_cust_sdl from @stg_int_sdl; --loaded 1k rows

select * from tbl_cust_sdl; -- 1k rows

------------------------------

--Lets create pipe that loads data from  external stage to table:

create or replace pipe extstg_to_tbl 
auto_ingest=true
as 
copy into tbl_cust_sdl from @stg_ext_sdl;

show pipes;
select system$pipe_status('extstg_to_tbl'); --{"executionState":"RUNNING","pendingFileCount":0,"lastIngestedTimestamp":"2025-05-05T19:51:15.418Z","lastIngestedFilePath":"Customer_simple2_delta3.csv","notificationChannelName":"arn:aws:sqs:eu-north-1:158664118160:sf-snowpipe-AIDASJ4I6J6IIUK74WMNH-jKM4ohW9KsieekaTpzZ55g","numOutstandingMessagesOnChannel":1,"lastReceivedMessageTimestamp":"2025-05-05T18:57:50.398Z","lastPulledFromChannelTimestamp":"2025-05-05T19:51:53.888Z","pendingHistoryRefreshJobsCount":0}


--lets create a STREAM that captures CDC on table tbl_cust_sdl.
create stream str_cust_sdl
on table tbl_cust_sdl;


--Lets enable SNS Notification in AWS S3.
--Go to S3 bucket and goto properties
--look for create notification option and create a SNS event notification.
--choose Destination option as 'SQS queue'
--Enter ARN of the Pipe you created.( Desc pipe and look for notification_channel column, take it and paste it in the ARN field in the AWS) (Make sure u create pipe with auto_ingest as true, otherwise you dont get the ARN for the pipe.)


desc pipe extstg_to_tbl  ;
alter pipe extstg_to_tbl refresh;
--notification_channel= arn:aws:sqs:eu-north-1:158664118160:sf-snowpipe-AIDASJ4I6J6IIUK74WMNH-jKM4ohW9KsieekaTpzZ55g

select * from str_cust_sdl; -- no records

select * from tbl_cust_sdl; --History records 1k rows

select $1 as custid,$2 as Cust_Name,$3 as Amount,$4 as Items,  metadata$filename, metadata$file_row_number
from @stg_ext_sdl
(File_format=> 'csv_ff');  --I have faced "Access Denied 403" After thorugh checking I found out that I had created a External Stage without binding the STORAGE_INTEGRATION. (i.e I created Ext stage without STORAGE_INTEGRATION='my_etl_storage_integration' Parameter). So, Make sure to bind the storage integration.

--lets upload a delta file in the AWS and check if its auto loading by pipe.


select * from str_cust_sdl; 

select * from tbl_cust_sdl; -- Delta records auto ingested from aws. 
---------------------------------------------------------------------------------

--LETS SWITCH TO HISTORY SCHEMA:

USE SCHEMA HISTORY_LAYER;

--Lets create a Table.
create or replace TABLE tbl_cust_ohl (custid number,name varchar,amount float,items varchar);

--Lets create a task that will ingest a data from Stream in the SDL.

create OR REPLACE task task_cust_ohl
warehouse=compute_wh
schedule='1 minute'
when 
system$stream_has_data('staging_layer.str_cust_sdl')
as
merge into my_etl_db.history_layer.tbl_cust_ohl customertbl 
      using my_etl_db.staging_layer.str_cust_sdl customerstream
      on customertbl.custid = customerstream.custid
      when matched 
         then update set 
            customertbl.name = customerstream.name,
            customertbl.amount = customerstream.amount,
            customertbl.items = customerstream.items
      when not matched then 
        insert ( custid ,name ,amount ,items  ) 
        values (
          customerstream.custid,
          customerstream.name,
          customerstream.amount,
          customerstream.items
          );


---NOTE: WE HAVE TO RESUME THE TASK
SHOW TASKS; --TASK IS IN SUSPENDED STATE
ALTER TASK TASK_CUST_OHL RESUME;

select * from tbl_cust_ohl; --351 records loaded from SDL Stream to OHL table.

select * from table(information_schema.task_history()) where name='TASK_CUST_OHL';

--RESULT:
--TASK_CUST_OHL	MY_ETL_DB	HISTORY_LAYER	merge into..	SKIPPED	Conditional expression for task evaluated to false.

--TASK_CUST_OHL	MY_ETL_DB	HISTORY_LAYER	merge into... SUCCEEDED		

ALTER TASK TASK_CUST_OHL SUSPEND; 


