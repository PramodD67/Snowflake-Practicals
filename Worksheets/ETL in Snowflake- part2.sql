use schema landing_zone;

--Lets create 3 External stages for each delta file
--Arrival in S3 Bucket.

CREATE or replace STORAGE INTEGRATION my_s3_integration2
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE 
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::277707120370:role/Role_SnowflakeAccess'
STORAGE_ALLOWED_LOCATIONS = ('*');

list @;
DESC INTEGRATION my_s3_integration2;
DESC stage stg_ext_cust_simple;

 -- order stage
    create or replace stage delta_orders_s3
   url='s3://mys3bucketpramod/delta/Orders/'
   STORAGE_INTEGRATION = my_s3_integration2;
        comment = 'feed delta order files';
        
    -- item stage
    create or replace stage delta_items_s3
      url='s3://mys3bucketpramod/delta/items/' 
        STORAGE_INTEGRATION = my_s3_integration2;
        comment = 'feed delta item files';

    -- customer stage
    create or replace stage delta_customer_s3
        url = 's3://mys3bucketpramod/delta/customers/' 
          STORAGE_INTEGRATION = my_s3_integration2;
        comment = 'feed delta customer files';
        
    show stages;

    --------------------------

--Lets create Pipes to push these delta files via copy command:

--Make sure each pipe has auto_ingest=True

create or replace pipe order_pipe
auto_ingest=true
as 
copy into tbl_landingzone_orders from @delta_orders_s3
file_format=(type='csv' compression=none)
on_error='continue'
pattern='.*orders.*[.]csv';


create or replace pipe customer_pipe
auto_ingest=true
as 
copy into tbl_landingzone_customers from @delta_customer_s3
file_format=(type='csv' compression=none)
on_error='continue'
pattern='.*cust.*[.]csv';

create or replace pipe items_pipe
auto_ingest=true
as 
copy into tbl_landingzone_items from @delta_items_s3
file_format=(type='csv' compression=none)
on_error='continue'
pattern='.*items.*[.]csv';

--check status if pipes:

select system$pipe_status('customer_pipe');

show pipes;

--copy the arn from notification_channel column by executing 'show pipes' command ( Bust Auto_ingest must be true inorder to get the arn no.)
--Got to AWS S3 bucket> select your bucket> go to properties > Create event notification > Give name, path, format and then > select SQS > paste the arn number. ( create 3 event notification for all three entities (cust,order,items))

-------------------------------------------------------

--PART 5 :

--Create Streams under landing zone and 
--Create tasks under curated zone.
--Resume and validate the task and make sure it is running in curated zone.

--NOTE: Cross Schema Task Linking is not possible in snowflake : Means Task from one schema cannot call task from another schema.


use schema landing_zone;

--Create 3 streams (items,orders,cust) all of them are append only streams

create stream str_landing_items on table tbl_landingzone_items
append_only=true;

create stream str_landing_orders on table tbl_landingzone_orders
append_only=true;

create stream str_landing_customers on table tbl_landingzone_customers
append_only=true;


select * from str_landing_customers;
------------
table tbl_curatedzone_orders;

use schema curated_zone;


create or replace task task_curated_orders
warehouse=compute_wh
schedule= '1 minute'
when system$stream_has_data('landing_zone.str_landing_orders')
as
merge into db_mydb.curated_zone.tbl_curatedzone_orders curated_order 
        using db_mydb.landing_zone.str_landing_orders landing_order_stm on
        curated_order.order_date = landing_order_stm.order_date and 
        curated_order.order_time = landing_order_stm.order_time and 
        curated_order.item_id = landing_order_stm.item_id and
        curated_order.item_desc = landing_order_stm.item_desc 
      when matched 
         then update set 
            curated_order.customer_id = landing_order_stm.customer_id,
            curated_order.salutation = landing_order_stm.salutation,
            curated_order.first_name = landing_order_stm.first_name,
            curated_order.last_name = landing_order_stm.last_name,
            curated_order.store_id = landing_order_stm.store_id,
            curated_order.store_name = landing_order_stm.store_name,
            curated_order.order_quantity = landing_order_stm.order_quantity,
            curated_order.sale_price = landing_order_stm.sale_price,
            curated_order.disount_amt = landing_order_stm.disount_amt,
            curated_order.coupon_amt = landing_order_stm.coupon_amt,
            curated_order.net_paid = landing_order_stm.net_paid,
            curated_order.net_paid_tax = landing_order_stm.net_paid_tax,
            curated_order.net_profit = landing_order_stm.net_profit
          when not matched then 
          insert (
            order_date ,
            order_time ,
            item_id ,
            item_desc ,
            customer_id ,
            salutation ,
            first_name ,
            last_name ,
            store_id ,
            store_name ,
            order_quantity ,
            sale_price ,
            disount_amt ,
            coupon_amt ,
            net_paid ,
            net_paid_tax ,
            net_profit ) 
          values (
            landing_order_stm.order_date ,
            landing_order_stm.order_time ,
            landing_order_stm.item_id ,
            landing_order_stm.item_desc ,
            landing_order_stm.customer_id ,
            landing_order_stm.salutation ,
            landing_order_stm.first_name ,
            landing_order_stm.last_name ,
            landing_order_stm.store_id ,
            landing_order_stm.store_name ,
            landing_order_stm.order_quantity ,
            landing_order_stm.sale_price ,
            landing_order_stm.disount_amt ,
            landing_order_stm.coupon_amt ,
            landing_order_stm.net_paid ,
            landing_order_stm.net_paid_tax ,
            landing_order_stm.net_profit );
----------------------------------------------------------
      create or replace task task_curated_customers
          warehouse = compute_wh 
          schedule  = '2 minute'
      when
          system$stream_has_data('str_landing_customers') AND system$stream_has_data('str_landing_orders')
      as
      merge into db_mydb.curated_zone.tbl_curatedzone_customers curated_customer 
      using db_mydb.landing_zone.str_landing_customers landing_customer_stm on
      curated_customer.customer_id = landing_customer_stm.customer_id
      when matched 
         then update set 
            curated_customer.salutation = landing_customer_stm.salutation,
            curated_customer.first_name = landing_customer_stm.first_name,
            curated_customer.last_name = landing_customer_stm.last_name,
            curated_customer.birth_day = landing_customer_stm.birth_day,
            curated_customer.birth_month = landing_customer_stm.birth_month,
            curated_customer.birth_year = landing_customer_stm.birth_year,
            curated_customer.birth_country = landing_customer_stm.birth_country,
            curated_customer.email_address = landing_customer_stm.email_address
      when not matched then 
        insert (
          customer_id ,
          salutation ,
          first_name ,
          last_name ,
          birth_day ,
          birth_month ,
          birth_year ,
          birth_country ,
          email_address ) 
        values (
          landing_customer_stm.customer_id ,
          landing_customer_stm.salutation ,
          landing_customer_stm.first_name ,
          landing_customer_stm.last_name ,
          landing_customer_stm.birth_day ,
          landing_customer_stm.birth_month ,
          landing_customer_stm.birth_year ,
          landing_customer_stm.birth_country ,
          landing_customer_stm.email_address );
------------------------------------------------------------------

create or replace task task_curated_items
warehouse=compute_wh
schedule='3 minute'
when system$stream_has_data('landing_zone.str_landing_items')
as merge into db_mydb.curated_zone.tbl_curatedzone_items item using db_mydb.landing_zone.str_landing_items landing_item_stm on
      item.item_id = landing_item_stm.item_id and 
      item.item_desc = landing_item_stm.item_desc and 
      item.start_date = landing_item_stm.start_date
      when matched 
         then update set 
            item.end_date = landing_item_stm.end_date,
            item.price = landing_item_stm.price,
            item.item_class = landing_item_stm.item_class,
            item.item_category = landing_item_stm.item_category
      when not matched then 
        insert (
          item_id,
          item_desc,
          start_date,
          end_date,
          price,
          item_class,
          item_category) 
        values (
          landing_item_stm.item_id,
          landing_item_stm.item_desc,
          landing_item_stm.start_date,
          landing_item_stm.end_date,
          landing_item_stm.price,
          landing_item_stm.item_class,
          landing_item_stm.item_category);
-----------------------------------------------------------------

--Lets resume the task:

alter task task_curated_items resume;
alter task task_curated_orders resume;
alter task task_curated_customers resume;

--------------------------------------------------------------------------------------------------------------------------------------

--PART 6 :

--Create Streams under Curated zone and 
--Create tasks under Consumption zone.
--Resume and validate the task and make sure it is running in curated zone.

--NOTE: Cross Schema Task Linking is not possible in snowflake : Means Task from one schema cannot call task from another schema.

use schema curated_zone;


--create streams in curated zone for all 3 tables: Here we are not creating append only stream because we have update and insert as well. ( ALthough its all upto your need)

create stream str_curated_items on table tbl_curatedzone_items;
create stream str_curated_orders on table tbl_curatedzone_orders;
create stream str_curated_customers on table tbl_curatedzone_customers;

show streams;

--We will now create a tasks in the Consumption layer.

--Customer table , Items table are Dimension tables
--Orders table is Fact table

create or replace task task_consumption_items
  warehouse = compute_wh 
  schedule  = '4 minute'
when
    sysxstem$stream_has_data('db_mydb.curated_zone.str_curated_items')
as
  merge into db_mydb.consumption_zone.tbl_consumptionzone_items item using db_mydb.curated_zone.str_curated_items curated_item_stm on
  item.item_id = curated_item_stm.item_id and 
  item.start_date = curated_item_stm.start_date and 
  item.item_desc = curated_item_stm.item_desc
when matched 
  and curated_item_stm.METADATA$ACTION = 'INSERT'
  and curated_item_stm.METADATA$ISUPDATE = 'TRUE'
  then update set 
      item.end_date = curated_item_stm.end_date,
      item.price = curated_item_stm.price,
      item.item_class = curated_item_stm.item_class,
      item.item_category = curated_item_stm.item_category
when matched 
  and curated_item_stm.METADATA$ACTION = 'DELETE'
  and curated_item_stm.METADATA$ISUPDATE = 'FALSE'
  then update set 
      item.active_flag = 'N',
      updated_timestamp = current_timestamp()
when not matched 
  and curated_item_stm.METADATA$ACTION = 'INSERT'
  and curated_item_stm.METADATA$ISUPDATE = 'FALSE'
then 
  insert (
    item_id,
    item_desc,
    start_date,
    end_date,
    price,
    item_class,
    item_category) 
  values (
    curated_item_stm.item_id,
    curated_item_stm.item_desc,
    curated_item_stm.start_date,
    curated_item_stm.end_date,
    curated_item_stm.price,
    curated_item_stm.item_class,
    curated_item_stm.item_category);
        
---------------------------------------------
create or replace task task_consumption_customers
    warehouse = compute_wh 
schedule  = '5 minute'
when
  system$stream_has_data('db_mydb.curated_zone.str_curated_customers')
as
  merge into db_mydb.consumption_zone.tbl_consumptionzone_customers customer using db_mydb.curated_zone.str_curated_customers curated_customer_stm on
  customer.customer_id = curated_customer_stm.customer_id 
when matched 
  and curated_customer_stm.METADATA$ACTION = 'INSERT'
  and curated_customer_stm.METADATA$ISUPDATE = 'TRUE'
  then update set 
      customer.salutation = curated_customer_stm.salutation,
      customer.first_name = curated_customer_stm.first_name,
      customer.last_name = curated_customer_stm.last_name,
      customer.birth_day = curated_customer_stm.birth_day,
      customer.birth_month = curated_customer_stm.birth_month,
      customer.birth_year = curated_customer_stm.birth_year,
      customer.birth_country = curated_customer_stm.birth_country,
      customer.email_address = curated_customer_stm.email_address
when matched 
  and curated_customer_stm.METADATA$ACTION = 'DELETE'
  and curated_customer_stm.METADATA$ISUPDATE = 'FALSE'
  then update set 
      customer.active_flag = 'N',
      customer.updated_timestamp = current_timestamp()
when not matched 
  and curated_customer_stm.METADATA$ACTION = 'INSERT'
  and curated_customer_stm.METADATA$ISUPDATE = 'FALSE'
then 
  insert (
    customer_id ,
    salutation ,
    first_name ,
    last_name ,
    birth_day ,
    birth_month ,
    birth_year ,
    birth_country ,
    email_address ) 
  values (
    curated_customer_stm.customer_id ,
    curated_customer_stm.salutation ,
    curated_customer_stm.first_name ,
    curated_customer_stm.last_name ,
    curated_customer_stm.birth_day ,
    curated_customer_stm.birth_month ,
    curated_customer_stm.birth_year ,
    curated_customer_stm.birth_country ,
    curated_customer_stm.email_address);
----------------------------------------------
create or replace task task_consumption_orders
warehouse = compute_wh 
schedule  = '6 minute'
when
  system$stream_has_data('db_mydb.curated_zone.str_curated_orders')
as
insert overwrite into db_mydb.consumption_zone.tbl_consumptionzone_orders (
order_date,
customer_dim_key ,
item_dim_key ,
order_count,
order_quantity ,
sale_price ,
disount_amt ,
coupon_amt ,
net_paid ,
net_paid_tax ,
net_profit) 
select 
      co.order_date,
      cd.customer_dim_key ,
      id.item_dim_key,
      count(1) as order_count,
      sum(co.order_quantity) ,
      sum(co.sale_price) ,
      sum(co.disount_amt) ,
      sum(co.coupon_amt) ,
      sum(co.net_paid) ,
      sum(co.net_paid_tax) ,
      sum(co.net_profit)  
  from db_mydb.curated_zone.tbl_curatedzone_orders co 
    join db_mydb.consumption_zone.tbl_consumptionzone_customers cd on cd.customer_id = co.customer_id
    join db_mydb.consumption_zone.tbl_consumptionzone_items id on id.item_id = co.item_id and id.item_desc = co.item_desc and id.end_date is null
    group by 
        co.order_date,
        cd.customer_dim_key ,
        id.item_dim_key
        order by co.order_date; 
              
alter task task_consumption_items resume;
alter task task_consumption_orders resume;
alter task task_consumption_customers resume;
        

select *  from table(information_schema.task_history()) 
where name in ('TASK_CONSUMPTION_ITEMS' ,'TASK_CONSUMPTION_CUSTOMERS','TASK_CONSUMPTION_ORDERS')
order by scheduled_time;


----------------------------------------------------------
--PART-7: upload the delta file in s3 bucket and check all the steps from landing zone to consumption to make sure everything is working fine.


show tables;


select *  from table(information_schema.task_history()) 
where name in ('TASK_CURATED_CUSTOMERS')
order by scheduled_time;

