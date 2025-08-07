--Source System --> AWS External Stage (S3) --> Snowflake Instance [LANDING ZONE, CURATED ZONE, CONSUMPTION ZONE]

-- 3 Tables in each Zone. (Item,Customer,Order) 

-- In LANDING ZONE we need : 3 Pipes, 3 Tables, 3 Streams
-- In CURATED ZONE we need : 3 Tables, 3 Tasks, 3 Streams
-- In CONSUMPTION ZONE we need : 3 Tasks, 3 Tables

--Step 1:
--Lets create 3 different Schemas called Landing, Curated and Consumption
Create schema landing_zone;
Create schema Curated_zone;
Create schema Consumption_zone;


--Step2:
--Create 3 tables in the landing zone. ( In landing zone tables will be transient and all attributes are varchar datatype to make sure all data is loaded):

use schema landing_zone;

create or replace transient table tbl_landingzone_customers( 
customer_id varchar,
    salutation varchar,
    first_name varchar,
    last_name varchar,
    birth_day varchar,
    birth_month varchar,
    birth_year varchar,
    birth_country varchar,
    email_address varchar) ;


create or replace transient table tbl_landingzone_items( 
 item_id varchar,
        item_desc varchar,
        start_date varchar,
        end_date varchar,
        price varchar,
        item_class varchar,
        item_CATEGORY varchar) ;

create or replace  transient table tbl_landingzone_orders(
    order_date varchar,
    order_time varchar,
    item_id varchar,
    item_desc varchar,
    customer_id varchar,
    salutation varchar,
    first_name varchar,
    last_name varchar,
    store_id varchar,
    store_name varchar,
    order_quantity varchar,
    sale_price varchar,
    disount_amt varchar,
    coupon_amt varchar,
    net_paid varchar,
    net_paid_tax varchar,
    net_profit varchar) ;

show tables;

---Create File Format:

create or replace file format ff_csv
type='csv'
field_delimiter=','
record_delimiter='\n'
field_optionally_enclosed_by='\042'
skip_header = 1
null_if=('\\N');

--Now lets load the history data via webui
--DONE

show tables;

--Part2: lets create a tables in curated zone and load data using simplw sql stmt:

use schema curated_zone;

--create 3 transient tables:

create or replace transient table tbl_curatedzone_customers (
      customer_pk number autoincrement,
      customer_id varchar(18),
      salutation varchar(10),
      first_name varchar(20),
      last_name varchar(30),
      birth_day number,
      birth_month number,
      birth_year number,
      birth_country varchar(20),
      email_address varchar(50)
    ) comment ='this is customer table with in curated schema';
    
    create or replace transient table tbl_curatedzone_items (
      item_pk number autoincrement,
      item_id varchar(16),
      item_desc varchar,
      start_date varchar,
      end_date varchar,
      price varchar,
      item_class varchar(50),
      item_category varchar(50)
    ) comment ='this is item table with in curated schema';

    create or replace transient table tbl_curatedzone_orders (
      order_pk number autoincrement,
      order_date varchar,
      order_time varchar,
      item_id varchar(16),
      item_desc varchar,
      customer_id varchar(18),
      salutation varchar(10),
      first_name varchar(20),
      last_name varchar(30),
      store_id varchar(16),
      store_name VARCHAR(50),
      order_quantity number,
      sale_price number(7,2),
      disount_amt number(7,2),
      coupon_amt number(7,2),
      net_paid number(7,2),
      net_paid_tax number(7,2),
      net_profit number(7,2)
    ) comment ='this is order table with in curated schema';
------------------------------

--load the data into these tables using 'insert as select' SQL statement and load the data from landing zone to curated zone:
--Here we dont have any transformations but in real world projects you will have diff transformation logics.


--Curated Customer First Time Load

    insert into curated_zone.tbl_curatedzone_customers (
      customer_id ,
      salutation ,
      first_name ,
      last_name ,
      birth_day ,
      birth_month ,
      birth_year ,
      birth_country ,
      email_address ) 
    select 
      customer_id ,
      salutation ,
      first_name ,
      last_name ,
      birth_day ,
      birth_month ,
      birth_year ,
      birth_country ,
      email_address 
    from db_mydb.landing_zone.tbl_landingzone_customers;
--------------

--Curated Dimension First Time Load
    insert into curated_zone.tbl_curatedzone_items (
        item_id,
        item_desc,
        start_date,
        end_date,
        price,
        item_class,
        item_category) 
    select 
        item_id,
        item_desc,
        start_date,
        end_date,
        price,
        item_class,
        item_category
    from db_mydb.landing_zone.tbl_landingzone_items;

    
--Curaterd Order First Time Load
    insert into tbl_curatedzone_orders (
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
      net_profit) 
    select 
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
      net_profit  
  from db_mydb.landing_zone.tbl_landingzone_orders;

  show tables;
  -----------------------------------------------------------

--  Part 3: Lets create a tables in CONSUMPTION ZONE and load data from Curated Zone:
--We will first create dimension tables : Customer and Items
--And then we will create Fact table : Order fact
--Here also we can apply transformations
--Here we use permanent table and not transient tables.

Use schema consumption_zone;

create or replace table tbl_consumptionzone_customers (
        customer_dim_key number autoincrement,
        customer_id varchar(18),
        salutation varchar(10),
        first_name varchar(20),
        last_name varchar(30),
        birth_day varchar,
        birth_month varchar,
        birth_year varchar,
        birth_country varchar(20),
        email_address varchar(50),
        added_timestamp timestamp default current_timestamp() ,
        updated_timestamp timestamp default current_timestamp() ,
        active_flag varchar(1) default 'Y'
    ) comment ='this is customer table with in curated schema';
    
    create or replace  table tbl_consumptionzone_items (
       item_dim_key number autoincrement,
        item_id varchar(16),
        item_desc varchar,
        start_date varchar,
        end_date varchar,
        price varchar,
        item_class varchar(50),
        item_category varchar(50),
        added_timestamp timestamp default current_timestamp() ,
        updated_timestamp timestamp default current_timestamp() ,
        active_flag varchar(1) default 'Y'
    ) comment ='this is item table with in curated schema';

    create or replace  table tbl_consumptionzone_orders (
      order_fact_key number autoincrement,
      order_date varchar,
      customer_dim_key varchar,
      item_dim_key varchar,
      order_count varchar,
      order_quantity varchar,
      sale_price varchar,
      disount_amt varchar,
      coupon_amt varchar,
      net_paid varchar,
      net_paid_tax varchar,
      net_profit varchar
    ) comment ='this is order table with in curated schema';


    show tables;
    --Lets insert first time data into consumption zone:

    insert into  tbl_consumptionzone_customers(
        customer_id ,
        salutation ,
        first_name ,
        last_name ,
        birth_day ,
        birth_month ,
        birth_year ,
        birth_country ,
        email_address ) 
    select 
        customer_id ,
        salutation ,
        first_name ,
        last_name ,
        birth_day ,
        birth_month ,
        birth_year ,
        birth_country ,
        email_address 
  from curated_zone.tbl_curatedzone_customers;


  insert into tbl_consumptionzone_items (
        item_id,
        item_desc,
        start_date,
        end_date,
        price,
        item_class,
        item_category) 
    select 
        item_id,
        item_desc,
        start_date,
        end_date,
        price,
        item_class,
        item_category
    from curated_zone.tbl_curatedzone_items;


    insert into tbl_consumptionzone_orders (
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
      net_profit 
    ) 
    select 
      co.order_date,
      cd.customer_dim_key ,
      id.item_dim_key,
      count(1) as order_count,
      sum(CAST(co.order_quantity as NUMBER)) ,
      sum(CAST(co.sale_price as NUMBER)) ,
      sum(CAST(co.disount_amt as NUMBER)) ,
      sum(CAST(co.coupon_amt as NUMBER)) ,
      sum(CAST(co.net_paid as NUMBER)) ,
      sum(CAST(co.net_paid_tax as NUMBER)) ,
      sum(CAST(co.net_profit as NUMBER))  
    from curated_zone.tbl_curatedzone_orders co 
    left join consumption_zone.tbl_consumptionzone_customers cd on cd.customer_id = co.customer_id
    left join consumption_zone.tbl_consumptionzone_items id on id.item_id = co.item_id and id.item_desc = co.item_desc and id.end_date is not null
    group by 
        co.order_date,
        cd.customer_dim_key ,
        id.item_dim_key
        order by co.order_date;


        -------------------------
show tables;

select * from TBL_LANDINGZONE_CUSTOMERS;

show pipes;
select system$pipe_status('CUSTOMER_PIPE'); --
alter pipe CUSTOMER_PIPE refresh;