--WORKING WITH PARQUET FILE FORMAT;


create or replace table customer_parquet_ff(
        my_data variant
    ) 
    STAGE_FILE_FORMAT = (TYPE = PARQUET);

list @%customer_parquet/;
    -- now lets query the data using $ notation
    select 
        metadata$filename, 
        metadata$file_row_number,
        $1:CUSTOMER_KEY::varchar,
        $1:NAME::varchar,
        $1:ADDRESS::varchar,
        $1:COUNTRY_KEY::varchar,
        $1:PHONE::varchar,
        $1:ACCT_BAL::decimal(10,2),
        $1:MKT_SEGMENT::varchar,
        $1:COMMENT::varchar
        from @%customer_parquet_ff ;

copy into customer_parquet_ff from @%customer_parquet_ff/customer.snappy.parquet;
      
      select * from  customer_parquet_ff;

copy into customer_parquet_ff 
        from @%customer_parquet_ff/customer.snappy.parquet
        force=true;

drop table my_customer;
    create or replace table my_customer (
        CUST_KEY NUMBER(38,0),
        NAME VARCHAR(25),
        ADDRESS VARCHAR(40),
        NATION_KEY NUMBER(38,0),
        PHONE VARCHAR(15),
        ACCOUNT_BALANCE NUMBER(12,2),
        MARKET_SEGMENT VARCHAR(10),
        COMMENT VARCHAR(117)
    );
    
    --lets see if it has any record
    select * from my_customer;