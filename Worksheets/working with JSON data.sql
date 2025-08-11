-- READ_FILE() reads the file from the stage.
-- PARSE_JSON() converts the file content into a JSON array.
-- FLATTEN() breaks the array into individual customer objects.
-- LATERAL FLATTEN() is used again to unpack the nested ORDERS array.

-- :: notation is used to cast the data types
-- Use try_cast() when data is inconsistent

create storage integration mys3
type=external_stage
enabled=true
storage_aws_role_arn='arn:aws:iam::277707120370:role/pramoddbt_role'
storage_provider='s3'
storage_allowed_locations=('s3://pramoddbt-bucket/dbt_files_json/');

desc integration mys3;

create or replace stage stg_ext_dbt
url='s3://pramoddbt-bucket/dbt_files_json/'
storage_integration=mys3
file_format=(format_name='ff_json');


create or replace stage ext2
url='s3://pramoddbt-bucket/dbt_files_json/'
storage_integration=mys3;

desc stage ext2;

create file format ff_json
type='json';

select $1::varchar as customer_id,$2 as name,$3,$4,$5,$6,$7,$8 from @stg_ext_dbt ;

select json_variant:customer_key::int as custkey from @stg_ext_dbt;

select 
t.$1 as customer_key
from @stg_ext_dbt
(file_format=>'ff_json') t;



SELECT
$1:ACCT_BAL::NUMBER(6, 2),
$1:ADDRESS::TEXT,
$1:COMMENT::TEXT,
$1:COUNTRY_KEY::NUMBER(2, 0),
$1:CUSTOMER_KEY::NUMBER(6, 0),
$1:MKT_SEGMENT::TEXT,
$1:NAME::TEXT,
$1:PHONE::TEXT
FROM @stg_ext_dbt
(file_format=>'ff_json') ;

select * from table(infer_schema(
location=>'@stg_ext_dbt',
file_format=>'ff_json'
));

select 
$1:CUSTOMER_KEY::integer,
$1:ACCT_BAL::integer,
$1:NAME::varchar
from @stg_ext_dbt 
(file_format=>'ff_json');
----------------------------------------

--TO read Nested Json:

[
  {
    "CUSTOMER_KEY": 90001,
    "NAME": "Annette Anderson",
    "PHONE": "(621)683-7574x25585",
    "ADDRESS": {
      "street": "04391 Ball Lakes Apt. 616",
      "city": "Port Misty",
      "zip": "83397"
    },--it is OBJECT so use ADDRESS.city to access the inner data
    "ORDERS": [
      {
        "order_id": "4fbe8275-403d-4748-bf4f-eff1d8c643cb",
        "amount": 302.9,
        "date": "2025-03-07"
      }
      ]}] --it is ARRAY type so use lateral flatten

      
create file format ff_json_nest
type='json'
strip_outer_array=true;


select * from table(infer_schema(
location=>'@stg_ext_dbt/nested_customers.json',
file_format=>'ff_json_nest'
));

list @stg_ext_dbt;


select 
cust.value:"CUSTOMER_KEY"::integer,
cust.value:"NAME"::VARCHAR,
cust.value:"PHONE"::VARCHAR,
cust.value:"ADDRESS"."city"::VARCHAR,
cust.value:"ADDRESS"."street"::varchar,
cust.value:"ADDRESS"."zip"::varchar,
orders.value:"order_id"::string,
orders.value:"amount"::float,
orders.value:"date"::date
from @stg_ext_dbt/nested_customers.json
(file_format=>'ff_json'),
lateral flatten(input=>$1) cust,
lateral flatten(input=> cust.value:"ORDERS") orders;
