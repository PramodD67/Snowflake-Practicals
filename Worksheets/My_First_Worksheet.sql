create database db_mydb;
create warehouse wh_mywarehouse;
create schema sc_myschema;


show warehouses;
select current_warehouse();
select current_role();
select current_database();
select current_schema();
select current_account();

create sequence sq_myseq_incr1;
create sequence sq_myseq_incr2 start with 1 increment by 2;

show sequences;
select all_user_names();

create file format ff_csv
type='csv' compression='auto';

create table sample_tbl (Name varchar, Phoneno number, Address varchar, ps_number number);

insert into sample_tbl values ( 'PramodDB_MYDB.SC_MYSCHEMA.SAMPLE_TBL',99999999,'HB halli',100 );

select * from sample_tbl;

create storage integration intobj_s3_integration_obj
type=external_stage
storage_provider=s3
enabled='yes';

