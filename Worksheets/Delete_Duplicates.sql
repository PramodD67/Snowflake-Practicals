
create table sample_tbl (Name varchar, Phoneno number, Address varchar, ps_number number);

insert into sample_tbl values ( 'PramodDB_MYDB.SC_MYSCHEMA.SAMPLE_TBL',99999999,'HB halli',100 );
insert into sample_tbl values ( 'PramodDB_MYDB.SC_MYSCHEMA.SAMPLE_TBL',99999999,'HB halli',101 );
insert into sample_tbl values ( 'PramodDB_MYDB.SC_MYSCHEMA.SAMPLE_TBL',99999999,'HB halli',102 );

select * from sample_tbl;

delete from sample_tbl where ps_number='xxxx';

delete from sample_tbl where ps_number in (
select  ps_number  from (
select ps_number,rownum from (
select ps_number, row_number() over (partition by Name,Phoneno,Address order by  ps_number) as rownum from sample_tbl
) where rownum>1 ));


with cte as (
select ps_number, row_number() over (partition by Name,Phoneno,Address order by  ps_number) as rownum from sample_tbl )


delete from sample_tbl where ps_number in (
select ps_number from cte where rownum>1;
)

