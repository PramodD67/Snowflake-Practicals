create or replace stream str 
on table tb;

select * from str;
-----------------------

insert into tb22(id) values(2);
insert into tb22(id) values(null);
----------------
select * from str2; -- Records are showing as INSERT

--DROP and UNDROP table
drop table tb22;
select * from str2; -- Error: Base table is deleted, cannot read from stream 'STR2' 

undrop table tb22; 
select * from str2; -- Records are still available.
--------------------------

--RENAME the table.
ALTER table tb22 rename to tb21; 
select * from str2; -- The underlying base table name also renamed automatically.
---------------

--Truncate the table
truncate table tb21;
select * from str2; --  All Records will be marked as DELETE
---------------------

create or replace stream str2
on table tb21;

select * from str2; --- Stream will be empty
-------------------------------------
