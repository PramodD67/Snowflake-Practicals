create OR REPLACE procedure unloadbydate()
returns string
language sql
execute as owner
as
$$

BEGIN 
let vardate:=to_char(Current_date(),'DD-MM-YYYY');
let vartime:=TO_CHAR(Current_timestamp(),'DD-MM-YYYY');
let code :='copy into @ext_stg'|| '/' || vardate ||'/'|| vartime || ' ' || 'from sales_data overwrite=true';
--let code := 'copy into @ext_stg' || '/' || vardate || ' '|| 'from sales_data overwrite=true';


EXECUTE IMMEDIATE:code;
end;

$$;