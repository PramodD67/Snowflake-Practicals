create or replace function calculate_profit(max_retail_p number,purchase_p number, sold_qty number)
returns number
as 
$$
select ((max_retail_p - purchase_p) * sold_qty)
$$;
-------
--call function:
select calculate_profit(50,30,1);
------
show functions;
show functions in database db_mydb;
show functions in schema sc_myschema;

--Use system function (Builtin function) result_scan() and last_query_id() to know the result of last query:
select * from table(result_scan(last_query_id()));


--To hide the underlying tables and defn details for an UDF, You can create secure function. so that  defn details are only visible to authorized users. ( Body will be hidden when you desc function)

create secure function Country_names( Countrycode string)
returns string
as 
$$
case 
when countrycode='91' then 'India'
end 
$$;

select country_names('91');
-----------------------------
desc function Country_names(string);
desc function calculate_profit(number,number,number);