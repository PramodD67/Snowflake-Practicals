--SELECT ALL CUSTOMERS WHO HAVE MADE PURCHASE ON EVERY MONTH SINCE FROM LAST 6 MONTHS----------
select * from Sales;

select  to_date(dateadd('month',-6,current_date)) as sixmonths from dual;
select  month(PURCHASE_DATE) from Sales; --months in numbers
select  MONTHNAME(date_trunc('MONTH',PURCHASE_DATE)) from Sales; --MONTH IN WORDS

CREATE TABLE Sales (
    customer_id INT,
    customer VARCHAR(255),
    product_id INT,
    product VARCHAR(255),
    purchase_date DATE,
    PRIMARY KEY (customer_id, product_id)
);

INSERT INTO Sales (customer_id, customer, product_id, product, purchase_date)
VALUES    
    (6, 'John', 106, 'Gaming Console', '2025-04-15'),
    (6, 'John', 107, 'Wireless Earbuds', '2025-03-10'),
    (6, 'John', 108, 'Bluetooth Speaker', '2025-02-20'),
    (6, 'John', 109, 'Smart TV', '2025-01-25'),
    (6, 'John', 110, 'Home Theater System', '2024-12-05'),
    (6, 'John', 111, 'VR Headset', '2024-11-18'),

    (1, 'Alice', 101, 'Laptop', '2025-05-12'),
    (2, 'Bob', 102, 'Smartphone', '2025-04-22'),
    (3, 'Charlie', 103, 'Tablet', '2025-03-08'),
    (4, 'David', 104, 'Headphones', '2025-02-15'),
    (5, 'Eve', 105, 'Smartwatch', '2025-01-30');
    

select customer from sales where customer >= all (
select customer from sales where month(PURCHASE_DATE)<month(current_date)
);



--Generate 100 sequence numbers--

WITH RECURSIVE Rec as (

  select 1 as num
  union all
  select num+1
  from Rec
  where num<100
)

select * from rec;


----CALCULATE THE PERCENTAGE OF EACH GENDER IN THE COMPANY--

CREATE TABLE Gender_table (
    id INT PRIMARY KEY,
    name VARCHAR(255),
    gender VARCHAR(10)
);

INSERT INTO Gender_table (id, name, gender)
VALUES
    (1, 'Alice', 'Female'),
    (2, 'Bob', 'Male'),
    (3, 'Charlie', 'Female'),
    (4, 'David', 'Male'),
    (5, 'Emma', 'Female');


--Remember how Group By works: Group by first groups the records and then apply a aggregation function on that group. So, it groups male and female and then apply count() on each group separately.

SELECT GENDER, (COUNT(GENDER)/ (select count(gender) from gender_table)) * 100 as Percentage
FROM GENDER_TABLE
GROUP BY GENDER;

--------------------------------------------------------------------

--SELECT ALL CUSTOMERS WHO HAVE MADE PURCHASE ON EVERY MONTH SINCE FROM LAST 6 MONTHS----------
select * from Sales;

select  to_date(dateadd('month',-6,current_date)) as sixmonths from dual;
select  month(PURCHASE_DATE) from Sales; --months in numbers
select  MONTHNAME(date_trunc('MONTH',PURCHASE_DATE)) from Sales; --MONTH IN WORDS

CREATE TABLE Sales (
    customer_id INT,
    customer VARCHAR(255),
    product_id INT,
    product VARCHAR(255),
    purchase_date DATE,
    PRIMARY KEY (customer_id, product_id)
);

INSERT INTO Sales (customer_id, customer, product_id, product, purchase_date)
VALUES    
    (6, 'John', 106, 'Gaming Console', '2025-04-15'),
    (6, 'John', 107, 'Wireless Earbuds', '2025-03-10'),
    (6, 'John', 108, 'Bluetooth Speaker', '2025-02-20'),
    (6, 'John', 109, 'Smart TV', '2025-01-25'),
    (6, 'John', 110, 'Home Theater System', '2024-12-05'),
    (6, 'John', 111, 'VR Headset', '2024-11-18'),

    (1, 'Alice', 101, 'Laptop', '2025-05-12'),
    (2, 'Bob', 102, 'Smartphone', '2025-04-22'),
    (3, 'Charlie', 103, 'Tablet', '2025-03-08'),
    (4, 'David', 104, 'Headphones', '2025-02-15'),
    (5, 'Eve', 105, 'Smartwatch', '2025-01-30');
    

select customer from sales where customer >= all (
select customer from sales where month(PURCHASE_DATE)<month(current_date)
);
-----------------------------------------------------------------------------------

--Recursive CTE:

select current_date;

with RECURSIVE rec_cte as (


 select current_date as today
 union all
 select today+ 1
 from rec_cte
 where today< current_date+10
 
)
select * from rec_cte;








with recursive rec_cte as (
     select last_day(current_date) as currmonth
     union all
     select currmonth+day(last_day(current_date))
     from rec_cte
     where currmonth < current_date+90
)

select * from rec_cte;