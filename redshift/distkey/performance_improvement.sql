create schema if not exists sales_details;

CREATE TABLE sales_details.Sales (
    list_id INT PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    sale_date DATE NOT NULL,
    quantity INT NOT NULL,
    amount DECIMAL(10, 2) NOT NULL
)
DISTKEY (customer_id)
SORTKEY (sale_date);

CREATE TABLE sales_details.Customers (
    customer_id BIGINT PRIMARY KEY,
    customer_name VARCHAR(255),
    customer_email VARCHAR(255),
    customer_phone VARCHAR(20)
)
DISTSTYLE ALL;

INSERT INTO sales_details.Sales (list_id, customer_id, product_id, sale_date, quantity, amount) 
VALUES 
(1, 1001, 2001, '2024-01-01', 2, 150.50), 
(2, 1002, 2002, '2024-01-02', 1, 99.99), 
(3, 1003, 2003, '2024-01-03', 3, 300.00), 
(4, 1004, 2004, '2024-01-04', 4, 400.75), 
(5, 1005, 2005, '2024-01-05', 1, 50.25);


CREATE VIEW sales_details.RecentSales AS
SELECT 
    list_id,
    sale_date,
    customer_id,
    product_id,
    quantity,
    amount
FROM 
    sales_details.Sales
WHERE 
    amount > 100.00; 

--############## Improve Query Performance with Distribution Style ##########################################

select diststyle, sortkey1 from svv_table_info
WHERE "schema" = 'public' AND "table" = 'sales';

explain
select sellerid, qtysold, pricepaid
 from sales s
 join users u on u.userid = s.sellerid
 where u.city='Omaha';

Alter table sales alter distkey sellerid;

create table sales_even
diststyle even
sortkey(dateid)
AS
select * from sales;

select diststyle, sortkey1 from svv_table_info
WHERE "schema" = 'public' AND "table" = 'sales_even';

explain
select sellerid, qtysold, pricepaid
 from sales_even s
 join users u on u.userid = s.sellerid
 where u.city='Omaha'

--###################Improve Query Performance with Sortkey#####################################################

explain
select * from sales where pricepaid < 1000;

create table sales_qty 
sortkey (pricepaid)
as 
select * from sales;

explain
select * from sales_qty where pricepaid < 1000;






