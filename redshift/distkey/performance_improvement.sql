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

------------------------------------------------------------------
diststyle                sortkey1
KEY(listid)	             dateid
-------------------------------------------------------------------

    
explain
select sellerid, qtysold, pricepaid
 from sales s
 join users u on u.userid = s.sellerid
 where u.city='Omaha';

--below output shows that the cost associated with this is 5526231, now we will make some changes and will see this cost again

/*
XN Hash Join DS_BCAST_INNER (cost=624.99..5526231.45 rows=164 width=22)
Hash Cond: ("outer".sellerid = "inner".userid)
-> XN Seq Scan on sales s (cost=0.00..1724.56 rows=172456 width=22)
-> XN Hash (cost=624.88..624.88 rows=46 width=4)
-> XN Seq Scan on users u (cost=0.00..624.88 rows=46 width=4)
Filter: ((city)::text = 'Omaha'::text)
*/



Alter table sales alter distkey sellerid;

select diststyle, sortkey1 from svv_table_info
WHERE "schema" = 'public' AND "table" = 'sales';

------------------------------------------------------------------
diststyle                sortkey1
KEY(sellerid)	         dateid
-------------------------------------------------------------------

explain
select sellerid, qtysold, pricepaid
 from sales s
 join users u on u.userid = s.sellerid
 where u.city='Omaha';

--see the cost now which is 6K only

/*
XN Hash Join DS_DIST_NONE (cost=624.99..6231.45 rows=164 width=22)
Hash Cond: ("outer".sellerid = "inner".userid)
-> XN Seq Scan on sales s (cost=0.00..1724.56 rows=172456 width=22)
-> XN Hash (cost=624.88..624.88 rows=46 width=4)
-> XN Seq Scan on users u (cost=0.00..624.88 rows=46 width=4)
Filter: ((city)::text = 'Omaha'::text)
*/

--#################### so in above, performance increased from 5526231 to 6231 ######################################

-- Now create a similar table and use even distribution style and see the cost associated with the similar query

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

/*XN Hash Join DS_BCAST_INNER (cost=624.99..5526231.45 rows=164 width=22)
Hash Cond: ("outer".sellerid = "inner".userid)
-> XN Seq Scan on sales_even s (cost=0.00..1724.56 rows=172456 width=22)
-> XN Hash (cost=624.88..624.88 rows=46 width=4)
-> XN Seq Scan on users u (cost=0.00..624.88 rows=46 width=4)
Filter: ((city)::text = 'Omaha'::text)*/
    
--###################Improve Query Performance with Sortkey#####################################################

explain
select * from sales where pricepaid < 1000;

/*XN Seq Scan on sales (cost=0.00..2155.70 rows=144921 width=64)
Filter: (pricepaid < 1000.00)*/

create table sales_qty 
sortkey (pricepaid)
as 
select * from sales;

explain
select * from sales_qty where pricepaid < 1000;

/*
XN Seq Scan on sales_qty (cost=0.00..1821.13 rows=145691 width=64)
Filter: (pricepaid < 1000.00)
*/

--########################## so in above, performance increased from 2155 to 1821######################################

--practically when I run below 2 queries, I found that 1st query took 40 second and 2nd query took 22 seconds

select * from sales where pricepaid < 1000

select * from sales_qty where pricepaid < 1000


