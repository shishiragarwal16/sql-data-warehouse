/*
Purpose:
	This script is used to perform quality checks on the views created in the gold layer
*/

select * from gold.fact_sales


-- checking if dimensions can join the facts perfectly
select *
from gold.fact_sales s
left join gold.dim_customers as c
on s.customer_key= c.customer_key
where  c.customer_key is null --no result=good

select *
from gold.fact_sales s
left join gold.dim_products as c
on s.product_key= c.product_key
where  c.product_key is null -- no result= good

