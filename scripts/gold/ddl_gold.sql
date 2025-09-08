/*
---------------------------------------------------
DDL Script: Creating Gold View Layers
---------------------------------------------------
Purpose:
	Running this script creates views under the Gold layer.
	The architecture used is Star arch and the arch is divided 
	dimensions and facts

	Usage: Can be directly queried
	-----------------------------------------------
	*/

/* Customers Dimension Forming*/

if object_id('gold.dim_customers','V') is not null
drop view gold.dim_customers
go
create view gold.dim_customers as
SELECT
row_number() over (order by ci.cst_id) as customer_key,
ci.cst_id as customer_id,
ci.cst_key as customer_number,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
clo.cntry as country,
ci.cst_marital_status as marital_status,
CASE 
WHEN ci.cst_gndr!= 'Unknown' THEN ci.cst_gndr --CRM is the master for customer information
ELSE COALESCE (caz.gen, 'Unknown')
END as gender,
caz.bdate birth_date,
ci.cst_create_date as create_date

from silver.crm_cust_info as ci
left join silver.erp_cust_az12 as caz
on ci.cst_key= caz.cid
left join silver.erp_loc_a101 as clo
on ci.cst_key= clo.cid
go


/* Products Dimension Forming*/
if object_id('gold.dim_products','V') is not null
drop view gold.dim_products
go
create view gold.dim_products as
select
ROW_NUMBER() over (order by cpi.prd_start_dt, cpi.prd_key) product_key,
cpi.prd_id product_id,
cpi.prd_key product_number,
cpi.prd_nm product_name,
cpi.cat_id product_category_id,
epc.cat product_category,
epc.subcat product_subcategory,
cpi.prd_cost product_cost,
epc.maintenance maintenance_required,
cpi.prd_line product_line,
cpi.prd_start_dt product_start_date
from silver.crm_prd_info as cpi
left join silver.erp_px_cat_g1v2 as epc
on cpi.cat_id= epc.id
where cpi.prd_end_dt is NULL --CURRENT PRODUCTS
go

/* Sales fact Forming*/


if object_id('gold.fact_sales','V') is not null
drop view gold.fact_sales
go
create view gold.fact_sales as
select
sd.sls_ord_num order_number,
pc.product_key,
gc.customer_key,
sd.sls_order_dt order_date,
sd.sls_ship_dt shipping_date,
sd.sls_due_dt due_date,
sd.sls_sales sales_amount,
sd.sls_quantity quantity_sold,
sd.sls_price price
from silver.crm_sales_details as sd
left join gold.dim_customers as gc
on sd.sls_cust_id= gc.customer_id
left join gold.dim_products as pc
on sd.sls_prd_key= pc.product_number

go