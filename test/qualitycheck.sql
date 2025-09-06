/*
------------------------------------------------
Quality Checks
------------------------------------------------
Purpose:
	This script performs quality checks for
		data consistency,
		accuracy,
		standardization
	while loading data from bronze to silver
	It includes checks for-
		-Null or duplicate primary keys
		-Unwated spaces in string fields
		-Data Standardization and consistency
		-Invalid dates ranges
		-Data Consistency
		-Derived Columns consistency
*/

-SILVER CHECK FOR silver.crm_cust_info 
-- to check for remove duplicates
select
cst_id, 
count(*)
from silver.crm_cust_info
group by cst_id
having count(*)>1 or cst_id is null

--to check for trim firstname, and lastname
select 
cst_id, cst_firstname, cst_lastname
from silver.crm_cust_info 
where cst_firstname!= trim(cst_firstname) or cst_lastname!=trim(cst_lastname)

--to check for standardize the data
select distinct
cst_id,
cst_marital_status,
cst_gndr
from silver.crm_cust_info 

--to check the overall data
select * from silver.crm_cust_info 


--SILVER CHECKS FOR bronze.crm_prd_info

--to check if there are duplicates on prd_id
select
prd_id,
count(*)
from bronze.crm_prd_info
group by prd_id 
having count(*)>1 or prd_id is null


-- to check if prod name has spaces
select 
prd_id,
prd_nm
from silver.crm_prd_info 
where prd_nm!= trim(prd_nm)

--checking if any product has cost 0
select 
prd_id,
prd_cost
from silver.crm_prd_info 
where prd_cost is null

--checking if any end date is less than start date
select 
prd_id,
prd_start_dt,
prd_end_dt
from silver.crm_prd_info 
where prd_start_dt>prd_end_dt


--for crm.sales_details
--check if all prd_keys and cust_ids exist in products and customer tables

select
*
from bronze.crm_sales_details
where sls_prd_key not in (select prd_key from silver.crm_prd_info) --no result-good

select
*
from bronze.crm_sales_details
where sls_cust_id not in (select cst_id from silver.crm_cust_info) --no result- good
--no transformations required

--check for dates
--invalid dates
--cast to date
--if you know the company's start and the maximum working date, all dates shud lie between them
--dates are integer, first check if any date is <=0 and if any date has len over 8

select
nullif(sls_order_dt, 0) sls_order_dt
from bronze.crm_sales_details
where sls_order_dt<=0 or len(sls_order_dt)!=8
or sls_order_dt>20500101
or sls_order_dt<19000101
-- Need transformation

--check ship dt
select
nullif(sls_ship_dt, 0) sls_ship_dt
from bronze.crm_sales_details
where sls_ship_dt<=0 or len(sls_ship_dt)!=8
or sls_ship_dt>20500101
or sls_ship_dt<19000101
-- Doesnt need transformation only casting required

-- check if order dt <= ship_date and order dt< due dt

select
*
from bronze.crm_sales_details
where sls_order_dt>sls_ship_dt or sls_order_dt>sls_due_dt
-- no error, no need of any transformations

--checking for sales qty price, sales= quantity*price
--and none of these values <=0

select distinct *
from bronze.crm_sales_details
where sls_sales!= (sls_quantity*sls_price)
or sls_sales is null or sls_quantity is null or sls_price is null 
or sls_sales <=0 or sls_quantity <=0 or sls_price <=0

--need transformation- calculate one var from other
-- also convert neg to pos

--Final quality check for silver.crm_sales_details
Select distinct *
from silver.crm_sales_details
where sls_sales!= (sls_quantity*sls_price)
or sls_sales is null or sls_quantity is null or sls_price is null 
or sls_sales <=0 or sls_quantity <=0 or sls_price <=0
--fixed
--silver.crm_sales_details is fixed

--TO CHECK NOW- erp_cust_az12
SELECT
cid,
bdate,
gen
from bronze.erp_cust_az12
--cid has NAS in some cases
--check if cid from silver is in cust_info of silver
SELECT
cid,
bdate,
gen
from silver.erp_cust_az12
where cid not in (select cst_id from bronze.crm_cust_info)
-- no result- good

--check for bdate, if bdate is greater than today's date, its invalid
SELECT
cid,
bdate,
gen
from bronze.erp_cust_az12
where bdate > getdate()
-- neeeds transformation

--check for gender
SELECT distinct

gen
from bronze.erp_cust_az12
-- needs transformation- standardization


--CHECK erp_loc_a101

--check if cid matches custkey of customer info
select
cid,
cntry
from bronze.erp_loc_a101

SELECT cst_key from silver.crm_cust_info
--needs transformation remove '-'

--now check country
SELECT DISTINCT
cntry
from bronze.erp_loc_a101

--after insert checking
select
cid,
cntry
from silver.erp_loc_a101
where cid not in (select cst_key from bronze.crm_cust_info)
--working fine
--check for distinct locs
select distinct
cntry
from silver.erp_loc_a101

-- CHECK FOR erp_px_cat_g1v2id

--check for distinct cat, subcat, maintenance
SELECT distinct
cat 
from bronze.erp_px_cat_g1v2 --no transfmtn needed

SELECT distinct
subcat 
from bronze.erp_px_cat_g1v2 --no trnsfrmtiin needed

SELECT distinct
maintenance
from bronze.erp_px_cat_g1v2 -- no trnsfmtn needed

--table is already cleaned source
