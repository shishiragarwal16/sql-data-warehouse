EXEC silver.load_bronze;

select count(*) from silver.crm_cust_info
select count(*) from silver.crm_prd_info
select count(*) from silver.crm_sales_details
select count(*) from silver.erp_cust_az12
select count(*) from silver.erp_loc_a101
select count(*) from silver.erp_px_cat_g1v2