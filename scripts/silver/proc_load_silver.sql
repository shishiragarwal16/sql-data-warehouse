/*
Stored Procedure: Load Silver Layer (Data transfer from Bronze to Schema Schema
==================================================

This stored procedure loads data into the 'silwr' schema from external input CSV Files.
Multiple transformations are performed on bronze schema tables and then inserted into silver schemas
It truncates the silver tables before loading data
It uses INSERT command to load data from csv files to bronze tables.


Parameters:
	None.
This stored procedure does not accept any parameters or return any values.

For usage:
EXEC silver.load_bronze;
==================================================
*/
CREATE OR ALTER PROCEDURE silver.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME;
	DECLARE @tabular_start_time DATETIME , @tabular_end_time DATETIME;
	DECLARE @silver_start_time DATETIME , @silver_end_time DATETIME;
	BEGIN TRY
		SET @silver_start_time= GETDATE();
		PRINT '---------------------------------------------------'
		PRINT 'Loading SILVER Layer'
		PRINT '---------------------------------------------------'

		SET @tabular_start_time= GETDATE();
		PRINT '---------------------------------------------------'
		PRINT 'Loading CRM Tables'
		PRINT '---------------------------------------------------'

		SET @start_time =GETDATE();
		PRINT '>>TRUNCATING silver.crm_cust_info<<'
		TRUNCATE TABLE silver.crm_cust_info
		PRINT '>>INSERTING INTO silver.crm_cust_info<<'

		insert into silver.crm_cust_info(cst_id, 
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status, 
		cst_gndr, 
		cst_create_date)
		select
		cst_id,
		cst_key,
		TRIM(cst_firstname) as cst_firstname,
		TRIM(cst_lastname) as cst_lastname,
		CASE
			WHEN UPPER(TRIM(cst_marital_status))='S' THEN 'Single'
			WHEN UPPER(TRIM(cst_marital_status)) ='M' THEN 'Married'
			ELSE 'Unknown'
		END as cst_marital_status,

		CASE
			WHEN UPPER(TRIM(cst_gndr))='M' THEN 'Male'
			WHEN UPPER(TRIM(cst_gndr)) ='F' THEN 'Female'
			ELSE 'Unknown'
		END AS cst_gndr,
		cst_create_date
		from
		(

		SELECT
		*,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date) rnk
		from bronze.crm_cust_info
		where cst_id is not null)t
		where rnk=1
		SET @end_time =GETDATE();
		PRINT '>>Load Duration: ' +CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds' ;
		
		/* Transformations for silver.crm_prd_info:
			-Defining prd_cat for erp_px_cat_g1v2
			-Defining prd_id for crm_sales_details
			-Trimming product names
			-Replacing Null with 0 in product cost
			-Replacing Abbrev
			-fix startdate>enddate
			-change ddl

			*/
		SET @start_time =GETDATE();
		PRINT '>>TRUNCATING silver.crm_prd_info<<'
		TRUNCATE TABLE silver.crm_prd_info
		PRINT '>>INSERTING INTO silver.crm_prd_info<<'
		INSERT INTO silver.crm_prd_info (
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt)
		SELECT
		prd_id,
		REPLACE(SUBSTRING(prd_key,1,5), '-','_') as cat_id,
		SUBSTRING(prd_key, 7, len(prd_key)) as prd_key,
		TRIM(prd_nm) as prd_nm,
		ISNULL(prd_cost,0) as prd_cost,
		CASE UPPER(TRIM(prd_line))
			WHEN 'M' THEN 'Mountain'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'R' THEN 'Road'
			WHEN 'T' THEN 'Touring'
			ELSE 'Unknown'
		END as prd_line,
		CAST(prd_start_dt AS DATE) as prd_start_dt,
		CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1
		AS DATE) as prd_end_dt
		from bronze.crm_prd_info
		SET @end_time =GETDATE();
		PRINT '>>Load Duration: ' +CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds' ;
		
		SET @start_time =GETDATE();
		PRINT '>>TRUNCATING silver.crm_sales_details<<'
		TRUNCATE TABLE silver.crm_sales_details
		PRINT '>>INSERTING INTO silver.crm_sales_details<<'
		insert into silver.crm_sales_details
		(sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price)

		select
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE 
			when sls_order_dt<=0 or len(sls_order_dt)!=8 THEN NULL
			else CAST(cast(sls_order_dt AS VARCHAR) AS DATE) 
		END as sls_order_dt,

		CASE 
			when sls_ship_dt<=0 or len(sls_ship_dt)!=8 THEN NULL
			else CAST(cast(sls_ship_dt AS VARCHAR) AS DATE) 
		END as sls_ship_dt,
		CASE 
			when sls_due_dt<=0 or len(sls_due_dt)!=8 THEN NULL
			else CAST(cast(sls_due_dt AS VARCHAR) AS DATE) 
		END as sls_due_dt,
		CASE 
			WHEN sls_sales<=0 or sls_sales is NULL or sls_sales!= sls_quantity* abs(sls_price) then sls_quantity* abs(sls_price)
			else sls_sales
		END as sls_sales,

		sls_quantity,
		CASE WHEN sls_price is null or sls_price<=0
			THEN sls_sales/ nullif(sls_quantity,0)
			else sls_price
		end as sls_price
		from bronze.crm_sales_details
		SET @end_time =GETDATE();
		PRINT '>>Load Duration: ' +CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds' ;
		
		SET @tabular_end_time =GETDATE();
		PRINT '>>Load Duration of CRM Tables: ' +CAST(DATEDIFF(second, @tabular_start_time, @tabular_end_time) AS NVARCHAR) + 'seconds' ;

		SET @tabular_start_time= GETDATE();
		PRINT '---------------------------------------------------'
		PRINT 'Loading ERP Tables'
		PRINT '---------------------------------------------------'
		SET @start_time =GETDATE();
		PRINT '>>TRUNCATING silver.erp_cust_az12<<'
		TRUNCATE TABLE silver.erp_cust_az12
		PRINT '>>INSERTING INTO erp_cust_az12<<'
		insert into silver.erp_cust_az12(
		cid,
		bdate,
		gen )
		SELECT
		CASE
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4, LEN(cid)) 
		ELSE cid
		END as cid,
		CAST(CASE 
			WHEN bdate> getdate() then NULL
			ELSE bdate
		END as DATE) AS bdate,
		CASE  
		WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		ELSE 'Unknown'
		END as gen
		from bronze.erp_cust_az12
		SET @end_time =GETDATE();
		PRINT '>>Load Duration: ' +CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds' ;
		
		SET @start_time =GETDATE();
		PRINT '>>TRUNCATING silver.erp_loc_a101<<'
		TRUNCATE TABLE silver.erp_loc_a101
		PRINT '>>INSERTING INTO silver.erp_loc_a101<<'
		insert into silver.erp_loc_a101(
		cid, 
		cntry
		)
		select
		replace(cid,'-', '') cid,
		CASE 
		WHEN TRIM(cntry)='DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		WHEN TRIM(cntry) ='' OR TRIM(cntry) is NULL THEN 'Unknown'
		ELSE TRIM(cntry)
		END as cntry
		from bronze.erp_loc_a101
		SET @end_time =GETDATE();
		PRINT '>>Load Duration: ' +CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds' ;
		
		SET @start_time =GETDATE();
		PRINT '>>TRUNCATING silver.erp_px_cat_g1v2<<'
		TRUNCATE TABLE silver.erp_px_cat_g1v2
		PRINT '>>INSERTING INTO silver.erp_px_cat_g1v2<<'
		INSERT INTO silver.erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		maintenance)

		SELECT
		id,
		trim(cat),
		trim(subcat),
		trim(maintenance)
		from bronze.erp_px_cat_g1v2

		SET @end_time =GETDATE();
		PRINT '>>Load Duration: ' +CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds' ;

		SET @tabular_end_time =GETDATE();
		PRINT '>>Load Duration of ERP Tables: ' +CAST(DATEDIFF(second, @tabular_start_time, @tabular_end_time) AS NVARCHAR) + 'seconds' ;

		SET @silver_end_time =GETDATE();

		PRINT '--------------------------------------------------------------------------------------' ;
		PRINT '>>Load Duration of Bronze Layer: ' +CAST(DATEDIFF(second, @silver_start_time, @silver_end_time) AS NVARCHAR) + 'seconds' ;
		PRINT '--------------------------------------------------------------------------------------' ;

	END TRY
	BEGIN CATCH
		PRINT '-------------------------------------------------------'
		PRINT 'ERROR occured while loading'
		PRINT 'ERROR: '+ ERROR_MESSAGE();
		PRINT 'ERROR: '+ CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR: '+ CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '-------------------------------------------------------'
	END CATCH
END 