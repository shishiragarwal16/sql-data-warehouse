/*
Stored Procedure: Load Bronze Layer (Data transfer from Source to Bronze Schema
==================================================

This stored procedure loads data into the 'bronze' schema from external input CSV Files.
It truncates the bronze tables before loading data
It uses BULK INSERT command to load data from csv files to bronze tables.


Parameters:
	None.
This stored procedure does not accept any parameters or return any values.

For usage:
EXEC bronze.load_bronze;
==================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME;
	DECLARE @tabular_start_time DATETIME , @tabular_end_time DATETIME;
	DECLARE @bronze_start_time DATETIME , @bronze_end_time DATETIME;
	BEGIN TRY
		SET @bronze_start_time= GETDATE();
		PRINT '---------------------------------------------------'
		PRINT 'Loading Bronze Layer'
		PRINT '---------------------------------------------------'

		SET @tabular_start_time= GETDATE();
		PRINT '---------------------------------------------------'
		PRINT 'Loading CRM Tables'
		PRINT '---------------------------------------------------'

		SET @start_time =GETDATE();
		PRINT 'Truncating and Loading DATA Into bronze.crm_cust_info'
		TRUNCATE TABLE bronze.crm_cust_info;
		BULK INSERT bronze.crm_cust_info
		FROM 'D:\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
		SET @end_time =GETDATE();
		PRINT '>>Load Duration: ' +CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds' ;
		
		SET @start_time =GETDATE();
		PRINT 'Truncating and Loading DATA Into bronze.crm_prd_info'
		TRUNCATE TABLE bronze.crm_prd_info;
		BULK INSERT bronze.crm_prd_info
		FROM 'D:\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
		SET @end_time =GETDATE();
		PRINT '>>Load Duration: ' +CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds' ;
		
		SET @start_time =GETDATE();

		PRINT 'Truncating and Loading DATA Into bronze.crm_sales_details'
		TRUNCATE TABLE bronze.crm_sales_details;
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
		SET @end_time =GETDATE();
		PRINT '>>Load Duration: ' +CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds' ;
		SET @tabular_end_time =GETDATE();
		PRINT '>>Load Duration of CRM Tables: ' +CAST(DATEDIFF(second, @tabular_start_time, @tabular_end_time) AS NVARCHAR) + 'seconds' ;

		SET @tabular_start_time= GETDATE();
		PRINT '---------------------------------------------------'
		PRINT 'Loading ERP Tables'
		PRINT '---------------------------------------------------'
		SET @start_time= GETDATE();
		PRINT 'Truncating and Loading DATA Into bronze.erp_cust_az12'
		TRUNCATE TABLE bronze.erp_cust_az12;
		BULK INSERT bronze.erp_cust_az12
		FROM 'D:\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
		SET @end_time =GETDATE();
		PRINT '>>Load Duration: ' +CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds' ;
		
		SET @start_time =GETDATE();
		PRINT 'Truncating and Loading DATA Into bronze.erp_loc_a101'
		TRUNCATE TABLE bronze.erp_loc_a101;
		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
		SET @end_time =GETDATE();
		PRINT '>>Load Duration: ' +CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds' ;
		
		SET @start_time =GETDATE();
		PRINT 'Truncating and Loading DATA Into bronze.erp_px_cat_g1v2'
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
		SET @end_time =GETDATE();
		PRINT '>>Load Duration: ' +CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds' ;

		SET @tabular_end_time =GETDATE();
		PRINT '>>Load Duration of ERP Tables: ' +CAST(DATEDIFF(second, @tabular_start_time, @tabular_end_time) AS NVARCHAR) + 'seconds' ;

		SET @bronze_end_time =GETDATE();

		PRINT '--------------------------------------------------------------------------------------' ;
		PRINT '>>Load Duration of Bronze Layer: ' +CAST(DATEDIFF(second, @tabular_start_time, @tabular_end_time) AS NVARCHAR) + 'seconds' ;
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

