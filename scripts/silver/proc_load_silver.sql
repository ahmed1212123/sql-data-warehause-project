/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/


Create or alter procedure silver.load_silver as
Begin
	begin try
		DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
		SET @batch_start_time = GETDATE();
			PRINT '================================================';
			PRINT 'Loading Silver Layer';
			PRINT '================================================';

			PRINT '------------------------------------------------';
			PRINT 'Loading CRM Tables';
			PRINT '------------------------------------------------';
		SET @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.crm_cust_info';
			TRUNCATE TABLE silver.crm_cust_info;
			PRINT '>> Inserting Data Into: silver.crm_cust_info';

		insert into silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
			)
		select 
		cst_id,
		cst_key,
		trim(cst_firstname) as cst_firstname,
		trim (cst_lastname) as cst_lastname,
		case when Upper(trim(cst_marital_status)) = 'S' then 'Single'
			 when Upper(trim(cst_marital_status)) = 'M' then 'Married'
			 else 'n/a'
		end cst_marital_status,
		case when Upper(trim(cst_gndr)) = 'M' then 'Male'
			 when Upper(trim(cst_gndr)) = 'F' then 'Female'
			 else 'n/a'
		end cst_gndr,
		cst_create_date
		from(
		Select 
		*,
		Row_number() over (partition by cst_id order by cst_create_date desc ) As flag_last
		from bronze.crm_cust_info
		)t where flag_last = 1;

		SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> -------------';
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.crm_prd_info';
			TRUNCATE TABLE silver.crm_prd_info;
			PRINT '>> Inserting Data Into: silver.crm_prd_info';
		insert into silver.crm_prd_info  
		(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		select 
		prd_id,
		REPLACE (SUBSTRING(prd_key , 1 ,5), '-','_' ) as cat_id,
		SUBSTRING(prd_key , 7 ,len(prd_key)) as prd_key,
		prd_nm,
		IsNull(prd_cost,0) As prd_cost,
		case Upper(trim(prd_line)) 
			 when 'M' then 'Mountain'
			 when 'R' then 'Road'
			 when 'S' then 'Other Sales'
			 when 'T' then 'Touring'
			 else 'n/a'
		end prd_line,  
		prd_start_dt,
		DATEADD ( DAY, -1, 
			LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
		from bronze.crm_prd_info

		SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> -------------';

			-- Loading crm_sales_details
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.crm_sales_details';
			TRUNCATE TABLE silver.crm_sales_details;
			PRINT '>> Inserting Data Into: silver.crm_sales_details';

		insert into silver.crm_sales_details(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		select 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		case when sls_order_dt = 0 or len(sls_order_dt) != 8 then NULL
			 else cast( cast(sls_order_dt as varchar) AS date )
		end sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		case when sls_sales is null or sls_sales <= 0  or sls_sales != sls_quantity * ABS(sls_price) 
				then sls_quantity * ABS(sls_price) 
			 else sls_sales 
		end as sls_sales,
		sls_quantity,
		case when sls_price is null or sls_price <= 0
				then sls_sales / sls_quantity
			else sls_price
		end sls_price
		from bronze.crm_sales_details

		SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> -------------';

			-- Loading erp_cust_az12
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.erp_cust_az12';
			TRUNCATE TABLE silver.erp_cust_az12;
			PRINT '>> Inserting Data Into: silver.erp_cust_az12';

		insert into silver.erp_CUST_AZ12(
			CID,
			BDATE,
			GEN
		)
		select 
		case when CID like 'NAS%' then Substring ( CID ,4, len(CID)) 
			 else CID
		end CID,
		case when BDATE > GETDATE() then null
			 else BDATE
		end BDATE,
		case when GEN = 'f' then 'Female'
			 when GEN = 'm' then 'Male'
			 when GEN is null or GEN = '' then 'n/a'
			 else GEN
		end GEN
		from bronze.erp_CUST_AZ12

		SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> -------------';

			PRINT '------------------------------------------------';
			PRINT 'Loading ERP Tables';
			PRINT '------------------------------------------------';

			-- Loading erp_loc_a101
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.erp_loc_a101';
			TRUNCATE TABLE silver.erp_loc_a101;
			PRINT '>> Inserting Data Into: silver.erp_loc_a101';

		insert into silver.erp_LOC_A101(CID,CNTRY)
		Select
		REPLACE(CID,'-','') As CID,
		Case when Trim(CNTRY) = 'DE' Then 'Germany'
			 when Trim(CNTRY) IN ('US','USA') Then 'United States'
			 when Trim(CNTRY) = '' or CNTRY is null  Then 'n/a'
			 else Trim(CNTRY)
		end CNTRY
		from bronze.erp_LOC_A101
	
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> -------------';
		
			-- Loading erp_px_cat_g1v2
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
			TRUNCATE TABLE silver.erp_px_cat_g1v2;
			PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';

		insert into silver.erp_PX_CAT_G1V2(
			ID,
			CAT,
			SUBCAT,
			MAINTENANCE
		)
		select
		ID,
		CAT,
		SUBCAT,
		MAINTENANCE
		from bronze.erp_PX_CAT_G1V2
	end try
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
end
