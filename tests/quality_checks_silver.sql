-- data analysing from bronze layer before loading to silver
SELECT TOP 10000 * FROM bronze.crm_cust_info
SELECT TOP 10000 * FROM bronze.crm_prd_info
SELECT TOP 10000 * FROM bronze.crm_sales_details
SELECT TOP 10000 * FROM bronze.erp_cust_az12

SELECT TOP 10000 * FROM bronze.erp_cust_az12;
SELECT TOP 10000 * FROM bronze.crm_cust_info;

SELECT TOP 10000 * FROM bronze.erp_loc_a101
SELECT TOP 10000 * FROM bronze.erp_px_cat_g1v2;
SELECT TOP 10000 * FROM bronze.crm_prd_info;

-- crm_cust_info table data quality checks.
-- Check for NULLs or Duplicate in primary key
-- Expectation: No result.
SELECT 
cst_id,
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) >1 OR cst_id IS NULL

-- We identified a problem i.e duplicates and NULLs here. Next, we get rid of them.
SELECT * FROM(
SELECT *,
ROW_NUMBER () OVER(PARTITION BY cst_id ORDER BY cst_create_date) as flag_last
FROM bronze.crm_cust_info) t
WHERE flag_last = 1

-- Now we don't have any duplicates or NULLs. 
-- Next, we check for unwanted spaces in string values. 
-- Expectations: No result
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

SELECT cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr)

SELECT cst_key
FROM bronze.crm_cust_info
WHERE cst_key != TRIM(cst_key)
-- Check is done. Now we modify previous query to clean the data

-- Data Standardization & Consistency
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info

SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info

-- crm_prd_info table data quality checks.
SELECT 
prd_id,
COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) >1 OR prd_id IS NULL

SELECT prd_cost
FROM bronze.crm_prd_info 
WHERE prd_cost < 1 OR prd_cost IS NULL

SELECT DISTINCT prd_line 
FROM bronze.crm_prd_info

SELECT * FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt

SELECT prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R','AC-HE-HL-U509')

-- crm_sales_details table data quality checks.
SELECT 
NULLIF(sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 
OR LEN(sls_order_dt) != 8
OR sls_order_dt > 20500101 OR sls_order_dt < 19000101

SELECT 
NULLIF(sls_ship_dt,0) sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0 
OR LEN(sls_ship_dt) != 8
OR sls_ship_dt > 20500101 OR sls_ship_dt < 19000101

SELECT 
NULLIF(sls_due_dt,0) sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
OR LEN(sls_due_dt) != 8
OR sls_due_dt > 20500101 OR sls_due_dt < 19000101

SELECT * FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

SELECT 
sls_sales AS old_sls_sales,
sls_quantity,
sls_price AS old_sls_price,

CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
	 THEN sls_quantity * ABS(sls_price)
	 ELSE sls_sales
END AS sls_sales,

CASE WHEN sls_price IS NULL OR sls_price <= 0
	 THEN sls_sales / NULLIF(sls_quantity,0)
	 ELSE sls_price
END AS sls_price

FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price

-- erp_cust_az12 table data quality checks.
SELECT DISTINCT 
bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

SELECT DISTINCT gen 
FROM bronze.erp_cust_az12

SELECT DISTINCT 
bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

SELECT DISTINCT gen 
FROM silver.erp_cust_az12

-- erp_cust_az12 table data quality checks.
--None.

-- erp_cust_az12 table data quality checks.
--None.
