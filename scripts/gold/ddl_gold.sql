/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

create view gold.dim_customer AS
select 
ROW_NUMBER() over (order by cst_id) AS customer_key,
ci.cst_id AS customer_id,
ci.cst_key AS customer_number,
ci.cst_firstname AS first_name,
ci.cst_lastname AS last_name,
ci.cst_marital_status AS marital_status,
case when ci.cst_gndr != 'n/a' then ci.cst_gndr
     else coalesce(ca.GEN , 'n/a')
end gender,
ci.cst_create_date AS create_date,
ca.BDATE AS birthday,
la.CNTRY as country
from silver.crm_cust_info ci
left join silver.erp_CUST_AZ12 ca 
on ci.cst_key = ca.CID 
left join silver.erp_LOC_A101 la
on ci.cst_key = la.CID

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO


create view gold.dim_products AS
select 
ROW_NUMBER() over (order by pi.prd_start_dt) AS product_key,
pi.prd_id AS product_id,
pi.prd_key AS product_number,
pi.prd_nm AS product_name,
pi.cat_id AS category_id,
pc.CAT AS category,
pc.SUBCAT AS subcategory,
pc.MAINTENANCE,
pi.prd_cost AS cost,
pi.prd_line AS product_line,
pi.prd_start_dt AS start_date
from silver.crm_prd_info pi
left join silver.erp_PX_CAT_G1V2 pc
on pi.cat_id = pc.ID


-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

create view gold.fact_sales AS
select 
sd.sls_ord_num As order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt As order_date,
sd.sls_ship_dt AS ship_date,
sd.sls_due_dt AS due_date,
sd.sls_sales AS sales_amount,
sd.sls_quantity AS quanity,
sd.sls_price price
from silver.crm_sales_details sd 
left join gold.dim_products pr
on sd.sls_prd_key = pr.product_number
left join gold.dim_customer cu
on sd.sls_cust_id = cu.customer_id
