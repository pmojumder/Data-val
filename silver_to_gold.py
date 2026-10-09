###############################################################
# SILVER TO GOLD LAYER - WITH SCD TYPE 2
###############################################################

from pyspark.sql.functions import *
from datetime import datetime

# Import validation function
from gold_validations import run_gold_validations

###############################################################
# Create Gold Database
###############################################################

spark.sql("""
CREATE DATABASE IF NOT EXISTS gold_beverages
""")


###############################################################
# Dimension Product
###############################################################

print("=" * 80)
print("CREATING DIM_PRODUCT")
print("=" * 80)

spark.sql("""
CREATE OR REPLACE TABLE gold_beverages.dim_product AS

SELECT
    product_id,
    product_name,
    category,
    status

FROM silver_beverages.product
""")


###############################################################
# Dimension Store
###############################################################

print("=" * 80)
print("CREATING DIM_STORE")
print("=" * 80)

spark.sql("""
CREATE OR REPLACE TABLE gold_beverages.dim_store AS

SELECT
    store_id,
    store_name,
    retailer_id,
    status

FROM silver_beverages.store
""")


###############################################################
# Dimension Retailer - SCD TYPE 2
###############################################################

print("=" * 80)
print("CREATING DIM_RETAILER_SCD (SCD TYPE 2)")
print("=" * 80)

if spark.catalog.tableExists("gold_beverages.dim_retailer_scd"):
    
    print("SCD table exists - performing MERGE operation")
    
    spark.sql("""
    CREATE OR REPLACE TEMP VIEW retailer_staging AS
    SELECT
        retailer_id,
        retailer_name,
        status,
        CURRENT_DATE() AS effective_date
    FROM silver_beverages.retailer
    """)
    
    spark.sql("""
    MERGE INTO gold_beverages.dim_retailer_scd AS target
    USING retailer_staging AS source
    ON target.retailer_id = source.retailer_id
       AND target.is_current = TRUE
    
    WHEN MATCHED AND (
        target.retailer_name != source.retailer_name OR
        target.status != source.status
    ) THEN UPDATE SET
        target.is_current = FALSE,
        target.end_date = source.effective_date
    
    WHEN NOT MATCHED THEN INSERT (
        retailer_id,
        retailer_name,
        status,
        effective_date,
        end_date,
        is_current
    ) VALUES (
        source.retailer_id,
        source.retailer_name,
        source.status,
        source.effective_date,
        DATE '9999-12-31',
        TRUE
    )
    """)
    
    spark.sql("""
    INSERT INTO gold_beverages.dim_retailer_scd
    SELECT
        s.retailer_id,
        s.retailer_name,
        s.status,
        s.effective_date,
        DATE '9999-12-31' AS end_date,
        TRUE AS is_current
    FROM retailer_staging s
    INNER JOIN gold_beverages.dim_retailer_scd t
    ON s.retailer_id = t.retailer_id
       AND t.is_current = FALSE
       AND t.end_date = s.effective_date
    WHERE NOT EXISTS (
        SELECT 1
        FROM gold_beverages.dim_retailer_scd t2
        WHERE t2.retailer_id = s.retailer_id
          AND t2.is_current = TRUE
    )
    """)

else:
    
    print("Creating initial SCD table")
    
    spark.sql("""
    CREATE TABLE gold_beverages.dim_retailer_scd AS
    
    SELECT
        retailer_id,
        retailer_name,
        status,
        CURRENT_DATE() AS effective_date,
        DATE '9999-12-31' AS end_date,
        TRUE AS is_current
    
    FROM silver_beverages.retailer
    """)


###############################################################
# Fact Sales
###############################################################

print("=" * 80)
print("CREATING FACT_SALES")
print("=" * 80)

spark.sql("""
CREATE OR REPLACE TABLE gold_beverages.fact_sales AS

SELECT

    s.transaction_id,
    s.transaction_date,
    s.product_id,
    s.store_id,
    s.quantity_sold,

    p.category,
    p.product_name,
    p.status AS product_status,
    
    st.store_name,
    st.retailer_id

FROM silver_beverages.sales s

LEFT JOIN silver_beverages.product p
ON s.product_id = p.product_id

LEFT JOIN silver_beverages.store st
ON s.store_id = st.store_id
""")


###############################################################
# Daily Sales
###############################################################

print("=" * 80)
print("CREATING SALES_DAILY")
print("=" * 80)

spark.sql("""
CREATE OR REPLACE TABLE gold_beverages.sales_daily AS

SELECT

    transaction_date,

    COUNT(DISTINCT transaction_id) AS transaction_count,
    SUM(quantity_sold) AS total_quantity,
    COUNT(DISTINCT product_id) AS unique_products,
    COUNT(DISTINCT store_id) AS unique_stores

FROM gold_beverages.fact_sales

GROUP BY transaction_date

ORDER BY transaction_date
""")


###############################################################
# Product Performance
###############################################################

print("=" * 80)
print("CREATING PRODUCT_PERFORMANCE")
print("=" * 80)

spark.sql("""
CREATE OR REPLACE TABLE gold_beverages.product_performance AS

SELECT

    product_id,
    product_name,
    category,

    COUNT(DISTINCT transaction_id) AS transaction_count,
    SUM(quantity_sold) AS units_sold,
    COUNT(DISTINCT store_id) AS stores_sold_in

FROM gold_beverages.fact_sales

GROUP BY

    product_id,
    product_name,
    category

ORDER BY units_sold DESC
""")


###############################################################
# Inventory Snapshot
###############################################################

print("=" * 80)
print("CREATING INVENTORY_SNAPSHOT")
print("=" * 80)

spark.sql("""
CREATE OR REPLACE TABLE gold_beverages.inventory_snapshot AS

SELECT

    i.inventory_id,
    i.product_id,
    p.product_name,
    p.category,
    i.stock_on_hand,
    i.reorder_level,
    i.expiry_date,
    
    CASE 
        WHEN i.stock_on_hand <= i.reorder_level THEN 'REORDER'
        WHEN i.expiry_date < CURRENT_DATE() THEN 'EXPIRED'
        WHEN i.expiry_date <= DATE_ADD(CURRENT_DATE(), 30) THEN 'EXPIRING_SOON'
        ELSE 'OK'
    END AS inventory_status,
    
    DATEDIFF(i.expiry_date, CURRENT_DATE()) AS days_to_expiry

FROM silver_beverages.inventory i

LEFT JOIN silver_beverages.product p
ON i.product_id = p.product_id
""")


###############################################################
# Products With Sales
###############################################################

print("=" * 80)
print("CREATING PRODUCTS_WITH_SALES")
print("=" * 80)

spark.sql("""
CREATE OR REPLACE TABLE gold_beverages.products_with_sales AS

SELECT *

FROM silver_beverages.product p

WHERE EXISTS

(
    SELECT 1
    FROM silver_beverages.sales s
    WHERE p.product_id = s.product_id
)
""")


###############################################################
# Products Without Sales
###############################################################

print("=" * 80)
print("CREATING PRODUCTS_WITHOUT_SALES")
print("=" * 80)

spark.sql("""
CREATE OR REPLACE TABLE gold_beverages.products_without_sales AS

SELECT *

FROM silver_beverages.product p

WHERE NOT EXISTS

(
    SELECT 1
    FROM silver_beverages.sales s
    WHERE p.product_id = s.product_id
)
""")


###############################################################
# Show Gold Tables
###############################################################

print("=" * 80)
print("GOLD TABLES")
print("=" * 80)

spark.sql("""
SHOW TABLES IN gold_beverages
""").show(truncate=False)


###############################################################
# Run Gold Validations
###############################################################

print("=" * 80)
print("RUNNING GOLD LAYER VALIDATIONS")
print("=" * 80)

run_gold_validations(spark)


###############################################################
# Gold Completed
###############################################################

print("=" * 80)
print("SILVER TO GOLD COMPLETED SUCCESSFULLY")
print("=" * 80)