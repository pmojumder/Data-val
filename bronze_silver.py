###############################################################
# BRONZE TO SILVER LAYER
#
# Purpose:
#   1. Read data from Bronze Delta tables
#   2. Apply data cleansing
#   3. Apply business transformations
#   4. Remove duplicate / invalid records
#   5. Convert columns into required data types
#   6. Write cleansed data into Silver Delta tables
#   7. Validate Bronze vs Silver record counts
#   8. Display transformation statistics
#
# Architecture:
#
#       BRONZE
#          |
#          |  Cleansing
#          |  Standardization
#          |  Business Rules
#          |  Validation
#          v
#       SILVER
#
###############################################################


###############################################################
# IMPORT REQUIRED LIBRARIES
###############################################################

# Import all commonly used PySpark SQL functions such as:
#
# col()
# trim()
# to_date()
# upper()
# when()
# current_date()
# etc.
#
# Using "*" allows us to directly call these functions.
from pyspark.sql.functions import *


# SparkSession is the entry point for working with Spark.
from pyspark.sql import SparkSession


# Import reusable functions from our common_functions.py file.
#
# Examples:
#
# header()
# footer()
# line()
# display_row_count()
# display_column_count()
# display_columns()
# display_data()
# success()
# failure()
#
from common_functions import *


###############################################################
# CREATE / GET SPARK SESSION
###############################################################

# Create a Spark session for this ETL pipeline.
#
# getOrCreate() means:
#
# - If a Spark session already exists, use it.
# - Otherwise create a new Spark session.
#
spark = SparkSession.builder \
    .appName("Bronze_To_Silver") \
    .getOrCreate()


###############################################################
# START MESSAGE
###############################################################

# Display a standard header so that the notebook/job output
# clearly indicates that Bronze -> Silver processing has started.

header("BRONZE TO SILVER STARTED")


###############################################################
# CREATE SILVER DATABASE
###############################################################

# Create the Silver database if it does not already exist.
#
# IF NOT EXISTS prevents the command from failing when the
# database is already available.
#
# Bronze:
#     bronze_beverages
#
# Silver:
#     silver_beverages
#
spark.sql("""
CREATE DATABASE IF NOT EXISTS silver_beverages
""")


###############################################################
# =============================================================
# SALES TABLE
# =============================================================
###############################################################

line()

print("PROCESSING: SALES")

line()


###############################################################
# READ SALES DATA FROM BRONZE
###############################################################

# Read the Bronze Sales Delta table.
#
# Bronze contains the raw / minimally processed data.
#
bronze_sales = spark.table(
    "bronze_beverages.sales"
)


###############################################################
# BRONZE DATA PROFILING
###############################################################

print("\n[BRONZE LAYER]")

# Display number of records available in Bronze.
display_row_count(bronze_sales)


# Display number of columns.
display_column_count(bronze_sales)


# Display column names.
display_columns(bronze_sales)


###############################################################
# SALES TRANSFORMATION
###############################################################

# Start with the Bronze DataFrame.
#
# The following transformations are performed:
#
# 1. Rename sales_id -> transaction_id
#
#    This gives the Silver layer a more business-friendly
#    and standardized column name.
#
# 2. Rename sale_date -> transaction_date
#
#    This makes the column meaning clearer.
#
# 3. Rename amount -> quantity_sold
#
#    The source column "amount" represents the quantity sold
#    in this project.
#
# 4. Convert transaction_date into DATE datatype.
#
# 5. Remove duplicate transactions.
#
# 6. Remove records where quantity_sold is zero or negative.
#
silver_sales = (
    bronze_sales

    # Rename source/business columns
    .withColumnRenamed(
        "sales_id",
        "transaction_id"
    )

    .withColumnRenamed(
        "sale_date",
        "transaction_date"
    )

    .withColumnRenamed(
        "amount",
        "quantity_sold"
    )

    # Convert transaction date into proper DATE datatype.
    .withColumn(
        "transaction_date",
        to_date("transaction_date")
    )

    # Keep only one record for each transaction.
    .dropDuplicates(
        ["transaction_id"]
    )

    # Business validation:
    # Quantity sold must be greater than zero.
    .filter(
        col("quantity_sold") > 0
    )
)


###############################################################
# WRITE SALES DATA TO SILVER
###############################################################

# Write the transformed Sales DataFrame into the Silver layer.
#
# format("delta")
#     Stores data in Delta Lake format.
#
# mode("overwrite")
#     Replaces the existing Silver table during this demo.
#
# overwriteSchema=true
#     Allows the Silver table schema to be updated if required.
#
silver_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .option(
        "overwriteSchema",
        "true"
    ) \
    .saveAsTable(
        "silver_beverages.sales"
    )


###############################################################
# READ SALES SILVER TABLE AGAIN
###############################################################

# Read the actual table from Silver.
#
# This is useful because validation is performed against the
# data that was actually written to the Silver layer.
#
silver_sales_table = spark.table(
    "silver_beverages.sales"
)


###############################################################
# SILVER DATA PROFILING
###############################################################

print("\n[SILVER LAYER]")

display_row_count(
    silver_sales_table
)

display_column_count(
    silver_sales_table
)

display_columns(
    silver_sales_table
)


###############################################################
# SALES TRANSFORMATION VALIDATION
###############################################################

# Count records in Bronze.
bronze_count = bronze_sales.count()


# Count records in Silver.
silver_count = silver_sales_table.count()


###############################################################
# CALCULATE DUPLICATES REMOVED
###############################################################

# Support both possible Bronze column names.
#
# Some Bronze versions may contain sales_id.
# Some may already contain transaction_id.
#
if "sales_id" in bronze_sales.columns:

    duplicates_removed = (
        bronze_sales.count()
        -
        bronze_sales
        .dropDuplicates(["sales_id"])
        .count()
    )

else:

    duplicates_removed = (
        bronze_sales.count()
        -
        bronze_sales
        .dropDuplicates(["transaction_id"])
        .count()
    )


###############################################################
# CALCULATE INVALID SALES RECORDS
###############################################################

# Identify records that would be removed by the Silver rule:
#
# quantity/amount is NULL
# OR
# quantity/amount is zero or negative.
#
if "sales_id" in bronze_sales.columns:

    invalid_records = bronze_sales.filter(
        col("amount").isNull()
        |
        (col("amount") <= 0)
    ).count()

else:

    invalid_records = bronze_sales.filter(
        col("quantity_sold").isNull()
        |
        (col("quantity_sold") <= 0)
    ).count()


###############################################################
# DISPLAY SALES VALIDATION RESULTS
###############################################################

print("\n[TRANSFORMATION VALIDATION]")

print(
    f"Bronze Records           : {bronze_count:,}"
)

print(
    f"Silver Records           : {silver_count:,}"
)

print(
    f"Duplicates Removed       : {duplicates_removed:,}"
)

print(
    f"Invalid Records Filtered : {invalid_records:,}"
)

print(
    f"Records Lost             : "
    f"{bronze_count - silver_count:,}"
)


###############################################################
# SALES SUCCESS / FAILURE CHECK
###############################################################

if silver_count > 0:

    success(
        "SALES transformation completed"
    )

else:

    failure(
        "SALES transformation - No records in silver"
    )


###############################################################
# DISPLAY SAMPLE SILVER SALES DATA
###############################################################

display_data(
    silver_sales_table,
    3
)


###############################################################
# =============================================================
# PRODUCT TABLE
# =============================================================
###############################################################

line()

print("PROCESSING: PRODUCT")

line()


###############################################################
# READ PRODUCT BRONZE TABLE
###############################################################

bronze_product = spark.table(
    "bronze_beverages.product"
)


###############################################################
# BRONZE PRODUCT PROFILING
###############################################################

print("\n[BRONZE LAYER]")

display_row_count(
    bronze_product
)

display_column_count(
    bronze_product
)

display_columns(
    bronze_product
)


###############################################################
# PRODUCT TRANSFORMATION
###############################################################

# Product transformation rules:
#
# 1. Remove duplicate product IDs.
#
# 2. Product ID cannot be NULL.
#
# 3. Remove leading/trailing spaces from product_name.
#
# 4. Remove leading/trailing spaces from category.
#
# 5. Remove leading/trailing spaces from status.
#
silver_product = (
    bronze_product

    # Keep only one record per product.
    .dropDuplicates(
        ["product_id"]
    )

    # Product ID is mandatory.
    .filter(
        col("product_id").isNotNull()
    )

    # Clean Product Name.
    .withColumn(
        "product_name",
        trim(col("product_name"))
    )

    # Clean Category.
    .withColumn(
        "category",
        trim(col("category"))
    )

    # Clean Status.
    .withColumn(
        "status",
        trim(col("status"))
    )
)


###############################################################
# WRITE PRODUCT TO SILVER
###############################################################

silver_product.write \
    .format("delta") \
    .mode("overwrite") \
    .option(
        "overwriteSchema",
        "true"
    ) \
    .saveAsTable(
        "silver_beverages.product"
    )


###############################################################
# READ PRODUCT SILVER TABLE
###############################################################

silver_product_table = spark.table(
    "silver_beverages.product"
)


###############################################################
# SILVER PRODUCT PROFILING
###############################################################

print("\n[SILVER LAYER]")

display_row_count(
    silver_product_table
)

display_column_count(
    silver_product_table
)

display_columns(
    silver_product_table
)


###############################################################
# PRODUCT TRANSFORMATION VALIDATION
###############################################################

bronze_count = bronze_product.count()

silver_count = silver_product_table.count()


# Calculate duplicate records in Bronze.
duplicates_removed = (
    bronze_product.count()
    -
    bronze_product
    .dropDuplicates(["product_id"])
    .count()
)


# Count NULL Product IDs in Bronze.
null_products = bronze_product.filter(
    col("product_id").isNull()
).count()


###############################################################
# DISPLAY PRODUCT VALIDATION
###############################################################

print("\n[TRANSFORMATION VALIDATION]")

print(
    f"Bronze Records        : {bronze_count:,}"
)

print(
    f"Silver Records        : {silver_count:,}"
)

print(
    f"Duplicates Removed    : {duplicates_removed:,}"
)

print(
    f"Null Product IDs      : {null_products:,}"
)

print(
    f"Records Lost          : "
    f"{bronze_count - silver_count:,}"
)


###############################################################
# PRODUCT SUCCESS / FAILURE
###############################################################

if silver_count > 0:

    success(
        "PRODUCT transformation completed"
    )

else:

    failure(
        "PRODUCT transformation - No records in silver"
    )


###############################################################
# DISPLAY PRODUCT SAMPLE
###############################################################

display_data(
    silver_product_table,
    3
)


###############################################################
# =============================================================
# INVENTORY TABLE
# =============================================================
###############################################################

line()

print("PROCESSING: INVENTORY")

line()


###############################################################
# READ INVENTORY BRONZE TABLE
###############################################################

bronze_inventory = spark.table(
    "bronze_beverages.inventory"
)


###############################################################
# BRONZE INVENTORY PROFILING
###############################################################

print("\n[BRONZE LAYER]")

display_row_count(
    bronze_inventory
)

display_column_count(
    bronze_inventory
)

display_columns(
    bronze_inventory
)


###############################################################
# INVENTORY TRANSFORMATION
###############################################################

# Inventory transformation rules:
#
# 1. Remove duplicate inventory IDs.
#
# 2. Stock cannot be negative.
#
# 3. Convert expiry_date into proper DATE datatype.
#
silver_inventory = (
    bronze_inventory

    # Remove duplicate inventory records.
    .dropDuplicates(
        ["inventory_id"]
    )

    # Business rule:
    # Stock quantity cannot be negative.
    .filter(
        col("stock_on_hand") >= 0
    )

    # Convert expiry_date to DATE.
    .withColumn(
        "expiry_date",
        to_date("expiry_date")
    )
)


###############################################################
# WRITE INVENTORY TO SILVER
###############################################################

silver_inventory.write \
    .format("delta") \
    .mode("overwrite") \
    .option(
        "overwriteSchema",
        "true"
    ) \
    .saveAsTable(
        "silver_beverages.inventory"
    )


###############################################################
# READ INVENTORY SILVER TABLE
###############################################################

silver_inventory_table = spark.table(
    "silver_beverages.inventory"
)


###############################################################
# SILVER INVENTORY PROFILING
###############################################################

print("\n[SILVER LAYER]")

display_row_count(
    silver_inventory_table
)

display_column_count(
    silver_inventory_table
)

display_columns(
    silver_inventory_table
)


###############################################################
# INVENTORY TRANSFORMATION VALIDATION
###############################################################

bronze_count = bronze_inventory.count()

silver_count = silver_inventory_table.count()


# Calculate duplicate inventory records.
duplicates_removed = (
    bronze_inventory.count()
    -
    bronze_inventory
    .dropDuplicates(["inventory_id"])
    .count()
)


# Count records having negative stock.
negative_stock = bronze_inventory.filter(
    col("stock_on_hand") < 0
).count()


###############################################################
# DISPLAY INVENTORY VALIDATION
###############################################################

print("\n[TRANSFORMATION VALIDATION]")

print(
    f"Bronze Records          : {bronze_count:,}"
)

print(
    f"Silver Records          : {silver_count:,}"
)

print(
    f"Duplicates Removed      : {duplicates_removed:,}"
)

print(
    f"Negative Stock Filtered : {negative_stock:,}"
)

print(
    f"Records Lost            : "
    f"{bronze_count - silver_count:,}"
)


###############################################################
# INVENTORY SUCCESS / FAILURE
###############################################################

if silver_count > 0:

    success(
        "INVENTORY transformation completed"
    )

else:

    failure(
        "INVENTORY transformation - No records in silver"
    )


###############################################################
# DISPLAY INVENTORY SAMPLE
###############################################################

display_data(
    silver_inventory_table,
    3
)


###############################################################
# =============================================================
# RETAILER TABLE
# =============================================================
###############################################################

line()

print("PROCESSING: RETAILER")

line()


###############################################################
# READ RETAILER BRONZE TABLE
###############################################################

bronze_retailer = spark.table(
    "bronze_beverages.retailer"
)


###############################################################
# BRONZE RETAILER PROFILING
###############################################################

print("\n[BRONZE LAYER]")

display_row_count(
    bronze_retailer
)

display_column_count(
    bronze_retailer
)

display_columns(
    bronze_retailer
)


###############################################################
# RETAILER TRANSFORMATION
###############################################################

# Retailer transformation rules:
#
# 1. Remove duplicate retailer IDs.
#
# 2. Retailer ID cannot be NULL.
#
# 3. Retailer name cannot be NULL.
#
# 4. Trim unwanted spaces from retailer_name.
#
# 5. Trim unwanted spaces from status.
#
silver_retailer = (
    bronze_retailer

    # Remove duplicate retailers.
    .dropDuplicates(
        ["retailer_id"]
    )

    # Retailer ID is mandatory.
    .filter(
        col("retailer_id").isNotNull()
    )

    # Retailer name is mandatory.
    .filter(
        col("retailer_name").isNotNull()
    )

    # Remove leading/trailing spaces.
    .withColumn(
        "retailer_name",
        trim(col("retailer_name"))
    )

    # Remove leading/trailing spaces from status.
    .withColumn(
        "status",
        trim(col("status"))
    )
)


###############################################################
# WRITE RETAILER TO SILVER
###############################################################

silver_retailer.write \
    .format("delta") \
    .mode("overwrite") \
    .option(
        "overwriteSchema",
        "true"
    ) \
    .saveAsTable(
        "silver_beverages.retailer"
    )


###############################################################
# READ RETAILER SILVER TABLE
###############################################################

silver_retailer_table = spark.table(
    "silver_beverages.retailer"
)


###############################################################
# SILVER RETAILER PROFILING
###############################################################

print("\n[SILVER LAYER]")

display_row_count(
    silver_retailer_table
)

display_column_count(
    silver_retailer_table
)

display_columns(
    silver_retailer_table
)


###############################################################
# RETAILER TRANSFORMATION VALIDATION
###############################################################

bronze_count = bronze_retailer.count()

silver_count = silver_retailer_table.count()


# Calculate duplicate retailer records.
duplicates_removed = (
    bronze_retailer.count()
    -
    bronze_retailer
    .dropDuplicates(["retailer_id"])
    .count()
)


# Count NULL retailer IDs.
null_ids = bronze_retailer.filter(
    col("retailer_id").isNull()
).count()


# Count NULL retailer names.
null_names = bronze_retailer.filter(
    col("retailer_name").isNull()
).count()


###############################################################
# DISPLAY RETAILER VALIDATION
###############################################################

print("\n[TRANSFORMATION VALIDATION]")

print(
    f"Bronze Records        : {bronze_count:,}"
)

print(
    f"Silver Records        : {silver_count:,}"
)

print(
    f"Duplicates Removed    : {duplicates_removed:,}"
)

print(
    f"Null Retailer IDs     : {null_ids:,}"
)

print(
    f"Null Retailer Names   : {null_names:,}"
)

print(
    f"Records Lost          : "
    f"{bronze_count - silver_count:,}"
)


###############################################################
# RETAILER SUCCESS / FAILURE
###############################################################

if silver_count > 0:

    success(
        "RETAILER transformation completed"
    )

else:

    failure(
        "RETAILER transformation - No records in silver"
    )


###############################################################
# DISPLAY RETAILER SAMPLE
###############################################################

display_data(
    silver_retailer_table,
    3
)


###############################################################
# =============================================================
# STORE TABLE
# =============================================================
###############################################################

line()

print("PROCESSING: STORE")

line()


###############################################################
# READ STORE BRONZE TABLE
###############################################################

bronze_store = spark.table(
    "bronze_beverages.store"
)


###############################################################
# BRONZE STORE PROFILING
###############################################################

print("\n[BRONZE LAYER]")

display_row_count(
    bronze_store
)

display_column_count(
    bronze_store
)

display_columns(
    bronze_store
)


###############################################################
# STORE TRANSFORMATION
###############################################################

# Store transformation rules:
#
# 1. Remove duplicate store IDs.
#
# 2. Store ID cannot be NULL.
#
# 3. Trim unwanted spaces from store_name.
#
# 4. Trim unwanted spaces from status.
#
silver_store = (
    bronze_store

    # Remove duplicate store records.
    .dropDuplicates(
        ["store_id"]
    )

    # Store ID is mandatory.
    .filter(
        col("store_id").isNotNull()
    )

    # Clean Store Name.
    .withColumn(
        "store_name",
        trim(col("store_name"))
    )

    # Clean Store Status.
    .withColumn(
        "status",
        trim(col("status"))
    )
)


###############################################################
# WRITE STORE TO SILVER
###############################################################

silver_store.write \
    .format("delta") \
    .mode("overwrite") \
    .option(
        "overwriteSchema",
        "true"
    ) \
    .saveAsTable(
        "silver_beverages.store"
    )


###############################################################
# READ STORE SILVER TABLE
###############################################################

silver_store_table = spark.table(
    "silver_beverages.store"
)


###############################################################
# SILVER STORE PROFILING
###############################################################

print("\n[SILVER LAYER]")

display_row_count(
    silver_store_table
)

display_column_count(
    silver_store_table
)

display_columns(
    silver_store_table
)


###############################################################
# STORE TRANSFORMATION VALIDATION
###############################################################

bronze_count = bronze_store.count()

silver_count = silver_store_table.count()


# Calculate duplicate stores.
duplicates_removed = (
    bronze_store.count()
    -
    bronze_store
    .dropDuplicates(["store_id"])
    .count()
)


# Count NULL Store IDs.
null_ids = bronze_store.filter(
    col("store_id").isNull()
).count()


###############################################################
# DISPLAY STORE VALIDATION
###############################################################

print("\n[TRANSFORMATION VALIDATION]")

print(
    f"Bronze Records        : {bronze_count:,}"
)

print(
    f"Silver Records        : {silver_count:,}"
)

print(
    f"Duplicates Removed    : {duplicates_removed:,}"
)

print(
    f"Null Store IDs        : {null_ids:,}"
)

print(
    f"Records Lost          : "
    f"{bronze_count - silver_count:,}"
)


###############################################################
# STORE SUCCESS / FAILURE
###############################################################

if silver_count > 0:

    success(
        "STORE transformation completed"
    )

else:

    failure(
        "STORE transformation - No records in silver"
    )


###############################################################
# DISPLAY STORE SAMPLE
###############################################################

display_data(
    silver_store_table,
    3
)


###############################################################
# =============================================================
# FINAL SILVER LAYER SUMMARY
# =============================================================
###############################################################

line()

header(
    "SILVER TABLES SUMMARY"
)


###############################################################
# CREATE SUMMARY LIST
###############################################################

# This list will contain:
#
#     Table Name
#     Silver Record Count
#
summary_data = []


###############################################################
# PROCESS ALL SILVER TABLES
###############################################################

for table in [
    "sales",
    "product",
    "inventory",
    "retailer",
    "store"
]:

    # Read the Silver table.
    silver_table = spark.table(
        f"silver_beverages.{table}"
    )

    # Calculate final record count.
    count = silver_table.count()

    # Add result to summary list.
    summary_data.append(
        (
            table.upper(),
            count
        )
    )

    # Print result.
    print(
        f"{table.upper():15} : {count:,}"
    )


###############################################################
# END OF PROCESS
###############################################################

line()

footer()