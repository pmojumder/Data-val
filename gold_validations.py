###############################################################
# ENHANCED GOLD LAYER VALIDATIONS
###############################################################

from pyspark.sql.functions import *
from datetime import datetime


def run_gold_validations(spark):

    print("""
============================================================
GOLD LAYER VALIDATIONS - ENHANCED VERSION
============================================================
""")

    ###############################################################
    # TABLE EXISTENCE VALIDATION
    ###############################################################

    print("\n========== TABLE EXISTENCE VALIDATION ==========")

    gold_tables = [
        "fact_sales",
        "dim_product",
        "dim_store",
        "dim_retailer_scd",
        "sales_daily",
        "product_performance",
        "products_with_sales",
        "products_without_sales",
        "inventory_snapshot"
    ]

    for table in gold_tables:
        if spark.catalog.tableExists(f"gold_beverages.{table}"):
            print(f"{table:<30} PASS")
        else:
            print(f"{table:<30} FAIL")


    ###############################################################
    # FACT SALES ROW COUNT
    ###############################################################

    print("\n========== FACT SALES ROW COUNT ==========")

    silver_count = spark.table("silver_beverages.sales").count()
    gold_count = spark.table("gold_beverages.fact_sales").count()

    print("Silver Count :", silver_count)
    print("Gold Count   :", gold_count)

    if silver_count == gold_count:
        print("PASS")
    else:
        print("FAIL")


    ###############################################################
    # DIM PRODUCT ROW COUNT
    ###############################################################

    print("\n========== DIM PRODUCT ROW COUNT ==========")

    silver = spark.table("silver_beverages.product").count()
    gold = spark.table("gold_beverages.dim_product").count()

    print("Silver :", silver)
    print("Gold   :", gold)

    if silver == gold:
        print("PASS")
    else:
        print("FAIL")


    ###############################################################
    # DIM STORE ROW COUNT
    ###############################################################

    print("\n========== DIM STORE ROW COUNT ==========")

    silver = spark.table("silver_beverages.store").count()
    gold = spark.table("gold_beverages.dim_store").count()

    print("Silver :", silver)
    print("Gold   :", gold)

    if silver == gold:
        print("PASS")
    else:
        print("FAIL")


    ###############################################################
    # COLUMN COUNT VALIDATION
    ###############################################################

    print("\n========== COLUMN COUNT VALIDATION ==========")

    silver_cols = len(spark.table("silver_beverages.sales").columns)
    gold_cols = len(spark.table("gold_beverages.fact_sales").columns)

    print("Silver Columns :", silver_cols)
    print("Gold Columns   :", gold_cols)

    if gold_cols >= silver_cols:
        print("PASS")
    else:
        print("FAIL")


    ###############################################################
    # SCHEMA VALIDATION
    ###############################################################

    print("\n========== FACT SALES SCHEMA ==========")

    spark.table("gold_beverages.fact_sales").printSchema()


    ###############################################################
    # NULL VALIDATION
    ###############################################################

    print("\n========== NULL VALIDATION ==========")

    gold_fact = spark.table("gold_beverages.fact_sales")

    gold_fact.select([
        count(when(col(c).isNull(), c)).alias(c)
        for c in gold_fact.columns
    ]).show()


    ###############################################################
    # DUPLICATE VALIDATION
    ###############################################################

    print("\n========== DUPLICATE VALIDATION ==========")

    duplicate = (
        gold_fact.groupBy("transaction_id")
            .count()
            .filter("count > 1")
    )

    if duplicate.count() == 0:
        print("PASS - No Duplicate Records")
    else:
        print("FAIL")
        duplicate.show()


    ###############################################################
    # PRODUCT LOOKUP VALIDATION
    ###############################################################

    print("\n========== PRODUCT LOOKUP VALIDATION ==========")

    missing = spark.sql("""
    SELECT *
    FROM gold_beverages.fact_sales f
    LEFT ANTI JOIN gold_beverages.dim_product p
    ON f.product_id = p.product_id
    """)

    print("Missing Products :", missing.count())

    if missing.count() == 0:
        print("PASS")
    else:
        print("FAIL")
        missing.show()


    ###############################################################
    # STORE LOOKUP VALIDATION
    ###############################################################

    print("\n========== STORE LOOKUP VALIDATION ==========")

    missing = spark.sql("""
    SELECT *
    FROM gold_beverages.fact_sales f
    LEFT ANTI JOIN gold_beverages.dim_store s
    ON f.store_id = s.store_id
    """)

    print("Missing Stores :", missing.count())

    if missing.count() == 0:
        print("PASS")
    else:
        print("FAIL")
        missing.show()


    ###############################################################
    # DAILY SALES VALIDATION (QUANTITY ONLY)
    ###############################################################

    print("\n========== DAILY SALES VALIDATION ==========")

    fact_qty = gold_fact.agg(sum("quantity_sold")).collect()[0][0]

    daily_qty = spark.table(
        "gold_beverages.sales_daily"
    ).agg(
        sum("total_quantity")
    ).collect()[0][0]

    print("Fact Quantity  :", fact_qty)
    print("Daily Quantity :", daily_qty)

    if fact_qty == daily_qty:
        print("PASS")
    else:
        print("FAIL")


    ###############################################################
    # PRODUCT PERFORMANCE VALIDATION
    ###############################################################

    print("\n========== PRODUCT PERFORMANCE VALIDATION ==========")

    fact_qty = gold_fact.agg(sum("quantity_sold")).collect()[0][0]

    perf_qty = spark.table(
        "gold_beverages.product_performance"
    ).agg(
        sum("units_sold")
    ).collect()[0][0]

    print("Fact Quantity :", fact_qty)
    print("Performance Quantity :", perf_qty)

    if fact_qty == perf_qty:
        print("PASS")
    else:
        print("FAIL")


    ###############################################################
    # DIM PRODUCT DUPLICATE VALIDATION
    ###############################################################

    print("\n========== DIM PRODUCT DUPLICATE VALIDATION ==========")

    dup = spark.table(
        "gold_beverages.dim_product"
    ).groupBy(
        "product_id"
    ).count().filter("count > 1")

    if dup.count() == 0:
        print("PASS")
    else:
        print("FAIL")
        dup.show()


    ###############################################################
    # DIM STORE DUPLICATE VALIDATION
    ###############################################################

    print("\n========== DIM STORE DUPLICATE VALIDATION ==========")

    dup = spark.table(
        "gold_beverages.dim_store"
    ).groupBy(
        "store_id"
    ).count().filter("count > 1")

    if dup.count() == 0:
        print("PASS")
    else:
        print("FAIL")
        dup.show()


    ###############################################################
    # DATA FRESHNESS VALIDATION
    ###############################################################

    print("\n========== DATA FRESHNESS VALIDATION ==========")

    latest_date = spark.sql("""
    SELECT MAX(transaction_date) AS latest_date
    FROM gold_beverages.fact_sales
    """).collect()[0][0]

    print("Latest Transaction Date :", latest_date)

    if latest_date:
        print("PASS")
    else:
        print("FAIL - No transaction dates found")


    ###############################################################
    # NEGATIVE QUANTITY VALIDATION
    ###############################################################

    print("\n========== NEGATIVE QUANTITY VALIDATION ==========")

    negative = gold_fact.filter(col("quantity_sold") < 0)

    print("Negative Quantity Records :", negative.count())

    if negative.count() == 0:
        print("PASS")
    else:
        print("FAIL")
        negative.show()


    ###############################################################
    # ZERO QUANTITY VALIDATION
    ###############################################################

    print("\n========== ZERO QUANTITY VALIDATION ==========")

    zero = gold_fact.filter(col("quantity_sold") == 0)

    print("Zero Quantity Records :", zero.count())

    if zero.count() == 0:
        print("PASS")
    else:
        print("WARNING - Found zero quantity records")
        zero.show(5)


    ###############################################################
    # DATE RANGE VALIDATION
    ###############################################################

    print("\n========== DATE RANGE VALIDATION ==========")

    date_range = spark.sql("""
    SELECT 
        MIN(transaction_date) AS min_date,
        MAX(transaction_date) AS max_date,
        DATEDIFF(MAX(transaction_date), MIN(transaction_date)) AS date_span_days
    FROM gold_beverages.fact_sales
    """)

    date_range.show()


    ###############################################################
    # CATEGORY DISTRIBUTION
    ###############################################################

    print("\n========== CATEGORY DISTRIBUTION ==========")

    spark.sql("""
    SELECT 
        category,
        COUNT(*) AS transaction_count,
        SUM(quantity_sold) AS total_quantity
    FROM gold_beverages.fact_sales
    GROUP BY category
    ORDER BY transaction_count DESC
    """).show()


    ###############################################################
    # DAILY TRANSACTION COUNT
    ###############################################################

    print("\n========== DAILY TRANSACTION COUNT ==========")

    spark.sql("""
    SELECT 
        transaction_date,
        COUNT(*) AS transaction_count
    FROM gold_beverages.fact_sales
    GROUP BY transaction_date
    ORDER BY transaction_date DESC
    LIMIT 10
    """).show()


    ###############################################################
    # SCD TYPE 2 VALIDATIONS
    ###############################################################

    print("\n========== SCD TYPE 2 VALIDATIONS ==========")

    if spark.catalog.tableExists("gold_beverages.dim_retailer_scd"):
        
        # 1. Multiple Current Records Check
        print("--- 1. Single Current Record Check ---")
        multiple_current = spark.sql("""
        SELECT 
            retailer_id,
            COUNT(*) AS active_record_count
        FROM gold_beverages.dim_retailer_scd
        WHERE is_current = TRUE
        GROUP BY retailer_id
        HAVING COUNT(*) > 1
        """)

        print("Retailers with multiple current records:", multiple_current.count())
        if multiple_current.count() == 0:
            print("PASS - Exactly one active record per retailer")
        else:
            print("FAIL - Found retailers with multiple active records")
            multiple_current.show()

        # 2. Date Range Logic Check
        print("\n--- 2. Date Range Logic Check ---")
        invalid_dates = spark.sql("""
        SELECT *
        FROM gold_beverages.dim_retailer_scd
        WHERE effective_date > end_date
        """)

        print("Records with effective_date > end_date:", invalid_dates.count())
        if invalid_dates.count() == 0:
            print("PASS - Effective date <= end date for all records")
        else:
            print("FAIL - Found invalid date ranges")
            invalid_dates.show()

        # 3. Active Record Expiry Check
        print("\n--- 3. Active Record End Date Check ---")
        invalid_active = spark.sql("""
        SELECT *
        FROM gold_beverages.dim_retailer_scd
        WHERE is_current = TRUE AND end_date != DATE '9999-12-31'
        """)

        print("Active records without 9999-12-31 end_date:", invalid_active.count())
        if invalid_active.count() == 0:
            print("PASS - Active records end on 9999-12-31")
        else:
            print("FAIL - Active records have invalid end dates")
            invalid_active.show()

        # 4. Historical Record Close-out Check
        print("\n--- 4. Closed Record End Date Check ---")
        invalid_closed = spark.sql("""
        SELECT *
        FROM gold_beverages.dim_retailer_scd
        WHERE is_current = FALSE AND end_date = DATE '9999-12-31'
        """)

        print("Inactive records with 9999-12-31 end_date:", invalid_closed.count())
        if invalid_closed.count() == 0:
            print("PASS - All historical records closed properly")
        else:
            print("FAIL - Inactive records pointing to high end date")
            invalid_closed.show()

        # 5. Overlaps / Gaps across versions
        print("\n--- 5. Overlap & Gap Check across Versions ---")
        overlaps = spark.sql("""
        WITH OrderedSCD AS (
            SELECT 
                retailer_id,
                effective_date,
                end_date,
                LEAD(effective_date) OVER (PARTITION BY retailer_id ORDER BY effective_date) AS next_effective_date
            FROM gold_beverages.dim_retailer_scd
        )
        SELECT *
        FROM OrderedSCD
        WHERE next_effective_date IS NOT NULL 
          AND end_date != next_effective_date
        """)

        print("Records with date gaps or overlaps between versions:", overlaps.count())
        if overlaps.count() == 0:
            print("PASS - Continuous date ranges across historical versions")
        else:
            print("WARNING/FAIL - Detected gap or overlap between SCD versions")
            overlaps.show()

        # Show SCD sample
        print("\n========== SCD RETAILER SAMPLE ==========")
        spark.sql("""
        SELECT retailer_id, retailer_name, status, effective_date, end_date, is_current 
        FROM gold_beverages.dim_retailer_scd
        ORDER BY retailer_id, effective_date
        """).show(10, False)


    ###############################################################
    # INVENTORY VALIDATION
    ###############################################################

    print("\n========== INVENTORY VALIDATION ==========")

    if spark.catalog.tableExists("gold_beverages.inventory_snapshot"):
        
        # Check for negative stock
        negative_stock = spark.sql("""
        SELECT *
        FROM gold_beverages.inventory_snapshot
        WHERE stock_on_hand < 0
        """)

        print("Negative Stock Records :", negative_stock.count())

        if negative_stock.count() == 0:
            print("PASS")
        else:
            print("FAIL")
            negative_stock.show()

        # Check for expired products
        expired = spark.sql("""
        SELECT *
        FROM gold_beverages.inventory_snapshot
        WHERE expiry_date < CURRENT_DATE()
        """)

        print("Expired Products :", expired.count())

        if expired.count() > 0:
            print("WARNING - Found expired products")
            expired.show(5)


    ###############################################################
    # REFERENTIAL INTEGRITY - RETAILER
    ###############################################################

    print("\n========== RETAILER REFERENTIAL INTEGRITY ==========")

    missing_retailer = spark.sql("""
    SELECT DISTINCT s.retailer_id
    FROM gold_beverages.dim_store s
    LEFT ANTI JOIN silver_beverages.retailer r
    ON s.retailer_id = r.retailer_id
    """)

    print("Missing Retailers :", missing_retailer.count())

    if missing_retailer.count() == 0:
        print("PASS")
    else:
        print("FAIL")
        missing_retailer.show()


    ###############################################################
    # SAMPLE DATA
    ###############################################################

    print("\n========== FACT SALES SAMPLE ==========")

    spark.table(
        "gold_beverages.fact_sales"
    ).show(5, False)


    print("""
============================================================
GOLD VALIDATIONS COMPLETED
============================================================
""")


# Allows direct execution if run as standalone script
if __name__ == "__main__":
    from pyspark.sql import SparkSession
    spark_session = SparkSession.builder.getOrCreate()
    run_gold_validations(spark_session)