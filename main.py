from pyspark.sql import SparkSession
from report_generator import report

spark = SparkSession.builder \
    .appName("Beverages_ETL_Framework") \
    .getOrCreate()

print("""
=================================================
BEVERAGES ETL AUTOMATION PIPELINE
=================================================
""")

# Bronze
print("STEP 1 : SOURCE TO BRONZE")
exec(open("source_to_bronze.py").read())

# Silver
print("STEP 2 : BRONZE TO SILVER")
exec(open("bronze_silver.py").read())

# Gold
print("STEP 3 : SILVER TO GOLD")
exec(open("silver_to_gold.py").read())

print("""
=================================================
ETL PIPELINE COMPLETED SUCCESSFULLY
=================================================
""")

print("Gold Tables:")

spark.sql("""
SHOW TABLES IN gold_beverages
""").show(truncate=False)

###############################################################
# GENERATE HTML REPORT
###############################################################

report.generate_report()

print("\n=================================================")
print("HTML REPORT GENERATED SUCCESSFULLY")
print("=================================================")