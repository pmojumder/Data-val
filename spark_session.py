###############################################################
# SPARK SESSION
###############################################################

from pyspark.sql import SparkSession

###############################################################
# CREATE SPARK SESSION
###############################################################

spark = SparkSession.builder \
    .appName("ETL Automation Framework") \
    .getOrCreate()

###############################################################
# DISPLAY MESSAGE
###############################################################

print("=" * 60)
print("Spark Session Created Successfully")
print("=" * 60)