###############################################################
# SOURCE TO BRONZE
# ETL AUTOMATION TESTING FRAMEWORK
###############################################################

from spark_session import spark
from config import SOURCE_TABLES
from common_functions import *
from validations import *


###############################################################
# SOURCE TO BRONZE PROCESS
###############################################################

def run_source_to_bronze():


    for name, table in SOURCE_TABLES.items():


        print("\n" + "="*80)
        print("SOURCE TO BRONZE :", name.upper())
        print("="*80)



        #######################################################
        # READ SOURCE FILE
        #######################################################

        source = spark.read.csv(

            table["file"],

            header=True,

            inferSchema=True

        )



        #######################################################
        # READ BRONZE TABLE
        #######################################################

        bronze = spark.table(

            table["bronze"]

        )



        #######################################################
        # FILE VALIDATION
        #######################################################

        file_validation(

            table["file"]

        )



        #######################################################
        # ROW COUNT VALIDATION
        #######################################################

        row_count_validation(

            source,

            bronze

        )



        #######################################################
        # COLUMN COUNT VALIDATION
        #######################################################

        column_count_validation(

            source,

            bronze

        )



        #######################################################
        # COLUMN NAME VALIDATION
        #######################################################

        column_name_validation(

            source,

            bronze

        )



        #######################################################
        # SCHEMA VALIDATION
        #######################################################

        schema_validation(

            source,

            bronze

        )



        #######################################################
        # DATA TYPE VALIDATION
        #######################################################
        
      

        #######################################################
        # COMPLETENESS VALIDATION
        #######################################################

        completeness_validation(

            source

        )



        #######################################################
        # NULL VALIDATION
        #######################################################

        null_validation(

            source

        )



        #######################################################
        # DUPLICATE VALIDATION
        #######################################################

        duplicate_validation(

            source,

            table["primary_key"]

        )



        #######################################################
        # PRIMARY KEY VALIDATION
        #######################################################

        primary_key_validation(

            source,

            table["primary_key"]

        )



        #######################################################
        # MISSING RECORD VALIDATION
        #######################################################

        missing_record_validation(

            source,

            bronze,

            table["primary_key"]

        )



        #######################################################
        # EXTRA RECORD VALIDATION
        #######################################################

        extra_record_validation(

            source,

            bronze,

            table["primary_key"]

        )



        #######################################################
        # SOURCE TARGET VALIDATION
        #######################################################

        source_target_validation(

            source,

            bronze,

            table["primary_key"]

        )



        #######################################################
        # DATA RECONCILIATION
        #######################################################

        reconciliation(

            source,

            bronze

        )



        #######################################################
        # SAMPLE DATA
        #######################################################

        print("\n========== BRONZE SAMPLE DATA ==========")

        bronze.show(

            5,

            False

        )



    print("\n" + "="*80)
    print("SOURCE TO BRONZE COMPLETED")
    print("="*80)





###############################################################
# MAIN EXECUTION
###############################################################

if __name__ == "__main__":


    run_source_to_bronze()