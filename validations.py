###############################################################
# ETL AUTOMATION TESTING VALIDATIONS FRAMEWORK
###############################################################

from pyspark.sql.functions import *
import os

from report_generator import report


###############################################################
# FILE VALIDATION
###############################################################

def file_validation(file_path):

    print("\n========== FILE VALIDATION ==========")

    if os.path.exists(file_path):

        print("PASS - File Exists")

        report.add_result(
            "File Validation",
            "PASS",
            file_path
        )

    else:

        print("FAIL - File Not Found")

        report.add_result(
            "File Validation",
            "FAIL",
            file_path
        )


###############################################################
# ROW COUNT VALIDATION
###############################################################

def row_count_validation(source, target):

    print("\n========== ROW COUNT VALIDATION ==========")

    source_count = source.count()
    target_count = target.count()

    print("Source Row Count :", source_count)
    print("Target Row Count :", target_count)

    if source_count == target_count:

        print("PASS")

        report.add_result(
            "Row Count Validation",
            "PASS",
            f"Source={source_count}, Target={target_count}"
        )

    else:

        print("FAIL")

        report.add_result(
            "Row Count Validation",
            "FAIL",
            f"Source={source_count}, Target={target_count}"
        )


###############################################################
# COLUMN COUNT VALIDATION
###############################################################

def column_count_validation(source, target):

    print("\n========== COLUMN COUNT VALIDATION ==========")

    source_count = len(source.columns)
    target_count = len(target.columns)

    print("Source Column Count :", source_count)
    print("Target Column Count :", target_count)

    if source_count == target_count:

        print("PASS")

        report.add_result(
            "Column Count Validation",
            "PASS",
            f"Source={source_count}, Target={target_count}"
        )

    else:

        print("FAIL")

        report.add_result(
            "Column Count Validation",
            "FAIL",
            f"Source={source_count}, Target={target_count}"
        )


###############################################################
# COLUMN NAME VALIDATION
###############################################################

def column_name_validation(source, target):

    print("\n========== COLUMN NAME VALIDATION ==========")

    print("Source Columns :", source.columns)
    print("Target Columns :", target.columns)

    if source.columns == target.columns:

        print("PASS")

        report.add_result(
            "Column Name Validation",
            "PASS",
            "Source and Target column names match"
        )

    else:

        print("FAIL")

        report.add_result(
            "Column Name Validation",
            "FAIL",
            f"Source={source.columns}, Target={target.columns}"
        )


###############################################################
# SCHEMA VALIDATION
###############################################################

def schema_validation(source, target):

    print("\n========== SCHEMA VALIDATION ==========")

    print("\nSource Schema")
    source.printSchema()

    print("\nTarget Schema")
    target.printSchema()

    if source.dtypes == target.dtypes:

        print("PASS")

        report.add_result(
            "Schema Validation",
            "PASS",
            "Source and Target schema matched"
        )

    else:

        print("FAIL")

        report.add_result(
            "Schema Validation",
            "FAIL",
            "Schema mismatch"
        )


###############################################################
# DATA TYPE VALIDATION
###############################################################

def datatype_validation(source, target):

    print("\n========== DATA TYPE VALIDATION ==========")

    source_types = dict(source.dtypes)
    target_types = dict(target.dtypes)

    mismatch = []

    for column in source.columns:

        if column in target_types:

            if source_types[column] != target_types[column]:

                mismatch.append(
                    f"{column} : {source_types[column]} -> {target_types[column]}"
                )

    if len(mismatch) == 0:

        print("PASS")

        report.add_result(
            "Data Type Validation",
            "PASS",
            "All data types matched"
        )

    else:

        print("FAIL")

        for item in mismatch:
            print(item)

        report.add_result(
            "Data Type Validation",
            "FAIL",
            ", ".join(mismatch)
        )
    
    ###############################################################
# NULL VALIDATION
###############################################################

def null_validation(df):

    print("\n========== NULL VALIDATION ==========")

    null_df = df.select(
        [
            count(
                when(col(c).isNull(), c)
            ).alias(c)
            for c in df.columns
        ]
    )

    null_df.show()

    result = null_df.collect()[0]

    if any(value > 0 for value in result):

        print("FAIL - NULL Values Found")

        report.add_result(
            "Null Validation",
            "FAIL",
            str(result)
        )

    else:

        print("PASS - No NULL Values")

        report.add_result(
            "Null Validation",
            "PASS",
            "No NULL values found"
        )


###############################################################
# COMPLETENESS VALIDATION
###############################################################

def completeness_validation(df):

    print("\n========== DATA COMPLETENESS ==========")

    rows = df.count()
    columns = len(df.columns)

    print("Rows :", rows)
    print("Columns :", columns)

    if rows > 0 and columns > 0:

        print("PASS")

        report.add_result(
            "Completeness Validation",
            "PASS",
            f"Rows={rows}, Columns={columns}"
        )

    else:

        print("FAIL")

        report.add_result(
            "Completeness Validation",
            "FAIL",
            f"Rows={rows}, Columns={columns}"
        )


###############################################################
# DUPLICATE VALIDATION
###############################################################

def duplicate_validation(df, primary_key):

    print("\n========== DUPLICATE VALIDATION ==========")

    duplicate = (
        df.groupBy(primary_key)
          .count()
          .filter(col("count") > 1)
    )

    duplicate.show()

    duplicate_count = duplicate.count()

    if duplicate_count == 0:

        print("PASS - No Duplicate Records")

        report.add_result(
            "Duplicate Validation",
            "PASS",
            "No duplicate records"
        )

    else:

        print("FAIL - Duplicate Records Found")

        report.add_result(
            "Duplicate Validation",
            "FAIL",
            f"Duplicate Records={duplicate_count}"
        )


###############################################################
# PRIMARY KEY VALIDATION
###############################################################

def primary_key_validation(df, primary_key):

    print("\n========== PRIMARY KEY VALIDATION ==========")

    result = (
        df.groupBy(primary_key)
          .count()
          .filter(col("count") > 1)
    )

    result.show()

    violation_count = result.count()

    if violation_count == 0:

        print("PASS - Primary Key Unique")

        report.add_result(
            "Primary Key Validation",
            "PASS",
            "Primary key is unique"
        )

    else:

        print("FAIL - Primary Key Violation")

        report.add_result(
            "Primary Key Validation",
            "FAIL",
            f"Violations={violation_count}"
        )


###############################################################
# BUSINESS RULE VALIDATION
###############################################################

def business_rule_validation(df, condition, rule_name):

    print("\n========== BUSINESS RULE VALIDATION ==========")

    failed_records = df.filter(condition)

###############################################################
# MISSING RECORD VALIDATION
###############################################################

def missing_record_validation(source, target, primary_key):

    print("\n========== MISSING RECORD VALIDATION ==========")

    missing = source.join(
        target,
        primary_key,
        "left_anti"
    )

    missing_count = missing.count()

    print("Missing Records :", missing_count)

    if missing_count == 0:

        print("PASS")

        report.add_result(
            "Missing Record Validation",
            "PASS",
            "No missing records"
        )

    else:

        print("FAIL")

        missing.show(10, False)

        report.add_result(
            "Missing Record Validation",
            "FAIL",
            f"Missing Records={missing_count}"
        )


###############################################################
# EXTRA RECORD VALIDATION
###############################################################

def extra_record_validation(source, target, primary_key):

    print("\n========== EXTRA RECORD VALIDATION ==========")

    extra = target.join(
        source,
        primary_key,
        "left_anti"
    )

    extra_count = extra.count()

    print("Extra Records :", extra_count)

    if extra_count == 0:

        print("PASS")

        report.add_result(
            "Extra Record Validation",
            "PASS",
            "No extra records"
        )

    else:

        print("FAIL")

        extra.show(10, False)

        report.add_result(
            "Extra Record Validation",
            "FAIL",
            f"Extra Records={extra_count}"
        )


###############################################################
# SOURCE TARGET VALIDATION
###############################################################

def source_target_validation(source, target, primary_key):

    print("\n========== SOURCE TARGET VALIDATION ==========")

    matched = source.alias("S").join(
        target.alias("T"),
        primary_key,
        "inner"
    )

    matched_count = matched.count()

    print("Matched Records :", matched_count)

    matched.show(10, False)

    report.add_result(
        "Source Target Validation",
        "PASS",
        f"Matched Records={matched_count}"
    )


###############################################################
# DATA RECONCILIATION
###############################################################

def reconciliation(source, target):

    print("\n========== DATA RECONCILIATION ==========")

    source_count = source.count()
    target_count = target.count()

    difference = target_count - source_count

    print("Source Count :", source_count)
    print("Target Count :", target_count)
    print("Difference   :", difference)

    if difference == 0:

        print("PASS")

        report.add_result(
            "Data Reconciliation",
            "PASS",
            f"Difference={difference}"
        )

    else:

        print("FAIL")

        report.add_result(
            "Data Reconciliation",
            "FAIL",
            f"Difference={difference}"
        )


###############################################################
# SAMPLE DATA
###############################################################

def sample_data(df):

    print("\n========== SAMPLE DATA ==========")

    df.show(5, False)

    report.add_result(
        "Sample Data",
        "INFO",
        "Displayed first 5 records"
    )    
###############################################################
# RUN ALL VALIDATIONS
###############################################################

def run_all_validations(source, target, primary_key):

    print("\n")
    print("=" * 80)
    print("RUNNING ALL VALIDATIONS")
    print("=" * 80)

    try:
        file_validation("Source File")
    except Exception as e:
        print("File Validation Failed :", e)
        report.add_result("File Validation", "ERROR", str(e))

    try:
        row_count_validation(source, target)
    except Exception as e:
        print("Row Count Validation Failed :", e)
        report.add_result("Row Count Validation", "ERROR", str(e))

    try:
        column_count_validation(source, target)
    except Exception as e:
        print("Column Count Validation Failed :", e)
        report.add_result("Column Count Validation", "ERROR", str(e))

    try:
        column_name_validation(source, target)
    except Exception as e:
        print("Column Name Validation Failed :", e)
        report.add_result("Column Name Validation", "ERROR", str(e))

    try:
        schema_validation(source, target)
    except Exception as e:
        print("Schema Validation Failed :", e)
        report.add_result("Schema Validation", "ERROR", str(e))

    try:
        datatype_validation(source, target)
    except Exception as e:
        print("Data Type Validation Failed :", e)
        report.add_result("Data Type Validation", "ERROR", str(e))

    try:
        completeness_validation(source)
    except Exception as e:
        print("Completeness Validation Failed :", e)
        report.add_result("Completeness Validation", "ERROR", str(e))

    try:
        null_validation(source)
    except Exception as e:
        print("Null Validation Failed :", e)
        report.add_result("Null Validation", "ERROR", str(e))

    try:
        duplicate_validation(source, primary_key)
    except Exception as e:
        print("Duplicate Validation Failed :", e)
        report.add_result("Duplicate Validation", "ERROR", str(e))

    try:
        primary_key_validation(source, primary_key)
    except Exception as e:
        print("Primary Key Validation Failed :", e)
        report.add_result("Primary Key Validation", "ERROR", str(e))

    try:
        missing_record_validation(source, target, primary_key)
    except Exception as e:
        print("Missing Record Validation Failed :", e)
        report.add_result("Missing Record Validation", "ERROR", str(e))

    try:
        extra_record_validation(source, target, primary_key)
    except Exception as e:
        print("Extra Record Validation Failed :", e)
        report.add_result("Extra Record Validation", "ERROR", str(e))

    try:
        source_target_validation(source, target, primary_key)
    except Exception as e:
        print("Source Target Validation Failed :", e)
        report.add_result("Source Target Validation", "ERROR", str(e))

    try:
        reconciliation(source, target)
    except Exception as e:
        print("Data Reconciliation Failed :", e)
        report.add_result("Data Reconciliation", "ERROR", str(e))

    try:
        sample_data(target)
    except Exception as e:
        print("Sample Data Failed :", e)
        report.add_result("Sample Data", "ERROR", str(e))

    print("\n")
    print("=" * 80)
    print("ALL VALIDATIONS COMPLETED")
    print("=" * 80)
    failure_count = failed_records.count()

    print(rule_name)
    print("Failed Records :", failure_count)

    if failure_count == 0:

        print("PASS")

        report.add_result(
            rule_name,
            "PASS",
            "Business rule satisfied"
        )

    else:

        print("FAIL")

        failed_records.show()

        report.add_result(
            rule_name,
            "FAIL",
            f"Failed Records={failure_count}"
        )