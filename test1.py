from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import re

# -----------------------------------------------
# UDF 1: Null Check
# -----------------------------------------------
@F.udf(returnType=StringType())
def null_check_udf(value):
    if value is None:
        return "Null Value"
    if str(value).strip() == "":
        return "Null Value"
    if str(value).strip().lower() in ("nan", "none", "null"):
        return "Null Value"
    return None

# -----------------------------------------------
# UDF 2: Email Validation
# -----------------------------------------------
@F.udf(returnType=StringType())
def email_check_udf(value):
    if value is None:
        return "Null Value"
    if str(value).strip() == "":
        return "Null Value"
    if str(value).strip().lower() in ("nan", "none", "null"):
        return "Null Value"
    
    pattern = r'^[\w\.-]+@[\w\.-]+\.\w{2,}$'
    if not re.match(pattern, str(value).strip()):
        return "Invalid Email Format"
    
    return None

# -----------------------------------------------
# UDF 3: Mobile Validation ✅ NEW
# -----------------------------------------------
@F.udf(returnType=StringType())
def mobile_check_udf(value):
    if value is None:
        return "Null Value"
    if str(value).strip() == "":
        return "Null Value"
    if str(value).strip().lower() in ("nan", "none", "null"):
        return "Null Value"
    
    # Remove spaces, dashes, brackets, plus sign
    cleaned = re.sub(r'[\s\-\(\)\+]', '', str(value).strip())
    
    # Check all digits
    if not cleaned.isdigit():
        return "Invalid Mobile - Non Numeric"
    
    # Check length
    if len(cleaned) == 10:
        return None  # Valid local (e.g. 9876543210)
    elif len(cleaned) in (11, 12, 13):
        return None  # Valid with country code (e.g. 919876543210)
    else:
        return "Invalid Mobile - Wrong Length"

# -----------------------------------------------
# Config
# -----------------------------------------------
pk_col = "agent_code"
dq_final_df = dq_audit_empty_df

# -----------------------------------------------
# Rules Filters
# -----------------------------------------------
Null_Check   = RULES_TABL_Df.filter("Rules == 'null check'")
Email_Check  = RULES_TABL_Df.filter("Rules == 'email check'")
Mobile_Check = RULES_TABL_Df.filter("Rules == 'mobile check'")  # ✅ NEW

# -----------------------------------------------
# Loop 1: Null Check
# -----------------------------------------------
for r in Null_Check.toLocalIterator():
    table    = r["Source_Table"]
    col_name = r["Column"]
    params   = r["Params"]

    df = spark.table(f"{BRONZE_CATALOG}.{table}") \
              .select(pk_col, col_name)

    dq_df = (
        df.withColumn("DQ_Issue", null_check_udf(F.col(col_name)))
          .withColumn("DQ_Dimension", F.lit("Conformity"))
          .filter(F.col("DQ_Issue").isNotNull())
          .select(
              pk_col,
              F.col(col_name).alias("Column_Value"),
              "DQ_Issue",
              "DQ_Dimension"
          )
          .withColumn("Column_Name", F.lit(col_name))
          .withColumn("Schema_Name", F.lit(BRONZE_CATALOG))
          .withColumn("Table_Name",  F.lit(table))
          .withColumn("Rule_ID",     F.lit(r["Rule_ID"]))
    )
    dq_final_df = dq_final_df.unionByName(dq_df)

# -----------------------------------------------
# Loop 2: Email Check
# -----------------------------------------------
for r in Email_Check.toLocalIterator():
    table    = r["Source_Table"]
    col_name = r["Column"]
    params   = r["Params"]

    df = spark.table(f"{BRONZE_CATALOG}.{table}") \
              .select(pk_col, col_name)

    dq_df = (
        df.withColumn("DQ_Issue", email_check_udf(F.col(col_name)))
          .withColumn("DQ_Dimension", F.lit("Conformity"))
          .filter(F.col("DQ_Issue").isNotNull())
          .select(
              pk_col,
              F.col(col_name).alias("Column_Value"),
              "DQ_Issue",
              "DQ_Dimension"
          )
          .withColumn("Column_Name", F.lit(col_name))
          .withColumn("Schema_Name", F.lit(BRONZE_CATALOG))
          .withColumn("Table_Name",  F.lit(table))
          .withColumn("Rule_ID",     F.lit(r["Rule_ID"]))
    )
    dq_final_df = dq_final_df.unionByName(dq_df)

# -----------------------------------------------
# Loop 3: Mobile Check ✅ NEW
# -----------------------------------------------
for r in Mobile_Check.toLocalIterator():
    table    = r["Source_Table"]
    col_name = r["Column"]
    params   = r["Params"]

    df = spark.table(f"{BRONZE_CATALOG}.{table}") \
              .select(pk_col, col_name)

    dq_df = (
        df.withColumn("DQ_Issue", mobile_check_udf(F.col(col_name)))
          .withColumn("DQ_Dimension", F.lit("Conformity"))
          .filter(F.col("DQ_Issue").isNotNull())
          .select(
              pk_col,
              F.col(col_name).alias("Column_Value"),
              "DQ_Issue",
              "DQ_Dimension"
          )
          .withColumn("Column_Name", F.lit(col_name))
          .withColumn("Schema_Name", F.lit(BRONZE_CATALOG))
          .withColumn("Table_Name",  F.lit(table))
          .withColumn("Rule_ID",     F.lit(r["Rule_ID"]))
    )
    dq_final_df = dq_final_df.unionByName(dq_df)

# -----------------------------------------------
# Final Audit Result
# -----------------------------------------------
dq_final_df.show()