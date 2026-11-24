from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace
import datetime 
from pyspark.sql import SparkSession , functions as F
import pyspark.sql.types as T
from pyspark.sql.functions import to_date,expr, col,when, length, substring,trim, concat, lit, create_map, to_timestamp, sum,broadcast, coalesce
from pyspark.sql.types import DecimalType, StringType, StructType, StructField, LongType 
from datetime import date, timedelta
from pyspark.sql.functions import desc
import sys
from py4j.java_gateway import java_import
import concurrent.futures
import builtins
from pyspark.sql import Row
import re
# =========== Common Methods =============================
from common.logger import setup_logger 
from common.properties import get_oracle_properties
from common.dateUtil import get_etl_date
from common.fileUtil import files_to_read
from common.fileUtil import paths_for_read
from common.processRun import get_run_id
from common.pattern import read_patterns
from common.createSpark import create_spark_session
from common.fileUtil import get_hdfs_base


HDFS_BASE = get_hdfs_base()
spark = create_spark_session("GLIF_PIPELINE", HDFS_BASE)
   
today = get_etl_date(spark)
print(today)
yesterday = today - timedelta(days=1)

posting_date_str = today.strftime("%Y-%m-%d")
RUN_ID = get_run_id(spark,posting_date_str)   
print(RUN_ID)
today_str = posting_date_str
yesterday=yesterday.strftime("%Y-%m-%d")
today= today.strftime("%Y-%m-%d")

logger = setup_logger("GLIF", RUN_ID , posting_date_str)

logger.info(f"=== Using current date for POST_DATE: {posting_date_str} ===")
logger.info(f"RUN_ID as : {RUN_ID}")

logger.info("Fetched oracle properties SUCCESFULLY :)")

oracle_properties=get_oracle_properties()
print(oracle_properties)

oracle_url = oracle_properties["url"]
oracle_user = oracle_properties["user"]
oracle_password = oracle_properties["password"]
oracle_driver = oracle_properties["driver"]
DriverManager = spark._jvm.java.sql.DriverManager


sc = spark.sparkContext
# 1. Setup Hadoop FileSystem Access
jvm = sc._jvm
jsc = sc._jsc
fs = jvm.org.apache.hadoop.fs.FileSystem.get(jsc.hadoopConfiguration())
Path = jvm.org.apache.hadoop.fs.Path


hdfs_paths = paths_for_read("glif",today,today_str)

HDFS_BASE = hdfs_paths["HDFS_BASE"]
HDFS_PATH = hdfs_paths["HDFS_PATH"]
GL_DATALAKE_PATH = hdfs_paths["GL_DATALAKE_PATH"]
manifest_filename = hdfs_paths["manifest_filename"]
spark_stream_path = hdfs_paths["spark_stream_path"] + manifest_filename

files = files_to_read(spark,"glif")
print(files)
file_types_with_extra = files["stream_files"]
file_types_without_extra = files["non_stream_files"]

paths_to_read = [f"{HDFS_PATH}*{ft}_[0-9][0-9][0-9]*.gz" for ft in file_types_with_extra]
paths_to_read.extend([f"{HDFS_PATH}*{ft}.gz" for ft in file_types_without_extra])
# =========================================================================================================================
# =========================================================================================================================

# =========================================================================================================================
# =========================================================================================================================
df_raw,listPatterns = read_patterns(spark,paths_to_read,posting_date_str,RUN_ID)

data =[listPatterns]
print(data)

df = spark.createDataFrame(data, ["filename"])
# 2. Define Regex for Dates
# Matches 8 consecutive digits OR 4 digits + sep + 2 digits + sep + 2 digits
date_regex = r"(\d{8}|\d{4}[-\_]\d{2}[-\_]\d{2})"
# 3. Apply Transformations
# Step A: Remove .gz
# Step B: Remove the Date pattern (replace with empty string or underscores)
# Step C: Replace remaining digits with [0-9]
result_df = df.withColumn(
   "cleaned_name",
   regexp_replace(col("filename"), "\.gz$", "")
).withColumn(
   "no_date_name",
   regexp_replace(col("cleaned_name"), date_regex, "") # Removes the date string entirely
).withColumn(
   "pattern",
   regexp_replace(col("no_date_name"), "[0-9]", "[0-9]")
)
# 4. Clean up trailing/leading underscores or double underscores left by date removal (Optional but Recommended)
# This handles cases where 'Name_20230101_123' becomes 'Name__123'
final_df = result_df.withColumn(
   "pattern",
   regexp_replace(col("pattern"), "_{2,}", "_") # Fix double underscores
).withColumn(
   "pattern",
   regexp_replace(col("pattern"), "_$", "")     # Fix trailing underscore
)
# 5. Show results
final_df.select("filename", "pattern").distinct().show(truncate=False)