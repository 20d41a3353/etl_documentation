import datetime 
from pyspark.sql import SparkSession , functions as F
from pyspark.sql.functions import to_date,expr, col,when, length, substring,trim, concat, lit, create_map, to_timestamp, sum,broadcast
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
def read_patterns(spark,all_paths):
    sc = spark.sparkContext
    jvm = sc._jvm
    jsc = sc._jsc
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jsc.hadoopConfiguration())
    Path = jvm.org.apache.hadoop.fs.Path
    valid_patterns = []
    invalid_patterns = []

    print("Checking paths...")

    for path in all_paths:
        if is_valid_glob(fs,Path,path):
            print(f"Found: {path}")
            valid_patterns.append(path)
        else:
            invalid_patterns.append(path)
            print(f"Skipping (No files match): {path}")

    return valid_patterns,invalid_patterns
    

  
def is_valid_glob(fs,Path,path_pattern):
    
    try:
        matches = fs.globStatus(Path(path_pattern))
        return matches is not None and len(matches) > 0
    except Exception as e:
        print(f"Error checking {path_pattern}: {e}")
        return False



    
   
   
   
   
   
   
   