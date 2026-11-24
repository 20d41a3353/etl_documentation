import datetime 
from pyspark.sql import SparkSession , functions as F
import pyspark.sql.types as T

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import  to_date,expr, round, lower, col,when, length, trim,count, substring,trim, concat, lit, create_map, to_timestamp, sum,broadcast, coalesce,current_timestamp, first, input_file_name,create_map, to_timestamp, sum,broadcast, coalesce,current_timestamp, first, input_file_name,substring_index
from pyspark.sql.types import DecimalType, StringType, StructType, StructField, LongType, DateType, TimestampType
from datetime import date, timedelta
from pyspark.sql.functions import desc
import sys
from py4j.java_gateway import java_import
from delta.tables import DeltaTable 
import concurrent.futures
import builtins
from pyspark.sql import Row
from pyspark.sql.functions import expr, lpad, floor, concat_ws
from pyspark.sql.functions import ltrim, col
from common.PPF_FCNB import trickel_feed_func

import re   
# =========== Common Methods =============================
from common.createSpark import create_spark_session
from common.fileUtil import get_hdfs_base
from datetime import datetime,date
from common.read_write_oracle import read_oracle
from common.create_path_if_not_exists import create_path_if_not_exists
from pyspark.sql.functions import col, trim, substring,regexp_replace
from collections import defaultdict
from common.pattern import read_patterns
from common.properties import get_oracle_properties
from common.dateUtil import get_etl_date

from common.log_etl_modified import log_etl
from common.constants import Process
from common.processRun import get_run_id
from common.processRun import get_process_run_id
from common.logger import setup_logger 


BASE_HDFS = get_hdfs_base()
spark = create_spark_session("CBS - Bulk Data PIPELINE", BASE_HDFS)
spark.conf.set("spark.sql.debug.maxToStringFields", "10000")   


date_format = "%d-%m-%Y"
today = get_etl_date(spark)

trickel_feed_func(spark,BASE_HDFS,today)