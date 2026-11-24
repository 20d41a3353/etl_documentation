import logging
import sys
import os
import common.constants as constants
from datetime import timedelta, date, datetime
from py4j.java_gateway import java_import
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import (
    to_date,
    expr,
    round,
    col,
    when,
    length,
    substring,
    trim,
    concat,
    lit,
    create_map,
    to_timestamp,
    sum,
    broadcast,
    coalesce,
    current_timestamp,
)
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from common.dateUtil import get_etl_date
from common.date_converter import get_date_formats
from common.properties import get_hdfs_base, get_oracle_properties
from common.createSpark import create_spark_session
from common.read_write_oracle import read_oracle
from common.fileUtil import write_csv_with_filename


import datetime
from pyspark.sql import SparkSession, functions as F
import pyspark.sql.types as T
from pyspark.sql.functions import (
    to_date,
    expr,
    round,
    col,
    when,
    length,
    substring,
    trim,
    concat,
    lit,
    create_map,
    to_timestamp,
    sum,
    broadcast,
    coalesce,
)
from pyspark.sql.types import (
    DecimalType,
    StringType,
    StructType,
    StructField,
    LongType,
    DateType,
)
from datetime import date, timedelta
from pyspark.sql.functions import desc
import sys
from py4j.java_gateway import java_import
import concurrent.futures
import builtins
from pyspark.sql import Row
import re

# =========== Common Methods =============================
from common.properties import get_oracle_properties
from common.dateUtil import get_etl_date

from common.processRun import get_run_id
from common.createSpark import create_spark_session
from common.fileUtil import get_hdfs_base
from common.date_converter import get_date_formats

def cgl_suspense_report_fun(spark,HDFS_BASE,date_str):

    today_dates = get_date_formats(date_str, +1)
    oracle_date = today_dates["oracle_string"]
    query = f"(SELECT * FROM GL_TRANSACTIONS WHERE TRANSACTION_DATE =  '{oracle_date}' AND (NARRATION LIKE '%INVALID%' OR  NARRATION LIKE '%OUT%') AND CGL = '1111111111') t"

    oracle_props = get_oracle_properties()
    df = read_oracle(spark, query)
    df.show(20, False)

    invalid_txns = df.filter(col("NARRATION").like("%INVALID%"))
    invalid_txns = invalid_txns.withColumn(
        "INVALID_CGL", substring(col("NARRATION"), 19, 10)
    )
    outofbal_txns = df.filter(col("NARRATION").like("%OUT OF BAL%"))
    outofbal_txns.show()

    invalid_txns.printSchema()


    try:
        generated_df = invalid_txns.select(
            F.explode(
                F.array(
                    F.struct(
                        F.col("BATCH_ID"),
                        F.col("JOURNAL_ID"),
                        concat(F.lit(f"INVALID_CGL_CORR-"),col("INVALID_CGL")).alias("NARRATION"),
                        F.lit(1).alias("TRANSACTION_COUNT"),
                        F.col("BRANCH_CODE"),
                        F.col("CURRENCY"),
                        F.col("CGL"),
                        (F.greatest(F.col("DEBIT_AMOUNT"), F.lit(0))).alias(
                            "DEBIT_AMOUNT"
                        ),  # position 8
                        (-F.least(F.col("DEBIT_AMOUNT"), F.lit(0))).alias(
                            "CREDIT_AMOUNT"
                        ),  # position 9
                    ),
                    F.struct(
                        F.col("BATCH_ID"),
                        F.col("JOURNAL_ID"),
                        concat(F.lit(f"INVALID_CGL_CORR-"),col("INVALID_CGL")).alias("NARRATION"),
                        F.lit(1).alias("TRANSACTION_COUNT"),
                        F.col("BRANCH_CODE"),
                        F.col("CURRENCY"),
                        F.col("CGL"),
                        (-F.greatest(F.col("CREDIT_AMOUNT"), F.lit(0))).alias(
                            "DEBIT_AMOUNT"
                        ),  # position 8
                        (F.least(F.col("CREDIT_AMOUNT"), F.lit(0))).alias(
                            "CREDIT_AMOUNT"
                        ),  # position 9
                    ),
                    F.struct(
                        F.col("BATCH_ID"),
                        F.col("JOURNAL_ID"),
                        concat(F.lit(f"INVALID_CGL_CORR-"),col("INVALID_CGL")).alias("NARRATION"),
                        F.lit(1).alias("TRANSACTION_COUNT"),
                        F.col("BRANCH_CODE"),
                        F.col("CURRENCY"),
                        F.col("INVALID_CGL").alias("CGL"),
                        (F.least(F.col("CREDIT_AMOUNT"), F.lit(0))).alias(
                            "DEBIT_AMOUNT"
                        ),  # position 8
                        (F.greatest(F.col("CREDIT_AMOUNT"), F.lit(0))).alias(
                            "CREDIT_AMOUNT"
                        ),  # position 9,
                    ),
                    F.struct(
                        F.col("BATCH_ID"),
                        F.col("JOURNAL_ID"),
                        concat(F.lit(f"INVALID_CGL_CORR-"),col("INVALID_CGL")).alias("NARRATION"),
                        F.lit(1).alias("TRANSACTION_COUNT"),
                        F.col("BRANCH_CODE"),
                        F.col("CURRENCY"),
                        F.col("INVALID_CGL").alias("CGL"),
                        (F.least(F.col("DEBIT_AMOUNT"), F.lit(0))).alias(
                            "DEBIT_AMOUNT"
                        ),  # position 8
                        (F.greatest(F.col("DEBIT_AMOUNT"), F.lit(0))).alias(
                            "CREDIT_AMOUNT"
                        ),  # position 9
                    ),
                )
            ).alias("r")
        ).select("r.*")
        # generated_df.show(20, False)
    except Exception as e:
        print(f"Error {e}")

    try:
        generated_outofbal_df = outofbal_txns.select(
            F.explode(
                F.array(
                    F.struct(
                        F.col("BATCH_ID"),
                        F.col("JOURNAL_ID"),
                        F.lit("OUT OF BAL CORRECTION").alias("NARRATION"),
                        F.lit(1).alias("TRANSACTION_COUNT"),
                        F.col("BRANCH_CODE"),
                        F.col("CURRENCY"),
                        F.col("CGL"),
                        (F.greatest(F.col("DEBIT_AMOUNT"), F.lit(0))).alias(
                            "DEBIT_AMOUNT"
                        ),  # position 8
                        (-F.least(F.col("DEBIT_AMOUNT"), F.lit(0))).alias(
                            "CREDIT_AMOUNT"
                        ),  # position 9
                    ),
                    F.struct(
                        F.col("BATCH_ID"),
                        F.col("JOURNAL_ID"),
                        F.lit("OUT OF BAL CORRECTION").alias("NARRATION"),
                        F.lit(1).alias("TRANSACTION_COUNT"),
                        F.col("BRANCH_CODE"),
                        F.col("CURRENCY"),
                        F.col("CGL"),
                        (-F.greatest(F.col("CREDIT_AMOUNT"), F.lit(0))).alias(
                            "DEBIT_AMOUNT"
                        ),  # position 8
                        (F.least(F.col("CREDIT_AMOUNT"), F.lit(0))).alias(
                            "CREDIT_AMOUNT"
                        ),  # position 9
                    ),
                )
            ).alias("r")
        ).select("r.*")
        # generated_outofbal_df.show(20, False)

        final_df = generated_outofbal_df.unionByName(generated_df)

        final_df = final_df.filter((col("DEBIT_AMOUNT") != 0) | (col("CREDIT_AMOUNT") != 0))

        final_df = final_df.groupby("BRANCH_CODE","CURRENCY","CGL","NARRATION").agg(
            F.sum("DEBIT_AMOUNT").alias("DEBIT_AMOUNT"),
            F.sum("CREDIT_AMOUNT").alias("CREDIT_AMOUNT"),
            F.sum("TRANSACTION_COUNT").alias("TRANSACTION_COUNT"),
        )
        final_df.show(20, False)

        write_csv_with_filename(final_df,f"{HDFS_BASE}/druid-data-lake/{today_dates['year']}/{today_dates['month']}/{today_dates['day']}/GLIF/",filename='CGL_Suspense_Report',sep="|")

    except Exception as e:
        print(f"Error {e}")

if __name__ == "__main__":
    HDFS_BASE = get_hdfs_base()
    spark = create_spark_session("CGL Suspense Report", HDFS_BASE)
    date_str = get_etl_date(spark)
    cgl_suspense_report_fun(spark,HDFS_BASE,date_str)






















































    

      
# generated_df=generated_df.groupBy("BRANCH_CODE", "CURRENCY", "CGL").agg(
#         F.sum("CREDIT_AMOUNT").alias("CREDIT_AMOUNT"),
#         F.sum("DEBIT_AMOUNT").alias("DEBIT_AMOUNT"),
#         F.lit("P").alias("SOURCE_FLAG"),
#         F.sum("TRANSACTION_COUNT").alias("TRANSACTION_COUNT"),
#         F.first("NARRATION").alias("NARRATION"),
#         F.first("BATCH_ID").alias("BATCH_ID"),
#         F.first("JOURNAL_ID").alias("JOURNAL_ID"),
#     ).select("BRANCH_CODE", "CURRENCY", "CGL","CREDIT_AMOUNT","DEBIT_AMOUNT","BATCH_ID","JOURNAL_ID","NARRATION","TRANSACTION_COUNT","SOURCE_FLAG")

# +--------------+--------+----------+-------------------+-------------------+-----------+--------+----------+----------------------------+------------+-------------+-----------------+-----------+
# |TRANSACTION_ID|BATCH_ID|JOURNAL_ID|TRANSACTION_DATE   |POST_DATE          |BRANCH_CODE|CURRENCY|CGL       |NARRATION                   |DEBIT_AMOUNT|CREDIT_AMOUNT|TRANSACTION_COUNT|SOURCE_FLAG|
# +--------------+--------+----------+-------------------+-------------------+-----------+--------+----------+----------------------------+------------+-------------+-----------------+-----------+
# |144461261     |2288    |NULL      |2026-04-02 00:00:00|2026-04-20 18:56:08|06260      |INR     |1111111111|CBS txns- INVALID-1006070640|-999.0000   |2468.0000    |3                |C          |
# +--------------+--------+----------+-------------------+-------------------+-----------+--------+----------+----------------------------+------------+-------------+-----------------+-----------+

# +--------+----------+----------------------+-----------------+-----------+--------+----------+------------+-------------+
# |BATCH_ID|JOURNAL_ID|NARRATION             |TRANSACTION_COUNT|BRANCH_CODE|CURRENCY|CGL       |DEBIT_AMOUNT|CREDIT_AMOUNT|
# +--------+----------+----------------------+-----------------+-----------+--------+----------+------------+-------------+
# |2288    |NULL      |INVALID CGL CORRECTION|1                |06260      |INR     |1111111111|0.0000      |999.0000     |
# |2288    |NULL      |INVALID CGL CORRECTION|1                |06260      |INR     |1111111111|-2468.0000  |0.0000       |
# |2288    |NULL      |INVALID CGL CORRECTION|1                |06260      |INR     |1006070640|0.0000      |2468.0000    |
# |2288    |NULL      |INVALID CGL CORRECTION|1                |06260      |INR     |1006070640|-999.0000   |0.0000       |
# +--------+----------+----------------------+-----------------+-----------+--------+----------+------------+-------------+
