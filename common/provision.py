import datetime 
from pyspark.sql import SparkSession , functions as F
import pyspark.sql.types as T
from pyspark.sql.functions import to_date,expr, count,round, col,when, length, substring,trim, concat, lit, create_map, to_timestamp, sum,broadcast, coalesce
from pyspark.sql.types import DecimalType, StringType, StructType, StructField, LongType, DateType
from datetime import date, timedelta
from pyspark.sql.functions import desc
import sys
from py4j.java_gateway import java_import
import concurrent.futures
import builtins
from pyspark.sql import Row
import re
from pyspark.sql.functions import col, date_format, to_date
from pyspark.sql.functions import current_date, last_day, date_sub
# =========== Common Methods =============================
from common.logger import setup_logger 
from common.properties import get_oracle_properties
from common.dateUtil import get_etl_date
from common.fileUtil import files_to_read
from common.fileUtil import paths_for_read
from common.fileUtil import tables_to_read
from common.processRun import get_run_id
from common.pattern import read_patterns
from common.createSpark import create_spark_session
from common.fileUtil import get_hdfs_base
from common.check_monthend import check_Previous_MonthEnd
from common.read_write_oracle import read_oracle
from common.read_write_oracle import write_oracle
from common.fileUtil import get_delta_path_by_file_type  
from common.batch_journal_id import get_batch_id
from common.fileUtil import file_processing_paths
# ========================================================
HDFS_BASE = get_hdfs_base()




# ||||||||| ORACLE TABLES |||||||||||||||
oracle_tables = tables_to_read("glif")

TRANSACTIONS = oracle_tables["TRANSACTIONS"]
BALANCE = oracle_tables["BALANCE"]
INVALID = oracle_tables["INVALID"]
CGLS = oracle_tables["CGLS"]
CURRENCY = oracle_tables["CURRENCY"]
BRANCH = oracle_tables["BRANCH"]
# ||||||||| ORACLE TABLES |||||||||||||||


def process_provision_data(spark,RUN_ID,BATCH_ID_LITERAL=None):

    # ===================== DATES =====================
    etl_date = get_etl_date(spark)
    etl_str = etl_date.strftime("%Y-%m-%d")
    

    # RUN_ID = get_run_id(spark, etl_str, "GLIF")
    logger = setup_logger("GLIF", RUN_ID, etl_str)

    logger.info(f"ETL_DATE : {etl_date}")

    # =========================================================
    # READING GLIF RAW DELTA (DATALAKE)
    # =========================================================
    
    paths = file_processing_paths(spark,"glif_processing")    
    GLIF_TXNS_DATALAKE =  paths["GLIF_TXNS_DATALAKE"]
    GLIF_TXNS_DATALAKE = f"{GLIF_TXNS_DATALAKE}/GLIF_TXNS/"
    df_raw = spark.read.format("delta").load(GLIF_TXNS_DATALAKE)

    # =========================================================
    # ONLY PROVISION FILES (GLIFPRI & GLIFPRB)
    # =========================================================

    provision_file_type_query = f"(select FILE_PATTERN from file_type_master where type = 'glif' and PRIORITY ='MONTH_END') e"
    provision_file_type = read_oracle(spark,provision_file_type_query)
    provision_file_type.show(20,False)
    provision_file_type_list = provision_file_type.select("FILE_PATTERN").rdd.flatMap(lambda x: x).collect()

    regex = "|".join(re.escape(p).replace(r"\*", ".*")
                        for p in provision_file_type_list
                        )

    df_raw = df_raw.filter(F.col("file_name").rlike(f"^({regex})"))
    

    # df_raw = df_raw.filter(
    #     (F.lower(F.col("file_name")).startswith("glifpri")) |
    #     (F.lower(F.col("file_name")).startswith("glifprb"))
    # )

    logger.info("Filtered only provision files")

    # =========================================================
    # CREATING GLCC USING PARSED COLUMNS (NO SUBSTRING ANYMORE)
    # =========================================================
    logger.info(f"Reading Patterns")
    df_final = df_raw.withColumn("GLCC", concat(col("BRANCH_CODE"), col("CURRENCY"), col("CGL"))).select("GLCC","BRANCH_CODE","CURRENCY","CGL","AMOUNT") 
   
    df_with_signed_amount = df_final.withColumns({
        "CREDIT_AMOUNT": when(df_final["AMOUNT"] > 0, df_final["AMOUNT"]).otherwise(0),
        "DEBIT_AMOUNT": when(df_final["AMOUNT"] < 0, df_final["AMOUNT"]).otherwise(0)
    })
    
    try:
        
        df_agg = df_with_signed_amount.groupBy("GLCC").agg(
            sum(col("CREDIT_AMOUNT")).alias("CREDIT_AMOUNT"),
            sum(col("DEBIT_AMOUNT")).alias("DEBIT_AMOUNT"),
            count(col("GLCC")).alias("TRANSACTION_COUNT")
        )
        
    except Exception as e:
        logger.info(f"Error reading files... {e}")
        spark.stop()
        sys.exit(1) 

    
    # =========================================================
    # JOURNAL CREATION
    # =========================================================
    df_mapped = df_agg.withColumns({
        "BATCH_ID": lit(BATCH_ID_LITERAL).cast(StringType()),
        "JOURNAL_ID": lit(None).cast(StringType()),
        "BRANCH_CODE": substring(col("GLCC"), 1, 5),
        "CURRENCY": substring(col("GLCC"), 6, 3),
        "CGL": substring(col("GLCC"), 9, 10),
        "NARRATION": concat(lit("Prov-"), date_format(to_date(lit(etl_str)), "MMMyyyy")),
        "SOURCE_FLAG": lit("V").cast(StringType())
    })

    provision_df = df_mapped.select(
        "BATCH_ID",
        "JOURNAL_ID",
        "BRANCH_CODE",
        "CURRENCY",
        "CGL",
        "NARRATION",
        "CREDIT_AMOUNT",
        "DEBIT_AMOUNT",
        "TRANSACTION_COUNT",
        "SOURCE_FLAG"
    )
    provision_df.show(20,False)

    logger.info("Provision dataframe created successfully")

    

    return provision_df

# =========================================================
def reverse_provision(spark,TRANSACTIONS):
    BATCH_ID_LITERAL = get_batch_id(spark)

    prev_month_end = check_Previous_MonthEnd()
    prev_month_end_str = prev_month_end.strftime("%Y-%m-%d")
    prev_month_end_oracle = prev_month_end.strftime("%d-%b-%y")
    print(f"{prev_month_end_str}")

    fincoredate = get_etl_date(spark)
    fincoredate = fincoredate.replace(day=25)
    fincoredate_str = fincoredate.strftime("%d-%b-%y")


    sql_Query = f"""(
    select * from {TRANSACTIONS}
    where TRANSACTION_DATE in('{prev_month_end_oracle}','{fincoredate_str}')
    and source_flag in ('V','I')
    ) t"""



    last_month_provision_df = read_oracle(spark, sql_Query)
    last_month_provision_df.show(20,False)

    final_df = last_month_provision_df.select(
        col("JOURNAL_ID"),
        col("BRANCH_CODE"),
        col("CURRENCY"),
        col("CGL"),
        when(col("SOURCE_FLAG") == "V", concat(lit("Prov Rev "),  date_format(to_date(lit(prev_month_end_str)), "MMMyyyy"))).
        when(col("SOURCE_FLAG") == "I", concat(lit("SB Rev "),    date_format(to_date(lit(fincoredate)), "MMMyyyy"))).alias("NARRATION"),
        (-col("DEBIT_AMOUNT")).alias("CREDIT_AMOUNT"),
        (-col("CREDIT_AMOUNT")).alias("DEBIT_AMOUNT"),
        col("TRANSACTION_COUNT"),
        col("SOURCE_FLAG")
    ).withColumn("SOURCE_FLAG",
    when(col("SOURCE_FLAG") == "V", "R")
    .when(col("SOURCE_FLAG") == "I", "S")
    .otherwise(col("SOURCE_FLAG"))).withColumn("BATCH_ID", F.lit(BATCH_ID_LITERAL))

    # final_df = last_month_provision_df.select(
    #     col("JOURNAL_ID"),
    #     col("BRANCH_CODE"),
    #     col("CURRENCY"),
    #     col("CGL"),
    #     concat(lit("Prov Rev"),  date_format(to_date(lit(prev_month_end_str)), "MMMyyyy")).alias("NARRATION"),
    #     (-col("DEBIT_AMOUNT")).alias("CREDIT_AMOUNT"),
    #     (-col("CREDIT_AMOUNT")).alias("DEBIT_AMOUNT"),
    #     col("TRANSACTION_COUNT"),
    #     # col("SOURCE_FLAG")
    # ).withColumn("BATCH_ID", F.lit(BATCH_ID_LITERAL)).withColumn("SOURCE_FLAG", lit("R"))

    return final_df