import logging
import sys
import os
import common.constants as constants
from datetime import timedelta, date, datetime
from py4j.java_gateway import java_import
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import (to_date, expr, round, col, when, length, substring, trim,concat, lit, create_map, to_timestamp, sum, broadcast,coalesce, current_timestamp
)
from common.check_monthend import check_MonthEnd
from common.logger import setup_logger
from common.dateUtil import get_etl_date
from common.fileUtil import paths_for_read, get_hdfs_base
from common.createSpark import create_spark_session
from common.processRun import get_run_id
from common.properties import get_oracle_properties
from common.log_etl_modified import log_etl
from pyspark.sql.types import DecimalType
from common.processRun import get_process_run_id
from common.fileUtil import tables_to_read


def run_glif_migration(spark, dt, run_id, process_run_id):
    """
    Migrates GLIF data from Oracle to Delta Lake for the given date.

    Returns:
        True  — migration successful, count verified
        False — no Oracle data, transformation failed, write failed, or count mismatch
    Raises:
        Exception — for truly unexpected failures (propagated to precheck)

    IMPORTANT: log_etl status=1 (Oracle INSERT) is NOT called here.
    It is called ONCE in run_pipeline_precheck BEFORE the migration loop
    to avoid PK violations on PROCESS_RUN_STAGE table.
    Only status=2/3 (Kafka UPDATE — no Oracle insert) are called here.
    """
    target_date       = dt
    target_date_str   = target_date.strftime("%d-%m-%y")
    required_date_str = dt.strftime("%Y-%m-%d")

    logger = setup_logger("GLIF_MIGRATION", target_date_str, run_id)
    logger.info(f"Starting GLIF migration for DATE = {dt}")
    logger.info(f"GLIF migration required_date_str: {required_date_str}")
    oracle_tables = tables_to_read("glif")

    
    BALANCE = oracle_tables["BALANCE"]
    

    try:
       
        query = f"""
        (
            SELECT
                BRANCH_CODE || CURRENCY || CGL AS GLCC,
                BALANCE                        AS CLOSING_BALANCE,
                INR_BALANCE,
                BALANCE_DATE
            FROM {BALANCE}
            WHERE BALANCE_DATE >= DATE '{required_date_str}'
            AND   BALANCE_DATE <  DATE '{required_date_str}' + 1
        ) t
        """

        oracle_props = get_oracle_properties()
        cfg          = paths_for_read("glif", target_date, target_date_str)
        HDFS_PATH    = cfg["PRECHECK_DATALAKE_PATH"]

        logger.info(f"GLIF delta lake path: {HDFS_PATH}")

        try:
            df_source = (
                spark.read.format("jdbc")
                .option("url",       oracle_props["url"])
                .option("user",      oracle_props["user"])
                .option("password",  oracle_props["password"])
                .option("driver",    oracle_props["driver"])
                .option("dbtable",   query)
                .option("fetchsize", "20000")
                .load()
            )
        except Exception:
            logger.error("Oracle read failed", exc_info=True)
            # status=3 → Kafka UPDATE only — safe inside loop
            log_etl(spark, process_run_id, "glif_migration", 3,
                    f"Oracle read failed for date {required_date_str}")
            return False

  
        oracle_count = df_source.count()
        if oracle_count == 0:
            logger.error(f"No GLIF data in Oracle for {required_date_str}")
            # status=3 → Kafka UPDATE only — safe inside loop
            log_etl(spark, process_run_id, "glif_migration", 3,
                    f"No data in Oracle for GLIF date {required_date_str}")
            return False

        logger.info(f"Oracle record count: {oracle_count}")

   
        try:
            df_final = df_source.select(
                F.col("GLCC").cast("string"),
                F.col("CLOSING_BALANCE").cast(DecimalType(25, 4)), 
                F.col("INR_BALANCE").cast(DecimalType(25, 4)),
                F.col("BALANCE_DATE").cast("date")
            )
        except Exception:
            logger.error("Schema transformation failed", exc_info=True)
            # status=3 → Kafka UPDATE only — safe inside loop
            log_etl(spark, process_run_id, "glif_migration", 3,
                    f"Schema transformation failed for date {required_date_str}")
            return False

        try:
            df_final.write \
                .format("delta") \
                .mode("overwrite") \
                .option("replaceWhere", f"BALANCE_DATE = DATE '{required_date_str}'") \
                .option("mergeSchema", "true") \
                .save(HDFS_PATH)
        except Exception:
            logger.error("Delta write failed", exc_info=True)
            # status=3 → Kafka UPDATE only — safe inside loop
            log_etl(spark, process_run_id, "glif_migration", 3,
                    f"Delta write failed for date {required_date_str}")
            return False

   
        delta_count = (
            spark.read.format("delta")
            .load(HDFS_PATH)
            .filter(F.col("BALANCE_DATE") == F.lit(target_date))
            .count()
        )

        logger.info(f"Delta count for {required_date_str}: {delta_count}")

        if oracle_count != delta_count:
            logger.error(
                f"COUNT MISMATCH | DATE={required_date_str} | "
                f"ORACLE={oracle_count} | DELTA={delta_count}"
            )
            # status=3 → Kafka UPDATE only — safe inside loop
            log_etl(spark, process_run_id, "glif_migration", 3,
                    f"Count mismatch Oracle={oracle_count} Delta={delta_count} "
                    f"for date {required_date_str}")
            return False

        logger.info(f"GLIF Migration completed successfully for {required_date_str}")
        # status=2 → Kafka UPDATE only — safe inside loop
        log_etl(spark, process_run_id, "glif_migration", 2,
                f"GLIF Migration Completed for date {required_date_str}")
        return True  

    except Exception:
        logger.error("GLIF Migration failed unexpectedly", exc_info=True)
        # status=3 → Kafka UPDATE only — safe inside loop
        log_etl(spark, process_run_id, "glif_migration", 3,
                f"Unexpected error in GLIF Migration for date {required_date_str}")
        raise














































































































































# import logging
# import sys
# import os
# import common.constants as constants
# from datetime import timedelta, date, datetime
# from py4j.java_gateway import java_import
# from pyspark.sql import SparkSession, functions as F
# from pyspark.sql.functions import (to_date, expr, round, col, when, length, substring,trim, concat, lit, create_map, to_timestamp, sum,broadcast, coalesce, current_timestamp)
# from common.check_monthend import check_MonthEnd
# from common.logger import setup_logger
# from common.dateUtil import get_etl_date
# from common.fileUtil import paths_for_read, get_hdfs_base
# from common.createSpark import create_spark_session
# from common.processRun import get_run_id
# from common.properties import get_oracle_properties
# from common.log_etl_modified import log_etl
# from pyspark.sql.types import DecimalType
# from common.processRun import get_process_run_id

# def run_glif_migration(spark, dt, run_id, process_run_id):
# # def run_glif_migration(spark, dt, run_id):
#     target_date     = dt
#     target_date_str = target_date.strftime("%d-%m-%y")
#     required_date_str = dt.strftime("%Y-%m-%d")

#     logger = setup_logger("GLIF_MIGRATION", target_date_str, run_id)
#     logger.info(f"Starting GLIF migration for DATE = {dt}")
#     # log_etl(spark, process_run_id, "glif_migration", 1, "GLIF Migration Started")
    
#     logger.info(f"Glif migration date::::::::::{required_date_str}")

#     try:
#         query = f"""
#         (
#             SELECT
#                 BRANCH_CODE || CURRENCY || CGL AS GLCC,
#                 BALANCE AS CLOSING_BALANCE,
#                 INR_BALANCE,
#                 BALANCE_DATE
#             FROM FINCORE.GLIF_PRECHECK
#             WHERE BALANCE_DATE >= DATE '{required_date_str}'
#             AND   BALANCE_DATE <  DATE '{required_date_str}' + 1
#         ) t
#         """

#         oracle_props   = get_oracle_properties()
#         cfg            = paths_for_read("glif", target_date, target_date_str)
#         HDFS_PATH      = cfg["PRECHECK_DATALAKE_PATH"]

#         logger.info(f"GLIF delta lake path: {HDFS_PATH}")


#         try:
#             df_source = (
#                 spark.read.format("jdbc")
#                 .option("url",       oracle_props["url"])
#                 .option("user",      oracle_props["user"])
#                 .option("password",  oracle_props["password"])
#                 .option("driver",    oracle_props["driver"])
#                 .option("dbtable",   query)
#                 .option("fetchsize", "20000")
#                 .load()
#             )
#         except Exception:
#             logger.error("Oracle read failed", exc_info=True)
#             log_etl(spark, process_run_id, "glif_migration", 3, "Oracle select failed")
#             return False

#         oracle_count = df_source.count() 
#         if oracle_count == 0:
#             logger.error(f"No GLIF data in Oracle for {required_date_str}")
#             log_etl(spark, process_run_id, "glif_migration", 3,f"No data in Oracle for GLIF date {required_date_str}")
#             return False

#         logger.info(f"Oracle record count: {oracle_count}")


#         try:
#             df_final = df_source.select(
#                 F.col("GLCC").cast("string"),
#                 F.col("CLOSING_BALANCE").cast(DecimalType(25, 4)),
#                 F.col("INR_BALANCE").cast(DecimalType(25, 4)),
#                 F.col("BALANCE_DATE").cast("date")
#             )
#         except Exception:
#             logger.error("Schema transformation failed", exc_info=True)
#             log_etl(spark, process_run_id, "glif_migration", 3, "Schema transformation failed")
#             return False

#         # ── Step 4: Write to Delta (safe overwrite for this date) ─
#         try:
#             df_final.write \
#                 .format("delta") \
#                 .mode("overwrite") \
#                 .option("replaceWhere", f"BALANCE_DATE = DATE '{required_date_str}'") \
#                 .option("mergeSchema", "true") \
#                 .save(HDFS_PATH)
#         except Exception:
#             logger.error("Delta write failed", exc_info=True)
#             log_etl(spark, process_run_id, "glif_migration", 3, "Delta write failed")
#             return False

#         delta_count = (
#             spark.read.format("delta")
#             .load(HDFS_PATH)
#             .filter(F.col("BALANCE_DATE") == F.lit(target_date))
#             .count()
#         )

#         logger.info(f"Delta count for {required_date_str}: {delta_count}")

#         if oracle_count != delta_count:
#             logger.error(
#                 f"COUNT MISMATCH | DATE={required_date_str} | "
#                 f"ORACLE={oracle_count} | DELTA={delta_count}"
#             )
#             log_etl(spark, process_run_id, "glif_migration", 3,f"Count mismatch Oracle={oracle_count} Delta={delta_count}")
#             return False

#         logger.info("GLIF Migration completed successfully")
#         log_etl(spark, process_run_id, "glif_migration", 2, "GLIF Migration Completed")
#         return True  

#     except Exception:
#         logger.error("GLIF Migration failed unexpectedly", exc_info=True)
#         log_etl(spark, process_run_id, "glif_migration", 3, "Error in GLIF Migration")
#         raise










































































































































































































































































































































# #!/usr/bin/env python3
 
# from pyspark.sql import SparkSession, functions as F, types as T # Note the 'as F'
# from pyspark.sql.types import DecimalType
# from datetime import datetime
# import traceback
# import sys
# from datetime import date, timedelta
# from common.logger import setup_logger
# from common.properties import get_oracle_properties
# from common.fileUtil import paths_for_read
# #-----------------------------------------------------------------------------------------------------
# from common.log_etl_modified import log_etl
# #--------------------------------------------
# def print_header(msg):
#     print("\n" + "="*6 + " " + msg + " " + "="*6 + "\n")
 
# # --- Optimization Function for Parallel Reads ---
# # def read_table_parallel_by_rownum(spark, jdbc_url, user, password, driver,
# #                                  base_table, select_cols_sql, target_date_str,
# #                                  branch_filter, num_partitions, fetchsize):
   
# #     subquery = (
# #         "(\n"
# #         "  SELECT\n"
# #         "    {cols},\n"
# #         "    ROW_NUMBER() OVER (ORDER BY (BRANCH_CODE || '|' || CURRENCY || '|' || CGL)) rn\n"
# #         "  FROM {table}\n"
# #         "  WHERE BALANCE_DATE = TO_DATE('{date}','DD-MM-YY')\n"
# #         ") tmp"
# #     ).format(cols=select_cols_sql, 
# #              table=base_table, 
# #              date=target_date_str,
# #              branch_code_val=branch_filter)
   
# #     print("################# PREDICATE PUSHDOWN QUERY #######################")
# #     print(subquery)
# #     print("##################################################################")
   
# #     ESTIMATED_UPPER_BOUND = 1000000
    
# #     df = (
# #         spark.read.format("jdbc")
# #             .option("url", jdbc_url)
# #             .option("dbtable", subquery)
# #             .option("user", user)
# #             .option("password", password)
# #             .option("driver", driver)
# #             .option("partitionColumn", "rn")
# #             .option("lowerBound", 1)
# #             .option("upperBound", ESTIMATED_UPPER_BOUND)
# #             .option("numPartitions", str(num_partitions)) 
# #             .option("fetchsize", str(fetchsize))
# #             .load()
# #     )
# #     return df


# # def read_table_parallel_by_rownum(spark, jdbc_url, user, password, driver,
# #                                  base_table, select_cols_sql, target_date_str,
# #                                  branch_filter, num_partitions, fetchsize):
    
# #     # Convert target_date_str (YYYY-MM-DD) to DD-MON-YY format e.g., 13-FEB-26
# #     try:
# #         parsed_date = datetime.strptime(target_date_str, "%Y-%m-%d")
# #         formatted_date = parsed_date.strftime("%d-%b-%y").upper()  # 13-FEB-26
# #     except ValueError:
# #         formatted_date = target_date_str.upper()

# #     subquery = """
# #         (
# #           SELECT
# #             {cols},
# #             ROW_NUMBER() OVER (ORDER BY (BRANCH_CODE || '|' || CURRENCY || '|' || CGL)) rn
# #           FROM {table}
# #           WHERE BALANCE_DATE = TO_DATE('{date}', 'YYYY-MM-DD')
# #         ) tmp
# #     """.format(cols=select_cols_sql, table=base_table, date=formatted_date)
   
# #     print("################# PREDICATE PUSHDOWN QUERY #######################")
# #     print(subquery)
# #     print("##################################################################")
   
# #     ESTIMATED_UPPER_BOUND = 1000000
    
# #     df = (
# #         spark.read.format("jdbc")
# #             .option("url", jdbc_url)
# #             .option("dbtable", subquery)
# #             .option("user", user)
# #             .option("password", password)
# #             .option("driver", driver)
# #             .option("partitionColumn", "rn")
# #             .option("lowerBound", 1)
# #             .option("upperBound", ESTIMATED_UPPER_BOUND)
# #             .option("numPartitions", str(num_partitions)) 
# #             .option("fetchsize", str(fetchsize))
# #             .option("sessionInitStatement", "ALTER SESSION SET NLS_DATE_FORMAT = 'DD-MON-YY'")
# #             .load()
# #     )
# #     return df



# # def run_glif_migration(spark,target_date,RUN_ID):
# #     try:
         
        
# #         spark.conf.set("spark.sql.adaptive.enabled", "true")
# #         spark.conf.set("spark.sql.shuffle.partitions", "200")
        
       
        
# #         # ORACLE_QUERY_DATE = "11-11-25"
        
        
# #         TARGET_DATE_VALUE = target_date.strftime("%d-%m-%y")
        
        
        
# #         logger = setup_logger(
# #         "GLIF_MIGRATION",
# #         TARGET_DATE_VALUE,
# #         RUN_ID
# #     )
    
# #        # --------- Path -----------
       
# #         cfg = paths_for_read("glif", target_date, TARGET_DATE_VALUE)
# #         HDFS_PATH = cfg["PRECHECK_DATALAKE_PATH"]
        
        
# #         oracle_props = get_oracle_properties()

# #         JDBC_URL = oracle_props["url"]
# #         ORACLE_USER = oracle_props["user"]
# #         ORACLE_PASSWORD = oracle_props["password"]
# #         ORACLE_DRIVER = oracle_props["driver"]
        

# #         NUM_PARTITIONS_READ = 5
# #         FETCHSIZE = 20000 
# #         BRANCH_FILTER_VALUE = '5632356'
        
# #         GL_TABLE = "FINCORE.GLIF_PRECHECK"
        
        
# #         print_header("START GL Data Extraction")
       
# #         select_cols_gl = "BRANCH_CODE || CURRENCY || CGL AS GLCC, BALANCE AS closing_balance,INR_BALANCE,BALANCE_DATE" 
        
# #         df_gl = read_table_parallel_by_rownum(
# #             spark, JDBC_URL, ORACLE_USER, ORACLE_PASSWORD, ORACLE_DRIVER,
# #             GL_TABLE, select_cols_gl, TARGET_DATE_VALUE, BRANCH_FILTER_VALUE, 
# #             NUM_PARTITIONS_READ, FETCHSIZE
# #         )
 
 
# #         oracle_count=df_gl.count()
        
# #         logger.info(f"Total records fetched: {df_gl.count()}")

# #         # Final preparation: select final columns and set the correct target date value
# #         # FIX: Use F.lit() instead of lit()     
# #         df_final = df_gl.select(
# #             F.col("GLCC"),
# #             F.col("closing_balance").cast(DecimalType(25, 4)),
# #             F.col("INR_BALANCE").cast(DecimalType(25, 4)),
# #             F.lit(target_date).cast("date").alias("BALANCE_DATE")
# #         )
        
       

# #         print_header("WRITE TO DELTA LAKE (HDFS)")
        
# #         df_final.write \
# #             .format("delta") \
# #             .mode("append") \
# #             .save(HDFS_PATH)
            
# #         datalake_count=(
# #             spark.read.format("delta")
# #             .load(HDFS_PATH)
# #             .filter(F.col("BALANCE_DATE")==F.lit(target_date))
# #             .count()
# #         )
        
# #         logger.info(f"DELTA COUNT FOR {target_date}:{datalake_count}")
            
# #         if oracle_count!=datalake_count:
# #             raise RuntimeError(f"GLIF count mismatch|DATE={target_date} |"f"ORACLE={oracle_count}|DATALAKE count={datalake_count}")
# #         else:
# #             logger.info(f"Successfully appended data to Delta Lake at: {HDFS_PATH}")
# #             print_header("GLIF MIGRATION COMPLETE")
       
      
 
# #     except Exception as e:
# #         print("ERROR during execution:")
# #         traceback.print_exc()
# #         raise
 
# # def run_glif_migration(spark, target_date, RUN_ID,process_run_id):
# #     try:
# #         spark.conf.set("spark.sql.adaptive.enabled", "true")
# #         spark.conf.set("spark.sql.shuffle.partitions", "200")

# #         target_date_str = target_date.strftime("%d-%m-%y")
# #         log_etl(spark, process_run_id, "glif_migration",1,"Glif migration stated")
# #         logger = setup_logger(
# #             "GLIF_MIGRATION",
# #             target_date_str,
# #             RUN_ID
# #         )

# #         # -------- Paths --------
# #         cfg = paths_for_read("glif", target_date, target_date_str)
# #         HDFS_PATH = cfg["PRECHECK_DATALAKE_PATH"]

# #         oracle_props = get_oracle_properties()

# #         NUM_PARTITIONS_READ = 5
# #         FETCHSIZE = 20000
# #         BRANCH_FILTER_VALUE = '5632356'
# #         GL_TABLE = "FINCORE.GLIF_PRECHECK"

# #         print_header("START GLIF DATA EXTRACTION")

        
# #         select_cols_gl = """
# #             BRANCH_CODE || CURRENCY || CGL AS GLCC,
# #             BALANCE AS closing_balance,
# #             INR_BALANCE
# #         """
# #         log_etl(spark, process_run_id, "glif_migration_tablereading",1,"Glif table reading starterd for migration")
# #         df_gl = read_table_parallel_by_rownum(
# #             spark,
# #             oracle_props["url"],
# #             oracle_props["user"],
# #             oracle_props["password"],
# #             oracle_props["driver"],
# #             GL_TABLE,
# #             select_cols_gl,
# #             target_date_str,
# #             BRANCH_FILTER_VALUE,
# #             NUM_PARTITIONS_READ,
# #             FETCHSIZE
# #         )
        
# #         oracle_count = df_gl.count()
# #         log_etl(spark, process_run_id, "glif_migration_tablereading",2,"Glif table reading completed for migration")
# #         logger.info(f"Total records fetched from Oracle: {oracle_count}")

# #         # -------- Schema-safe transformation --------
# #         df_final = df_gl.select(
# #             F.col("GLCC").cast(T.StringType()),
# #             F.col("closing_balance").cast(DecimalType(25, 4)),
# #             F.col("INR_BALANCE").cast(DecimalType(25, 4)),
# #             F.lit(target_date).cast(T.DateType()).alias("BALANCE_DATE")
# #         )

# #         print_header("WRITE TO DELTA LAKE")
# #         log_etl(spark, process_run_id, "glif_migration_writingdelta",1,"Glif writing to delta lake started.")
# #         df_final.write \
# #             .format("delta") \
# #             .option("mergeschema","true")\
# #             .mode("append") \
# #             .save(HDFS_PATH)
# #         log_etl(spark, process_run_id, "glif_migration_writingdelta",2,"Glif writing to delta lake completed.")
# #         datalake_count = (
# #             spark.read.format("delta")
# #             .load(HDFS_PATH)
# #             .filter(F.col("BALANCE_DATE") == target_date)
# #             .count()
# #         )

# #         logger.info(f"Delta count for {target_date}: {datalake_count}")

# #         if oracle_count != datalake_count:
# #             raise RuntimeError(
# #                 f"GLIF COUNT MISMATCH | DATE={target_date} | "
# #                 f"ORACLE={oracle_count} | DELTA={datalake_count}"
# #             )

# #         logger.info("GLIF MIGRATION COMPLETED SUCCESSFULLY")
# #         print_header("GLIF MIGRATION COMPLETE")

# #     except Exception:
# #         logger.error(
# #             f"GLIF MIGRATION FAILED | DATE={target_date} | RUN_ID={RUN_ID}",
# #             exc_info=True
# #         )
# #         log_etl(spark, process_run_id, "glif_migration",3,"error in Glif migration.")
# #         raise

# from pyspark.sql import functions as F
# from pyspark.sql.types import DecimalType
# from datetime import datetime
# import logging


# def run_glif_migration(spark, dt,run_id,process_run_id):
#     target_date=dt
#     target_date_str=target_date.strftime("%d-%m-%y")
#     log_etl(spark, process_run_id, "glif_migration",1,"Glif migration started")

#     logger = logging.getLogger("GLIF_MIGRATION")

#     logger.info(f"Starting GLIF migration for DATE = {dt}")

#     try:
      
#         required_date_str = dt.strftime("%Y-%m-%d")
        
#         logger.info(f"Required date for run glif migration{required_date_str}")

       
#         # query = f"""
#         # (
#         #     SELECT * FROM (
#         #     SELECT
#         #     BRANCH_CODE || CURRENCY || CGL AS GLCC,
#         #     BALANCE AS CLOSING_BALANCE,
#         #     INR_BALANCE,
#         #     BALANCE_DATE
#         #     FROM FINCORE.GLIF_PRECHECK
#         #     WHERE TRUNC(BALANCE_DATE) = TO_DATE('{required_date_str}', 'YYYY-MM-DD')
#         #     )
#         # )
#         # """
        
#         query=f"""
#         (
#             SELECT 
#                 BRANCH_CODE || CURRENCY || CGL AS glcc,
#                 BALANCE AS closing_balance,
#                 INR_BALANCE,
#                 BALANCE_DATE
#             FROM FINCORE.GL_Balance
#             WHERE BALANCE_DATE >= DATE '{required_date_str}'
#             AND BALANCE_DATE < DATE '{required_date_str}'+1
#         )t
#         """
        
 
        
#         oracle_props = get_oracle_properties()
        
#         #-------- Paths --------
#         cfg = paths_for_read("glif", target_date, target_date_str)
#         HDFS_PATH = cfg["PRECHECK_DATALAKE_PATH"]
#         logger.info(f"glif delta lake path{HDFS_PATH}")
#         JDBC_URL = oracle_props["url"]
#         ORACLE_USER = oracle_props["user"]
#         ORACLE_PASSWORD = oracle_props["password"]
#         ORACLE_DRIVER = oracle_props["driver"]
        
        
#         try:
#             df_source = (
#                 spark.read.format("jdbc")
#                 .option("url",JDBC_URL)
#                 .option("user", ORACLE_USER)
#                 .option("password", ORACLE_PASSWORD)
#                 .option("driver", ORACLE_DRIVER)
#                 .option("dbtable", query)
#                 .option("fetchsize","20000")
#                 .load()
#             )
            
#         except Exception as db_error:
#             logger.error("Oracle select failed",exc_info=True)
#             log_etl(spark,process_run_id,"GLIF_MIGRATION",3,"Oracle select failed")

#         if df_source.rdd.isEmpty():
#         #     log_etl(spark, process_run_id, "glif_migration",3,"data not present in GL_BALANCE table")
#         #     raise ValueError(f"No GLIF data found in Oracle for {required_date_str}")
#               logger.error(f"No GLIF data found in Oracle for{required_date_str}")
            

#         logger.info(f"Oracle record count: {df_source.count()}")

#         try:
#             df_final = df_source.select(
#                 F.col("glcc").cast("string"),
#                 F.col("closing_balance").cast(DecimalType(25, 4)),
#                 F.col("INR_BALANCE").cast(DecimalType(25, 4)),
#                 F.col("BALANCE_DATE").cast("date")
#             )
#         except Exception as read_error:
#             logger.error("Delta write failed",exc_info=True)

#         try:
#             df_final.write \
#                 .format("delta") \
#                 .mode("append") \
#                 .option("mergeSchema", "true") \
#                 .save(HDFS_PATH)
#         except Exception as write_error:
#             logger.error("Delta write failed",exc_info=True)
#             log_etl(spark, process_run_id, "GLIF_MIGRATION",3,f"Glif Delta write failed:{str(write_error)}")
            
#         # df_final.cache().show(df_final.count(), truncate=False)
            
#         log_etl(spark, process_run_id, "glif_migration",2,"Glif migration completed")

#         logger.info("GLIF migration completed successfully")

#     except Exception as e:
#         logger.error("GLIF migration failed", exc_info=True)
#         log_etl(spark, process_run_id, "glif_migration",3,"error in glif migration")
#         raise
