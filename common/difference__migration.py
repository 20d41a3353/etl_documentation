#!/usr/bin/env python3

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.types import DecimalType
import traceback
import sys
from datetime import date, timedelta
from common.logger import setup_logger
from common.properties import get_oracle_properties
from common.fileUtil import paths_for_read
from common.log_etl_modified import log_etl
from common.fileUtil import tables_to_read

def run_difference_migration(spark, dt, run_id, process_run_id):
    """
    Migrates DIFFERENCE data from Oracle to Delta Lake for the given date.

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

    logger = setup_logger(
        "difference_migration",
        target_date.strftime("%Y-%m-%d"),
        run_id
    )
    TABLES = tables_to_read("difference")
    DIFFERENCE = TABLES["DIFFERENCE"]
    

    logger.info(f"Starting difference migration for DATE = {dt}")
    logger.info(f"Required date for difference migration: {required_date_str}")

    try:

        query = f"""
        (
            SELECT
                BRANCH_CODE || CURRENCY || CGL AS GLCC,
                DIFFERENCE_AMOUNT,
                FIRST_ERROR_DATE,
                RECON_RUN_DATE
            FROM {DIFFERENCE}
            WHERE RECON_RUN_DATE >= DATE '{required_date_str}'
            AND   RECON_RUN_DATE <  DATE '{required_date_str}' + 1
        ) t
        """

        oracle_props = get_oracle_properties()

        cfg       = paths_for_read("difference", target_date, target_date_str)
        HDFS_PATH = cfg["PRECHECK_DATALAKE_PATH"]
        logger.info(f"Difference delta lake path: {HDFS_PATH}")

       
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
            log_etl(spark, process_run_id, "difference_migration", 3,
                    f"Oracle read failed for date {required_date_str}")
            return False

        
        oracle_count = df_source.count()  
        if oracle_count == 0:
            logger.error(f"No DIFFERENCE data found in Oracle for {required_date_str}")
            # status=3 → Kafka UPDATE only — safe inside loop
            log_etl(spark, process_run_id, "difference_migration", 3,
                    f"No data in Oracle for DIFFERENCE date {required_date_str}")
            return False

        logger.info(f"Oracle record count: {oracle_count}") 

        try:
            df_final = df_source.select(
                F.col("GLCC").cast("string"),
                F.col("DIFFERENCE_AMOUNT").cast(DecimalType(25, 4)),
                F.col("FIRST_ERROR_DATE").cast("date"),
                F.col("RECON_RUN_DATE").cast("date").alias("RECON_DATE")
            )
        except Exception:
            logger.error("Schema transformation failed", exc_info=True)
            # status=3 → Kafka UPDATE only — safe inside loop
            log_etl(spark, process_run_id, "difference_migration", 3,f"Schema transformation failed for date {required_date_str}")
            return False

        try:
            df_final.write \
                .format("delta") \
                .mode("overwrite") \
                .option("replaceWhere", f"RECON_DATE = DATE '{required_date_str}'") \
                .option("mergeSchema", "true") \
                .save(HDFS_PATH)
        except Exception:
            logger.error("Delta write failed", exc_info=True)
            # status=3 → Kafka UPDATE only — safe inside loop
            log_etl(spark, process_run_id, "difference_migration", 3,f"Delta write failed for date {required_date_str}")
            return False

        delta_count = (
            spark.read.format("delta")
            .load(HDFS_PATH)
            .filter(F.col("RECON_DATE") == F.lit(target_date))
            .count()
        )

        logger.info(f"Delta count for {required_date_str}: {delta_count}")

        if oracle_count != delta_count:
            logger.error(
                f"COUNT MISMATCH | DATE={required_date_str} | "
                f"ORACLE={oracle_count} | DELTA={delta_count}"
            )
            # status=3 → Kafka UPDATE only — safe inside loop
            log_etl(spark, process_run_id, "difference_migration", 3,f"Count mismatch Oracle={oracle_count} Delta={delta_count} "
                    f"for date {required_date_str}")
            return False

        logger.info(f"Difference migration completed successfully for {required_date_str}")
        # status=2 → Kafka UPDATE only — safe inside loop
        log_etl(spark, process_run_id, "difference_migration", 2,f"Difference Migration Completed for date {required_date_str}")
        return True 

    except Exception:
        logger.error("Difference migration failed unexpectedly", exc_info=True)
        # status=3 → Kafka UPDATE only — safe inside loop
        log_etl(spark, process_run_id, "difference_migration", 3,f"Unexpected error in Difference Migration for date {required_date_str}")
        raise






































































































































































































# #!/usr/bin/env python3

# from pyspark.sql import SparkSession, functions as F, types as T
# from pyspark.sql.types import DecimalType
# import traceback
# import sys
# from datetime import date, timedelta
# from common.logger import setup_logger
# from common.properties import get_oracle_properties
# from common.fileUtil import paths_for_read
# from common.log_etl_modified import log_etl
# #--------------------------------------------
# def run_difference_migration(spark,dt,run_id,process_run_id):
# # def run_difference_migration(spark,required_date,run_id):
# # def run_difference_migration(spark,dt,run_id):
#     target_date=dt
#     target_date_str=target_date.strftime("%d-%m-%y")
#     # log_etl(spark, process_run_id, "difference_migration",1,"difference migration started")
#     # logger = setup_logger(
#     #     "difference migration",
#     #     target_date.strftime("%Y-%m-%d"),
#     #     process_run_id
#     # )
   
#     logger = setup_logger(
#         "difference migration",
#         target_date.strftime("%Y-%m-%d"),
#         run_id
#     )


#     logger.info(f"Starting difference migration for DATE = {dt}")

#     try:
#         # Convert date properly for Oracle
#         required_date_str = dt.strftime("%Y-%m-%d")
        
#         logger.info(f"Required date for run difference migration:::{required_date_str}")

        
#         # query=f"""
#         #    (
#         #        SELECT * FROM (
#         #        SELECT
#         #        BRANCH_CODE || CURRENCY || CGL AS GLCC,
#         #        DIFFERENCE_AMOUNT,
#         #        FIRST_ERROR_DATE,
#         #        RECON_RUN_DATE
#         #        FROM FINCORE.DIFFERENCE_PRECHECK
#         #        WHERE TRUNC(RECON_RUN_DATE) = TO_DATE('{required_date_str}', 'YYYY-MM-DD')
#         #        )
#         #     )
#         # """
        
#         query=f"""
#         (
#         SELECT
#             BRANCH_CODE || CURRENCY || CGL AS GLCC,
#             DIFFERENCE_AMOUNT,
#             FIRST_ERROR_DATE,
#             RECON_RUN_DATE
#         FROM FINCORE.DIFFERENCE_PRECHECK
#         WHERE RECON_RUN_DATE >= DATE '{required_date_str}'
#         AND RECON_RUN_DATE < DATE '{required_date_str}' +1
#         )t
#         """
        
#         oracle_props = get_oracle_properties()
        
#         #-------- Paths --------
#         cfg = paths_for_read("difference", target_date, target_date_str)
#         HDFS_PATH = cfg["PRECHECK_DATALAKE_PATH"]
#         logger.info(f"Difference delta lake path::::::{HDFS_PATH}")
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
#             return False

#         if df_source.rdd.isEmpty():
#             log_etl(spark, process_run_id, "difference_migration",3,"no data in difference table")
#             logger.info(f"No DIFFERENCE data found in Oracle for {required_date_str}")
#             return False

#         logger.info(f"Oracle record count: {df_source.count()}")

    
#         df_final = df_source.select(
#             F.col("GLCC").cast("string"),
#             F.col("DIFFERENCE_AMOUNT").cast(DecimalType(25, 4)),
#             F.col("FIRST_ERROR_DATE").cast("date"),
#             F.col("RECON_RUN_DATE").cast("date").alias("RECON_DATE")
#         )

        
#         df_final.write \
#             .format("delta") \
#             .mode("append") \
#             .option("mergeSchema", "true") \
#             .save(HDFS_PATH)
            
#         df_final.cache().show(df_final.count(), truncate=False)   
            
#         log_etl(spark, process_run_id, "difference_migration",2,"difference migration completed")

#         logger.info("difference migration completed successfully")
#         return True

#     except Exception as e:
#         logger.error("difference migration failed", exc_info=True)
#         log_etl(spark, process_run_id, "difference_migration",3,"Error in difference migration")
#         raise










































































































# #!/usr/bin/env python3

# from pyspark.sql import SparkSession, functions as F, types as T
# from pyspark.sql.types import DecimalType
# import traceback
# import sys
# from datetime import date, timedelta
# from common.logger import setup_logger
# from common.properties import get_oracle_properties
# from common.fileUtil import paths_for_read
# from common.log_etl_modified import log_etl
# #--------------------------------------------
# def run_difference_migration(spark,dt,run_id,process_run_id):
#     target_date=dt
#     target_date_str=target_date.strftime("%d-%m-%y")
#     log_etl(spark, process_run_id, "difference_migration",1,"difference migration started")
#     logger = setup_logger(
#         "difference migration",
#         target_date.strftime("%Y-%m-%d"),
#         process_run_id
#     )


#     logger.info(f"Starting difference migration for DATE = {dt}")

#     try:
#         # Convert date properly for Oracle
#         required_date_str = dt.strftime("%Y-%m-%d")
        
#         logger.info(f"Required date for run difference migration{required_date_str}")

        
#         # query=f"""
#         #    (
#         #        SELECT * FROM (
#         #        SELECT
#         #        BRANCH_CODE || CURRENCY || CGL AS GLCC,
#         #        DIFFERENCE_AMOUNT,
#         #        FIRST_ERROR_DATE,
#         #        RECON_RUN_DATE
#         #        FROM FINCORE.DIFFERENCE_PRECHECK
#         #        WHERE TRUNC(RECON_RUN_DATE) = TO_DATE('{required_date_str}', 'YYYY-MM-DD')
#         #        )
#         #     )
#         # """
        
#         query=f"""
#         (
#         SELECT
#             BRANCH_CODE || CURRENCY || CGL AS GLCC,
#             DIFFERENCE_AMOUNT,
#             FIRST_ERROR_DATE,
#             RECON_RUN_DATE
#         FROM FINCORE.DIFFERENCE
#         WHERE RECON_RUN_DATE >= DATE '{required_date_str}'
#         AND RECON_RUN_DATE < DATE '{required_date_str}' +1
#         )t
#         """
        
#         oracle_props = get_oracle_properties()
        
#         #-------- Paths --------
#         cfg = paths_for_read("difference", target_date, target_date_str)
#         HDFS_PATH = cfg["PRECHECK_DATALAKE_PATH"]
#         logger.info(f"Difference delta lake path{HDFS_PATH}")
#         JDBC_URL = oracle_props["url"]
#         ORACLE_USER = oracle_props["user"]
#         ORACLE_PASSWORD = oracle_props["password"]
#         ORACLE_DRIVER = oracle_props["driver"]

#         df_source = (
#             spark.read.format("jdbc")
#             .option("url",JDBC_URL)
#             .option("user", ORACLE_USER)
#             .option("password", ORACLE_PASSWORD)
#             .option("driver", ORACLE_DRIVER)
#             .option("dbtable", query)
#             .load()
#         )

#         if df_source.rdd.isEmpty():
#             log_etl(spark, process_run_id, "difference_migration",3,"no data in difference table")
#             raise ValueError(f"No DIFFERENCE data found in Oracle for {required_date_str}")

#         logger.info(f"Oracle record count: {df_source.count()}")

    
#         df_final = df_source.select(
#             F.col("GLCC").cast("string"),
#             F.col("DIFFERENCE_AMOUNT").cast(DecimalType(25, 4)),
#             F.col("FIRST_ERROR_DATE").cast("date"),
#             F.col("RECON_RUN_DATE").cast("date").alias("RECON_DATE")
#         )

        
#         df_final.write \
#             .format("delta") \
#             .mode("append") \
#             .option("mergeSchema", "true") \
#             .save(HDFS_PATH)
            
#         # df_final.cache().show(df_final.count(), truncate=False)   
            
#         log_etl(spark, process_run_id, "difference_migration",2,"difference migration completed")

#         logger.info("difference migration completed successfully")

#     except Exception as e:
#         logger.error("difference migration failed", exc_info=True)
#         log_etl(spark, process_run_id, "difference_migration",3,"Error in difference migration")
#         raise
















































































# # #!/usr/bin/env python3

# # from pyspark.sql import SparkSession, functions as F, types as T
# # from pyspark.sql.types import DecimalType
# # import traceback
# # import sys
# # from datetime import date, timedelta
# # from common.logger import setup_logger
# # from common.properties import get_oracle_properties
# # from common.fileUtil import paths_for_read
# # from common.log_etl_modified import log_etl
# # #--------------------------------------------
# # def run_difference_migration(spark,required_date,run_id,process_run_id):
# #     target_date=required_date
# #     target_date_str=target_date.strftime("%d-%m-%y")
# #     log_etl(spark, process_run_id, "difference_migration",1,"difference migration started")
# #     logger = setup_logger(
# #         "difference migration",
# #         target_date.strftime("%Y-%m-%d"),
# #         process_run_id
# #     )


# #     logger.info(f"Starting difference migration for DATE = {required_date}")

# #     try:
# #         # Convert date properly for Oracle
# #         required_date_str = required_date.strftime("%Y-%m-%d")
        
# #         logger.info(f"Required date for run difference migration{required_date_str}")

        
# #         query=f"""
# #            (
# #                SELECT * FROM (
# #                SELECT
# #                BRANCH_CODE || CURRENCY || CGL AS GLCC,
# #                DIFFERENCE_AMOUNT,
# #                FIRST_ERROR_DATE,
# #                RECON_RUN_DATE
# #                FROM FINCORE.DIFFERENCE_PRECHECK
# #                WHERE TRUNC(RECON_RUN_DATE) = TO_DATE('{required_date_str}', 'YYYY-MM-DD')
# #                )
# #             )
# #         """
        
# #         oracle_props = get_oracle_properties()
        
# #         #-------- Paths --------
# #         cfg = paths_for_read("difference", target_date, target_date_str)
# #         HDFS_PATH = cfg["PRECHECK_DATALAKE_PATH"]
# #         logger.info(f"Difference delta lake path{HDFS_PATH}")
# #         JDBC_URL = oracle_props["url"]
# #         ORACLE_USER = oracle_props["user"]
# #         ORACLE_PASSWORD = oracle_props["password"]
# #         ORACLE_DRIVER = oracle_props["driver"]

# #         df_source = (
# #             spark.read.format("jdbc")
# #             .option("url",JDBC_URL)
# #             .option("user", ORACLE_USER)
# #             .option("password", ORACLE_PASSWORD)
# #             .option("driver", ORACLE_DRIVER)
# #             .option("dbtable", query)
# #             .load()
# #         )

# #         if df_source.rdd.isEmpty():
# #             log_etl(spark, process_run_id, "difference_migration",3,"no data in difference table")
# #             raise ValueError(f"No DIFFERENCE data found in Oracle for {required_date_str}")

# #         logger.info(f"Oracle record count: {df_source.count()}")

    
# #         df_final = df_source.select(
# #             F.col("GLCC").cast("string"),
# #             F.col("DIFFERENCE_AMOUNT").cast(DecimalType(25, 4)),
# #             F.col("FIRST_ERROR_DATE").cast(DecimalType(25, 4)),
# #             F.col("RECON_RUN_DATE").cast("date").alias("RECON_DATE")
# #         )

        
# #         df_final.write \
# #             .format("delta") \
# #             .mode("overwrite") \
# #             .option("overwriteSchema", "true") \
# #             .save(HDFS_PATH)
            
# #         df_final.cache().show(df_final.count(), truncate=False)   
            
# #         log_etl(spark, process_run_id, "difference_migration",2,"difference migration completed")

# #         logger.info("difference migration completed successfully")

# #     except Exception as e:
# #         logger.error("difference migration failed", exc_info=True)
# #         log_etl(spark, process_run_id, "difference_migration",3,"Error in difference migration")
# #         raise
