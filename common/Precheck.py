import logging
from datetime import timedelta
from pyspark.sql.functions import col
from py4j.java_gateway import java_import

# from glif_balance_migration import run_glif_migration
# from difference_migration import run_difference_migration

from common.logger import setup_logger
from common.dateUtil import get_etl_date
from common.fileUtil import paths_for_read, get_hdfs_base
from common.createSpark import create_spark_session
from common.processRun import get_run_id
from common.properties import get_oracle_properties
# #-----------------------------------------------------------------------------------------------------------------------------------------------------------
# def create_and_upload_manifest(spark, filename, base_path, content):

#     java_import(spark._jvm, "org.apache.hadoop.fs.FileSystem")
#     java_import(spark._jvm, "org.apache.hadoop.fs.Path")

#     fs = spark._jvm.FileSystem.get(spark._jsc.hadoopConfiguration())
#     Path = spark._jvm.Path

#     if not fs.exists(Path(base_path)):
#         raise RuntimeError(f"HDFS manifest path does not exist: {base_path}")

#     with open(filename, "w") as f:
#         f.write(content)

#     dest = Path(f"{base_path}/{filename}")

#     fs.copyFromLocalFile(
#         False,   # keep local
#         True,    # overwrite
#         Path(filename),
#         dest
#     )

#     if not fs.exists(dest):
#         raise RuntimeError(f"Manifest NOT written to HDFS: {dest}")
    
# # ==================================================
# # CURRENCY PRECHECK (MONTH-END ONLY)
# # ==================================================
# def run_currency_precheck(spark, etl_date, RUN_ID):
#     """
#     - Check FINCORE.CURRENCY_MASTER
#     - For FLAG = 1 currencies
#     - RATE_DATE must be == ETL_DATE (month-end)
#     """

#     logger = setup_logger(
#         "CURRENCY_PRECHECK",
#         etl_date.strftime("%Y-%m-%d"),
#         RUN_ID
#     )

#     oracle = get_oracle_properties()

#     query = f"""
#     (
#         SELECT COUNT(*) AS missing_cnt
#         FROM FINCORE.CURRENCY_MASTER
#         WHERE FLAG = 1
#           AND (RATE_DATE IS NULL OR RATE_DATE <> DATE '{etl_date.strftime("%Y-%m-%d")}')
#     )
#     """

#     df = (
#         spark.read.format("jdbc")
#         .option("url", oracle["url"])
#         .option("user", oracle["user"])
#         .option("password", oracle["password"])
#         .option("driver", oracle["driver"])
#         .option("dbtable", query)
#         .load()
#     )

#     missing_cnt = df.collect()[0]["MISSING_CNT"]

#     if missing_cnt > 0:
#         logger.error(
#             f"CURRENCY PRECHECK FAILED : {missing_cnt} currencies missing RATE_DATE={etl_date}"
#         )
#         return False
#     else:
#         logger.info("CURRENCY PRECHECK PASSED")
#         return True

# # ==================================================
# # PRECHECK + MIGRATION (GLIF / DIFFERENCE)
# # ==================================================
# def run_precheck_and_migrate(pipeline, spark, etl_date, RUN_ID):

#     target_date = etl_date - timedelta(days=1)
#     target_str = target_date.strftime("%Y-%m-%d")

#     logger = setup_logger(f"{pipeline.upper()}_PRECHECK", target_str, RUN_ID)

#     cfg = paths_for_read(pipeline, etl_date, target_str)

#     datalake_path = cfg["PRECHECK_DATALAKE_PATH"]
#     manifest_base = cfg["MANIFEST_BASE_PATH"]
#     manifests = cfg["MANIFEST_NAMES"]

#     # ---------- PRECHECK ----------
#     try:
#         data_exists = (
#             spark.read.format("delta")
#             .load(datalake_path)
#             .filter(col("BALANCE_DATE") == target_date)
#             .limit(1)
#             .count() > 0
#         )
#     except Exception:
#         data_exists = False

#     # ---------- DATA ALREADY PRESENT ----------
#     if data_exists:
#         create_and_upload_manifest(
#             spark,
#             manifests["DATA_ALREADY_PRESENT"],
#             manifest_base,
#             f"{pipeline.upper()} data already present for {target_str}\n"
#         )
#         logger.info("STATUS=DATA_ALREADY_PRESENT")
#         return

#     # ---------- DATA NOT PRESENT ----------
#     create_and_upload_manifest(
#         spark,
#         manifests["DATA_NOT_PRESENT"],
#         manifest_base,
#         f"{pipeline.upper()} data NOT present for {target_str}\n"
#     )
#     logger.info("STATUS=DATA_NOT_PRESENT")

#     # ---------- MIGRATION ----------
#     if pipeline == "glif":
#         run_glif_migration(spark, target_date, RUN_ID)
#     else:
#         run_difference_migration(spark, target_date, RUN_ID)

#     # ---------- DATA MIGRATED ----------
#     create_and_upload_manifest(
#         spark,
#         manifests["DATA_MIGRATED"],
#         manifest_base,
#         f"{pipeline.upper()} data migrated successfully for {target_str}\n"
#     )

#     logger.info("STATUS=DATA_MIGRATED")
    
    
# # ==================================================
# # RUN ALL (SINGLE ENTRY POINT)
# # ==================================================
# def run_all_pipelines():

#     HDFS_BASE = get_hdfs_base()
#     spark = create_spark_session("DATALAKE_PRECHECK", HDFS_BASE)
#     spark.conf.set("spark.hadoop.fs.defaultFS", HDFS_BASE)

#     etl_date = get_etl_date(spark)
#     RUN_ID = get_run_id(spark, etl_date.strftime("%Y-%m-%d"))

#     # ---- Currency check (does NOT stop pipelines) ----
#     run_currency_precheck(spark, etl_date, RUN_ID)

#     # ---- Independent pipelines ----
#     for pipeline in ["glif", "difference"]:
#         run_precheck_and_migrate(pipeline, spark, etl_date, RUN_ID)

#     spark.stop()


# # ==================================================
# # ENTRY POINT
# # ==================================================
# if __name__ == "__main__":
#     run_all_pipelines()


def run_currency_precheck(spark, etl_date, RUN_ID):
    """
    - Check FINCORE.CURRENCY_MASTER
    - For FLAG = 1 currencies
    - RATE_DATE must be == ETL_DATE (month-end)
    """

    logger = setup_logger(
        "CURRENCY_PRECHECK",
        etl_date.strftime("%Y-%m-%d"),
        RUN_ID
    )

    oracle = get_oracle_properties()

    query = f"""
    (
        SELECT COUNT(*) AS missing_cnt
        FROM FINCORE.CURRENCY_MASTER
        WHERE FLAG = 1
          AND (RATE_DATE IS NULL OR RATE_DATE <> DATE '{etl_date.strftime("%Y-%m-%d")}')
    )
    """

    df = (
        spark.read.format("jdbc")
        .option("url", oracle["url"])
        .option("user", oracle["user"])
        .option("password", oracle["password"])
        .option("driver", oracle["driver"])
        .option("dbtable", query)
        .load()
    )

    missing_cnt = df.collect()[0]["MISSING_CNT"]

    if missing_cnt > 0:
        logger.error(
            f"CURRENCY PRECHECK FAILED : {missing_cnt} currencies missing RATE_DATE={etl_date}"
        )
        return False
    else:
        logger.info("CURRENCY PRECHECK PASSED")
        return True
