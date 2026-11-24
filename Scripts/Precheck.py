import logging
import sys
import os
from common.constants import Process
from datetime import timedelta
from pyspark.sql.functions import col
from py4j.java_gateway import java_import
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import to_date,expr, round, col,when, length, substring,trim, concat, lit, create_map, to_timestamp, sum,broadcast, coalesce,current_timestamp
from pyspark.sql.functions import to_date,expr, round, col,when, length, substring,trim, concat, lit, create_map, to_timestamp, sum,broadcast, coalesce,current_timestamp
from common.glif_balance_migration import run_glif_migration
from common.difference__migration import run_difference_migration
from common.check_monthend import check_MonthEnd
from datetime import date, timedelta, datetime
from common.logger import setup_logger
from common.dateUtil import get_etl_date
from common.fileUtil import paths_for_read, get_hdfs_base
from common.createSpark import create_spark_session
from common.processRun import get_run_id
from common.properties import get_oracle_properties
from common.log_etl_modified import log_etl
from common.processRun import get_process_run_id
#-----------------------------------------------------------------------------------------------------------------------------------------------------------
def create_manifest(spark, filename, manifest_base, etl_date, content):

    # Format date
    if isinstance(etl_date, str):
        etl_date_str = etl_date
    else:
        etl_date_str = etl_date.strftime("%Y%m%d")

    # Final manifest folder
    final_manifest_dir = f"{manifest_base.rstrip('/')}/{etl_date_str}"

    print("Final Manifest Path:", final_manifest_dir)

    java_import(spark._jvm, "org.apache.hadoop.fs.FileSystem")
    java_import(spark._jvm, "org.apache.hadoop.fs.Path")

    fs = spark._jvm.FileSystem.get(spark._jsc.hadoopConfiguration())
    Path = spark._jvm.Path

    hdfs_dir = Path(final_manifest_dir)

    # Create ETL folder if not exists
    if not fs.exists(hdfs_dir):
        print("Creating ETL folder:", final_manifest_dir)
        fs.mkdirs(hdfs_dir)

    # Write locally
    with open(filename, "w") as f:
        f.write(content)

    dest_path = Path(f"{final_manifest_dir}/{filename}")

    fs.copyFromLocalFile(
        False,
        True,
        Path(filename),
        dest_path
    )

    if not fs.exists(dest_path):
        raise RuntimeError(f"Manifest NOT written: {dest_path}")

    print("Manifest successfully created at:", dest_path)
#----------------------------------------------------------------------------------------------------------------------------------   
def delta_table_exists(spark, path):
    java_import(spark._jvm, "org.apache.hadoop.fs.Path")
    java_import(spark._jvm, "org.apache.hadoop.fs.FileSystem")

    fs = spark._jvm.FileSystem.get(spark._jsc.hadoopConfiguration())
    delta_log_path = spark._jvm.Path(f"{path}/_delta_log")
    return fs.exists(delta_log_path)   
#-----------------------------------------------------------------------------------------------------------------------------------------    
def run_pipeline_precheck(pipeline, spark, etl_date, run_id, process_run_id):
    pipeline = pipeline.lower()
    logger = setup_logger(
        f"{pipeline.upper()}_PRECHECK",
        etl_date.strftime("%Y-%m-%d"),
        run_id
    )
    
    # --------------------------------------------------
    # REQUIRED DATE LOGIC
    # --------------------------------------------------
    if pipeline == "glif":
        log_etl(spark, process_run_id, "precheck_started_glif",1,"Glif Precheck Pipline Started")
        required_date = etl_date if run_id == 1 else etl_date - timedelta(days=1)

    elif pipeline == "difference":
        log_etl(spark, process_run_id, "precheck_started_diffrence",1,"Diffrence Precheck Pipline Started")
        required_date = etl_date - timedelta(days=1)

    else:
        raise ValueError(f"Unknown pipeline: {pipeline}")

    required_date_str = required_date.strftime("%d-%m-%Y")
    
    logger.info(f"PIPELINE = {pipeline.upper()}")
    logger.info(f"REQUIRED_DATE = {required_date_str}")
    
    # --------------------------------------------------
    # PATH CONFIG
    # --------------------------------------------------
    cfg = paths_for_read(pipeline, etl_date, required_date_str)
    datalake_path = cfg["PRECHECK_DATALAKE_PATH"]
    manifest_base = cfg["MANIFEST_BASE_PATH"]

    manifest_file = f"{pipeline.upper()}_precheck_successfull_{run_id}_Manifest.txt"

    logger.info(f"DATALAKE_PATH = {datalake_path}")
    if pipeline == "glif":
        log_etl(spark, process_run_id,"glif_datalake_reading", 1,"GLIF datalake reading started")
    else:
        log_etl(spark, process_run_id,"difference_datalake_reading", 1," DIFFERENCE datalake reading started")

    try:
        if not delta_table_exists(spark, datalake_path):
            logger.info("Delta table not found. Migration required.")
            data_exists = False
        else:
            delta_df = spark.read.format("delta").load(datalake_path)
            
            if pipeline=="glif":
                date_column="BALANCE_DATE"
            elif pipeline=="difference":
                date_column="RECON_DATE"
                
            data_exists = (
            delta_df.filter(to_date(col(date_column))==to_date(lit(required_date)))
            .limit(1)
            .count() > 0
            )

            logger.info(f"DATA_EXISTS = {data_exists}")
            
           

    except Exception as e:
        logger.error("Failed to read Delta table", exc_info=True)
        if pipeline == "glif":
            log_etl(spark, process_run_id, "glif_datalake_reading",3,"Error in reading Glif Datalake ")

        else:  # difference
            log_etl(spark, process_run_id, "difference_datalake_reading",3,"Error in reading difference Datalake")

        raise  
    
   
    if data_exists:

        if pipeline == "glif":
            log_etl(spark, process_run_id,"glif_datalake_reading", 2,"Data present in GLIF datalake")

        elif pipeline=="difference":
            log_etl(spark, process_run_id,"difference_datalake_reading", 2,"Data present in DIFFERENCE datalake")

        create_manifest(
            spark,
            manifest_file,
            manifest_base,
            etl_date,
            f"{pipeline.upper()} | DATA_ALREADY_PRESENT | DATE={required_date_str}"
        )

        logger.info("STATUS = DATA_ALREADY_PRESENT")
        return data_exists

    else:

        try:
            if pipeline == "glif":
                log_etl(spark, process_run_id, "glif_datalake_reading",3,"Glif Datalake DATA NOT PRESENT | MIGRATION REQUIRED")
            else:  # difference
                log_etl(spark, process_run_id, "difference_datalake_reading",3,"difference Datalake DATA NOT PRESENT | MIGRATION REQUIRED")
            if pipeline == "glif":
                

                run_glif_migration(spark,required_date,run_id,process_run_id)

                log_etl(spark, process_run_id,"glif_migration", 2,"GLIF Migration Completed")

            elif pipeline=="difference":
                
                

                run_difference_migration(
                    spark,
                    required_date,
                    run_id,
                    process_run_id
                )

                log_etl(spark, process_run_id,"difference_migration", 2,"DIFFERENCE Migration Completed")

            # create manifest only AFTER successful migration
            create_manifest(
                spark,
                manifest_file,
                manifest_base,
                etl_date,
                f"{pipeline.upper()} | DATA_MIGRATED | DATE={required_date_str}"
            )

            logger.info("STATUS = DATA_MIGRATED")

        except Exception:
            logger.error("MIGRATION FAILED", exc_info=True)
            if pipeline == "glif":
                log_etl(spark, process_run_id, "glif_migration",3,"Error in glif migration")
            else:  # difference
                log_etl(spark, process_run_id, "difference_migration",3,"Error in diffrence migration")
            raise
    
    
    
def run_currency_precheck_monthend(spark, etl_date, RUN_ID,process_run_id):
    try:
        log_etl(spark, process_run_id, "precheck_started_currency_monthend",1,"Precheck Pipline Started")
        """
        - Check FINCORE.CURRENCY_MASTER
        - For FLAG = 1 currencies
        - RATE_DATE must be == ETL_DATE (month-end)
        """
        process_type="CURRENCY_PRECHECK"
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
            AND (RATE_DATE IS NULL OR RATE_DATE < DATE '{etl_date.strftime("%Y-%m-%d")}')
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
            log_etl(spark, process_run_id, "precheck_started_currency_monthend",3,"currency precheck failed ")
            
            return False
        else:
            logger.info("CURRENCY PRECHECK PASSED")
            log_etl(spark, process_run_id, "precheck_started_currency_monthend",2,"currency precheck Passed. ")
            return True
    except Exception as e:
        logger.info(f"Error : {e}")
        log_etl(spark, process_run_id, "precheck_started_currency_monthend",3,"Error in Precheck Pipline.")
        
        
def main(args):
    
    HDFS_BASE = get_hdfs_base()
    spark = create_spark_session("DATALAKE_PRECHECK", HDFS_BASE)
    spark.conf.set("spark.hadoop.fs.defaultFS", HDFS_BASE)
    PRECHECK_MASTER="PRECHECK"

    PROCESS_ID= Process.PRECHECK_PIPELINE
    
    etl_date = get_etl_date(spark)
    # etl_date=datetime(2025,12,31).date()
    process_run_id = sys.argv[1] if len(sys.argv) > 1 else get_process_run_id(spark,PROCESS_ID,etl_date.strftime("%Y-%m-%d"))
    pipeline_run_id = get_run_id(spark,etl_date.strftime("%Y-%m-%d"),PROCESS_ID) 
    log_etl(spark, process_run_id, "precheck_started",1,"Precheck Pipline Started")
    master_logger = setup_logger(
        "PRECHECK_MASTER",
        etl_date.strftime("%Y-%m-%d"),
        pipeline_run_id
    )

    master_logger.info(f"Precheck ETL Date:::::{etl_date}")
    master_logger.info("===== PRECHECK STARTED =====")
    
    check_monthend=check_MonthEnd()
    if check_monthend:
            run_currency_precheck_monthend(spark, etl_date, pipeline_run_id,process_run_id)
        
    count=0
    for pipeline in ["glif","difference"]:
        try:
            run_id=get_run_id(spark,etl_date.strftime("%Y-%m-%d"),PROCESS_ID)
            run_pipeline_precheck(pipeline, spark, etl_date, run_id,process_run_id)
            if pipeline == "glif":
                log_etl(spark, process_run_id, "precheck_started_glif",2,"Glif Precheck Pipline Completed")
            elif pipeline == "difference":
                log_etl(spark, process_run_id, "precheck_started_diffrence",2,"Diffrence Precheck Pipline Completed")
            master_logger.info("===== PRECHECK COMPLETED =====")  
        except Exception:
            master_logger.error(
                f"{pipeline.upper()} PRECHECK FAILED",
                exc_info=True
            )
        
            if pipeline == "glif":
                log_etl(spark, process_run_id, "precheck_started_glif",3,"Error in Glif Precheck Pipeline")
                log_etl(spark, process_run_id, "precheck_started",3,"Error in Glif precheck pipeline .")
                count=count+1
            else:  # difference
                log_etl(spark, process_run_id, "precheck_started_diffrence",3,"Error in Diffrence Precheck Pipeline.")
                log_etl(spark, process_run_id, "precheck_started",3,"Error in Difference precheck pipeline .")
                count=count+1
    if(count==0):
        log_etl(spark, process_run_id, "precheck_started",2,"Precheck Pipline completed")
        spark.stop()
    else:
        spark.stop()
        os._exit(1)

    
# ============================================================
if __name__ == "__main__":
    main(sys.argv)