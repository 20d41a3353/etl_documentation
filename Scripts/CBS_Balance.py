#!/usr/bin/env python3
from pyspark.sql.functions import desc
import os 
import logging
import sys
from py4j.java_gateway import java_import
import concurrent.futures
from datetime import date, timedelta, datetime
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, trim, substring, lit, to_date
from pyspark.sql.types import DecimalType,StructType, StructField, StringType
from pyspark.storagelevel import StorageLevel
from delta.tables import DeltaTable
# =========== Common Methods =============================
from common.logger import setup_logger 
from common.properties import get_oracle_properties
from common.dateUtil import get_etl_date
from common.fileUtil import paths_for_read
from common.fileUtil import get_hdfs_base
from common.processRun import get_run_id
from common.createSpark import create_spark_session
from common.fileUtil import tables_to_read
from common.constants import Process
#from common.log_etl_modified import log_etl
from common.processRun import get_process_run_id
from common.fileUtil import get_delta_path_by_file_type
from common.utilites import create_hdfs_dir
# from common.fileUtil import file_processing_paths
from common.create_path_if_not_exists import create_path_if_not_exists

HDFS_BASE = get_hdfs_base()
spark = create_spark_session("CBS_PIPELINE", HDFS_BASE)
today = get_etl_date(spark)

print(f"ETL date::: {today}")  
yesterday = today - timedelta(days=1)
posting_date_str = today.strftime("%Y%m%d")
today_str = posting_date_str
print("sparkstarted")
oracle_properties=get_oracle_properties()

today_str = today.strftime("%Y%m%d")
yesterday = today - timedelta(days=1)
daybfyes = today -timedelta(days=3)

spark_date = today.strftime("%Y-%m-%d")
PROCESS_ID = Process.CBS_PIPELINE
process_run_id = sys.argv[1] if len(sys.argv) > 1 else get_process_run_id(spark,PROCESS_ID,spark_date)
run_id =  get_run_id(spark,spark_date,PROCESS_ID) 
logger = setup_logger("CBS",run_id,spark_date)

hdfs_paths = paths_for_read("bancs24",today,today_str)

HDFS_BASE = hdfs_paths["HDFS_BASE"]
HDFS_PATH = hdfs_paths["HDFS_PATH"]
CBS_BALANCE_DATALAKE_PATH = hdfs_paths["CBS_BALANCE_DATALAKE_PATH"]    
manifest_filename = hdfs_paths["manifest_filename"]
spark_stream_path = hdfs_paths["spark_stream_path"] + manifest_filename

date_str = spark_date
date_obj = datetime.strptime(date_str, "%Y-%m-%d")    
year = date_obj.strftime("%Y")
month = date_obj.strftime("%b").upper()
day = date_obj.strftime("%d")

BANCS_DATALAKE =  f"{HDFS_BASE}/druid-data-lake/{year}/{month}/{day}/BANCS24"
create_path_if_not_exists(spark,BANCS_DATALAKE)
oracle_url = oracle_properties["url"]
oracle_user = oracle_properties["user"]
oracle_password = oracle_properties["password"]
oracle_driver = oracle_properties["driver"]
    
oracle_tables = tables_to_read("bancs24")
BALANCE = oracle_tables["BALANCE"]

spark.conf.set("spark.hadoop.fs.defaultFS", HDFS_BASE)

posting_date_str = today.strftime("%Y-%m-%d")

#log_etl(spark, process_run_id, "source_dir_check", 1, f"checking if source dir : {CBS_BALANCE_DATALAKE_PATH} exists")
if not create_hdfs_dir(spark, CBS_BALANCE_DATALAKE_PATH, logger=logger):
    #log_etl(spark, process_run_id, "source_dir_check", 3, f"Could not create or find path: {CBS_BALANCE_DATALAKE_PATH}")
    spark.stop()
    os._exit(1)
#log_etl(spark, process_run_id, "source_dir_check", 2, f"Source dir : {CBS_BALANCE_DATALAKE_PATH} exists")

if(run_id>1):
            #log_etl(spark, process_run_id, "CBS_deletion_oracle", 1, "In CBS deletion started")
            try:
                oracle_properties=get_oracle_properties()

                oracle_url = oracle_properties["url"]
                oracle_user = oracle_properties["user"]
                oracle_password = oracle_properties["password"]
                oracle_driver = oracle_properties["driver"]
                conn = spark._sc._gateway.jvm.java.sql.DriverManager.getConnection(oracle_url,oracle_user,oracle_password)
           
                sql_query=f"""
                Delete from {BALANCE} where BALANCE_DATE = to_date('{posting_date_str}','yyyy-mm-dd')
                """
                conn.setAutoCommit(False)             
                statement = conn.createStatement()
                print("stmt")
                statement.execute(sql_query)
                conn.commit()
                logger.info(f"Deleted from CBS table successfully")
                #log_etl(spark, process_run_id, "CBS_deletion_oracle", 2, "In CBS deletion success")
            except Exception as e:
                logger.error(f"error {e} while deleting from CBS table")
                #log_etl(spark, process_run_id, "CBS_deletion_oracle", 3, "In CBS deletion failed")
                #log_etl(spark, process_run_id, "cbs_started",3,"Error in Cbs pipeline.")
                spark.stop()
                sys.exit(1)

            try:
                #log_etl(spark, process_run_id, "CBS_deletion_datalake", 1, "In CBS deletion datalake started")
                delta_table = DeltaTable.forPath(spark, CBS_BALANCE_DATALAKE_PATH)
                delta_table.delete(f"BALANCE_DATE = '{posting_date_str}'")
                #log_etl(spark, process_run_id, "CBS_deletion_datalake", 2, "Diffrence deletion datalake completed")
            except Exception as e:
                logger.info(f"Error during saving in datalake  {e}")
                #log_etl(spark, process_run_id, "CBS_deletion_datalake", 3, "Error in Diffrence deletion")
                #log_etl(spark,process_run_id , "file_reading_Started",3,"Error in deleting diffrence  to delta lake ")
                spark.stop()
                sys.exit(1)

def invtogl(spark):
    try:
        INV = f"{BANCS_DATALAKE}/INV"
        logger.info(f"INV datalake path:{INV}")
        try:
            
            #log_etl(spark, process_run_id, "invtogl_filereading_started",1,"INVTOGL File Reading Started")
            df_raw = spark.read.format("delta").load(INV).select(col("GLCC"),col("BALANCE"))
            # logger.info(f"invtogl bulk data lake count {df_raw.count()}")
            logger.info("INVTOGL datalake schema")
            df_raw.printSchema()
            
           
        except Exception as e:
            print(f"Error {e}")
            #log_etl(spark, process_run_id, "invtogl_filereading_started",3,"ERROR IN READING INVTOGL FILES")
            spark.stop()
            exit()

        
        return df_raw
    

    except Exception as e:
        logger.info("Error processing invtogl", exc_info=True)
        #log_etl(spark, process_run_id, "invtogl_filereading_started",3,"ERROR IN READING INVTOGL")
        #log_etl(spark, process_run_id, "cbs_started",3,"Error in Cbs pipeline.")
        spark.stop()
        exit()


def bortogl(spark):
    try:
        #log_etl(spark, process_run_id, "bortogl_filereading_started",1,"BORTOGL File Reading Started")
        BOR = f"{BANCS_DATALAKE}/BOR"
        logger.info(f"BOR datalake path:{BOR}")
        try:
            df_raw = spark.read.format("delta").load(BOR).select(col("GLCC"),col("BALANCE"))
            # logger.info(f"bortogl bulk data lake count {df_raw.count()}")
            logger.info("BORTOGL datalake schema")
            df_raw.printSchema()
            
           
        except Exception as e:
            print(f"Error {e}")
            #log_etl(spark, process_run_id, "bortogl_filereading_started",3,"Error in reading BORTOGL.")
            spark.stop()
            exit()

        
        return df_raw

    except Exception as e:
        logger.info("Error processing bortogl", exc_info=True)
        #log_etl(spark, process_run_id, "bortogl_filereading_started",3,"ERROR IN READING BORTOGL")
        spark.stop()
        exit()

def glccgen(spark):
    try:
        #log_etl(spark, process_run_id, "glccgen_filereading_started",1,"GLCCGEN File Reading Started")
        GLCCGEN = f"{BANCS_DATALAKE}/GLCCGEN"
        logger.info(f"GLCCGEN data kale path:{GLCCGEN}")
        try:
            df_raw = spark.read.format("delta").load(GLCCGEN).select(col("GLCC"),col("BALANCE"))
            # logger.info(f"glccgen bulk data lake count {df_raw.count()}")
            logger.info("GLCCGEN datalake schema")
            df_raw.printSchema()
            
            
        except Exception as e:
            #log_etl(spark, process_run_id, "glccgen_filereading_started",3,"ERROR in Reading GLCCGEN  Files")
            raise
        
        return df_raw

    except Exception as e:
        logger.info("Error processing glccgen", exc_info=True)
        #log_etl(spark, process_run_id, "glccgen_filereading_started",3,"ERROR IN READING GLCCGEN")
        spark.stop()

def glcccta(spark):
    try:
        #log_etl(spark, process_run_id, "glcccta_filereading_started",1,"GLCCCTA File Reading Started")
        GLCCTA =f"{BANCS_DATALAKE}/GLCCCTA"
        try:
            # df_raw = spark.read.text(f"{HDFS_PATH}/GLCCCTA*")
            df_raw = spark.read.format("delta").load(GLCCTA).select(col("GLCC"),col("BALANCE"))
            # logger.info(f"glcccta bulk data lake count {df_raw.count()}")
        except Exception as e:
            print(f"Error {e}")
            #log_etl(spark, process_run_id, "glcccta_filereading_started",3,"ERROR in Reading GLCCCTA  Files")
            spark.stop()
            sys.exit(1)
        

        return df_raw


    except Exception as e:
        logger.info("Error processing glcccta", exc_info=True)
        #log_etl(spark, process_run_id, "glcccta_filereading_started",3,"ERROR IN READING GLCCCTA")
        spark.stop()
        sys.exit(1)
        
def glccinv(spark):
    schema = StructType([
        StructField("GLCC", StringType(), True),
        StructField("BALANCE", DecimalType(25, 4), True)
    ])
    try:
        #log_etl(spark, process_run_id, "glccinv_filereading_started",1,"GLCCINV File Reading Started")
        GLCCINV = f"{BANCS_DATALAKE}/GLCCINV"
        
        try:
            # df_raw = spark.read.text(f"{HDFS_PATH}/GLCCCTA*")
            df_raw = spark.read.format("delta").load(GLCCINV).select(col("GLCC"),col("BALANCE"))
            # logger.info(f"glccinv bulk data lake count {df_raw.count()}")
        except Exception as e:
            #log_etl(spark, process_run_id, "glccinv_filereading_started",3,"ERROR in Reading GLCCINV Files")
            return spark.createDataFrame([], schema)
            spark.stop()
            sys.exit(1)
        
        return df_raw


    except Exception as e:
        logger.info("Error processing glccinv", exc_info=True)
        #log_etl(spark, process_run_id, "glccinv_filereading_started",3,"ERROR IN READING GLCCINV")
        return spark.createDataFrame([], schema)
        # spark.stop()
        # exit()
        
# ============================ MAIN ============================
def main(args):
   
    HDFS_BASE = get_hdfs_base()
    
    #============================ CONFIG & DATE ============================
    spark = create_spark_session("HDFS_Text_to_DataLake", HDFS_BASE)
    startTime= datetime.now()

    #log_etl(spark, process_run_id, "cbs_started",1,"cbs pipeline started.")
    logger.info(f"CBS Balance job strated | run_id={run_id}")

    logger.info(f"ETL date resolved as: {today}")
    logger.info(f"Oracle properties fetched successfully")



    logger.info(f"Balance date for processing: {posting_date_str}")
    logger.info(f"Starting parallel file processing: invtogl, bortogl, glccgen")
    # ---------- PARALLEL FILE PROCESSING ----------
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            f1 = executor.submit(invtogl, spark)
            f2 = executor.submit(bortogl, spark)
            f3 = executor.submit(glccgen, spark)
            f4 = executor.submit(glcccta,spark)
            f5 = executor.submit(glccinv,spark)

            df_invtogl = f1.result()
            df_bortogl = f2.result()
            df_glcccta = f4.result()
            df_glccinv = f5.result()
            df_glccgen = f3.result()

      
        #log_etl(spark, process_run_id, "merging_all_filedata",1,"Merging all file data process started.")
        
        final_df = df_invtogl.unionByName(df_bortogl).unionByName(df_glccgen).unionByName(df_glcccta).unionByName(df_glccinv)
        # logger.info(f"Final union record count: {final_df.count()}")
     
        final_result_df = final_df.groupBy("GLCC").agg(F.sum("BALANCE").alias("BALANCE"),F.count(F.col("GLCC")).alias("TRANSACTION_COUNT")).select("GLCC", "BALANCE","TRANSACTION_COUNT")
        # logger.info(f"Final record count after grp by: {final_result_df.count()}")
        logger.info(f"Final aggregation completed successfully")

    except Exception as e:
        logger.info(f"Error during parallel load", exc_info=True)
        #log_etl(spark, process_run_id, "merging_all_filedata",3,"Error in merging all file data.")
        raise
   

    # ---------- COMPUTE TODAY'S BALANCE ----------
    try:
        final_aggregated_df = (
            final_result_df.select(
                col("GLCC").alias("GLCC"),
                col("TRANSACTION_COUNT").alias("TRANSACTION_COUNT"),
                col("BALANCE").cast(DecimalType(25, 4)).alias("closing_balance"),
            )
            .withColumn("BALANCE_DATE", to_date(lit(posting_date_str)))
        )
     
        # ------------------ WRITE TO DELTA ------------------
        # final_aggregated_df.cache()
        try:
            
            create_path_if_not_exists(spark,CBS_BALANCE_DATALAKE_PATH)
            final_aggregated_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(CBS_BALANCE_DATALAKE_PATH)
            logger.info(f"Data written to Delta Lake at path: {CBS_BALANCE_DATALAKE_PATH}")
            # logger.info(f"INVTOGL record count: {df_invtogl.count()}")
            #log_etl(spark, process_run_id, "invtogl_filereading_started",2,"INVTOGL File Reading COMPLETED")
            # logger.info(f"BORTOGL record count: {df_bortogl.count()}")
            #log_etl(spark, process_run_id, "bortogl_filereading_started",2,"BORTOGL File Reading COMPLETED")
            # logger.info(f"GLCCGEN record count: {df_glccgen.count()}")
            #log_etl(spark, process_run_id, "glccgen_filereading_started",2,"GLCCGEN File Reading COMPLETED")
            #logger.info(f"GLCCCTA record count: {df_glcccta.count()}")
            #log_etl(spark, process_run_id, "glcccta_filereading_started",2,"GLCCCTA File Reading COMPLETED")
            #logger.info(f"GLCCCTA record count: {df_glccinv.count()}")
            #log_etl(spark, process_run_id, "glccinv_filereading_started",2,"GLCCINV File Reading COMPLETED")
            #log_etl(spark, process_run_id, "merging_all_filedata",2,"Merging all data completed.")

            #log_etl(spark, process_run_id, "cbs_balance_deltalake",2,"CBS balance sucessfully written to delta lake.",0)
        except Exception as e:
            logger.error(f"Error {e} while writing into delta lake path ->[ {CBS_BALANCE_DATALAKE_PATH} ]")       
            #log_etl(spark, process_run_id, "merging_all_filedata",3,"Error in writing cbs_balance to delta lake")
            #log_etl(spark, process_run_id, "cbs_started",3,"Error in Cbs pipeline.")
            spark.stop()
            exit()        
    except Exception as e:
        logger.error(f"Error {e} while calculating today's balance")       
        #log_etl(spark, process_run_id, "merging_all_filedata",3,"Error in calculating today's balance")
        #log_etl(spark, process_run_id, "cbs_started",3,"Error in Cbs pipeline.")
        spark.stop()

    # ---------- CREATE AND UPLOAD MANIFEST FILE ----------
       
        # logger.info(f"Creating manifest file for Spark Stream trigger...")
        ## log_etl(spark, process_run_id, "cbs_balance_manifest",1,"manifest file creation state started.")
        # today_str = datetime.now().strftime("%Y%m%d")
                
    #     java_import(spark._jvm, "org.apache.hadoop.fs.FileSystem")
    #     java_import(spark._jvm, "org.apache.hadoop.fs.Path")

    #     try:
    #         with open(manifest_filename, "w") as f:
    #             f.write("cbs_balance successful")
                
    #     except Exception as e:
    #         logger.error(f"Error {e} while writing manifest file -> [{manifest_filename}]")
    ##         log_etl(spark, process_run_id, "cbs_balance_manifest",3," Error in writing cbs manifest file.")
    #         spark.stop()
    #         exit()           
        
    #     fs = spark._jvm.FileSystem.get(spark._jsc.hadoopConfiguration())
        
    #     fs.copyFromLocalFile(False, True, spark._jvm.Path(manifest_filename), spark._jvm.Path(spark_stream_path))
    ##     log_etl(spark, process_run_id, "cbs_balance_manifest",2,"manifest file uploaded successfully.")
    #     logger.info(f"Manifest file uploaded to spark-stream path: {spark_stream_path}")
    # except Exception as e:
    #     logger.info(f"Error during Delta or manifest stage", exc_info=True)
    ##     log_etl(spark, process_run_id, "cbs_balance_manifest",3," Error in writing cbs manifest file.")
    #     spark.stop()



    # 1. Define root path
    BASE_MANIFEST_PATH = "/ManifestFolder"
    try:
        # 2. Get ETL date for the ETL folder path
        etl_date_path = today.strftime("%Y%m%d") 
        hdfs_dir = f"{BASE_MANIFEST_PATH}/{etl_date_path}"
        
        # Initialize Hadoop FileSystem
        java_import(spark._jvm, "org.apache.hadoop.fs.FileSystem")
        java_import(spark._jvm, "org.apache.hadoop.fs.Path")
        fs = spark._jvm.FileSystem.get(spark._jsc.hadoopConfiguration())

        # 3. Define manifest path
        manifest_filename = f"CBS_balance_successful_{run_id}_Manifest.txt"
        dest_file_path = spark._jvm.Path(f"{hdfs_dir}/{manifest_filename}")

        #log_etl(spark, process_run_id, "cbs_balance_mainfest_writing", 1, "Starting manifest stage.")

        # 4. Ensure HDFS directory exists
        hdfs_dir_obj = spark._jvm.Path(hdfs_dir)
        if not fs.exists(hdfs_dir_obj):
            fs.mkdirs(hdfs_dir_obj)

        # 5. Write content directly to HDFS
        # Using FSDataOutputStream via Py4J
        out_stream = fs.create(dest_file_path, True)
        out_stream.write(bytearray("CBS_balance successful", "utf-8"))
        out_stream.close()

        logger.info(f"Manifest written to HDFS: {dest_file_path}")
        #log_etl(spark, process_run_id, "cbs_balance_mainfest_writing", 2, "Manifest uploaded successfully.")

    except Exception as e:
        logger.error(f"Error in manifest creation: {e}")
        #log_etl(spark, process_run_id, "cbs_balance_mainfest_writing", 3, "Error in creating manifest file")
        # spark.stop()
        # exit(1)



    # ---------- INSERT INTO ORACLE ----------
    
    df_mapped = (
        final_aggregated_df
        .withColumn("BRANCH_CODE", substring(col("GLCC"), 1, 5))
        .withColumn("CURRENCY", substring(col("GLCC"), 6, 3))
        .withColumn("CGL", substring(col("GLCC"), 9, 10))
    )

    processed_df = df_mapped.select(
        "CGL", "BALANCE_DATE","TRANSACTION_COUNT",
        col("closing_balance").alias("BALANCE"),
        "CURRENCY", "BRANCH_CODE"
    )
    # logger.info(f"Final record count before writing to datalake : {processed_df.count()}")
    processed_df.printSchema()
    try:
        
        processed_df.write \
        .format("jdbc") \
        .option("url", oracle_url) \
        .option("dbtable", "CBS_BALANCE") \
        .option("user", oracle_user) \
        .option("password", oracle_password) \
        .option("driver", oracle_driver) \
        .mode("append") \
        .save()
        logger.info(f"Data Successfully wrote to Oracle DB")
        #log_etl(spark, process_run_id, "cbs_balance_oracle",2, "CBS balance sucessfully written to oracle.",0)
        
    except Exception as e:
        logger.info(f"Error while writing into JDBC {e}")
        #log_etl(spark, process_run_id, "cbs_started",3,"Error in writing cbs balance in oracle")
        #log_etl(spark, process_run_id, "cbs_started",3,"Error in Cbs pipeline.")
        spark.stop()
    #log_etl(spark, process_run_id, "cbs_started",2,"Cbs pipeline completed successfully.")
    spark.stop()



if __name__=="__main__":
    main(sys.argv)























# #!/usr/bin/env python3
# from pyspark.sql.functions import desc
# import os 
# import logging
# import sys
# from py4j.java_gateway import java_import
# import concurrent.futures
# from datetime import date, timedelta, datetime
# from pyspark.sql import functions as F
# from pyspark.sql.functions import col, when, trim, substring, lit, to_date
# from pyspark.sql.types import DecimalType,StructType, StructField, StringType
# from pyspark.storagelevel import StorageLevel
# from delta.tables import DeltaTable
# # =========== Common Methods =============================
# from common.logger import setup_logger 
# from common.properties import get_oracle_properties
# from common.dateUtil import get_etl_date
# from common.fileUtil import paths_for_read
# from common.fileUtil import get_hdfs_base
# from common.processRun import get_run_id
# from common.createSpark import create_spark_session
# from common.fileUtil import tables_to_read
# from common.constants import Process
# from common.log_etl_modified import log_etl
# from common.processRun import get_process_run_id
# from common.fileUtil import get_delta_path_by_file_type
# from common.utilites import create_hdfs_dir
# HDFS_BASE = get_hdfs_base()
# spark = create_spark_session("CBS_PIPELINE", HDFS_BASE)
# today = get_etl_date(spark)
# # today=datetime(2025,11,21).date()
# print(f"ETL date::: {today}")  
# yesterday = today - timedelta(days=1)
# posting_date_str = today.strftime("%Y%m%d")
# today_str = posting_date_str
# print("sparkstarted")
# oracle_properties=get_oracle_properties()

# today_str = today.strftime("%Y%m%d")
# yesterday = today - timedelta(days=1)
# daybfyes = today -timedelta(days=3)

# spark_date = today.strftime("%Y-%m-%d")
# PROCESS_ID = Process.CBS_PIPELINE
# process_run_id = sys.argv[1] if len(sys.argv) > 1 else get_process_run_id(spark,PROCESS_ID,spark_date)
# run_id = get_run_id(spark,spark_date,PROCESS_ID) 
# logger = setup_logger("CBS",run_id,spark_date)

# hdfs_paths = paths_for_read("bancs24",today,today_str)

# HDFS_BASE = hdfs_paths["HDFS_BASE"]
# HDFS_PATH = hdfs_paths["HDFS_PATH"]
# CBS_BALANCE_DATALAKE_PATH = hdfs_paths["CBS_BALANCE_DATALAKE_PATH"]    
# manifest_filename = hdfs_paths["manifest_filename"]
# spark_stream_path = hdfs_paths["spark_stream_path"] + manifest_filename
 
# oracle_url = oracle_properties["url"]
# oracle_user = oracle_properties["user"]
# oracle_password = oracle_properties["password"]
# oracle_driver = oracle_properties["driver"]
    
# oracle_tables = tables_to_read("bancs24")
# BALANCE = oracle_tables["BALANCE"]

# spark.conf.set("spark.hadoop.fs.defaultFS", HDFS_BASE)

# posting_date_str = today.strftime("%Y-%m-%d")

# log_etl(spark, process_run_id, "source_dir_check", 1, f"checking if source dir : {CBS_BALANCE_DATALAKE_PATH} exists")
# if not create_hdfs_dir(spark, CBS_BALANCE_DATALAKE_PATH, logger=logger):
#     log_etl(spark, process_run_id, "source_dir_check", 3, f"Could not create or find path: {CBS_BALANCE_DATALAKE_PATH}")
#     spark.stop()
#     os._exit(1)
# log_etl(spark, process_run_id, "source_dir_check", 2, f"Source dir : {CBS_BALANCE_DATALAKE_PATH} exists")

# if(run_id>1):
#             log_etl(spark, process_run_id, "CBS_deletion_oracle", 1, "In CBS deletion started")
#             try:
#                 oracle_properties=get_oracle_properties()

#                 oracle_url = oracle_properties["url"]
#                 oracle_user = oracle_properties["user"]
#                 oracle_password = oracle_properties["password"]
#                 oracle_driver = oracle_properties["driver"]
#                 conn = spark._sc._gateway.jvm.java.sql.DriverManager.getConnection(oracle_url,oracle_user,oracle_password)
           
#                 sql_query=f"""
#                 Delete from {BALANCE} where BALANCE_DATE = to_date('{posting_date_str}','yyyy-mm-dd')
#                 """
#                 conn.setAutoCommit(False)             
#                 statement = conn.createStatement()
#                 print("stmt")
#                 statement.execute(sql_query)
#                 conn.commit()
#                 logger.info(f"Deleted from CBS table successfully")
#                 log_etl(spark, process_run_id, "CBS_deletion_oracle", 2, "In CBS deletion success")
#             except Exception as e:
#                 logger.error(f"error {e} while deleting from CBS table")
#                 log_etl(spark, process_run_id, "CBS_deletion_oracle", 3, "In CBS deletion failed")
#                 log_etl(spark, process_run_id, "cbs_started",3,"Error in Cbs pipeline.")
#                 spark.stop()
#                 sys.exit(1)

#             try:
#                 log_etl(spark, process_run_id, "CBS_deletion_datalake", 1, "In CBS deletion datalake started")
#                 delta_table = DeltaTable.forPath(spark, CBS_BALANCE_DATALAKE_PATH)
#                 delta_table.delete(f"BALANCE_DATE = '{posting_date_str}'")
#                 log_etl(spark, process_run_id, "CBS_deletion_datalake", 2, "Diffrence deletion datalake completed")
#             except Exception as e:
#                 logger.info(f"Error during saving in datalake  {e}")
#                 log_etl(spark, process_run_id, "CBS_deletion_datalake", 3, "Error in Diffrence deletion")
#                 log_etl(spark,process_run_id , "file_reading_Started",3,"Error in deleting diffrence  to delta lake ")
#                 spark.stop()
#                 sys.exit(1)

# def invtogl(spark):
#     try:
#         INV = get_delta_path_by_file_type("INV", posting_date_str)
#         logger.info(f"INV datalake path:{INV}")
#         try:
            
#             log_etl(spark, process_run_id, "invtogl_filereading_started",1,"INVTOGL File Reading Started")
#             df_raw = spark.read.format("delta").load(INV)
#             # logger.info(f"invtogl bulk data lake count {df_raw.count()}")
#             logger.info("INVTOGL datalake schema")
#             df_raw.printSchema()
           
#         except Exception as e:
#             log_etl(spark, process_run_id, "invtogl_filereading_started",3,"ERROR IN READING INVTOGL FILES")
#             # spark.stop()
#             # exit()

#         df_final = df_raw.select(
#             col("CGL").alias("Id"),
#             when( 
#                 # 1. Check the very last character for the sign
#                 substring(col("AMOUNT"), -1, 1) == "-", 
#                 # 2. Substring from start (pos 1) and take 17 characters (excluding the sign)
#                -1 *  trim(substring(col("AMOUNT"), 1, 17)).cast(DecimalType(25, 4)) / 1000
#             ).otherwise(
#                 # 3. Same extraction, then apply negative sign
#                 trim(substring(col("AMOUNT"), 1, 17)).cast(DecimalType(25, 4)) / 1000
#             ).alias("Amount")
#         )
#         return df_final
    

#     except Exception as e:
#         logger.info("Error processing invtogl", exc_info=True)
#         log_etl(spark, process_run_id, "invtogl_filereading_started",3,"ERROR IN READING INVTOGL")
#         log_etl(spark, process_run_id, "cbs_started",3,"Error in Cbs pipeline.")
#         spark.stop()
#         exit()


# def bortogl(spark):
#     try:
#         log_etl(spark, process_run_id, "bortogl_filereading_started",1,"BORTOGL File Reading Started")
#         BOR = get_delta_path_by_file_type("BOR", posting_date_str)
#         logger.info(f"BOR datalake path:{BOR}")
#         try:
#             df_raw = spark.read.format("delta").load(BOR)
#             # logger.info(f"bortogl bulk data lake count {df_raw.count()}")
#             logger.info("BORTOGL datalake schema")
#             df_raw.printSchema()
           
#         except Exception as e:
#             log_etl(spark, process_run_id, "bortogl_filereading_started",3,"Error in reading BORTOGL.")
#             # spark.stop()
#             # exit()

#         df_final = df_raw.select(
#             col("CGL").alias("Id"),
#             when(
#                 # 1. Check the very last character for the sign
#                 substring(col("AMOUNT"), -1, 1) == "-", 
#                 # 2. Substring from start (pos 1) and take 17 characters (excluding the sign)
#                  trim(substring(col("AMOUNT"), 1, 17)).cast(DecimalType(25, 4)) / 1000
#             ).otherwise(
#                 # 3. Same extraction, then apply negative sign
#                 -1 * trim(substring(col("AMOUNT"), 1, 17)).cast(DecimalType(25, 4)) / 1000
#             ).alias("Amount")
#         )
#         return df_final

#     except Exception as e:
#         logger.info("Error processing bortogl", exc_info=True)
#         log_etl(spark, process_run_id, "bortogl_filereading_started",3,"ERROR IN READING BORTOGL")
#         # spark.stop()
#         # exit()

# def glccgen(spark):
#     try:
#         log_etl(spark, process_run_id, "glccgen_filereading_started",1,"GLCCGEN File Reading Started")
#         GLCCGEN = get_delta_path_by_file_type("GEN", posting_date_str)
#         logger.info(f"GLCCGEN data kale path:{GLCCGEN}")
#         try:
#             df_raw = spark.read.format("delta").load(GLCCGEN)
#             # logger.info(f"glccgen bulk data lake count {df_raw.count()}")
#             logger.info("GLCCGEN datalake schema")
#             df_raw.printSchema()
            
#         except Exception as e:
#             log_etl(spark, process_run_id, "glccgen_filereading_started",3,"ERROR in Reading GLCCGEN  Files")
#             raise
#         df_final = df_raw.select(
#             col("CGL").alias("Id"),
#             when(
#                 # 1. Check the very last character for the sign
#                 substring(col("AMOUNT"), -1, 1) == "-", 
#                 # 2. Substring from start (pos 1) and take 17 characters (excluding the sign)
#                  trim(substring(col("AMOUNT"), 1, 20)).cast(DecimalType(25, 4)) / 10000
#             ).otherwise(
#                 # 3. Same extraction, then apply negative sign
#                 -1 * trim(substring(col("AMOUNT"), 1, 20)).cast(DecimalType(25, 4)) / 10000
#             ).alias("Amount")
#         )
#         return df_final

#     except Exception as e:
#         logger.info("Error processing glccgen", exc_info=True)
#         log_etl(spark, process_run_id, "glccgen_filereading_started",3,"ERROR IN READING GLCCGEN")
#         # spark.stop()

# def glcccta(spark):
#     try:
#         log_etl(spark, process_run_id, "glcccta_filereading_started",1,"GLCCCTA File Reading Started")
#         GLCCTA = get_delta_path_by_file_type("CTA", posting_date_str)
#         try:
#             # df_raw = spark.read.text(f"{HDFS_PATH}/GLCCCTA*")
#             df_raw = spark.read.format("delta").load(GLCCTA)
#             # logger.info(f"glcccta bulk data lake count {df_raw.count()}")
#         except Exception as e:
#             log_etl(spark, process_run_id, "glcccta_filereading_started",3,"ERROR in Reading GLCCCTA  Files")
#             # spark.stop()
#             # exit()
#         df_final = df_raw.select(
#             col("CGL").alias("Id"),
#             when(
#                 # 1. Check the very last character for the sign
#                 substring(col("AMOUNT"), -1, 1) == "-", 
#                 # 2. Substring from start (pos 1) and take 17 characters (excluding the sign)
#                  trim(substring(col("AMOUNT"), 1, 20)).cast(DecimalType(25, 4)) / 10000
#             ).otherwise(
#                 # 3. Same extraction, then apply negative sign
#                  -1 * trim(substring(col("AMOUNT"), 1, 20)).cast(DecimalType(25, 4)) / 10000
#             ).alias("Amount")
#         )

#         return df_final


#     except Exception as e:
#         logger.info("Error processing glcccta", exc_info=True)
#         log_etl(spark, process_run_id, "glcccta_filereading_started",3,"ERROR IN READING GLCCCTA")
#         # spark.stop()
#         # exit()
        
# def glccinv(spark):
#     schema = StructType([
#         StructField("Id", StringType(), True),
#         StructField("Amount", DecimalType(25, 4), True)
#     ])
#     try:
#         log_etl(spark, process_run_id, "glccinv_filereading_started",1,"GLCCINV File Reading Started")
#         GLCCINV = get_delta_path_by_file_type("GINV", posting_date_str)
        
#         try:
#             # df_raw = spark.read.text(f"{HDFS_PATH}/GLCCCTA*")
#             df_raw = spark.read.format("delta").load(GLCCINV)
#             # logger.info(f"glccinv bulk data lake count {df_raw.count()}")
#         except Exception as e:
#             log_etl(spark, process_run_id, "glccinv_filereading_started",3,"ERROR in Reading GLCCINV Files")
#             return spark.createDataFrame([], schema)
#             # spark.stop()
#             # exit()
#         df_final = df_raw.select(
#             col("CGL").alias("Id"),
#             when(
#                 # 1. Check the very last character for the sign
#                 substring(col("AMOUNT"), -1, 1) == "-", 
#                 # 2. Substring from start (pos 1) and take 17 characters (excluding the sign)
#                  trim(substring(col("AMOUNT"), 1, 20)).cast(DecimalType(25, 4)) / 10000
#             ).otherwise(
#                 # 3. Same extraction, then apply negative sign
#                  -1 * trim(substring(col("AMOUNT"), 1, 20)).cast(DecimalType(25, 4)) / 10000
#             ).alias("Amount")
#         )
#         return df_final


#     except Exception as e:
#         logger.info("Error processing glccinv", exc_info=True)
#         log_etl(spark, process_run_id, "glccinv_filereading_started",3,"ERROR IN READING GLCCINV")
#         return spark.createDataFrame([], schema)
#         # spark.stop()
#         # exit()
        
# # ============================ MAIN ============================
# def main(args):
   
#     HDFS_BASE = get_hdfs_base()
    
#     #============================ CONFIG & DATE ============================
#     spark = create_spark_session("HDFS_Text_to_DataLake", HDFS_BASE)
#     startTime= datetime.now()
#     # today = get_etl_date(spark)
#     # print("sparkstarted")
#     # oracle_properties=get_oracle_properties()
#     # # print(oracle_properties)
    
#     # today_str = today.strftime("%Y%m%d")
#     # # print(today)
  
#     # yesterday = today - timedelta(days=1)
#     # daybfyes = today -timedelta(days=3)

#     # spark_date = today.strftime("%Y-%m-%d")
   
#     # print(spark_date)
#     # PROCESS_ID= getattr(constants,"CBS_PIPELINE_ID",1)
#     # process_run_id = sys.argv[1] if len(sys.argv) > 1 else get_process_run_id(spark,PROCESS_ID,spark_date)
#     # run_id = get_run_id(spark,spark_date,PROCESS_ID) 
#     # logger = setup_logger("CBS",run_id,spark_date)
#     log_etl(spark, process_run_id, "cbs_started",1,"cbs pipeline started.")
#     logger.info(f"CBS Balance job strated | run_id={run_id}")

#     logger.info(f"ETL date resolved as: {today}")
#     logger.info(f"Oracle properties fetched successfully")


#     # hdfs_paths = paths_for_read("bancs24",today,today_str)

#     # HDFS_BASE = hdfs_paths["HDFS_BASE"]
#     # HDFS_PATH = hdfs_paths["HDFS_PATH"]
#     # CBS_BALANCE_DATALAKE_PATH = hdfs_paths["CBS_BALANCE_DATALAKE_PATH"]    
#     # manifest_filename = hdfs_paths["manifest_filename"]
#     # spark_stream_path = hdfs_paths["spark_stream_path"] + manifest_filename
 
#     # oracle_url = oracle_properties["url"]
#     # oracle_user = oracle_properties["user"]
#     # oracle_password = oracle_properties["password"]
#     # oracle_driver = oracle_properties["driver"]
    
#     # oracle_tables = tables_to_read("bancs24")


#     # spark.conf.set("spark.hadoop.fs.defaultFS", HDFS_BASE)

#     # posting_date_str = today.strftime("%Y-%m-%d")
#     # print("BALANCE DATE",posting_date_str)
#     logger.info(f"Balance date for processing: {posting_date_str}")
#     logger.info(f"Starting parallel file processing: invtogl, bortogl, glccgen")
#     # ---------- PARALLEL FILE PROCESSING ----------
#     try:
#         with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
#             f1 = executor.submit(invtogl, spark)
#             f2 = executor.submit(bortogl, spark)
#             f3 = executor.submit(glccgen, spark)
#             f4 = executor.submit(glcccta,spark)
#             f5 = executor.submit(glccinv,spark)

#             df_invtogl = f1.result()
#             df_bortogl = f2.result()
#             df_glccgen = f3.result()
#             df_glcccta = f4.result()
#             df_glccinv = f5.result()

            
#         log_etl(spark, process_run_id, "merging_all_filedata",1,"Merging all file data process started.")
        
#         final_df = df_invtogl.unionByName(df_bortogl).unionByName(df_glccgen).unionByName(df_glcccta).unionByName(df_glccinv)
#         # logger.info(f"Final union record count: {final_df.count()}")
     
#         final_result_df = final_df.groupBy("Id").agg(F.sum("Amount").alias("Amount"),F.count(F.col("Id")).alias("TRANSACTION_COUNT")).select("Id", "Amount","TRANSACTION_COUNT")
#         # logger.info(f"Final record count after grp by: {final_result_df.count()}")
#         logger.info(f"Final aggregation completed successfully")

#     except Exception as e:
#         logger.info(f"Error during parallel load", exc_info=True)
#         log_etl(spark, process_run_id, "merging_all_filedata",3,"Error in merging all file data.")
#         raise
   

#     # ---------- COMPUTE TODAY'S BALANCE ----------
#     try:
#         final_aggregated_df = (
#             final_result_df.select(
#                 col("Id").alias("GLCC"),
#                 col("TRANSACTION_COUNT").alias("TRANSACTION_COUNT"),
#                 col("Amount").cast(DecimalType(25, 4)).alias("closing_balance"),
#             )
#             .withColumn("BALANCE_DATE", to_date(lit(posting_date_str)))
#         )
     
#         # ------------------ WRITE TO DELTA ------------------
#         # final_aggregated_df.cache()
#         try:
#             final_aggregated_df.write \
#             .format("delta") \
#             .mode("append") \
#             .option("mergeSchema", "true") \
#             .save(CBS_BALANCE_DATALAKE_PATH)
#             logger.info(f"Data written to Delta Lake at path: {CBS_BALANCE_DATALAKE_PATH}")
#             # logger.info(f"INVTOGL record count: {df_invtogl.count()}")
#             log_etl(spark, process_run_id, "invtogl_filereading_started",2,"INVTOGL File Reading COMPLETED")
#             # logger.info(f"BORTOGL record count: {df_bortogl.count()}")
#             log_etl(spark, process_run_id, "bortogl_filereading_started",2,"BORTOGL File Reading COMPLETED")
#             # logger.info(f"GLCCGEN record count: {df_glccgen.count()}")
#             log_etl(spark, process_run_id, "glccgen_filereading_started",2,"GLCCGEN File Reading COMPLETED")
#             #logger.info(f"GLCCCTA record count: {df_glcccta.count()}")
#             log_etl(spark, process_run_id, "glcccta_filereading_started",2,"GLCCCTA File Reading COMPLETED")
#             #logger.info(f"GLCCCTA record count: {df_glccinv.count()}")
#             log_etl(spark, process_run_id, "glccinv_filereading_started",2,"GLCCINV File Reading COMPLETED")
#             log_etl(spark, process_run_id, "merging_all_filedata",2,"Merging all data completed.")

#             log_etl(spark, process_run_id, "cbs_balance_deltalake",2,"CBS balance sucessfully written to delta lake.",0)
#         except Exception as e:
#             logger.error(f"Error {e} while writing into delta lake path ->[ {CBS_BALANCE_DATALAKE_PATH} ]")       
#             log_etl(spark, process_run_id, "merging_all_filedata",3,"Error in writing cbs_balance to delta lake")
#             log_etl(spark, process_run_id, "cbs_started",3,"Error in Cbs pipeline.")
#             spark.stop()
#             exit()        
#     except Exception as e:
#         logger.error(f"Error {e} while calculating today's balance")       
#         log_etl(spark, process_run_id, "merging_all_filedata",3,"Error in calculating today's balance")
#         log_etl(spark, process_run_id, "cbs_started",3,"Error in Cbs pipeline.")
#         spark.stop()

#     # ---------- CREATE AND UPLOAD MANIFEST FILE ----------
       
#         # logger.info(f"Creating manifest file for Spark Stream trigger...")
#         # log_etl(spark, process_run_id, "cbs_balance_manifest",1,"manifest file creation state started.")
#         # today_str = datetime.now().strftime("%Y%m%d")
                
#     #     java_import(spark._jvm, "org.apache.hadoop.fs.FileSystem")
#     #     java_import(spark._jvm, "org.apache.hadoop.fs.Path")

#     #     try:
#     #         with open(manifest_filename, "w") as f:
#     #             f.write("cbs_balance successful")
                
#     #     except Exception as e:
#     #         logger.error(f"Error {e} while writing manifest file -> [{manifest_filename}]")
#     #         log_etl(spark, process_run_id, "cbs_balance_manifest",3," Error in writing cbs manifest file.")
#     #         spark.stop()
#     #         exit()           
        
#     #     fs = spark._jvm.FileSystem.get(spark._jsc.hadoopConfiguration())
        
#     #     fs.copyFromLocalFile(False, True, spark._jvm.Path(manifest_filename), spark._jvm.Path(spark_stream_path))
#     #     log_etl(spark, process_run_id, "cbs_balance_manifest",2,"manifest file uploaded successfully.")
#     #     logger.info(f"Manifest file uploaded to spark-stream path: {spark_stream_path}")
#     # except Exception as e:
#     #     logger.info(f"Error during Delta or manifest stage", exc_info=True)
#     #     log_etl(spark, process_run_id, "cbs_balance_manifest",3," Error in writing cbs manifest file.")
#     #     spark.stop()



#     # 1. Define root path
#     BASE_MANIFEST_PATH = "/ManifestFolder"
#     try:
#         # 2. Get ETL date for the ETL folder path
#         etl_date_path = today.strftime("%Y%m%d") 
#         hdfs_dir = f"{BASE_MANIFEST_PATH}/{etl_date_path}"
        
#         # Initialize Hadoop FileSystem
#         java_import(spark._jvm, "org.apache.hadoop.fs.FileSystem")
#         java_import(spark._jvm, "org.apache.hadoop.fs.Path")
#         fs = spark._jvm.FileSystem.get(spark._jsc.hadoopConfiguration())

#         # 3. Define manifest path
#         manifest_filename = f"CBS_balance_successful_{run_id}_Manifest.txt"
#         dest_file_path = spark._jvm.Path(f"{hdfs_dir}/{manifest_filename}")

#         log_etl(spark, process_run_id, "cbs_balance_mainfest_writing", 1, "Starting manifest stage.")

#         # 4. Ensure HDFS directory exists
#         hdfs_dir_obj = spark._jvm.Path(hdfs_dir)
#         if not fs.exists(hdfs_dir_obj):
#             fs.mkdirs(hdfs_dir_obj)

#         # 5. Write content directly to HDFS
#         # Using FSDataOutputStream via Py4J
#         out_stream = fs.create(dest_file_path, True)
#         out_stream.write(bytearray("CBS_balance successful", "utf-8"))
#         out_stream.close()

#         logger.info(f"Manifest written to HDFS: {dest_file_path}")
#         log_etl(spark, process_run_id, "cbs_balance_mainfest_writing", 2, "Manifest uploaded successfully.")

#     except Exception as e:
#         logger.error(f"Error in manifest creation: {e}")
#         log_etl(spark, process_run_id, "cbs_balance_mainfest_writing", 3, "Error in creating manifest file")
#         # spark.stop()
#         # exit(1)



#     # ---------- INSERT INTO ORACLE ----------
    
#     df_mapped = (
#         final_aggregated_df
#         .withColumn("BRANCH_CODE", substring(col("GLCC"), 1, 5))
#         .withColumn("CURRENCY", substring(col("GLCC"), 6, 3))
#         .withColumn("CGL", substring(col("GLCC"), 9, 10))
#     )

#     processed_df = df_mapped.select(
#         "CGL", "BALANCE_DATE","TRANSACTION_COUNT",
#         col("closing_balance").alias("BALANCE"),
#         "CURRENCY", "BRANCH_CODE"
#     )
#     # logger.info(f"Final record count before writing to datalake : {processed_df.count()}")

#     try:
#         processed_df.write \
#         .format("jdbc") \
#         .option("url", oracle_url) \
#         .option("dbtable", "CBS_BALANCE") \
#         .option("user", oracle_user) \
#         .option("password", oracle_password) \
#         .option("driver", oracle_driver) \
#         .mode("append") \
#         .save()
#         logger.info(f"Data Successfully wrote to Oracle DB")
#         log_etl(spark, process_run_id, "cbs_balance_oracle",2, "CBS balance sucessfully written to oracle.",0)
        
#     except Exception as e:
#         logger.info(f"Error while writing into JDBC {e}")
#         log_etl(spark, process_run_id, "cbs_started",3,"Error in writing cbs balance in oracle")
#         log_etl(spark, process_run_id, "cbs_started",3,"Error in Cbs pipeline.")
#         spark.stop()
#     log_etl(spark, process_run_id, "cbs_started",2,"Cbs pipeline completed successfully.")
#     spark.stop()



# if __name__=="__main__":
#     main(sys.argv)