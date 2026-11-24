import datetime 
from pyspark.sql import SparkSession , functions as F
import pyspark.sql.types as T
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import  to_date,expr, round, lower, col,when, length, count, substring,trim, concat, lit, create_map, to_timestamp, sum,broadcast, coalesce,current_timestamp, first
from pyspark.sql.types import DecimalType, StringType, StructType, StructField, LongType, DateType, TimestampType
from datetime import date, timedelta
from pyspark.sql.functions import desc
import sys
from py4j.java_gateway import java_import
from delta.tables import DeltaTable 
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
from common.fileUtil import tables_to_read
from common.processRun import get_run_id
from common.pattern import read_patterns
from common.createSpark import create_spark_session
from common.fileUtil import get_hdfs_base
from common.check_monthend import check_MonthEnd
from common.check_monthend import check_MonthEnd_minus1
from common.check_monthend import check_Previous_MonthEnd
from common.batch_journal_id import get_batch_id
from common.batch_journal_id import get_journal_id
from common.constants import Process
from common.Precheck import run_currency_precheck
from datetime import datetime,date
from common.log_etl_modified import log_etl
from common.PPF_FCNB import ppf_postings
from common.PPF_FCNB import monthend_posting,trickel_feed_func
from common.read_write_oracle import read_oracle
from common.read_write_oracle import write_oracle
from common.provision import process_provision_data
from common.fileUtil import get_delta_path_by_file_type
from common.processRun import get_process_run_id
from common.sql_deletion import delete_table_data_by_query
from common.fileUtil import file_processing_paths

def main(args):
    HDFS_BASE = get_hdfs_base()
    spark = create_spark_session("GLIF_PIPELINE", HDFS_BASE)
    
    today = get_etl_date(spark)
    spark_etl_date =  today
    todayYearEnd =  today
    yesterday = today - timedelta(days=1)
    posting_date_str = today.strftime("%Y-%m-%d")
    today_str = posting_date_str
    yesterday=yesterday.strftime("%Y-%m-%d")
    today= today.strftime("%Y-%m-%d")
    PROCESS_ID= Process.GLIF_PIPELINE
    RUN_ID = get_run_id(spark,today,PROCESS_ID) 
    logger = setup_logger("GLIF", RUN_ID , posting_date_str)
    today1 = date.today()   
    logger.info(f"=== Using current date for POST_DATE: {posting_date_str} ===")
    logger.info(f"RUN_ID as : {RUN_ID}")

    process_run_id = sys.argv[1] if len(sys.argv) > 1 else get_process_run_id(spark,PROCESS_ID,today)
    # process_run_id= argv[1]
    # process_run_id=202
    log_etl(spark, process_run_id, "glif_started",1)

    fncb_cgl_query="(SELECT FROM_CGL,TO_CGL,CGL_TYPE FROM FCNB_CGL) T1" 

    fcnb_cgl_df = read_oracle(spark, fncb_cgl_query)

    fcnb_currency_query="(SELECT CURRENCY_CODE,DEPOSIT_EXPENSE_FLAG,LOAN_INCOME_FLAG FROM FCNB_CURRENCY_CONFIG) T1"

    fcnb_currency = read_oracle(spark,fcnb_currency_query)



    BATCH_ID_LITERAL = None
    try:
        BATCH_ID_LITERAL = get_batch_id(spark)
    except Exception as e:
        print(f"Error fetching BATCH_SEQ via JDBC in gl : {e}")
        spark.stop()
        sys.exit(1)


    sc = spark.sparkContext
    # 1. Setup Hadoop FileSystem Access
    jvm = sc._jvm
    jsc = sc._jsc
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jsc.hadoopConfiguration())
    Path = jvm.org.apache.hadoop.fs.Path


    hdfs_paths = paths_for_read("glif",today,today_str)

    HDFS_BASE = hdfs_paths["HDFS_BASE"]    
    GL_DATALAKE_PATH = hdfs_paths["GL_DATALAKE_PATH"]
    manifest_filename = hdfs_paths["manifest_filename"]    
    BASE_MANIFEST_PATH = hdfs_paths["MANIFEST_BASE_PATH"]
    
    
    paths = file_processing_paths(spark,"glif_processing")    
    GLIF_TXNS_DATALAKE =  paths["GLIF_TXNS_DATALAKE"]
    GLIF_TXNS_DATALAKE = f"{GLIF_TXNS_DATALAKE}/GLIF_TXNS"



    oracle_tables = tables_to_read("glif")

    TRANSACTIONS = oracle_tables["TRANSACTIONS"]
    BALANCE = oracle_tables["BALANCE"]
    INVALID = oracle_tables["INVALID"]
    CGLS = oracle_tables["CGLS"]
    CURRENCY = oracle_tables["CURRENCY"]
    BRANCH = oracle_tables["BRANCH"]

    logger.info(f"ETL date : {today}")
    logger.info(f"Oracle properties fetched successfully")

    # ===================================================================================
    #   MODIFIED: READING PRE-PARSED GLIF DATA FROM DELTA LAKE (DYNAMIC PARSING OUTPUT)
    # ===================================================================================
    
    
    if(RUN_ID>1):
        log_etl(spark, process_run_id, "GLIF_deletion_oracle", 1, "In GLIF deletion started")
        try:
            sql_query=f"""
            Delete from {BALANCE} where BALANCE_DATE = to_date('{posting_date_str}','yyyy-mm-dd')
            """
            delete_table_data_by_query(spark,sql_query)
            log_etl(spark, process_run_id, "GLIF_deletion_oracle", 2, "In GLIF deletion success")
        except Exception as e:
            logger.error(f"error {e} while deleting from GLIF table")
            log_etl(spark, process_run_id, "GLIF_deletion_oracle", 3, "In GLIF deletion failed")
            spark.stop()
            sys.exit(1)
        try:
            sql_query=f"""
            DELETE FROM {TRANSACTIONS}
            WHERE TRANSACTION_DATE = TO_DATE('{posting_date_str}', 'yyyy-mm-dd')
              AND SOURCE_FLAG IN ('C', 'F', 'P', 'T', 'V')
            """
            delete_table_data_by_query(spark,sql_query)
            log_etl(spark, process_run_id, "GLIF_deletion_oracle", 2, "In GLIF deletion success")
        except Exception as e:
            logger.error(f"error {e} while deleting from GLIF table")
            log_etl(spark, process_run_id, "GLIF_deletion_oracle", 3, "In GLIF deletion failed")
            spark.stop()
            sys.exit(1)
        try:
            sql_query=f"""
            DELETE FROM {INVALID}
            WHERE TRANSACTION_DATE = TO_DATE('{posting_date_str}', 'yyyy-mm-dd')                 
            """
            delete_table_data_by_query(spark,sql_query)
            
            logger.info(f"Deleted from GLIF invalid table successfully")
            log_etl(spark, process_run_id, "GLIF_deletion_oracle", 2, "In GLIF deletion success")
        except Exception as e:
            logger.error(f"error {e} while deleting from GLIF {INVALID} table")
            log_etl(spark, process_run_id, "GLIF_deletion_oracle", 3, "In GLIF deletion failed")
            spark.stop()
            sys.exit(1)
        try:
            log_etl(spark, process_run_id, "GLIF_deletion_datalake", 1, "In GLIF deletion datalake started")
            delta_table = DeltaTable.forPath(spark, GL_DATALAKE_PATH)
            delta_table.delete(f"BALANCE_DATE = '{posting_date_str}'")
            print(f"Deleting Datalake for path {GL_DATALAKE_PATH} for Run -{RUN_ID} . ")
            log_etl(spark, process_run_id, "GLIF_deletion_datalake", 2, "GLIF deletion datalake completed")
        except Exception as e:
            logger.info(f"Error during saving in datalake  {e}")
            log_etl(spark, process_run_id, "GLIF_deletion_datalake", 3, "Error in GLIF deletion")
            log_etl(spark,process_run_id , "file_reading_Started",3,"Error in deleting GLIF to delta lake ")
            log_etl(spark, process_run_id, "glif_started",3,"Error in Glif ETL process")
            spark.stop()
            sys.exit(1)
    try:
        log_etl(spark,process_run_id ,"file_reading_started",1,"File Reading Started (Delta)")
        df_raw = spark.read.format("delta").load(GLIF_TXNS_DATALAKE)
        print(f"Reading from {GLIF_TXNS_DATALAKE}")
        #print(f"Reading from  BULK GLIF DELTA PATH with pri prb  {df_raw.count()}")

        provision_file_type_query = f"(select FILE_PATTERN from file_type_master where type = 'glif' and PRIORITY ='MONTH_END') e"
        provision_file_type = read_oracle(spark,provision_file_type_query)
        provision_file_type.show(20,False)
        provision_file_type_list = provision_file_type.select("FILE_PATTERN").rdd.flatMap(lambda x: x).collect()

        regex = "|".join(re.escape(p).replace(r"\*", ".*")
                   for p in provision_file_type_list
                    )

        df_raw = df_raw.filter(~F.col("file_name").rlike(f"^({regex})"))
        
        
        
        # df_raw = df_raw.filter(
        # (~lower(col("file_name")).startswith("glifpri")) &
        # (~lower(col("file_name")).startswith("glifprb")))
        #print(f"Reading from BULK GLIF DELTA PATH wtihout pri prb {df_raw.count()}")

    except Exception as e:
        logger.info(f"Error reading files... {e}")
        log_etl(spark,process_run_id ,"file_reading_started", 3 , "ERROR READING FILES : 201 : 411")
        log_etl(spark, process_run_id, "glif_started",3,"Error in Glif ETL process")
        spark.stop()
        sys.exit(1)


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
        log_etl(spark,process_run_id ,"file_reading_started", 3 , "ERROR in grpby   : 202")
        log_etl(spark, process_run_id, "glif_started",3,"Error in Glif ETL process")
        spark.stop()
        sys.exit(1) 

    df_mapped = df_agg.withColumns({ 
        "BATCH_ID": lit(BATCH_ID_LITERAL).cast(StringType()),
        "JOURNAL_ID": lit(None).cast(StringType()),
        "BRANCH_CODE": substring(col("GLCC"), 1, 5),
        "CURRENCY": substring(col("GLCC"), 6, 3),
        "CGL": substring(col("GLCC"), 9, 10),
        "NARRATION": lit("CBS txns").cast(StringType()),
        "SOURCE_FLAG": lit("C").cast(StringType())
    })

    df_final_schema = df_mapped.select(
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
    logger.info(f"Raw Data Got Filtered")

  
    if(check_MonthEnd()):
        provision_df = process_provision_data(spark,RUN_ID,BATCH_ID_LITERAL)
        df_final_schema = df_final_schema.unionByName(provision_df)
        
    df_final_schema.cache()
 

    # ===================================================================================
                    #    || BRANCH CODE ||
    # ===================================================================================

    try:
        branch_query = f"""
        (
            SELECT code FROM {BRANCH} 
        ) T1
        """
        branch_list = read_oracle(spark,branch_query)
        logger.info(f"Branch list loaded successfully")
    except Exception as e:
        logger.error(f"Error {e} while fetching the branchlist from oracle.")
        log_etl(spark,process_run_id,"file_reading_started", 3 , "ERROR in datafetching branchmaster  : 205 : 513")
        log_etl(spark, process_run_id, "glif_started",3,"Error in Glif ETL process")
        spark.stop()
        sys.exit(1)
    try:    
        df_validated_branch = df_final_schema.join(
            broadcast(branch_list.select(col("CODE").alias("BRANCH_CODE")).withColumn("IS_VALID_BRANCH", lit(1))),
            on="BRANCH_CODE",
            how="left"
        )
    except Exception as e:
        logger.error(f"Error {e} while joining with branch code")
        log_etl(spark,process_run_id,"file_reading_started", 3 , "ERROR in branchcode join")
        log_etl(spark, process_run_id, "glif_started",3,"Error in Glif ETL process")
        spark.stop()
        sys.exit(1)

    df_invalid_branches = df_validated_branch.filter(col("IS_VALID_BRANCH").isNull()).withColumn("REASON",lit("INVALID BRANCH")).select(
    "BRANCH_CODE",
    "BATCH_ID",
    "CURRENCY",
    "CGL",
    "NARRATION",
    "CREDIT_AMOUNT",
    "DEBIT_AMOUNT",
    "TRANSACTION_COUNT",
    "SOURCE_FLAG",
    "REASON"    
    )

    df_valid_branches = df_validated_branch.filter(col("IS_VALID_BRANCH").isNotNull())

    # # ===================================================================================
    #                 #    || CURRENCY ||
    # # ===================================================================================
    try:
        currency_query = f"""
        (
            SELECT CURRENCY_CODE,CURRENCY_RATE from {CURRENCY}
        ) T1
        """
        currency_list = read_oracle(spark,currency_query)
        logger.info(f"Sucessfully currencylist loaded.")
    except Exception as e:
        logger.error(f"Error {e} while fetching the currencylist from oracle.")
        log_etl(spark,process_run_id,"file_reading_started", 3 , "Error  while fetching the currencylist from oracle")
        log_etl(spark, process_run_id, "glif_started",3,"Error in Glif ETL process")
        spark.stop()
        sys.exit(1)


    df_validated_currency = df_valid_branches.join(
        broadcast(currency_list.select(col("CURRENCY_CODE").alias("CURRENCY")).withColumn("IS_VALID_CURRENCY", lit(1))),
        on="CURRENCY",
        how="left"
    ).withColumn(
        "VALIDATED_CURRENCY",
        when(col("IS_VALID_CURRENCY").isNotNull(), col("CURRENCY"))
        .otherwise(lit("invalid_CURRENCY"))
    )


    df_invalid_currency = df_validated_currency.where(col("VALIDATED_CURRENCY") == "invalid_CURRENCY").withColumn("REASON",lit("INVALID CURRENCY")).select(
    "BRANCH_CODE",
    "BATCH_ID",
    "CURRENCY",
    "CGL",
    "NARRATION",
    "CREDIT_AMOUNT",
    "DEBIT_AMOUNT",
    "TRANSACTION_COUNT",
    "SOURCE_FLAG",
    "REASON"    
    )

    df_valid_currency = df_validated_currency.where(col("VALIDATED_CURRENCY") != "invalid_CURRENCY")

    df_valid_currency =  df_valid_currency.drop(col("VALIDATED_CURRENCY"),col("IS_VALID_CURRENCY"))


    invalid_df = df_invalid_branches.unionByName(df_invalid_currency).withColumn("TRANSACTION_DATE",F.to_date(F.lit(posting_date_str), "yyyy-MM-dd"))

    # # ===================================================================================
    #                 #    || CGL ||
    # # ===================================================================================
    try:
        cgl_query = f"""
        (
        SELECT CGL_NUMBER FROM {CGLS} where STATUS ='1'
        ) T1
        """
            # SELECT CGL_NUMBER FROM {CGLS} 
        master_cgl_list = read_oracle(spark,cgl_query)
        logger.info(f"Sucessfully cgl list loaded")
    except Exception as e:
        logger.error(f"Error {e} while loading cgl list")  
        log_etl(spark,process_run_id,"file_reading_started", 3 , "Error  while loading cgl list")
        log_etl(spark, process_run_id, "glif_started",3,"Error in Glif ETL process")
        spark.stop()
        sys.exit(1)

    try: 
        df_validated = df_valid_currency.join(
            broadcast(master_cgl_list.select(col("CGL_NUMBER").alias("CGL")).withColumn("IS_VALID", lit(1))),
            on="CGL",
            how="left"
        ).withColumn(
            "VALIDATED_CGL",
            when(col("IS_VALID").isNotNull(), col("CGL"))
            .when(col("CGL").startswith("5"), lit("5000000000"))
            .otherwise(lit("1111111111"))
        ).withColumn(
            "NARRATION1",   
            when(col("IS_VALID").isNull(), 
                concat( col("NARRATION"),lit("- INVALID-"),col("CGL")))
            .otherwise(col("NARRATION"))
        )
        #print(f"before grp by bulk gl validated:  {df_validated.count()}")
    except Exception as e:
        logger.error(f"Error {e} while joining cgl")
        log_etl(spark,process_run_id,"file_reading_started", 3 , "Error in joining cgl")
        log_etl(spark, process_run_id, "glif_started",3,"Error in Glif ETL process")
        spark.stop()
        sys.exit(1)

    try:
        result = df_validated.groupBy("VALIDATED_CGL", "CURRENCY", "BRANCH_CODE","SOURCE_FLAG").agg(
            sum("CREDIT_AMOUNT").alias("CREDIT_AMOUNT"),
            sum("DEBIT_AMOUNT").alias("DEBIT_AMOUNT"),
            sum("TRANSACTION_COUNT").alias("TRANSACTION_COUNT"),
            first("NARRATION1").alias("NARRATION"),    
            first("BATCH_ID").alias("BATCH_ID"),
            first("JOURNAL_ID").alias("JOURNAL_ID"),
        ).select(col("VALIDATED_CGL").alias("CGL"), "CURRENCY", "BRANCH_CODE","NARRATION", "SOURCE_FLAG", "BATCH_ID", "CREDIT_AMOUNT", "DEBIT_AMOUNT", "TRANSACTION_COUNT","JOURNAL_ID")
        #print(f"After grp by bulk gl validated:  {result.count()}")
    except Exception as e:
        logger.error(f"Error {e} while grouping final validated dataframe")
        log_etl(spark,process_run_id,"file_reading_started", 3 , "Error in grping final validated dataframe.")
        #log_etl(spark,"GLIF",process_run_id , "ERROR in grpby  : 209: 658","error_in_grpby",3,startTime)
        log_etl(spark, process_run_id, "glif_started",3,"Error in Glif ETL process")
        spark.stop()
        sys.exit(1)
        

    # ===================================================================================
    #    || Balancing and PPF Logic ||
    # ===================================================================================

    result_net1 = result.withColumn(
        "check",
        when(substring(col("CGL"), 1, 1) == "5", lit("5000000000"))
        .otherwise(lit("1111111111"))
    )

    try:
        #print(f"before  grping memo and non memo:  {result_net1.count()}")
        new_result =result_net1.groupBy("BRANCH_CODE","CURRENCY",col("check").alias("CGL"),"SOURCE_FLAG").agg(
            sum("CREDIT_AMOUNT").alias("CREDIT_AMOUNT"),
            sum("DEBIT_AMOUNT").alias("DEBIT_AMOUNT"),
            first("NARRATION").alias("NARRATION"),     
            first("BATCH_ID").alias("BATCH_ID"),
            first("JOURNAL_ID").alias("JOURNAL_ID"),
            sum("TRANSACTION_COUNT").alias("TRANSACTION_COUNT")
            ).withColumn(
            "NET", 
            col("CREDIT_AMOUNT") + col("DEBIT_AMOUNT")
        )
        #print(f"After  grping memo and non memo:  {new_result.count()}")
    except Exception as e:
        logger.error(f"Error {e} while grouping Balancing and PPF Logic")
        log_etl(spark,process_run_id,"file_reading_started", 3 , "Error grpby of Balancing and PPF Logic")
        # log_etl(spark,"GLIF", process_run_id , "ERROR in grpby  :","error_in_grpby",3,startTime)
        log_etl(spark, process_run_id, "glif_started",3,"Error in Glif ETL process")
        spark.stop()
        sys.exit(1)


    synthetic = new_result.filter(col("NET") != 0).select(
        col("CGL"),col("CURRENCY"),col("BRANCH_CODE"),
        when(col("NET") < 0, -col("NET")).otherwise(lit(0)).alias("CREDIT_AMOUNT"),
        when(col("NET") > 0, -col("NET")).otherwise(lit(0)).alias("DEBIT_AMOUNT"),
        col("TRANSACTION_COUNT"),
        lit("OUT OF BAL").alias("NARRATION"),
        col("SOURCE_FLAG"),
        col("BATCH_ID"),
        col("JOURNAL_ID"))

    try:
        final_balanced = result.unionByName(synthetic)
    except Exception as e:
        logger.error(f"Error {e} in finding the synthetic dataframe")
        log_etl(spark,process_run_id,"file_reading_started", 3 , "Error in finding the synthetic df")
        #log_etl(spark,"GLIF",process_run_id ,"ERROR in union :","error_in_union",3,startTime)
        log_etl(spark, process_run_id, "glif_started",3,"Error in Glif ETL process")
        spark.stop()
        sys.exit(1)
      
    try:
        journal_id = get_journal_id(spark)
    except Exception as e:
        print(f"Error fetching JOURNAL_SEQ via JDBC in gl : {e}")
        spark.stop()
        sys.exit(1)
    #print(f"final balance count : {final_balanced.count()}")
    PPF_Posting = ppf_postings(final_balanced).withColumn("JOURNAL_ID", lit(journal_id))

    PPF_Posting_df = final_balanced.unionByName(PPF_Posting)

    PPF_Posting = PPF_Posting_df.withColumn("TRANSACTION_DATE",F.to_date(F.lit(posting_date_str), "yyyy-MM-dd"))
    
    
    # ===============================================================================
    # Journal entries data from the database
    # ===============================================================================
    #log_etl(spark,process_run_id , "balance_calculated",1,"balance Calculation started")
    filter_date_str = posting_date_str


    if(RUN_ID>0):
        sql_query_J = f"""(
                            SELECT BRANCH_CODE, CURRENCY, CGL,CREDIT_AMOUNT,DEBIT_AMOUNT,BATCH_ID,JOURNAL_ID,NARRATION,TRANSACTION_COUNT,SOURCE_FLAG 
                            FROM {TRANSACTIONS} 
                            WHERE TRANSACTION_DATE = TO_DATE('{filter_date_str}', 'YYYY-MM-DD') and SOURCE_FLAG in ('J')
                        ) T1"""
        sql_query_J_R = f"""(
                            SELECT BRANCH_CODE, CURRENCY, CGL,CREDIT_AMOUNT,DEBIT_AMOUNT,BATCH_ID,JOURNAL_ID,NARRATION,TRANSACTION_COUNT,SOURCE_FLAG 
                            FROM {TRANSACTIONS} 
                            WHERE TRANSACTION_DATE = TO_DATE('{filter_date_str}', 'YYYY-MM-DD') and SOURCE_FLAG in ('J','R','S')
                        ) T1"""
        try:
            if(RUN_ID ==1):
                print(f"Reading {TRANSACTIONS} for SOURCE_FLAG 'J'.")
                journal_entries = read_oracle(spark,sql_query_J)
            if(RUN_ID >1):
                print(f"Reading {TRANSACTIONS} for SOURCE_FLAG 'J', 'R', 'S' ")
                journal_entries = read_oracle(spark,sql_query_J_R)

            logger.info(f"Journal entries loaded from db for {filter_date_str} date.")
        except Exception as e:
            logger.error(f"Error {e} while loading Journal entries for {filter_date_str} date.")
            log_etl(spark,process_run_id,"file_reading_started", 3 , "Error in fetching journal transactions")
            log_etl(spark, process_run_id, "glif_started",3,"Error in Glif ETL process")
            spark.stop()
            sys.exit(1)
        #print(f"ppf posting before journal entries:  {PPF_Posting_df.count()}")
        PPF_Posting_df = PPF_Posting_df.unionByName(journal_entries).groupBy("CGL","BRANCH_CODE","CURRENCY","SOURCE_FLAG").agg(
            sum(col("CREDIT_AMOUNT")).alias("CREDIT_AMOUNT"),
            sum(col("DEBIT_AMOUNT")).alias("DEBIT_AMOUNT"),
            sum(col("TRANSACTION_COUNT")).alias("TRANSACTION_COUNT"),
            first("NARRATION").alias("NARRATION"),
            first("BATCH_ID").alias("BATCH_ID"),
            first("JOURNAL_ID").alias("JOURNAL_ID"),
            ).select("BRANCH_CODE", "CURRENCY", "CGL","CREDIT_AMOUNT","DEBIT_AMOUNT","BATCH_ID","JOURNAL_ID","NARRATION","TRANSACTION_COUNT","SOURCE_FLAG")
        #print(f"ppf posting after journal journal entries:  {PPF_Posting_df.count()}")
    processed_df = PPF_Posting_df.withColumn(
        "BALANCE", 
        col("CREDIT_AMOUNT") + col("DEBIT_AMOUNT")
    ).select("CGL", "BALANCE","CURRENCY","BRANCH_CODE")
    
    # ====================================================================================================================================================================================================================================================================

    # ====================================================================================================================================================================================================================================================================


    processed_df=processed_df.select(concat(
        col("BRANCH_CODE"),
        col("CURRENCY"),
        col("CGL")).alias("glcc"), 
        col("BALANCE"))
    #print(f"Today's balance:  {processed_df.count()}")
    is_year_start = (todayYearEnd.month == 4 and todayYearEnd.day == 1)
    if not is_year_start:
        if(RUN_ID == 1):
            try:
                print(f"Reading from {GL_DATALAKE_PATH}")
                df_gl = spark.read.format("delta").load(GL_DATALAKE_PATH).filter(col("BALANCE_DATE") == today_str)
                df_gl = df_gl.select(col("glcc"),col("closing_balance").cast(DecimalType(25,4)).alias("BALANCE"))
                logger.info(f"Data loaded from deltalake.")
                #print(f"Reading from todays GLIF DELTA PATH {df_gl.count()}")
            except Exception as e:
                logger.error(f"Error {e} while loading data for {today_str} from delta lake")
                log_etl(spark,process_run_id , "file_reading_started",3,"error in loading today data from delta lake")
                try:
                    BALANCE_query = f"""(select CGL, CURRENCY, BRANCH_CODE, BALANCE from GL_BALANCE where BALANCE_DATE = TO_DATE('{today_str}', 'YYYY-MM-DD')) a"""
                    gl_yesterday = read_oracle(spark,BALANCE_query)

                    print("yesterday gl")
                    print("yesterday gl")
                    df_gl= gl_yesterday.select(concat(col("BRANCH_CODE"),col("CURRENCY"),col("CGL")).alias("glcc"), col("BALANCE"))

                except Exception as e:
                
                    df_gl=spark.createDataFrame([],schema="""glcc STRING, BALANCE DECIMAL(25,4)""")
                    logger.error(f"Error {e} while loading data from delta lake.")
                    log_etl(spark,process_run_id , "file_reading_started",3,"error in loading today data in oracle")
                    #log_etl(spark,"GLIF",process_run_id , "Error while loading data from delta lake. : 213: 792","error_loading_delta",3,startTime)
                    log_etl(spark, process_run_id, "glif_started",3,"Error in Glif ETL process")
                    spark.stop()

        else:

            try:
                df_gl = spark.read.format("delta").load(GL_DATALAKE_PATH).filter(col("BALANCE_DATE") == yesterday)
                df_gl = df_gl.select(col("glcc"),col("closing_balance").cast(DecimalType(25,4)).alias("BALANCE"))
                #print(f"Reading from yesterday GLIF DELTA PATH {df_gl.count()}")
                logger.info(f"Data loaded from deltalake.")
            except Exception as e:
                logger.error(f"Error {e} while loading previous day {yesterday} data from delta lake")
                log_etl(spark,process_run_id , "file_reading_started",3,"error in loading previous day data from delta lake ")
                try:
                    BALANCE_query = f"""(select CGL, CURRENCY, BRANCH_CODE, BALANCE from GL_BALANCE where BALANCE_DATE = TO_DATE('{yesterday}', 'YYYY-MM-DD')) a"""
                    gl_yesterday = read_oracle(spark,BALANCE_query)

                    print("yesterday gl")
                    df_gl= gl_yesterday.select(concat(col("BRANCH_CODE"),col("CURRENCY"),col("CGL")).alias("glcc"), col("BALANCE"))

                except Exception as e:
                
                    df_gl=spark.createDataFrame([],schema="""glcc STRING, BALANCE DECIMAL(25,4)""")
                    logger.error(f"Error {e} while loading data from delta lake.")
                    log_etl(spark,process_run_id , "file_reading_started",3,"error in loading previous day data from oracle")
                    log_etl(spark, process_run_id, "glif_started",3,"Error in Glif ETL process")
                    spark.stop()
    else:
        BALANCE_query = f"""(select CGL, CURRENCY, BRANCH_CODE, BALANCE from GL_BALANCE_YEAR_END where BALANCE_DATE = TO_DATE('{posting_date_str}', 'YYYY-MM-DD')) a"""
        gl_yesterday = read_oracle(spark,BALANCE_query)
        print("yesterday gl")
        print("yesterday gl")
        df_gl= gl_yesterday.select(concat(col("BRANCH_CODE"),col("CURRENCY"),col("CGL")).alias("glcc"), col("BALANCE"))


    try:
        
        combined_df = processed_df.unionByName(df_gl)


        #print(f"combined dataframe after union todays and yesterday {combined_df.count()}")
    except Exception as e:
        logger.error(f"Error {e} in union")
        log_etl(spark,process_run_id , "file_reading_started",3,"error in doing union of current balance to closing balance ")
        log_etl(spark, process_run_id, "glif_started",3,"Error in Glif ETL process")
        spark.stop()
        sys.exit(1)
        
    final_aggregated_df = combined_df.groupBy("glcc").agg(sum("BALANCE").alias("BALANCE"))
    #print(f"combined dataframe todays and yesterday after union and grp by  {final_aggregated_df.count()}")
    final_aggregated_df=final_aggregated_df.withColumn("BALANCE_DATE",F.to_date(F.lit(posting_date_str), "yyyy-MM-dd"))

    dl_df=final_aggregated_df.select("glcc",col("BALANCE").alias("closing_balance").cast(DecimalType(25,4)),"BALANCE_DATE")


    # ======================================================================================================================================================
    #                                         Month End FCNB 
    # ======================================================================================================================================================

    
    monthend = check_MonthEnd() 


    logger.info(f"monthend is {monthend}")
    FCNB_transactions = spark.createDataFrame([], StructType([]))
    if(monthend):
        logger.info("Start FCNB Monthend")
        try:
            FCNB_journal_id = get_journal_id(spark)
        except Exception as e:
            print(f"Error {e} while fetching JOURNAL_SEQUENCE")
        FCNB_transactions = monthend_posting(dl_df,fcnb_cgl_df,fcnb_currency,currency_list,posting_date_str).withColumn("JOURNAL_ID", lit(FCNB_journal_id)).withColumn("BATCH_ID", lit(BATCH_ID_LITERAL))
        FCNB_result=FCNB_transactions.withColumn(
        "BALANCE", 
        col("CREDIT_AMOUNT") + col("DEBIT_AMOUNT")
        ).select(concat(col("BRANCH_CODE"),col("CURRENCY"),col("CGL")).alias("glcc"),col("BALANCE").alias("closing_balance")).withColumn("BALANCE_DATE",F.to_date(F.lit(posting_date_str), "yyyy-MM-dd"))
        dl_df = dl_df.unionAll(FCNB_result)
        
        
        

    # # # ==============================================================================================================
    dl_df =dl_df.groupBy("glcc").agg(
        sum("closing_balance").alias("BALANCE"),
        first("BALANCE_DATE").alias("BALANCE_DATE")
    ).select("glcc","BALANCE","BALANCE_DATE")


    df_gl_balance_final = dl_df.withColumns({ 
        "BRANCH_CODE": substring(col("glcc"), 1, 5),
        "CURRENCY": substring(col("glcc"), 6, 3),
        "CGL": substring(col("glcc"), 9, 10),
    }).select("BRANCH_CODE","CURRENCY","CGL","BALANCE_DATE","BALANCE")

    df_gl_balance_final.cache()

    

    df_converted = (
    df_gl_balance_final
    .join(
        broadcast(currency_list),
        df_gl_balance_final["CURRENCY"] == currency_list["CURRENCY_CODE"],
        "left"
    )
    .select(
        col("BRANCH_CODE"),
        col("CURRENCY"),
        col("CGL"),
        col("BALANCE_DATE"),
        col("BALANCE"),
        col("CURRENCY_RATE")  
    )
    .withColumn(
        "APPLIED_RATE",
        when(col("CURRENCY") == "INR", lit(1))
        .otherwise(coalesce(col("CURRENCY_RATE"), lit(1)))
    )
    .withColumn(
        "INR_BALANCE",
        round(col("BALANCE") * col("APPLIED_RATE"),2).cast(DecimalType(25, 2))
    )
    )
    balance_df = df_converted.select(
    "CGL",
    "CURRENCY",
    "BRANCH_CODE",
    "BALANCE",
    "INR_BALANCE",
    "BALANCE_DATE"
    )
    
    

    # ==================================================================================

    deltalake_final = balance_df.select(concat(col("BRANCH_CODE"),col("CURRENCY"),col("CGL")).alias("glcc"),col("BALANCE").cast(DecimalType(25,4)).alias("closing_balance"),col("INR_BALANCE").cast(DecimalType(25,4))).withColumn("BALANCE_DATE", F.to_date(F.lit(posting_date_str), "yyyy-MM-dd"))
    # print(f"final balance delta lake before writing to delta lake : {deltalake_final.count()}")
    # ==================================================================================
    # log_etl(spark,process_run_id , "balance_calculated",2,"Balance calculated successfully.")
    # #log_etl(spark,"GLIF",process_run_id , "Balance Calculated","balance_calculated",2,startTime) 
    # log_etl(spark,process_run_id , "closing_Balance_updated",1,"closing balance saving stage started ")
 

    try:
        # 2. Get today's date for the ETL folder path
        today_obj = date.today() 
        etl_date_path = get_etl_date(spark).strftime("%Y%m%d") 
        hdfs_dir = f"{BASE_MANIFEST_PATH}/{etl_date_path}"
        
        # Initialize Hadoop FileSystem
        java_import(spark._jvm, "org.apache.hadoop.fs.FileSystem")
        java_import(spark._jvm, "org.apache.hadoop.fs.Path")
        fs = spark._jvm.FileSystem.get(spark._jsc.hadoopConfiguration())

        # 3. Define manifest path
        manifest_filename = f"Gl_balance_successful_{RUN_ID}_Manifest.txt"
        dest_file_path = spark._jvm.Path(f"{hdfs_dir}/{manifest_filename}")

        log_etl(spark, process_run_id, "gl_balance_manifest_writing", 1, "Starting manifest stage.")

        # 4. Ensure HDFS directory exists
        hdfs_dir_obj = spark._jvm.Path(hdfs_dir)
        if not fs.exists(hdfs_dir_obj):
            fs.mkdirs(hdfs_dir_obj)

        # 5. Write content directly to HDFS
        # Using FSDataOutputStream via Py4J
        out_stream = fs.create(dest_file_path, True)
        out_stream.write(bytearray("GL_balance successful", "utf-8"))
        out_stream.close()

        logger.info(f"Manifest written to HDFS: {dest_file_path}")
        log_etl(spark, process_run_id, "gl_balance_manifest_writing", 2, "Manifest uploaded successfully.")

    except Exception as e:
        logger.error(f"Error in manifest creation: {e}")
        log_etl(spark, process_run_id, "gl_balance_manifest_writing", 3, f"Error in creating manifest file")


# ===============================================================================================================================================================
    try:
        # ADD HERE TRUNCATE
        if RUN_ID==1:
            
            try:
                sql_query=f"""
                DELETE FROM {BALANCE}
                WHERE BALANCE_DATE = TO_DATE('{posting_date_str}', 'YYYY-MM-DD')                
                """
                delete_table_data_by_query(spark,sql_query)
                                
            except Exception as e:
                print(f"Error {e} Occured while deleting {BALANCE} Table")
                spark.stop()
                sys.exit(1)


        write_oracle(balance_df,BALANCE)

        logger.info(f"Successfully data written into balance table")
        log_etl(spark,process_run_id , "closing_balance_updated_oracle",2,"Closing balance updated successfully in oracle",0)
    except Exception as e:
        logger.info(f"Error writing BALANCE: {e}")
        log_etl(spark,process_run_id , "file_reading_started",3,"Error saving gl_balance to oracle ")
        log_etl(spark, process_run_id, "glif_started",3,"Error in Glif ETL process")
        spark.stop()
        sys.exit(1)

    try:
        #print(f"final ppf transaction before writing to oracle : {PPF_Posting.count()}")      
        write_oracle(PPF_Posting,TRANSACTIONS)
        # log_etl(spark,process_run_id , "transaction_calculated",1,"Transactions Calculation started")
        logger.info(f"=== Data successfully written to transaction table Oracle DB ===") 
        log_etl(spark,process_run_id ,"transaction_posted",2, "Transactions posting posted successfuly",0)
    except Exception as e:
        logger.info(f"Error writing to Oracle DB: {e}")
        log_etl(spark,process_run_id , "file_reading_started",3,"Error in posting transactions ")
        log_etl(spark, process_run_id, "glif_started",3,"Error in Glif ETL process")
        #log_etl(spark,"GLIF",process_run_id , "Error writing Transactions to Oracle DB: 218: 947","Transactions_error",3,startTime)
        spark.stop()
        sys.exit(1)
    try:
        #print(f"final FCNB_transactions before writing to oracle : {FCNB_transactions.count()}") 
        write_oracle(FCNB_transactions,TRANSACTIONS)

        logger.info(f"=== Data successfully written to transaction table Oracle DB ===")
        log_etl(spark,process_run_id ,"transaction_posted_fncb",2, "FNCB Transactions posting posted successfuly",0)
    except Exception as e:
        logger.error(f"Error writing to Oracle DB: {e}") 
        log_etl(spark,process_run_id , "file_reading_started",3,"Error in posting fncb transactions ")
        log_etl(spark, process_run_id, "glif_started",3,"Error in Glif ETL process")
        spark.stop()
        sys.exit(1)

    try:
        #print(f"final invalid_df transactions before writing to oracle : {invalid_df.count()}")
        write_oracle(invalid_df,INVALID)
        logger.info(f"=== Invalid records written to DB ===")
    except Exception as e:
        logger.info(f"Error writing INVALID records: {e}")
        log_etl(spark,process_run_id , "file_reading_started",3,"Error in posting invalid items.")
        log_etl(spark, process_run_id, "glif_started",3,"Error in Glif ETL process")
        spark.stop()
        sys.exit(1)
    

    try:
        if RUN_ID == 1:
            deltalake_final.write.format("delta").mode("overwrite").option("replaceWhere", f"BALANCE_DATE = '{posting_date_str}'").save(GL_DATALAKE_PATH)
            log_etl(spark,process_run_id , "closing_balance_updated_datalake",2,"Closing balance updated successfully in delta lake" ,0 )

        else:
            deltalake_final.write.format("delta").mode("append").save(GL_DATALAKE_PATH)
            log_etl(spark,process_run_id , "closing_balance_updated_datalake",2,"Closing balance updated successfully in delta lake" ,0 )
    except Exception as e:
        logger.info(f"Error during saving in datalake  {e}")
        log_etl(spark,process_run_id , "file_reading_started",3,"Error saving gl_balance to delta lake ")
        log_etl(spark, process_run_id, "glif_started",3,"Error in Glif ETL process")
        spark.stop()
        sys.exit(1)


    log_etl(spark,process_run_id , "file_reading_started",2,"File reading completed.")
    log_etl(spark, process_run_id, "glif_started",2,"Glif ETL process Ended")

    try:
        if(monthend):
            try:
                trickel_feed_func(spark,HDFS_BASE,spark_etl_date)
            except Exception as e:
                print(f"Error {e} while executing trickel_feed.")
            

    except Exception as e:
        print(f"Error {e} occured while generating Tricklefeed Data")


    spark.stop()
    exit(0)

if __name__=="__main__":
    main(sys.argv)

    
    