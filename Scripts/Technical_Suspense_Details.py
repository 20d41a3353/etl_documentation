import sys
from common.createSpark import create_spark_session
from common.fileUtil import get_hdfs_base
import common.constants as constants
from common.dateUtil import get_etl_date
import datetime
from pyspark.sql.functions import col , sum as spark_sum
from datetime import datetime
from common.processRun import get_process_run_id
from common.constants import Process
from common.log_etl_modified import log_etl



CIBA_CGL= "1106505001"
TECHNICAL_SUSPENSE_CGL="1260505001"


BATCH_ID_COLUMN="BATCH_ID"
AMOUNT_COLUMN="AMOUNT"
TOTAL_AMOUNT="total_amount"
JOURNAL_AMOUNT="JOURNAL_AMOUNT"
JOURNAL_NUMBER="JOURNAL_NUMBER"
CURRENCY="CURRENCY"
JOURNAL_PATTERN="JOURNAL_PATTERN"
BRANCH_COLUMN="BRANCH_CODE"


CIBA="ciba_details_report/ciba_details_report"
SUSPENSE="technical_suspense_details_report/technical_suspense_details_report"
HDFS_BASE = get_hdfs_base()


CIBA_COLUMNS=["POST_DATE",JOURNAL_NUMBER,"TRANSACTION_CODE","BRCH","FCY_CODE","BRANCH_CODE",CURRENCY,"CGL","DESCRIPTION",AMOUNT_COLUMN,"FCY_AMT","ACCOUNT","SOURCE","LCY_AMT","FILE_NAME"]

def path_builder(etl_date,type):
    input_formatted_date = etl_date.strftime("%Y/%b/%d").upper()
    input_path= f"{HDFS_BASE}/druid-data-lake/{input_formatted_date}/GLIF/GLIF_TXNS/"    
    report_path_date=etl_date.strftime("%Y-%m-%d")
    report_date=etl_date.strftime("%d%m%Y")
    output_path=f"{HDFS_BASE}/reports/{report_path_date}/{type}_{report_date}.xlsx"
    return input_path,output_path
    

def generate_ciba_details_report(spark,etl_date,process_run_id):
    input_path,output_path=path_builder(etl_date,CIBA)
    
    print(f"Reading from path : {input_path}")
    
    try:
        log_etl(spark, process_run_id, "ciba_read_started",1,"Started Reading CIBA CGL Transactions.")
        journal_df = spark.read.format("delta").load(input_path).filter(col("CGL") == CIBA_CGL)
        log_etl(spark, process_run_id, "ciba_read_started",2,"Started Reading CIBA CGL Transactions.")
    except Exception as e:
        log_etl(spark, process_run_id, "ciba_read_started",3," Failed to read CIBA CGL Transactions.")
        print(f"Error {e} occured.") 
        spark.stop()
        sys.exit(1)
    

    try:
        log_etl(spark, process_run_id, "journal_extract",1,"Extracting Non-Zero journals.")
        unbalanced_journals = journal_df.groupBy(JOURNAL_NUMBER, CURRENCY) \
        .agg(spark_sum(AMOUNT_COLUMN).alias(JOURNAL_AMOUNT)) \
        .filter(col(JOURNAL_AMOUNT) != 0) \
        .select(JOURNAL_NUMBER) \
        .distinct()
        
        unbalanced_journals = journal_df .groupBy(JOURNAL_NUMBER, CURRENCY) \
                                    .agg(
                                        spark_sum(AMOUNT_COLUMN).alias(JOURNAL_AMOUNT)
                                    ) \
                                    .filter(col(JOURNAL_AMOUNT) != 0) \
                                    .withColumn(
                                        JOURNAL_PATTERN, 
                                        col(JOURNAL_NUMBER).substr(3, 6) 
                                    ) \
                                    .select(JOURNAL_NUMBER, JOURNAL_PATTERN, JOURNAL_AMOUNT) \
                                    .distinct()
        
        unbalanced_journal_patterns_df = unbalanced_journals.groupBy(JOURNAL_PATTERN) \
                                            .agg(spark_sum(JOURNAL_AMOUNT).alias(TOTAL_AMOUNT))\
                                            .filter(col(TOTAL_AMOUNT) !=0) \
                                            .select(JOURNAL_PATTERN) \
                                            .distinct()
        final_unbalanced_journals = unbalanced_journals.join(
                        unbalanced_journal_patterns_df,
                        on=JOURNAL_PATTERN,
                        how="left_semi"
                    ).select(JOURNAL_NUMBER)
        
        unbalanced_ids_list = [row.JOURNAL_NUMBER for row in final_unbalanced_journals.collect()]
        if unbalanced_ids_list:
            broadcast_ids = spark.sparkContext.broadcast(unbalanced_ids_list)
            final_result_df = spark.read.format("delta").load(input_path) \
                .filter(col(JOURNAL_NUMBER).isin(broadcast_ids.value)) \
                .select(CIBA_COLUMNS)
            log_etl(spark, process_run_id, "journal_extract",2,"Completed Extracting Non-Zero journals.")
        else:
            final_result_df = spark.createDataFrame([], schema=journal_df.schema)
            log_etl(spark, process_run_id, "journal_extract",2,"Completed Extracting Non-Zero journals. No Non-Zero Journals found.")    
    except Exception as e:
        log_etl(spark, process_run_id, "journal_extract",3,"Failed to Extract Non-Zero journals.")   
        print(f"Error {e} occured.") 
        spark.stop()
        sys.exit(1)
    print(f"Writing report to path :{output_path}")
    
    try:
        log_etl(spark, process_run_id, "ciba_report_generation",1,"Started Writing CIBA Detailed Report in HDFS.")
        final_result_df.write \
        .format("com.crealytics.spark.excel") \
        .option("dataAddress", "'CIBA'!A1") \
        .option("header", "true") \
        .option("maxRowsInMemory", 5000) \
        .mode("overwrite") \
        .save(output_path)    
        log_etl(spark, process_run_id, "ciba_report_generation",2,"Completed Writing CIBA Detailed Report in HDFS.")
    
    except Exception as e:
        log_etl(spark, process_run_id, "ciba_report_generation",3," Failed to write CIBA Detailed Report in HDFS.")
        print(f"Error {e} occured.") 
        spark.stop()
        sys.exit(1)
    


def generate_technical_suspense_report(spark, etl_date, process_run_id):

    input_path, hdfs_output_path = path_builder(etl_date, SUSPENSE)

    print(f"Reading from path: {input_path}")

    

    base_df = spark.read.format("delta").load(input_path).filter(col("CGL") == TECHNICAL_SUSPENSE_CGL)

    base_df.cache()
    report_date=etl_date.strftime("%d%m%Y")
    report_path_date=etl_date.strftime("%Y-%m-%d")
    

    batch_df = base_df.filter(col(BATCH_ID_COLUMN).isNotNull())

    journal_df = base_df.filter(col(BATCH_ID_COLUMN).isNull())

    

    

    

    unbalanced_batches = batch_df.groupBy(BATCH_ID_COLUMN, BRANCH_COLUMN, CURRENCY) \
        .agg(spark_sum(AMOUNT_COLUMN).alias(TOTAL_AMOUNT)) \
        .filter(col(TOTAL_AMOUNT) != 0) \
        .select(BATCH_ID_COLUMN, BRANCH_COLUMN) \
        .distinct()

    unbalanced_batches_ids_list = [getattr(row, BATCH_ID_COLUMN) for row in unbalanced_batches.collect()]

    if unbalanced_batches_ids_list:

        unbalanced_batches_df = spark.read.format("delta").load(input_path) \
                .filter(col(BATCH_ID_COLUMN).isin(unbalanced_batches_ids_list)) 

    else:

        unbalanced_batches_df = spark.createDataFrame([], schema=base_df.schema)

    

    

    

    unbalanced_journals = journal_df.groupBy(JOURNAL_NUMBER, BRANCH_COLUMN, CURRENCY) \
       .agg(spark_sum(AMOUNT_COLUMN).alias(TOTAL_AMOUNT)) \
       .filter(col(TOTAL_AMOUNT) != 0) \
       .select(JOURNAL_NUMBER, BRANCH_COLUMN) \
        .distinct()

    unbalanced_journal_ids_list = [getattr(row, JOURNAL_NUMBER) for row in unbalanced_journals.collect()]

    if unbalanced_journal_ids_list:

        unbalanced_journals_df = spark.read.format("delta").load(input_path) \
            .filter(col(JOURNAL_NUMBER).isin(unbalanced_journal_ids_list)) 

    else:

        unbalanced_journals_df = spark.createDataFrame([], schema=base_df.schema)

    
    

    batch_branches = {getattr(row, BRANCH_COLUMN) for row in unbalanced_batches.collect() if getattr(row, BRANCH_COLUMN) is not None}

    journal_branches = {getattr(row, BRANCH_COLUMN) for row in unbalanced_journals.collect() if getattr(row, BRANCH_COLUMN) is not None}

    unique_branches = batch_branches.union(journal_branches)

    print(f"Production Metric: Generating specialized multi-sheet reports for {len(unique_branches)} branches.")

    

    unbalanced_batches_df.cache()

    unbalanced_journals_df.cache()


    

    

    

    for branch_code in unique_branches:

        print(f"Compiling report file sheets for Branch: {branch_code}")

        

        branch_batch_df = unbalanced_batches_df.filter(col(BRANCH_COLUMN) == branch_code)

        branch_journal_df = unbalanced_journals_df.filter(col(BRANCH_COLUMN) == branch_code)

        

        local_file_name = f"{HDFS_BASE}/{report_path_date}/technical_suspense_report/{branch_code}/technical_suspense_report_{report_date}_{branch_code}.xlsx"



        

        branch_batch_df.write \
           .format("com.crealytics.spark.excel") \
           .option("dataAddress", "'BATCH'!A1") \
           .option("header", "true") \
           .option("maxRowsInMemory", 5000) \
           .mode("overwrite") \
           .save(local_file_name) 

        
        branch_journal_df.write \
           .format("com.crealytics.spark.excel") \
           .option("dataAddress", "'JOURNAL'!A1") \
           .option("header", "true") \
           .option("maxRowsInMemory", 5000) \
           .mode("append") \
           .save(local_file_name)



    

    base_df.unpersist()

    unbalanced_batches_df.unpersist()

    unbalanced_journals_df.unpersist()

    print("All production branch file distributions generated and transferred successfully.")
 


def main(args):
    
    spark = create_spark_session("Technical_Suspense_Details",HDFS_BASE)
    
    etl_date=get_etl_date(spark)
    
    
    PROCESS_ID= Process.DIFFERENCE_PIPELINE
    spark_date = etl_date.strftime("%Y-%m-%d")
    process_run_id = args[1] if len(args) > 1 else get_process_run_id(spark,PROCESS_ID,spark_date)
    
    print(f"Logging for Process Id - {PROCESS_ID} and Process Run Id - {process_run_id}")

    
    generate_ciba_details_report(spark,etl_date,process_run_id)
    #generate_technical_suspense_report(spark,etl_date,process_run_id)
    

if __name__ == "__main__":
    main(sys.argv)    
    
    
    













    











    

       
       
    