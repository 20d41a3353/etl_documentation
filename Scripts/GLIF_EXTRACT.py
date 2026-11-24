import sys
from datetime import datetime
from common.createSpark import create_spark_session
from common.fileUtil import get_hdfs_base
import common.constants as constants
from common.dateUtil import get_etl_date
from pyspark.sql.functions import col, sum as spark_sum
from common.processRun import get_process_run_id
from common.constants import Process
from pyspark.sql.functions import col, sum as spark_sum, broadcast
CIBA_CGL = "1106505001"
TECHNICAL_SUSPENSE_CGL = "1260505001"
BATCH_ID_COLUMN = "BATCH_ID"
AMOUNT_COLUMN = "AMOUNT"
TOTAL_AMOUNT = "total_amount"
JOURNAL_NUMBER = "JOURNAL_NUMBER"
CURRENCY = "CURRENCY"
BRANCH_COLUMN = "BRANCH_CODE"
CIBA = "ciba_details_report/ciba_details_report"
SUSPENSE = "technical_suspense_details_report/technical_suspense_details_report"
HDFS_BASE = get_hdfs_base()
CIBA_COLUMNS = [
   "POST_DATE",
   JOURNAL_NUMBER,
   "BRCH",
   "FCY_CODE",
   "BRANCH_CODE",
   CURRENCY,
   "CGL",
   "DESCRIPTION",
   "POST_DATE",
   AMOUNT_COLUMN,
   "FCY_AMT",
   "ACCOUNT",
   "SOURCE",
   "LCY_AMT",
   "FILE_NAME",
]

def path_builder(etl_date, report_type):
   input_formatted_date = etl_date.strftime("%Y/%b/%d").upper()
   input_path = (
       f"{HDFS_BASE}/druid-data-lake/"
       f"{input_formatted_date}/GLIF/GLIF_TXNS/"
   )

   report_path_date = etl_date.strftime("%Y-%m-%d")
   report_date = etl_date.strftime("%d%m%Y")
   output_path = (
       f"{HDFS_BASE}/reports/"
       f"{report_path_date}/{report_type}_{report_date}.xlsx"
   )
   return input_path, output_path


def generate_technical_suspense_report(spark, etl_date, process_run_id):
   input_path, _ = path_builder(etl_date, SUSPENSE)
   print(f"Reading data from : {input_path}")
   report_date = etl_date.strftime("%d%m%Y")
   # ------------------------------------------------------------------
   # Read complete day's transactions
   # ------------------------------------------------------------------
   source_df = (
       spark.read
           .format("delta")
           .load(input_path)
           .filter(col(BRANCH_COLUMN)=="00300")
   )
   source_df.coalesce(1).write.csv("/technical_suspense_report/00301", mode="overwrite", header=True)


   print("Technical Suspense Reports generated successfully.")




def main(args):
    spark = create_spark_session("GLIF_EXTRACT",HDFS_BASE)
    gateway = spark._jvm
    gateway.org.apache.poi.util.IOUtils.setByteArrayMaxOverride(500 * 1024 * 1024)
    custom_moment = datetime(2026, 7,20 , 14, 30, 0)
    etl_date = get_etl_date(spark)
    etl_date = custom_moment

    process_id = Process.DIFFERENCE_PIPELINE
    spark_date = etl_date.strftime("%Y-%m-%d")
    process_run_id = (args[1] if len(args) > 1 else get_process_run_id(spark,process_id,spark_date))
    print(f"Logging for Process Id : {process_id} "f"and Process Run Id : {process_run_id}")
    generate_technical_suspense_report(spark,etl_date,process_run_id)
if __name__ == "__main__":
    main(sys.argv)  