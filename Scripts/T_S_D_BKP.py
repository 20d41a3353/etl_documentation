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
PATTERN = "^[\\s]*$"
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
   )
   # ------------------------------------------------------------------
   # Technical Suspense transactions
   # ------------------------------------------------------------------
   base_df = (
       source_df
           .filter(col("CGL") == TECHNICAL_SUSPENSE_CGL)
           .cache()
   )
   # ------------------------------------------------------------------
   # Find branches having unbalanced Technical Suspense.
   # Branch balancing is across ALL currencies.
   # ------------------------------------------------------------------
   unbalanced_branches = (
       base_df
           .groupBy(BRANCH_COLUMN)
           .agg(
               spark_sum(AMOUNT_COLUMN).alias(TOTAL_AMOUNT)
           )
           .filter(col(TOTAL_AMOUNT) != 0)
           .select(BRANCH_COLUMN)
           .distinct()
   )
   branch_list = sorted(
       row[BRANCH_COLUMN]
       for row in unbalanced_branches.collect()
   )
   branch_list=["00300"]
   print(f"Total unbalanced branches : {len(branch_list)}")
   # ------------------------------------------------------------------
   # Generate one Excel per branch
   # ------------------------------------------------------------------
   for branch_code in branch_list:
       print(f"Generating report for Branch : {branch_code}")
       # --------------------------------------------------------------
       # All transactions (all CGLs) for this branch
       # --------------------------------------------------------------
       branch_df = (
           source_df
               .filter(col(BRANCH_COLUMN) == branch_code)
               .cache()
       )
       # --------------------------------------------------------------
       # Technical Suspense transactions only
       # Used only for identifying unbalanced batches/journals
       # --------------------------------------------------------------
       branch_ts_df = (
           branch_df.filter(
               col("CGL") == TECHNICAL_SUSPENSE_CGL
           )
       )
       batch_df = (
           branch_ts_df.filter(
               ~col(BATCH_ID_COLUMN).rlike(PATTERN)
           )
       )
       journal_df = (
           branch_ts_df.filter(
               col(BATCH_ID_COLUMN).rlike(PATTERN)
           )
       )
       # ------------------------------------------------------------------

        # Identify unbalanced batches

        # Batch must balance independently for each currency

        # ------------------------------------------------------------------

       unbalanced_batches = (

            batch_df

                .groupBy(

                    BATCH_ID_COLUMN,
                    CURRENCY
                )
                .agg(

                    spark_sum(AMOUNT_COLUMN).alias(TOTAL_AMOUNT)

                )

                .filter(col(TOTAL_AMOUNT) != 0)

                .select(

                    BATCH_ID_COLUMN

                )

                .distinct()

        )
       unbalanced_batches_ids_list = [getattr(row, BATCH_ID_COLUMN) for row in unbalanced_batches.collect()]


       unbalanced_journals = (

            journal_df

                .groupBy(

                    JOURNAL_NUMBER,

                    BRANCH_COLUMN,

                    CURRENCY

                )

                .agg(

                    spark_sum(AMOUNT_COLUMN).alias(TOTAL_AMOUNT)

                )

                .filter(col(TOTAL_AMOUNT) != 0)

                .select(

                    JOURNAL_NUMBER

                    

                )

                .distinct()

        )

        # ------------------------------------------------------------------

        # Fetch all transactions (all CGLs and all currencies)

        # for the identified journals of this branch

        # ------------------------------------------------------------------
       unbalanced_journal_ids_list = [getattr(row, JOURNAL_NUMBER) for row in unbalanced_journals.collect()]

       batch_report_df = branch_df.filter(col(BATCH_ID_COLUMN).isin(unbalanced_batches_ids_list))
       journal_report_df = branch_df.filter(col(JOURNAL_NUMBER).isin(unbalanced_journal_ids_list))


       print(

            f"Branch: {branch_code} | "

            f"Batch Records: {batch_report_df.count()} | "

            f"Journal Records: {journal_report_df.count()}"

        )

       output_file = (

            f"{HDFS_BASE}/technical_suspense_report/"

            f"{branch_code}/"

            f"technical_suspense_report_{report_date}_{branch_code}.xlsx"

        )

       print(f"Writing report to: {output_file}")

       (

            batch_report_df.write

                .format("com.crealytics.spark.excel")

                .option("dataAddress", "'BATCH'!A1")

                .option("header", "true")

                .option("maxRowsInMemory", 5000)

                .mode("overwrite")

                .save(output_file)

        )

       (

            journal_report_df.write

                .format("com.crealytics.spark.excel")

                .option("dataAddress", "'JOURNAL'!A1")

                .option("header", "true")

                .option("maxRowsInMemory", 5000)

                .mode("append")

                .save(output_file)

        )

       branch_df.unpersist()

   base_df.unpersist()

   print("Technical Suspense Reports generated successfully.")






def main(args):
    spark = create_spark_session("Technical_Suspense_Details",HDFS_BASE)
    gateway = spark._jvm
    gateway.org.apache.poi.util.IOUtils.setByteArrayMaxOverride(500 * 1024 * 1024)
    etl_date = get_etl_date(spark)

    custom_moment = datetime(2026, 7,20 , 14, 30, 0)
    
    etl_date = custom_moment
    process_id = Process.DIFFERENCE_PIPELINE
    spark_date = etl_date.strftime("%Y-%m-%d")
    process_run_id = (args[1] if len(args) > 1 else get_process_run_id(spark,process_id,spark_date))
    print(f"Logging for Process Id : {process_id} "f"and Process Run Id : {process_run_id}")
    generate_technical_suspense_report(spark,etl_date,process_run_id)
if __name__ == "__main__":
    main(sys.argv)  