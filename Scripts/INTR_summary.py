from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.functions import col, lit, current_timestamp, coalesce, when, substring, expr,concat, to_date,count ,to_json ,struct
from pyspark.sql.functions import collect_list, struct, to_json, sum , broadcast
import traceback
import sys
from datetime import date,timedelta
from pyspark.sql.functions import desc
# =========== Common Methods =============================
import common.constants  as constants
from common.properties import get_oracle_properties,get_jdbc_url
from common.dateUtil import get_etl_date
from common.processRun import get_process_run_id
from common.log_etl_modified import log_etl
from common.fileUtil import tables_to_read




INSERT=0
UPDATE=1


def main(args):
    
    try:
        
         spark = (SparkSession.builder.appName("intr_SUMMARY_COUNT_SCRIPT").getOrCreate())        
        
         oracle_properties=get_oracle_properties()
        # print(oracle_properties)
         
         today = get_etl_date(spark)
         run_date= today.strftime("%d-%m-%Y")
         PROCESS_ID= getattr(constants,"DIFFERENCE_PIPELINE_ID",3)
         process_run_id = sys.argv[1] if len(sys.argv) > 1 else get_process_run_id(spark,PROCESS_ID,run_date)
         oracle_tables = tables_to_read("glif")

         TRANSACTIONS = oracle_tables["TRANSACTIONS"]
         BALANCE = oracle_tables["BALANCE"]
         
         
         log_etl(spark,process_run_id,"intr_summary_started",1)
         
         
        #  query = f"""
        #                 (SELECT cgl,
        #                         currency,
        #                         SUM(debit_amount + credit_amount) AS amount
        #                     FROM
        #                         {TRANSACTIONS}
        #                     WHERE
        #                         cgl IN ( '1106505001', '1106505002' )
        #                         AND transaction_date =TO_DATE('{run_date}', 'DD-MM-YYYY')
        #                     GROUP BY
        #                         cgl,
        #                         currency
        #                     HAVING
        #                         SUM(debit_amount + credit_amount) <> 0
        #                     ) e
        #                 """    
         query = f"""
                         (SELECT
                               cgl,
                               currency,
                               SUM(balance) AS amount
                           FROM
                               gl_balance
                           WHERE balance_date = to_date('{run_date}' ,'dd/mm/yyyy') and
                               cgl IN ( '1106505001', '1106505002' )
                           GROUP BY
                               currency,
                               cgl
                           HAVING
                               SUM(balance) != 0
                            ) e 
                        """

         jdbc_url = get_jdbc_url()
         oracle_properties = get_oracle_properties()
         result_df = spark.read.jdbc(url=jdbc_url, table=query, properties=oracle_properties)

         
         summary_df = result_df.select(
                                       collect_list(
                                           struct("cgl", "currency", "amount")
                                       ).alias("summary")
                                    )
         final_df = summary_df.select(
                                       lit("INTR Summary").alias("Summary Name"),
                                       lit(run_date).alias("date"),
                                       "summary"
                                    )
         final_json = final_df.selectExpr(
                                               "to_json(struct(*)) AS value"
                                            ).first()["value"]
         log_etl(spark,process_run_id,"intr_summary_calculated",2,"INTR Summary Succesfully Calculated",0,final_json)
         log_etl(spark,process_run_id,"intr_summary_started",2,"INTR Summary generated successfully.")
         print(final_json)
         spark.stop()
         sys.exit(0)
        
    except Exception as e:
        print("ERROR during reconciliation:")
        log_etl(spark,process_run_id,"intr_summary_calculated",3,"Error while calculating INTR Summary")
        log_etl(spark,process_run_id,"intr_summary_started",3,"Error while generating INTR Summary")
        traceback.print_exc()
        try:
            spark.stop()
        except:
            pass
        sys.exit(1)
        

if __name__=="__main__":
    main(sys.argv)