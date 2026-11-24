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

from common.dateUtil import get_etl_date
from common.constants import Process
from common.processRun import get_run_id
from common.createSpark import create_spark_session
from common.fileUtil import get_hdfs_base
from datetime import datetime
from common.read_write_oracle import read_oracle
from common.read_write_oracle import write_oracle
from common.create_path_if_not_exists import create_path_if_not_exists
from common.properties import get_oracle_properties
from common.sql_deletion import delete_table_data_by_query
from common.processRun import get_process_run_id
from common.log_etl_modified import log_etl


def main(spark,BASE_HDFS,etl_date,args):      
    try:
        today = etl_date
        yesterday = today - timedelta(days=1)

        date_str = today.strftime("%Y-%m-%d")
        posting_date_str = today.strftime("%Y-%m-%d")
        yesterday_str = yesterday.strftime("%Y-%m-%d")

        date_obj = datetime.strptime(date_str, "%Y-%m-%d")  
        yesterday_date_obj = datetime.strptime(yesterday_str, "%Y-%m-%d")  

        oracle_format_etl_date = date_obj.strftime("%d-%b-%y").upper()
        oracle_format_yesterday_date = yesterday_date_obj.strftime("%d-%b-%y").upper()

        path = f"{BASE_HDFS}/data-lake/Fincore/Data-Lake/GL_BALANCE_AVERAGE"
        create_path_if_not_exists(spark,path)

        PROCESS_ID= Process.REPORT_GENERATION_PIPELINE
        today_date = get_etl_date(spark)
        today_date_formated= today_date.strftime("%Y-%m-%d")
        RUN_ID = get_run_id(spark,today_date_formated,PROCESS_ID)

        process_run_id = sys.argv[1] if len(sys.argv) > 1 else get_process_run_id(spark,PROCESS_ID,today_date_formated)
        log_etl(spark, process_run_id, f"GL_Average_Balance_Pipeline_Started",1)

        try:
            oracle_properties=get_oracle_properties()
            oracle_url = oracle_properties["url"]
            oracle_user = oracle_properties["user"]
            oracle_password = oracle_properties["password"]
            oracle_driver = oracle_properties["driver"]
            conn = spark._sc._gateway.jvm.java.sql.DriverManager.getConnection(oracle_url,oracle_user,oracle_password)
            sql_query=f"""
            Delete from GL_BALANCE_AVERAGE where BALANCE_DATE = to_date('{posting_date_str}','YYYY-MM-DD')
            """
            delete_table_data_by_query(spark,sql_query)
        
        except Exception as e:
            print(f"Error {e} occured")
            sql_query=f"""Delete from GL_BALANCE_AVERAGE where BALANCE_DATE = to_date('{posting_date_str}','YYYY-MM-DD')"""
            print(f"Error {e} occured while deleting {sql_query}.")           
            
    except Exception as e:
        log_etl(spark, process_run_id, f"GL_Average_Balance_Pipeline_Started",3,f"Error {e} occured.")
        print(f"Error {e} occured .")
        spark.stop()
        sys.exit(1)

    try:
    
        posting_date_string = today.strftime("%Y-%m-%d")
        log_etl(spark, process_run_id, f"reading_gl_balance_datalake",1)
        print("Started Reading GL Balance from datalake")
        GL_DATALAKE_PATH= f"{BASE_HDFS}/data-lake/Fincore/Data-Lake/GLIF"
        today_gl_df = spark.read.format("delta").load(GL_DATALAKE_PATH).filter(col("BALANCE_DATE") == posting_date_string)
        today_gl_df = today_gl_df.select("GLCC",col("CLOSING_BALANCE").alias("BALANCE"))
        today_gl_df.show(20,False)
        today_gl_df = today_gl_df.dropDuplicates(["GLCC","BALANCE"])
        log_etl(spark, process_run_id, f"reading_gl_balance_datalake",2,"Successfully read GL_BALANCE")

    except Exception as e:
        log_etl(spark, process_run_id, f"reading_gl_balance_datalake",3,f"Error {e} occured while reading GL_Balance Datalake")
        print(f"eror {e} occured.")
        print("Started Reading GL Balance from oracle")
        log_etl(spark, process_run_id, f"reading_gl_balance_oracletable",1)
        gl_bal_query = f"(Select *  from GL_BALANCE where BALANCE_DATE = '{oracle_format_etl_date}')e"

        try:
            today_gl_df = read_oracle(spark,gl_bal_query)
            today_gl_df = today_gl_df.withColumn("GLCC", concat(col("BRANCH_CODE"),col("CURRENCY"),col("CGL"))).select("GLCC","BALANCE")
            today_gl_df = today_gl_df.dropDuplicates(["GLCC","BALANCE"])
        except Exception as e:
            print(f"eror {e} occured while reading GL_BALANCE oracle.")
            log_etl(spark, process_run_id, f"reading_gl_balance_oracletable",3)
        
        log_etl(spark, process_run_id, f"reading_gl_balance_oracletable",2)

    try:
        print("Started Reading GL Balance Avg from datalake")
        log_etl(spark, process_run_id, f"reading_gl_avg_datalake",1)
        yester_avg_gl = spark.read.format("delta").load(path).filter(col("BALANCE_DATE") == yesterday_date_obj)
        yester_avg_gl.printSchema()
        yester_avg_gl = yester_avg_gl.withColumn("GLCC", concat(col("BRANCH_CODE"),col("CURRENCY"),col("CGL"))).select("GLCC","AVERAGE_BALANCE","DAY_COUNT")
        yester_avg_gl = yester_avg_gl.dropDuplicates(["GLCC","AVERAGE_BALANCE","DAY_COUNT"])
        yester_avg_gl.show(20,False)
        log_etl(spark, process_run_id, f"reading_gl_avg_datalake",2)
    except Exception as e:
        log_etl(spark, process_run_id, f"reading_gl_avg_datalake",3,f"Error {e} occured while reading gl avg from datalake.")
        print(f"Error {e} occured while reading data from datalake.")
        print("Started Reading GL Balance Avg from oracle")
        avg_bal_query = f"(select  * from GL_BALANCE_AVERAGE where BALANCE_DATE = '{oracle_format_yesterday_date}')e"
        try:
            log_etl(spark, process_run_id, f"reading_gl_avg_oracletable",1)
            yester_avg_gl = read_oracle(spark,avg_bal_query)
            yester_avg_gl = yester_avg_gl.withColumn("GLCC", concat(col("BRANCH_CODE"),col("CURRENCY"),col("CGL"))).select("GLCC","AVERAGE_BALANCE","DAY_COUNT")
            yester_avg_gl = yester_avg_gl.dropDuplicates(["GLCC","AVERAGE_BALANCE","DAY_COUNT"])
            log_etl(spark, process_run_id, f"reading_gl_avg_oracletable",2)
        except Exception as e:
            log_etl(spark, process_run_id, f"reading_gl_avg_oracletable",3,f"Error {e} occured.")
            print(f"eror {e} occured while reading GL_BALANCE_AVERAGE oracle.")
   
    try:
        log_etl(spark, process_run_id, f"calculating_gl_average",1)
        yester_avg_gl.cache()
        today_gl_df.cache()
    
        join_df = today_gl_df.join(yester_avg_gl , on=["GLCC"] ,how='outer')
    
        join_df = join_df.withColumn("DAY_COUNT", when(col("DAY_COUNT").isNull(), lit(1)).otherwise(col("DAY_COUNT")+1)).withColumn("BALANCE", when(col("BALANCE").isNull(), lit(0)).otherwise(col("BALANCE"))).withColumn("AVERAGE_BALANCE", when(col("AVERAGE_BALANCE").isNull(), lit(0)).otherwise(col("AVERAGE_BALANCE")))
    
        avg_bal_calc = join_df.withColumn("AVERAGE_BALANCE", 
                                          (col("AVERAGE_BALANCE")*(col("DAY_COUNT")-1)+col("BALANCE"))/col("DAY_COUNT")).withColumn("BALANCE_DATE",F.to_date(F.lit(posting_date_str), "yyyy-MM-dd"))
    
        # avg_bal_calc.show(10,False)
        final_df = avg_bal_calc.withColumns({ 
                "BRANCH_CODE": substring(col("glcc"), 1, 5),
                "CURRENCY": substring(col("glcc"), 6, 3),
                "CGL": substring(col("glcc"), 9, 10),
            }).select("BRANCH_CODE","CURRENCY","CGL",col("AVERAGE_BALANCE").cast(DecimalType(25,4)),"BALANCE_DATE",col("DAY_COUNT").cast(DecimalType(10,0)))
        # final_df.orderBy(F.desc("BALANCE")).show(20,False)
        final_df.dropDuplicates(["BRANCH_CODE", "CURRENCY", "CGL", "AVERAGE_BALANCE", "BALANCE_DATE", "DAY_COUNT"])
        
    
        try:
            currency_query = f"""
            (
                SELECT CURRENCY_CODE,CURRENCY_RATE from CURRENCY_MASTER
            ) T1
            """
            currency_list = read_oracle(spark,currency_query)
        except Exception as e:
            print(f"Error {e} while fetching the currency list from oracle.")
            spark.stop()
            sys.exit(1)
    
        df_converted = (
        final_df
        .join(
            broadcast(currency_list),
            final_df["CURRENCY"] == currency_list["CURRENCY_CODE"],
            "left"
        )
        .select(
            col("BRANCH_CODE"),
            col("CURRENCY"),
            col("CGL"),
            col("BALANCE_DATE"),
            col("AVERAGE_BALANCE"),
            col("CURRENCY_RATE"),
            col("DAY_COUNT")
        )
        .withColumn(
            "APPLIED_RATE",
            when(col("CURRENCY") == "INR", lit(1))
            .otherwise(coalesce(col("CURRENCY_RATE"), lit(1)))
        )
        .withColumn(
            "INR_AVERAGE_BALANCE",
            round(col("AVERAGE_BALANCE") * col("APPLIED_RATE"),2).cast(DecimalType(25, 2))
        )
        )
    
    
        balance_df = df_converted.select(
        "CGL",
        "CURRENCY",
        "BRANCH_CODE",
        "AVERAGE_BALANCE",
        "INR_AVERAGE_BALANCE",
        "BALANCE_DATE",
        "DAY_COUNT"
        )
        balance_df.cache()
        balance_df.printSchema()
        balance_df.show(20,False)
    
        try:
            log_etl(spark, process_run_id, f"writing_to_oracle",1)
            print("started writing to oracle")
            write_oracle(balance_df, "GL_BALANCE_AVERAGE")
            log_etl(spark, process_run_id, f"writing_to_oracle",2)
        except Exception as e:
            log_etl(spark, process_run_id, f"writing_to_oracle",3,f"Error {e} occured while writing to oracle table.")
            print(f"Error {e} occured while writing to oracle table")
    
        
        
    
       
    
        try:
            print("started writing to datalake ")
            log_etl(spark, process_run_id, f"writing_to_datalake",1)
            balance_df.write.format('delta').mode('overwrite').option("overwriteSchema", "true").option("replaceWhere", f"BALANCE_DATE = '{posting_date_str}'").save(path)
            log_etl(spark, process_run_id, f"writing_to_datalake",2)
    
        except Exception as e:
            log_etl(spark, process_run_id, f"writing_to_datalake",3,"Error {e} occured while writing to datalake.")
            print(f"Error {e} occured while writing to datalake")
        balance_df.show(20,False)
        print(f"+++++++++++++++++++++++++++++++++++++Success++++++++++++A+++++++++++B++++++++++++{date_str} Gl_balancecount====> {today_gl_df.count()} glAvg===>{balance_df.count()}")

        log_etl(spark, process_run_id, f"calculating_gl_average",2,f"GL Average Calculated Successfull.")
        log_etl(spark, process_run_id, f"GL_Average_Balance_Pipeline_Started",2,f"GL Average Pipeline run Successfully.")
    except Exception as e:
            log_etl(spark, process_run_id, f"calculating_gl_average",3,f"Error {e} occured while calculating GL Average.")
            log_etl(spark, process_run_id, f"GL_Average_Balance_Pipeline_Started",3,f"Error {e} occured.")
            print(f"Error {e} occured while Calculating Average Balance")



if __name__ == "__main__":
   
    BASE_HDFS = get_hdfs_base()
    
    spark = create_spark_session("GLIF BALANCE AVERAGE PIPELINE", BASE_HDFS)
    today = get_etl_date(spark)
    posting_date_str = today.strftime("%Y-%m-%d")
    dates = [posting_date_str, posting_date_str]

    date_format = "%Y-%m-%d"
    # dates = ["30-06-2026","30-06-2026"] 
    # dates = ["12-08-2026","12-08-2026"]   #[From_Date, To_Date]  
    
    start_date = datetime.strptime(dates[0], date_format)
    end_date = datetime.strptime(dates[1], date_format)
    
    generated_dates = []
   
    current_date = start_date

    while current_date <= end_date:       
        generated_dates.append(current_date)        
        current_date += timedelta(days=1)
    
    print(generated_dates)

    try:
        for etl_date in generated_dates:
            main(spark,BASE_HDFS,etl_date,sys.argv)
        spark.stop()
        sys.exit(0)
    except Exception as e:
        print(f"Error {e} occured while running the pipeline.")
        spark.stop()
        sys.exit(1)
    












