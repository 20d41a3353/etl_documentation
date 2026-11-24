
import datetime
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lit, coalesce, when, collect_list, struct
import traceback
import sys
from common.createSpark import create_spark_session
from common.fileUtil import get_hdfs_base
from datetime import timedelta
import common.constants as constants
from common.dateUtil import get_etl_date
from common.sql_deletion import delete_table_data_by_query
from common.fileUtil import paths_for_read
from common.processRun import get_process_run_id
from common.read_write_oracle import read_oracle, write_oracle
from common.fileUtil import tables_to_read
from common.date_converter import get_date_formats

def suspense_tracker(spark,BASE_HDFS,etl_date):
    HDFS_BASE = BASE_HDFS
    spark = create_spark_session("CNTR_AND_SUSPENSE_TRACKER", HDFS_BASE)

    try:
        oracle_tables = tables_to_read("glif")
        
        date_str = etl_date
        today_date = get_date_formats(date_str)
        yesterday_date = get_date_formats(date_str, -1)

        print(today_date)
        print(yesterday_date)
        run_date =today_date['date_string']
        posting_date_str = run_date
        yesterday = yesterday_date['date_string']
        print(f"RUN DATE      : {run_date}")
        print(f"YESTERDAY DATE: {yesterday}")
        hdfs_paths = paths_for_read("glif",today_date['date_string'],posting_date_str)
        BALANCE = oracle_tables["BALANCE"]
        GL_DATALAKE_PATH = hdfs_paths["GL_DATALAKE_PATH"]

        PROCESS_ID = getattr(constants,"DIFFERENCE_PIPELINE_ID",3) 
 
        process_run_id = ( 
            sys.argv[1] 
            if len(sys.argv) > 1 
            else get_process_run_id( 
                spark, 
                PROCESS_ID, 
                run_date 
            ) 
        ) 
        print(f"Process RunId : {process_run_id}") 

        if process_run_id>1:
            try:
                print(f"Deleting {run_date} data from oracle.")
                sql_query=f"""
                            Delete from SUSPENSE_TRACKER where BALANCE_DATE = to_date('{posting_date_str}','YYYY-MM-DD')
                            """
                delete_table_data_by_query(spark,sql_query)
            except Exception as e:
                print(f"Error {e} occured while executing query")


        # ======================================================
        # ======================================================
        # PART 2 : SUSPENSE TRACKER STARTED
        # ======================================================
        # ======================================================
        try:

            print("====================================")
            print("STARTING SUSPENSE_TRACKER")
            print("====================================")


            path_config = paths_for_read("glif", today_date['date_string'], posting_date_str)
            gl_delta_path = path_config["GL_DATALAKE_PATH"]

            target_cgls = [
                '1111111111', '1111111112', '1260505001', '2148505001',
                '1148505004', '1148505005', '1148505006', '1148505007',
                '1148505010', '1148505012', '1148505014', '1148505020',
                '2148505004', '2148505006', '2148505007', '2148505010',
                '2148505011', '2148505014', '2148505016', '2148505018',
                '2148505019', '2148505021', '2148505022', '2148505024',
                '2148505033', '2148505035', '1260505002', '1260505003'
            ]
            cgl_list =  ", ".join([f"'{x}'" for x in target_cgls])
            
            # ======================================================
            # READING TODAY + YESTERDAY 
            # ======================================================

            try:

                balance_df = (
                    spark.read.format("delta")   
                    .load(gl_delta_path))

                    
                balance_df =balance_df.filter(
                        F.col("BALANCE_DATE").isin(
                            posting_date_str,
                            yesterday
                        ).withColumns({
                        "BRANCH_CODE": F.substring(F.col("glcc"), 1, 5),
                        "CURRENCY": F.substring(F.col("glcc"), 6, 3),
                        "CGL": F.substring(F.col("glcc"), 9, 10)
                    })
                    .filter(F.col("CGL").isin(target_cgls)))
                

                today_df = (
                    balance_df
                    .filter(
                        F.col("BALANCE_DATE") == run_date
                    )
                    .select(
                        "BRANCH_CODE",
                        "CURRENCY",
                        "CGL",
                        F.col("CLOSING_BALANCE").alias("T_BAL"),
                        F.col("INR_BALANCE").alias("T_INR_BAL")
                    )
                )

                yest_df = (
                    balance_df
                    .filter(
                        F.col("BALANCE_DATE") == yesterday
                    )
                    .select(
                        "BRANCH_CODE",
                        "CURRENCY",
                        "CGL",
                        F.col("CLOSING_BALANCE").alias("Y_BAL"),
                        F.col("INR_BALANCE").alias("Y_INR_BAL")
                    )
                )

            except Exception as e:

                print(f"Error {e} while fetching data from datalake")

                balance_df = read_oracle(
                    spark,
                    f"""
                    (
                        SELECT
                            BALANCE_DATE,
                            BRANCH_CODE,
                            CURRENCY,
                            CGL,
                            BALANCE,
                            INR_BALANCE
                        FROM {BALANCE}
                        WHERE CGL IN ({cgl_list})
                        AND BALANCE_DATE BETWEEN
                            '{yesterday_date['oracle_string']}'
                        AND
                            '{today_date['oracle_string']}'
                    ) T
                    """
                )
                balance_df.cache()
                # balance_df.show(20,False)

                # spark.stop()
                # sys.exit()
                

                today_df = (
                    balance_df
                    .filter(
                        F.date_format(
                            F.col("BALANCE_DATE"),
                            "yyyy-MM-dd"
                        ) == run_date
                    )
                    .select(
                        "BRANCH_CODE",
                        "CURRENCY",
                        "CGL",
                        F.col("BALANCE").alias("T_BAL"),
                        F.col("INR_BALANCE").alias("T_INR_BAL")
                    )
                )

                yest_df = (
                    balance_df
                    .filter(
                        F.date_format(
                            F.col("BALANCE_DATE"),
                            "yyyy-MM-dd"
                        ) == yesterday
                    )
                    .select(
                        "BRANCH_CODE",
                        "CURRENCY",
                        "CGL",
                        F.col("BALANCE").alias("Y_BAL"),
                        F.col("INR_BALANCE").alias("Y_INR_BAL")
                    )
                )

                # today_df.show(20,False)
                # yest_df.show(20,False)
                # spark.stop()
                # sys.exit()
            # ======================================================
            # EXISTING DF
            # ======================================================
            existing_df = read_oracle(
                spark,
                f"""
                (
                    SELECT
                        BRANCH_CODE,
                        CURRENCY,
                        CGL,
                        FIRST_ERROR_DATE
                    FROM SUSPENSE_TRACKER
                    WHERE BALANCE_DATE = TO_DATE(
                    '{yesterday}',
                    'yyyy-MM-dd')
                ) E
                """   
            )
            # ======================================================
            # JOINING TODAY AND YESTERDAY
            # ======================================================
            joined_df = today_df.alias("t").join(
                yest_df.alias("y"),
                on=[
                    "BRANCH_CODE",
                    "CURRENCY",
                    "CGL"
                ],
                how="full_outer"
            )

            # ======================================================
            # TYPE LOGIC
            # ======================================================
            result_df = joined_df.withColumn(
                "TYPE",
                when(
                    (coalesce(col("Y_BAL"), lit(0)  ) == 0) &
                    (coalesce(col("T_BAL"), lit(0)) != 0),
                    lit("NEW_ENTRY")
                )
                .when(

                    (coalesce(col("Y_BAL"), lit(0)) != 0) &
                    (coalesce(col("T_BAL"), lit(0)) != 0) & 
                    (
                        (col("Y_BAL") != col("T_BAL")) |
                        (col("Y_INR_BAL") != col("T_INR_BAL"))
                    ),
                    lit("DIFFERENCE")
                )
                .when(
                    (coalesce(col("Y_BAL"), lit(0)) != 0) &
                    (coalesce(col("T_BAL"), lit(0)) == 0),
                    lit("CLOSED")
                )
                .when(
                    (coalesce(col("Y_BAL"), lit(0)) != 0) &
                    (coalesce(col("T_BAL"), lit(0)) != 0) &
                    (col("Y_BAL")==col("T_BAL")),
                    lit("ZERO_DIFFERENCE")
                )
                .otherwise(
                    lit("IGNORE")
                )
            )

            # ======================================================
            # JOINING EXISTING FIRST ERROR DATE
            # ======================================================
            result_df = result_df.join(
                existing_df,
                on=[
                    "BRANCH_CODE",
                    "CURRENCY",
                    "CGL"
                ],
                how="left"
            )
            # ======================================================
            # FIRST ERROR DATE LOGIC
            # ======================================================
            result_df = result_df.withColumn(
                "FINAL_FIRST_ERROR_DATE",
                when(col("TYPE") == "NEW_ENTRY",F.to_date(F.lit(today_date['date_string']), "yyyy-MM-dd"))
                .when(col("TYPE").isin("DIFFERENCE", "ZERO_DIFFERENCE", "CLOSED"),
                    coalesce(col("FIRST_ERROR_DATE"), F.to_date(F.lit(today_date['date_string']), "yyyy-MM-dd"))
                )
            )
            # ======================================================   F.to_date(F.lit(today_date['date_string']), "yyyy-MM-dd")
            # REMOVING IGNORE
            # ======================================================
            result_df = result_df.filter(col("TYPE") != "IGNORE")
            # ======================================================
            # FINAL DF
            # ======================================================
            final_df = result_df.select(
                F.to_date(F.lit(today_date['date_string']), "yyyy-MM-dd").alias("BALANCE_DATE"),
                col("BRANCH_CODE"),
                col("CURRENCY"),
                col("CGL"),
                coalesce(col("T_BAL"),lit(0)).alias("BALANCE"),
                coalesce(col("T_INR_BAL"),lit(0)).alias("INR_BALANCE"),
                col("TYPE"),
                col("FINAL_FIRST_ERROR_DATE").alias("FIRST_ERROR_DATE")
            )
            final_df.show(20, False)

            # ======================================================
            # WRITING TO ORACLE
            # ======================================================
            write_oracle(final_df,"SUSPENSE_TRACKER")

            print("====================================")
            print("SUSPENSE_TRACKER COMPLETED")
            print("====================================")
            print("FULL JOB COMPLETED")
        except Exception as e:
            print(f"ERROR {e} IN SUSPENSE_TRACKER")
            traceback.print_exc()
            spark.stop()
            
    except Exception as e:
        print(f"ERROR {e} IN CNTR_AND_SUSPENSE_TRACKER ")


def suspense_track(spark,BASE_HDFS,today):
    # BASE_HDFS = get_hdfs_base()
    # spark = create_spark_session("PRODUCT WISE PIPELINE", BASE_HDFS)
    # today = get_etl_date(spark)
    today_dates = get_date_formats(today)

    last_date_query = f"""(select distinct(BALANCE_DATE) from SUSPENSE_TRACKER order by BALANCE_DATE desc)t """

    last_day = read_oracle(spark,last_date_query)
    last_day.show()

    last_processed_date = last_day.collect()[0]["BALANCE_DATE"]


    last_processed_date = get_date_formats(last_processed_date, +1)
    print(f"last_processed_date==================={last_processed_date}")
    print(f"today_dates  =================== {today_dates}")
  

    date_format = "%Y-%m-%d"
    # dates = ["30-06-2026","30-06-2026"] 
    dates = [last_processed_date['date_string'],today_dates['date_string']]   #[From_Date, To_Date]  
    
    start_date = datetime.strptime(dates[0], date_format)
    end_date = datetime.strptime(dates[1], date_format)
    
    generated_dates = []
   
    current_date = start_date

    while current_date <= end_date:       
        generated_dates.append(current_date.strftime(date_format))        
        current_date += timedelta(days=1)
    
    print(generated_dates)


    for etl_date in generated_dates:
        print(f"date = {etl_date}")
        suspense_tracker(spark,BASE_HDFS,etl_date)
        

if __name__ == "__main__":
    suspense_track()