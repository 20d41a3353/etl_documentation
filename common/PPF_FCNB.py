import datetime 
from pyspark.sql import SparkSession , functions as F
import pyspark.sql.types as T
from pyspark.sql.functions import to_date,expr, round, col,when, length, substring,trim, concat, lit, create_map, to_timestamp, sum,broadcast, coalesce
from pyspark.sql.types import DecimalType, StringType, StructType, StructField, LongType, DateType
from datetime import date, timedelta
from pyspark.sql.functions import desc
import sys
from py4j.java_gateway import java_import
import concurrent.futures
import builtins
from pyspark.sql import Row
import re
# =========== Common Methods =============================
from common.properties import get_oracle_properties
from common.dateUtil import get_etl_date
from common.read_write_oracle import read_oracle,execute_oracle_procedures,write_oracle
import os

from common.fileUtil import write_csv_with_filename
from common.createSpark import create_spark_session
from common.fileUtil import get_hdfs_base
from common.create_path_if_not_exists import create_path_if_not_exists
from common.fileUtil import get_hdfs_base,write_text_with_filename
from common.date_converter import get_date_formats



HDFS_BASE = get_hdfs_base()
spark = create_spark_session("GLIF_PIPELINE", HDFS_BASE)

today = get_etl_date(spark)
yesterday = today - timedelta(days=1)

posting_date_str = today.strftime("%Y-%m-%d")

today_str = posting_date_str
yesterday=yesterday.strftime("%Y-%m-%d")
today= today.strftime("%Y-%m-%d")

oracle_properties=get_oracle_properties()


oracle_url = oracle_properties["url"]
oracle_user = oracle_properties["user"]
oracle_password = oracle_properties["password"]
oracle_driver = oracle_properties["driver"]
DriverManager = spark._jvm.java.sql.DriverManager

def ppf_postings(df,source_cgl="2115505002",transfer_cgl="1106505001",destination_cgl="2111505002",other_branch="09595"):
    base_df = (
        df
        .filter(
            (F.col("CGL") == source_cgl) &
            (F.col("BRANCH_CODE") != other_branch) &
            ((F.col("CREDIT_AMOUNT") != 0) | (F.col("DEBIT_AMOUNT") != 0))& ((F.col("CREDIT_AMOUNT") + F.col("DEBIT_AMOUNT")) != 0)
        )
        .select(
            "BATCH_ID",
            "JOURNAL_ID",
            "BRANCH_CODE",
            "CURRENCY",
            "CGL",
            "NARRATION",
            "TRANSACTION_COUNT",      
            (F.col("CREDIT_AMOUNT") + F.col("DEBIT_AMOUNT")).alias("NET")
        )
    )
    generated_df = base_df.select(
    F.explode(F.array(
       F.struct(
           F.col("BATCH_ID"),
           F.col("JOURNAL_ID"),
           F.lit("PPF TFR TO GAD").alias("NARRATION"),
           F.lit(1).alias("TRANSACTION_COUNT"),
           F.col("BRANCH_CODE"),
           F.col("CURRENCY"),
           F.col("CGL"),
           (-F.least(F.col("NET"), F.lit(0))).alias("CREDIT_AMOUNT"),  # if NET<=0
           (-F.greatest(F.col("NET"), F.lit(0))).alias("DEBIT_AMOUNT")  # if NET>0
       ),
       F.struct(
           F.col("BATCH_ID"),
           F.col("JOURNAL_ID"),
           F.lit("PPF TFR TO GAD").alias("NARRATION"),
           F.lit(1).alias("TRANSACTION_COUNT"),
           F.col("BRANCH_CODE"),
           F.col("CURRENCY"),
           F.lit(transfer_cgl).alias("CGL"),
           F.greatest(F.col("NET"), F.lit(0)).alias("CREDIT_AMOUNT"),  # if NET>0
           F.least(F.col("NET"), F.lit(0)).alias("DEBIT_AMOUNT")    # if NET<=0
       ),
       F.struct(
           F.col("BATCH_ID"),
           F.col("JOURNAL_ID"),
           F.lit("PPF TFR TO GAD").alias("NARRATION"),
           F.lit(1).alias("TRANSACTION_COUNT"),
           F.lit(other_branch).alias("BRANCH_CODE"),
           F.col("CURRENCY"),
           F.lit(transfer_cgl).alias("CGL"),
           (-F.least(F.col("NET"), F.lit(0))).alias("CREDIT_AMOUNT"),
           (-F.greatest(F.col("NET"), F.lit(0))).alias("DEBIT_AMOUNT")
       ),
       F.struct(
           F.col("BATCH_ID"),
           F.col("JOURNAL_ID"),
           F.lit("PPF TFR TO GAD").alias("NARRATION"),
           F.lit(1).alias("TRANSACTION_COUNT"),
           F.lit(other_branch).alias("BRANCH_CODE"),
           F.col("CURRENCY"),
           F.lit(destination_cgl).alias("CGL"),
           F.greatest(F.col("NET"), F.lit(0)).alias("CREDIT_AMOUNT"),
           F.least(F.col("NET"), F.lit(0)).alias("DEBIT_AMOUNT")
       ))).alias("r")).select("r.*")
    generated_df=generated_df.groupBy("BRANCH_CODE", "CURRENCY", "CGL").agg(
            F.sum("CREDIT_AMOUNT").alias("CREDIT_AMOUNT"),
            F.sum("DEBIT_AMOUNT").alias("DEBIT_AMOUNT"),
            F.lit("P").alias("SOURCE_FLAG"),
            F.sum("TRANSACTION_COUNT").alias("TRANSACTION_COUNT"),
            F.first("NARRATION").alias("NARRATION"),     
            F.first("BATCH_ID").alias("BATCH_ID"),
            F.first("JOURNAL_ID").alias("JOURNAL_ID"),
        ).select("BRANCH_CODE", "CURRENCY", "CGL","CREDIT_AMOUNT","DEBIT_AMOUNT","BATCH_ID","JOURNAL_ID","NARRATION","TRANSACTION_COUNT","SOURCE_FLAG")
    # Union + aggregate (final 2N + 2 rows)
    

    return generated_df

def monthend_posting(dl_df,fcnb_cgl_df,fcnb_currency,currency_list,posting_date_str,TARGET_BRANCH="01111",TRANSFER_CGL="1106505001"):
    try:

        print("TRANSFER_CGL =>>>>>>>>>>>>>>>>>>> {TRANSFER_CGL}")
       
        print("TARGET_BRANCH =>>>>>>>>>>>>>>>>>>> {TARGET_BRANCH}")
       
        # STEP 1: BASE DF
        base_df = (
            dl_df
            .filter(F.col("closing_balance") != 0)
            .withColumn("BRANCH_CODE", F.substring("glcc", 1, 5))
            .withColumn("CURRENCY", F.substring("glcc", 6, 3))
            .withColumn("CGL", F.substring("glcc", 9, 10))
            .join(fcnb_cgl_df, F.col("CGL") == F.col("FROM_CGL"), "inner")
            .join(
                fcnb_currency,
                F.col("CURRENCY") == F.col("CURRENCY_CODE"),
                "inner"
            )
            .filter(
                (F.col("CGL_TYPE") == "E") & (F.col("DEPOSIT_EXPENSE_FLAG") == "Y") |
                (F.col("CGL_TYPE") == "I")  & (F.col("LOAN_INCOME_FLAG") == "Y")
                )
            .select(
                F.col("BRANCH_CODE").cast("string"),
                F.col("CURRENCY").cast("string"),
                F.col("CGL").cast("string"),
                F.col("closing_balance").alias("NET"),
                F.col("FROM_CGL").cast("string"),
                F.col("TO_CGL").cast("string")
            )
            
        )

        # print("===== BASE DF SCHEMA =====")

        if base_df.rdd.isEmpty():
            print("No eligible FCY rows found in base_df")
            df_empty=spark.createDataFrame([],schema="""TRANSACTION_DATE DATE , BRANCH_CODE STRING, CURRENCY STRING, CGL STRING, NARRATION STRING, CREDIT_AMOUNT DECIMAL(25,4), DEBIT_AMOUNT DECIMAL(25,4), TRANSACTION_COUNT DECIMAL(10,0), SOURCE_FLAG STRING""")
            return df_empty 
        
        
        
        fcy_df = (
        base_df
        .select(
            F.explode(
                F.array(
                # Credit FROM_CGL (if AMOUNT > 0)
                F.struct(
                    F.col("BRANCH_CODE").alias("branch"),
                    F.col("CURRENCY").alias("currency"),
                    F.col("FROM_CGL").alias("cgl"),
                    (-F.least(F.col("NET"), F.lit(0))).alias("CREDIT_AMOUNT"),  # if NET<=0
                    (-F.greatest(F.col("NET"), F.lit(0))).alias("DEBIT_AMOUNT"), # if NET>0
       
                    F.lit("FCY TRANSFER").alias("narration"),
                    
                ),
                # Debit TRANSFER_CGL at same branch (if AMOUNT > 0)
                F.struct(
                    F.col("BRANCH_CODE").alias("branch"),
                    F.col("CURRENCY").alias("currency"),
                    F.lit(TRANSFER_CGL).alias("cgl"),
                    F.greatest(F.col("NET"), F.lit(0)).alias("CREDIT_AMOUNT"),  # if NET>0
                    F.least(F.col("NET"), F.lit(0)).alias("DEBIT_AMOUNT"),    # if NET<=0
      
                    F.lit("FCY TRANSFER").alias("narration"),
                    
                ),
                # Credit TRANSFER_CGL at TARGET_BRANCH (if AMOUNT > 0)
                F.struct(
                    F.lit(TARGET_BRANCH).alias("branch"),
                    F.col("CURRENCY").alias("currency"),
                    F.lit(TRANSFER_CGL).alias("cgl"),
                    (-F.least(F.col("NET"), F.lit(0))).alias("CREDIT_AMOUNT"),
                    (-F.greatest(F.col("NET"), F.lit(0))).alias("DEBIT_AMOUNT"),
       
                    F.lit("FCY TRANSFER").alias("narration"),
                    
                ),
                # Debit TO_CGL at TARGET_BRANCH (if AMOUNT <= 0)
                F.struct(
                    F.lit(TARGET_BRANCH).alias("branch"),
                    F.col("CURRENCY").alias("currency"),
                    F.col("TO_CGL").alias("cgl"),
                    F.greatest(F.col("NET"), F.lit(0)).alias("CREDIT_AMOUNT"),
                    F.least(F.col("NET"), F.lit(0)).alias("DEBIT_AMOUNT"),
                    F.lit("FCY TRANSFER").alias("narration"),
                    
                )
                )
                ).alias("r"),
            
            
            # #F.col("JOURNAL_ID"),
           
            )
        .select(
            
            #F.col("JOURNAL_ID"),
            F.col("r.branch").alias("BRANCH_CODE"),
            F.col("r.currency").alias("CURRENCY"),
            F.col("r.cgl").alias("CGL"),
            F.col("r.CREDIT_AMOUNT"),
            F.col("r.DEBIT_AMOUNT"),
            F.col("r.narration").alias("NARRATION"),
            F.lit("F").alias("SOURCE_FLAG"),
            F.lit(1).alias("TRANSACTION_COUNT"),
            F.lit(posting_date_str).cast("date").alias("TRANSACTION_DATE")
            
            )
        )
        inr_df = (
            fcy_df.alias("fcy")
            .filter(F.col("NARRATION") == "FCY TRANSFER")
            .join(
                currency_list.alias("curr"),
                F.col("fcy.CURRENCY") == F.col("curr.CURRENCY_CODE"),
                "inner"
            )
            .select(
                
                #F.col("JOURNAL_ID"),
                F.col("TRANSACTION_DATE"),
                F.col("BRANCH_CODE"),
                F.lit("INR").alias("CURRENCY"),
                F.col("CGL"),
                # (-F.col("DEBIT_AMOUNT") * F.col("CURRENCY_RATE"))
                #     .cast(DecimalType(25,2)).alias("CREDIT_AMOUNT"),
                # (-F.col("CREDIT_AMOUNT") * F.col("CURRENCY_RATE"))
                #     .cast(DecimalType(25,2)).alias("DEBIT_AMOUNT"),
                
               F.round(
                   -F.col("DEBIT_AMOUNT").cast("double")*F.col("CURRENCY_RATE").cast(("double")),
                   2
               ).cast(DecimalType(25,4)).alias("CREDIT_AMOUNT"),
               
               F.round(
                   -F.col("CREDIT_AMOUNT").cast("double")*F.col("CURRENCY_RATE").cast("double"),
                   2
               ).cast(DecimalType(25,4)).alias("DEBIT_AMOUNT"),
               
                F.lit("INR VALUATION").alias("NARRATION"),
                F.lit("F").alias("SOURCE_FLAG"),
                F.lit(1).alias("TRANSACTION_COUNT")
                
                )
            )
        
        # print("==========INR Output Before Aggregation=============")
        # if debug_cgl:
        #      print("===================Generated Four Legs===============================")
        #      return inr_df
       
        
        final_df = fcy_df.unionByName(inr_df)
        return final_df
    
    except Exception as e:
        print("ERROR IN monthend_posting")
        raise


def interest_cap_func(spark,dataframe,posting_date_str,journal_id,BATCH_ID_LITERAL):
    
    BASE_HDFS = get_hdfs_base()
    print(f" Started Exexution of INTEREST CAPITALISATION")
    query = f"""
                DECLARE
                    v_dummy_header VARCHAR2(4000);
                    v_dummy_footer VARCHAR2(4000);
                    v_dummy_file   VARCHAR2(4000);
                BEGIN 
                    SP_PARSE_REPORT_TEMPLATE(
                        p_report_id      => 'nwsa_report', 
                        p_run_id         => 'nwsa_report',
                        p_header_id       => v_dummy_header,
                        p_footer_id       => v_dummy_footer,
                        p_file_identifier => v_dummy_file
                    ); 
                END;
            """
    df =execute_oracle_procedures(spark,query)
    df.show(20,False)
  

    l31a_cgls = read_oracle(spark, f"(select CGL,UPDATED_AT from rb_parsed_template where HEAD_CODE='L31A') q")

    l31a_cgls.show(5,False)

    mapping_cgls = read_oracle(spark, f"(select from_cgl as CGL,TO_CGL from interest_capitalisation_cgl) q")

    cgls = l31a_cgls.join(mapping_cgls,on="CGL", how="left" )

    cgls = cgls.withColumn("TO_CGL",when(col("TO_CGL").isNull(), lit("8006070601")).otherwise(col("TO_CGL")))
    
    
    cgl_transfer_join = dataframe.join(broadcast(cgls),
                                       on="CGL", 
                                       how="inner"  )
    
    cgl_transfer_join= cgl_transfer_join.cache()
    

    balancing_transactions = cgl_transfer_join.select(
            F.explode(
                F.array(
                # Credit FROM_CGL (if AMOUNT > 0)
                F.struct(
                    F.col("BRANCH_CODE").alias("BRANCH_CODE"),
                    F.col("CURRENCY").alias("CURRENCY"),
                    F.col("CGL").alias("CGL"),                    
                    (-F.least(F.col("BALANCE"), F.lit(0))).alias("CREDIT_AMOUNT"),  # if NET<=0
                    (-F.greatest(F.col("BALANCE"), F.lit(0))).alias("DEBIT_AMOUNT"), # if NET>0
                    F.lit("INTREST CAP BALANCE").alias("NARRATION"),                    
                ))).alias("r")).select("r.*")
    balancing_transactions.show(20,False)
    new_transactions = cgl_transfer_join.select(
            F.explode(
                F.array(
                # Credit FROM_CGL (if AMOUNT > 0)
                F.struct(
                    F.col("BRANCH_CODE").alias("BRANCH_CODE"),
                    F.col("CURRENCY").alias("CURRENCY"),                    
                    F.col("TO_CGL").alias("CGL"),
                    F.greatest(F.col("BALANCE"), F.lit(0)).alias("CREDIT_AMOUNT"),  # if NET>0
                    F.least(F.col("BALANCE"), F.lit(0)).alias("DEBIT_AMOUNT"),    # if NET<=0
                    F.lit("INTREST CAP TRANSFER").alias("NARRATION"),                    
                ))).alias("r")).select("r.*")
    
    interest_cap_txns = balancing_transactions.unionByName(new_transactions)

    interest_cap_txns = interest_cap_txns.cache()
    print("started")

    interest_cap_txns = interest_cap_txns.withColumn("SOURCE_FLAG", lit("I")).withColumn("TRANSACTION_DATE",F.to_date(F.lit(posting_date_str), "yyyy-MM-dd"))
    
    # final_df.show(20,False)
    interest_cap_txns = interest_cap_txns.withColumn("JOURNAL_ID",lit(journal_id)).withColumn("BATCH_ID",lit(BATCH_ID_LITERAL)).withColumn("TRANSACTION_COUNT", lit(1))
    interest_cap_txns.show(20,False)
    write_oracle(interest_cap_txns, "GL_TRANSACTIONS")

    currency_query = f"(SELECT CURRENCY_CODE AS CURRENCY, CURRENCY_RATE FROM CURRENCY_MASTER) e"
    currency_master = read_oracle(spark, currency_query)

    interest_cap_balance = interest_cap_txns.withColumn("BALANCE", col("CREDIT_AMOUNT")+col("DEBIT_AMOUNT")).withColumn("BALANCE_DATE", col("TRANSACTION_DATE")).select("BRANCH_CODE","CURRENCY","CGL","BALANCE","BALANCE_DATE")
    # interest_cap_balance = interest_cap_balance.withColumn("GLCC", concat(col("BRANCH_CODE"), col("CURRENCY"), col("CGL"))).select("GLCC","BRANCH_CODE","CURRENCY","CGL","AMOUNT")
    interest_cap_balance = interest_cap_balance.cache()
    currency_df = interest_cap_balance.join(currency_master,on ="CURRENCY",how = "left")
    result_df = currency_df.withColumn(
                                        "effective_rate",
                                            F.when(F.col("CURRENCY") == "INR", F.lit(1.0)).otherwise(F.coalesce(F.col("CURRENCY_RATE"), F.lit(1.0)))
                                        ).withColumn(
                                            "INR_BALANCE",
                                            F.round(F.col("BALANCE") * F.col("effective_rate"), 2).cast(DecimalType(25, 2))
                                        )\
                            .select("BRANCH_CODE","CURRENCY","CGL","BALANCE","BALANCE_DATE","INR_BALANCE")

    result_df.show(20,False)
     
    result_df = result_df.groupBy("BRANCH_CODE", "CURRENCY", "CGL").agg(
            F.sum("BALANCE").alias("INR_BALANCE"),
            F.sum("INR_BALANCE").alias("INR_BALANCE"))
    result_df = result_df.unionByName(dataframe)
    # write_oracle(result_df,"GL_BALANCE")

    # result_df.write.format("delta").mode("overwrite").option("replaceWhere", f"BALANCE_DATE = '{posting_date_str}'").save(f"{BASE_HDFS}/data-lake/Fincore/Data-Lake/GLIF")

    return result_df

    
def get_financial_month(frequency, value):
    
    fiscal_map = {
        'M': [1,2,3,4,5,6,7,8,9,10,11,12],
        'Q': [3,6,9,12],
        'H': [3,9],
        'Y': [3]
    }

    interest_cap_day_check = int(value) in fiscal_map[frequency]
    try:
        print("=======================")
        print("Interest Capitalisation")
        print(interest_cap_day_check)
        print("=======================")
        return  interest_cap_day_check

    except KeyError:
        return "Invalid frequency or month code provided."






def trickel_feed_func(spark,HDFS_BASE,etl_date):

    today_dates = get_date_formats(etl_date)
    print(today_dates)
    yesterday_dates = get_date_formats(etl_date, -1)

    query = f"""(WITH balance_29 AS (
                                    SELECT g.cgl AS from_cgl,
                                           g.currency,
                                           SUM(g.balance) AS opening_balance
                                    FROM gl_balance g
                                    JOIN FCNB_CURRENCY_CONFIG c ON g.currency = c.CURRENCY_CODE
                                    JOIN FCNB_CGL fc ON g.cgl = fc.FROM_CGL
                                    WHERE g.balance_date = '{yesterday_dates["oracle_string"]}'
                                      AND ((c.DEPOSIT_EXPENSE_FLAG = 'Y' AND g.cgl LIKE '8%')
                                        OR (c.LOAN_INCOME_FLAG     = 'Y' AND g.cgl LIKE '7%'))
                                    GROUP BY g.cgl, g.currency
                                ),
                                txn_30 AS (
                                    SELECT t.cgl AS from_cgl,
                                           t.currency,
                                           SUM(t.debit_amount + t.credit_amount) AS txn_amount
                                    FROM gl_transactions t
                                    JOIN FCNB_CURRENCY_CONFIG c ON t.currency = c.CURRENCY_CODE
                                    JOIN FCNB_CGL fc ON t.cgl = fc.FROM_CGL
                                    WHERE t.TRANSACTION_DATE = '{today_dates["oracle_string"]}'
                                      AND t.source_flag IN ('C','J','V','S','I','R')
                                      AND ((c.DEPOSIT_EXPENSE_FLAG = 'Y' AND t.cgl LIKE '8%')
                                        OR (c.LOAN_INCOME_FLAG     = 'Y' AND t.cgl LIKE '7%'))
                                    GROUP BY t.cgl, t.currency
                                ),
                                clubbed AS (
                                    SELECT COALESCE(b.from_cgl, x.from_cgl) AS from_cgl,
                                           COALESCE(b.currency, x.currency) AS currency,
                                           fc.CGL_TYPE,
                                           NVL(b.opening_balance, 0) + NVL(x.txn_amount, 0) AS month_end_balance
                                    FROM balance_29 b
                                    FULL OUTER JOIN txn_30 x
                                      ON  b.from_cgl = x.from_cgl
                                      AND b.currency = x.currency
                                    JOIN FCNB_CGL fc
                                      ON COALESCE(b.from_cgl, x.from_cgl) = fc.FROM_CGL
                                ),
                                aggregated AS (
                                    SELECT currency,
                                           CGL_TYPE,
                                           SUM(month_end_balance) AS total_amount
                                    FROM clubbed
                                    GROUP BY currency, CGL_TYPE
                                ),
                                final_data AS (
                                    SELECT a.currency, a.total_amount, 'I' AS CGL_TYPE
                                    FROM aggregated a WHERE a.CGL_TYPE = 'I'
                                    UNION ALL
                                    SELECT a.currency, a.total_amount, 'E' AS CGL_TYPE
                                    FROM aggregated a WHERE a.CGL_TYPE = 'E'
                                    UNION ALL
                                    SELECT a.currency, SUM(a.total_amount) * -1 AS total_amount, 'T' AS CGL_TYPE
                                    FROM aggregated a
                                    GROUP BY a.currency
                                ),
                                bgl_map AS (
                                    SELECT b.currency,
                                           MAX(CASE WHEN fc.CGL_TYPE = 'I' THEN b.CGL END) AS cgl_income,
                                           MAX(CASE WHEN fc.CGL_TYPE = 'E' THEN b.CGL END) AS cgl_expense,
                                           MAX(CASE WHEN b.CGL = 1175505001 THEN b.CGL END) AS cgl_total,
                                           MAX(CASE WHEN fc.CGL_TYPE = 'I' THEN b.BGL END) AS bgl_income,
                                           MAX(CASE WHEN fc.CGL_TYPE = 'E' THEN b.BGL END) AS bgl_expense,
                                           MAX(CASE WHEN b.CGL = 1175505001 THEN b.BGL END) AS bgl_total,
                                           MAX(CASE WHEN fc.CGL_TYPE = 'I' THEN b.BGL_NAME END) AS bgl_name_income,
                                           MAX(CASE WHEN fc.CGL_TYPE = 'E' THEN b.BGL_NAME END) AS bgl_name_expense,
                                           MAX(CASE WHEN b.CGL = 1175505001 THEN b.BGL_NAME END) AS bgl_name_total,
                                           MAX(CASE WHEN fc.CGL_TYPE = 'I' THEN cm.DESCRIPTION END) AS desc_income,
                                           MAX(CASE WHEN fc.CGL_TYPE = 'E' THEN cm.DESCRIPTION END) AS desc_expense,
                                           MAX(CASE WHEN b.CGL = 1175505001 THEN cm.DESCRIPTION END) AS desc_total
                                    FROM CGLTOBGL b
                                    LEFT JOIN FCNB_CGL fc ON b.CGL = fc.TO_CGL
                                    LEFT JOIN CGL_MASTER cm ON b.CGL = cm.CGL_NUMBER
                                    GROUP BY b.currency
                                )
                                SELECT
                                    '01111' AS brch,
                                    fd.currency AS CUR,
                                    CASE fd.CGL_TYPE
                                        WHEN 'I' THEN bm.cgl_income
                                        WHEN 'E' THEN bm.cgl_expense
                                        WHEN 'T' THEN bm.cgl_total
                                    END AS CGL,
                                    CASE fd.CGL_TYPE
                                        WHEN 'I' THEN bm.bgl_income
                                        WHEN 'E' THEN bm.bgl_expense
                                        WHEN 'T' THEN bm.bgl_total
                                    END AS BGL,
                                    CASE fd.CGL_TYPE
                                        WHEN 'I' THEN bm.bgl_name_income
                                        WHEN 'E' THEN bm.bgl_name_expense
                                        WHEN 'T' THEN bm.bgl_name_total
                                    END AS BGL_NAME,
                                    CASE fd.CGL_TYPE
                                        WHEN 'I' THEN bm.desc_income
                                        WHEN 'E' THEN bm.desc_expense
                                        WHEN 'T' THEN bm.desc_total
                                    END AS CGL_NAME,
                                    fd.CGL_TYPE,
                                    fd.total_amount AS AMOUNT,
                                    fd.currency || '-'
                                    || CASE WHEN fd.total_amount >= 0 THEN '54' ELSE '04' END
                                    || LPAD(CASE fd.CGL_TYPE
                                        WHEN 'I' THEN bm.bgl_income
                                        WHEN 'E' THEN bm.bgl_expense
                                        WHEN 'T' THEN bm.bgl_total
                                    END, 11, '0')
                                    || '01111'
                                    || LPAD(ABS(ROUND(fd.total_amount, 2)) * 100, 17, '0')
                                    || ' FD PROVISIONING ENTRIES' AS file_line,
                                    CASE WHEN fd.total_amount >= 0 THEN 'CR' ELSE 'DR' END AS SIGN
                                FROM final_data fd
                                JOIN bgl_map bm ON fd.currency = bm.currency
                                WHERE bm.bgl_total IS NOT NULL
                                  AND CASE fd.CGL_TYPE
                                        WHEN 'I' THEN bm.bgl_income
                                        WHEN 'E' THEN bm.bgl_expense
                                        WHEN 'T' THEN bm.bgl_total
                                      END IS NOT NULL
                                UNION ALL
                                SELECT
                                    '01111' AS brch,
                                    'INR' AS CUR,
                                    CASE inr.CGL_TYPE
                                        WHEN 'I' THEN bm.cgl_income
                                        WHEN 'E' THEN bm.cgl_expense
                                        WHEN 'T' THEN bm.cgl_total
                                    END AS CGL,
                                    CASE inr.CGL_TYPE
                                        WHEN 'I' THEN bm.bgl_income
                                        WHEN 'E' THEN bm.bgl_expense
                                        WHEN 'T' THEN bm.bgl_total
                                    END AS BGL,
                                    CASE inr.CGL_TYPE
                                        WHEN 'I' THEN bm.bgl_name_income
                                        WHEN 'E' THEN bm.bgl_name_expense
                                        WHEN 'T' THEN bm.bgl_name_total
                                    END AS BGL_NAME,
                                    CASE inr.CGL_TYPE
                                        WHEN 'I' THEN bm.desc_income
                                        WHEN 'E' THEN bm.desc_expense
                                        WHEN 'T' THEN bm.desc_total
                                    END AS CGL_NAME,
                                    inr.CGL_TYPE,
                                    inr.total_amount AS AMOUNT,
                                    'INR-'
                                    || CASE WHEN inr.total_amount >= 0 THEN '04' ELSE '54' END
                                    || LPAD(CASE inr.CGL_TYPE
                                        WHEN 'I' THEN bm.bgl_income
                                        WHEN 'E' THEN bm.bgl_expense
                                        WHEN 'T' THEN bm.bgl_total
                                    END, 11, '0')
                                    || '01111'
                                    || LPAD(ABS(ROUND(inr.total_amount, 2)) * 100, 17, '0')
                                    || ' FD PROVISIONING ENTRIES' AS file_line,
                                    CASE WHEN inr.total_amount >= 0 THEN 'CR' ELSE 'DR' END AS SIGN
                                FROM (
                                    SELECT a.CGL_TYPE,
                                           SUM(a.total_amount * cm.CURRENCY_RATE) AS total_amount
                                    FROM aggregated a
                                    JOIN currency_master cm ON a.currency = cm.currency_code
                                    GROUP BY a.CGL_TYPE
                                    UNION ALL
                                    SELECT 'T',
                                           SUM(a.total_amount * cm.CURRENCY_RATE) * -1 AS total_amount
                                    FROM aggregated a
                                    JOIN currency_master cm ON a.currency = cm.currency_code
                                ) inr
                                JOIN bgl_map bm ON bm.currency = 'INR'
                                WHERE bm.bgl_total IS NOT NULL
                                  AND CASE inr.CGL_TYPE
                                        WHEN 'I' THEN bm.bgl_income
                                        WHEN 'E' THEN bm.bgl_expense
                                        WHEN 'T' THEN bm.bgl_total
                                      END IS NOT NULL
                                ORDER BY 1  ) e""" 

    df = read_oracle(spark,query)
    df = df.withColumns({
        "CURRENCY" : substring("FILE_LINE", 1,3),
        "TFS" : substring("FILE_LINE", 5, 65)
    })

    df.show(20,False)


    report_generation = df.select("BRCH","CUR","CGL","BGL","BGL_NAME","CGL_NAME","AMOUNT","SIGN")

    common_path = f"{HDFS_BASE}/druid-data-lake/{today_dates['year']}/{today_dates['month']}/{today_dates['day']}/GLIF"
    file_path = f"{common_path}/FCNB_TFS/"
    write_csv_with_filename(report_generation, f"{common_path}",filename='TFS_Report', sep="|")

    fcnb_txns_sql = f"""(SELECT BRANCH_CODE,CGL,CURRENCY,DEBIT_AMOUNT,CREDIT_AMOUNT FROM GL_TRANSACTIONS WHERE TRANSACTION_DATE ='{today_dates["oracle_string"]}' AND source_flag IN ('F')) t"""
    FCNB_TXNS = read_oracle(spark,fcnb_txns_sql)

    write_csv_with_filename(FCNB_TXNS, f"{common_path}",filename='FCNB_TRANSACTIONS', sep="|")

    create_path_if_not_exists(spark, file_path)

    currencies = [row.CURRENCY for row in df.select("CURRENCY").distinct().collect()]
    print(currencies)   

    for currency in currencies:
        # Filter DF for specific currency
        df_currency = df.filter(F.col("CURRENCY") == currency)  
        df_currency = df_currency.select("TFS")  


        df_currency.show(10,False)
        write_text_with_filename(df_currency,file_path,f"TFS_01111_{today_dates['date_file_name']}_000000_{today_dates['date_file_name']}_{currency}.txt")

        # Eg
        # Type of Txn.     2       54  
        # Account number   17      000+00+997694 +01111+ 0
        # Amount           14+2    0000000148411548
        # Narration Text   50      asdfghjkl;*5
        # AUD
        # 04 + 
        # 000 + 00 + 397734 + 01111
        # 00000000020445141
        # FD POVISIONING ENTRIES
 

# def trickel_feed_func(spark,HDFS_BASE,etl_date):

#     today_dates = get_date_formats(etl_date)
#     print(today_dates)
#     yesterday_dates = get_date_formats(etl_date, -1)

#     query = f"""(WITH balance_29 AS (
#                                         SELECT g.cgl AS from_cgl,
#                                         g.currency,
#                                         SUM(g.balance) AS opening_balance
#                                         FROM gl_balance g
#                                         JOIN FCNB_CURRENCY_CONFIG c ON g.currency = c.CURRENCY_CODE
#                                         JOIN FCNB_CGL fc ON g.cgl = fc.FROM_CGL
#                                         WHERE g.balance_date ='{yesterday_dates["oracle_string"]}'
#                                         AND (
#                                         (c.DEPOSIT_EXPENSE_FLAG = 'Y' AND g.cgl LIKE '8%')
#                                         OR (c.LOAN_INCOME_FLAG     = 'Y' AND g.cgl LIKE '7%')
#                                         )
#                                         GROUP BY g.cgl, g.currency
#                                         ),
#                                         txn_30 AS (
#                                         SELECT t.cgl AS from_cgl,
#                                         t.currency,
#                                         SUM(t.debit_amount + t.credit_amount) AS txn_amount
#                                         FROM gl_transactions t
#                                         JOIN FCNB_CURRENCY_CONFIG c ON t.currency = c.CURRENCY_CODE
#                                         JOIN FCNB_CGL fc ON t.cgl = fc.FROM_CGL
#                                         WHERE t.TRANSACTION_DATE = '{today_dates["oracle_string"]}'
#                                         AND t.source_flag IN ('C','J','V','S','I','R')
#                                         AND (
#                                         (c.DEPOSIT_EXPENSE_FLAG = 'Y' AND t.cgl LIKE '8%')
#                                         OR (c.LOAN_INCOME_FLAG     = 'Y' AND t.cgl LIKE '7%')
#                                         )
#                                         GROUP BY t.cgl, t.currency
#                                         ),
#                                         clubbed AS (
#                                         SELECT COALESCE(b.from_cgl, x.from_cgl) AS from_cgl,
#                                         COALESCE(b.currency, x.currency) AS currency,
#                                         fc.CGL_TYPE,
#                                         NVL(b.opening_balance, 0) + NVL(x.txn_amount, 0) AS month_end_balance
#                                         FROM balance_29 b
#                                         FULL OUTER JOIN txn_30 x
#                                         ON  b.from_cgl = x.from_cgl
#                                         AND b.currency = x.currency
#                                         JOIN FCNB_CGL fc
#                                         ON COALESCE(b.from_cgl, x.from_cgl) = fc.FROM_CGL
#                                         ),
#                                         aggregated AS (
#                                         SELECT currency,
#                                         CGL_TYPE,
#                                         SUM(month_end_balance) AS total_amount
#                                         FROM clubbed
#                                         GROUP BY currency, CGL_TYPE
#                                         ),
#                                         final_data AS (
#                                         SELECT a.currency, a.total_amount, 'I' AS CGL_TYPE
#                                         FROM aggregated a WHERE a.CGL_TYPE = 'I'
#                                         UNION ALL
#                                         SELECT a.currency, a.total_amount, 'E' AS CGL_TYPE
#                                         FROM aggregated a WHERE a.CGL_TYPE = 'E'
#                                         UNION ALL
#                                         SELECT a.currency, SUM(a.total_amount) * -1 AS total_amount, 'T' AS CGL_TYPE
#                                         FROM aggregated a
#                                         GROUP BY a.currency
#                                         ),
#                                         bgl_map AS (
#                                         SELECT b.currency,
#                                         MAX(CASE WHEN fc.CGL_TYPE = 'I' THEN b.BGL END) AS bgl_income,
#                                         MAX(CASE WHEN fc.CGL_TYPE = 'E' THEN b.BGL END) AS bgl_expense,
#                                         MAX(CASE WHEN b.CGL = 1175505001 THEN b.BGL END) AS bgl_total
#                                         FROM CGLTOBGL b
#                                         LEFT JOIN FCNB_CGL fc ON b.CGL = fc.TO_CGL
#                                         GROUP BY b.currency
#                                         )
#                                         SELECT
#                                         fd.currency || '-'
#                                         || CASE WHEN fd.total_amount >= 0 THEN '54' ELSE '04' END
#                                         || LPAD(CASE fd.CGL_TYPE
#                                         WHEN 'I' THEN bm.bgl_income
#                                         WHEN 'E' THEN bm.bgl_expense
#                                         WHEN 'T' THEN bm.bgl_total
#                                         END, 11, '0')
#                                         || '01111'
#                                         || LPAD(ABS(ROUND(fd.total_amount, 2)) * 100, 17, '0')
#                                         || ' FD POVISIONING ENTRIES' AS file_line
#                                         FROM final_data fd
#                                         JOIN bgl_map bm ON fd.currency = bm.currency
#                                         WHERE
#                                         bm.bgl_total IS NOT NULL
#                                         AND
#                                         CASE fd.CGL_TYPE
#                                         WHEN 'I' THEN bm.bgl_income
#                                         WHEN 'E' THEN bm.bgl_expense
#                                         WHEN 'T' THEN bm.bgl_total
#                                         END IS NOT NULL

#                                         UNION ALL

#                                         SELECT
#                                         'INR-'
#                                         || CASE WHEN inr.total_amount >= 0 THEN '04' ELSE '54' END
#                                         || LPAD(CASE inr.CGL_TYPE
#                                         WHEN 'I' THEN bm.bgl_income
#                                         WHEN 'E' THEN bm.bgl_expense
#                                         WHEN 'T' THEN bm.bgl_total
#                                         END, 11, '0')
#                                         || '01111'
#                                         || LPAD(ABS(ROUND(inr.total_amount, 2)) * 100, 17, '0')
#                                         || ' FD POVISIONING ENTRIES' AS file_line
#                                         FROM (
#                                         SELECT a.CGL_TYPE,
#                                         SUM(a.total_amount * cm.CURRENCY_RATE) AS total_amount
#                                         FROM aggregated a
#                                         JOIN currency_master cm ON a.currency = cm.currency_code
#                                         GROUP BY a.CGL_TYPE
#                                         UNION ALL
#                                         SELECT 'T',
#                                         SUM(a.total_amount * cm.CURRENCY_RATE) * -1 AS total_amount
#                                         FROM aggregated a
#                                         JOIN currency_master cm ON a.currency = cm.currency_code
#                                         ) inr
#                                         JOIN bgl_map bm ON bm.currency = 'INR'
#                                         WHERE
#                                         bm.bgl_total IS NOT NULL
#                                         AND
#                                         CASE inr.CGL_TYPE
#                                         WHEN 'I' THEN bm.bgl_income
#                                         WHEN 'E' THEN bm.bgl_expense
#                                         WHEN 'T' THEN bm.bgl_total
#                                         END IS NOT NULL

#                                         ORDER BY 1) e""" 
    
#     df = read_oracle(spark,query)
#     df = df.withColumns({
#         "CURRENCY" : substring("FILE_LINE", 1,3),
#         "TFS" : substring("FILE_LINE", 5, 65)
#     })

#     df.show(20,False)

    
#     file_path = f"{HDFS_BASE}/druid-data-lake/{today_dates['year']}/{today_dates['month']}/{today_dates['day']}/GLIF/FCNB_TFS/"
#     create_path_if_not_exists(spark, file_path)

#     currencies = [row.CURRENCY for row in df.select("CURRENCY").distinct().collect()]
#     print(currencies)   

#     for currency in currencies:
#         # Filter DF for specific currency
#         df_currency = df.filter(F.col("CURRENCY") == currency)  
#         df_currency = df.select("TFS")  

                      
#         df_currency.show(10,False)
#         write_text_with_filename(df_currency,file_path,f"TFS_01111_{today_dates['date_file_name']}_000000_{today_dates['date_file_name']}_{currency}.txt")

#         # Eg
#         # Type of Txn.     2       54  
#         # Account number   17      000+00+997694 +01111+ 0
#         # Amount           14+2    0000000148411548
#         # Narration Text   50      asdfghjkl;*5
#         # AUD
#         # 04 + 
#         # 000 + 00 + 397734 + 01111
#         # 00000000020445141
#         # FD POVISIONING ENTRIES

