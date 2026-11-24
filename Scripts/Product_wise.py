

import datetime
from datetime import datetime, timedelta

from common.branch_util import get_drill_down_branch_mapping
from common.report_generation_mapping import get_nwsa_bsa_mapping_list
from pyspark.sql import functions as F
from pyspark.sql.functions import array_join, col, concat, lit, substring

from common.create_path_if_not_exists import create_path_if_not_exists

# =========== Common Methods =============================
from common.createSpark import create_spark_session
from common.dateUtil import get_etl_date
from common.fileUtil import get_hdfs_base
import sys

def main(spark,BASE_HDFS,etl_date):
    print("hello update")

    date_str = etl_date

    date_obj = datetime.strptime(date_str, "%d-%m-%Y")    
    year = date_obj.strftime("%Y")
    month = date_obj.strftime("%b").upper()
    day = date_obj.strftime("%d")

    date_obj = datetime.strptime(date_str, "%d-%m-%Y")

    output_date = date_obj.strftime("%d-%b-%y").upper()

    print(output_date)

    # +++++++++++++++++++++++++++++++++++++++++++
    # +++++++++++++ PATHS +++++++++++++++++++++++ 121483603
    # +++++++++++++++++++++++++++++++++++++++++++
    output_path =  f"{BASE_HDFS}/druid-data-lake/{year}/{month}/{day}/PRODUCT_WISE"
    
    raw_data_path = f"{BASE_HDFS}/druid-data-lake/{year}/{month}/{day}/BANCS24"

    columns = ["GLCC","BALANCE","ACCT_TYPE","INT_CAT"]
    
    INV_data = spark.read.format("delta").load(f"{raw_data_path}/INV").select(*columns).withColumn("TYPE",lit("INV"))
    BOR_data = spark.read.format("delta").load(f"{raw_data_path}/BOR").select(*columns).withColumn("TYPE",lit("BOR"))
    INV_data.cache()
    BOR_data.cache()

    union_df = INV_data.union(BOR_data)

    filtered_df = union_df.withColumns({        
        "PRODUCT": concat(col("ACCT_TYPE"),col("INT_CAT"))
    }).select("GLCC","PRODUCT","BALANCE","TYPE")

    filtered_df = filtered_df.withColumns({
           "BRANCH_CODE": substring(col("GLCC"), 1, 5),
        "CURRENCY": substring(col("GLCC"), 6, 3),
        "CGL": substring(col("GLCC"), 9, 10)})

    filtered_df.cache()
    
    transformed_df = filtered_df.groupBy("BRANCH_CODE","CURRENCY","CGL","PRODUCT","TYPE").agg(
        F.sum(col("BALANCE")).alias("BALANCE"),
       ).withColumn("DATE", (F.lit(etl_date)))   
    
    datalake_path = f"{output_path}/prod_wise_datalake"
    res = create_path_if_not_exists(spark,datalake_path)
    print(res)
    
    branch_mapping = get_drill_down_branch_mapping(spark)
    circle_mapped_df = transformed_df.join(branch_mapping, on="BRANCH_CODE", how="left")

    head_mapping = get_nwsa_bsa_mapping_list(spark).select(col("CGL"),col("PNL_HEADCODE"),col("NWSA_HEADCODE"),col("YSA_HEADCODE"))
    final_df = circle_mapped_df.join(head_mapping, on="CGL",how="left")
    final_df.cache()
    
    
    final_df.write.format("delta").mode("overwrite").save(datalake_path)

    # final_df.write.format("delta").write \
    #                     .mode("overwrite") \
    #                     .option("mergeSchema", "true") \
    #                     .option("replaceWhere", f"DATE = '{date_obj}' AND TYPE  IN ('INV','BOR')") \
    #                     .save(datalake_path)
    # final_df.show(20,False)

    # print(f"Total count of rows ==== {final_df.count()}")   
    # csv_df = final_df.select("CGL","BRANCH_CODE","CURRENCY","PRODUCT","TYPE","BALANCE","DATE","CIRCLE_CODE","CIRCLE_NAME","BRANCH_TYPE",array_join(col("PNL_HEADCODE"),"/").alias("PNL_HEADCODE"),array_join(col("YSA_HEADCODE"),"/").alias("YSA_HEADCODE"),array_join(col("NWSA_HEADCODE"),"/").alias("NWSA_HEADCODE"))
    # # write_csv_with_filename(csv_df,csv_path,"extract1.csv")
    # write_multiple_csv_with_filename(csv_df,csv_path,"extract1.csv")
    # print(csv_path)
    cgl_dict = filtered_df.groupBy("CURRENCY","CGL","PRODUCT","TYPE").agg(  #"SEGMENT",
                F.sum(col("BALANCE")).alias("BALANCE"),
               ).withColumn("DATE", (F.lit(etl_date)))    
    
    cgl_dict_path = f"{output_path}/cgl_dictionary_datalake"
    
    create_path_if_not_exists(spark,cgl_dict_path)
    cgl_dict = cgl_dict.select("CGL","CURRENCY","PRODUCT","BALANCE","TYPE","DATE")
    cgl_dict.write.format("delta").mode("overwrite").save(cgl_dict_path)
    print(f"+++++++++++++++++++++++++++++++++++++Success++++++++++++A+++++++++++B++++++++++++{date_str}")


if __name__ == "__main__":
   
    BASE_HDFS = get_hdfs_base()
    
    spark = create_spark_session("PRODUCT WISE PIPELINE", BASE_HDFS)
    today = get_etl_date(spark)
    posting_date_str = today.strftime("%d-%m-%Y")
    dates = [posting_date_str, posting_date_str]

    date_format = "%d-%m-%Y"
    # dates = ["30-06-2026","30-06-2026"] 
    # dates = ["01-06-2026","01-06-2026"]   #[From_Date, To_Date]  
    
    start_date = datetime.strptime(dates[0], date_format)
    end_date = datetime.strptime(dates[1], date_format)
    
    generated_dates = []
   
    current_date = start_date

    while current_date <= end_date:       
        generated_dates.append(current_date.strftime(date_format))        
        current_date += timedelta(days=1)
    
    print(generated_dates)


    try:
        for etl_date in generated_dates:
            main(spark,BASE_HDFS,etl_date)
            
        spark.stop()
        sys.exit(0)
    except Exception as e:
        spark.stop()
        sys.exit(1)