import datetime
from datetime import datetime, timedelta
import sys
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col,
    concat,
)

# =========== Common Methods =============================
from common.create_path_if_not_exists import create_path_if_not_exists
from common.createSpark import create_spark_session
from common.fileUtil import get_hdfs_base
from common.dateUtil import get_etl_date


def main(spark,BASE_HDFS,etl_date):
    print("hello update")

    date_str = etl_date

    date_obj = datetime.strptime(date_str, "%d-%m-%Y")    
    year = date_obj.strftime("%Y")
    month = date_obj.strftime("%b").upper()
    day = date_obj.strftime("%d")
    
    output_date = date_obj.strftime("%d-%b-%y").upper()

    print(output_date)

    # +++++++++++++++++++++++++++++++++++++++++++
    # +++++++++++++ PATHS +++++++++++++++++++++++ 121483603
    # +++++++++++++++++++++++++++++++++++++++++++
    output_path =  f"{BASE_HDFS}/druid-data-lake/{year}/{month}/{day}/TXNS_WISE"
    
    raw_data_path = f"{BASE_HDFS}/druid-data-lake/{year}/{month}/{day}/GLIF/GLIF_TXNS"

    columns = ["BRANCH_CODE","CURRENCY","CGL","TRANSACTION_CODE","AMOUNT"]
    GLIF_TXNS = spark.read.format("delta").load(f"{raw_data_path}").select(*columns).withColumn("GLCC", concat(col("BRANCH_CODE"), col("CURRENCY"), col("CGL")))
    
   

    transformed_df = GLIF_TXNS.groupBy("GLCC","TRANSACTION_CODE").agg(
        F.sum(col("AMOUNT")).alias("AMOUNT"),
        ).withColumn("DATE", (F.lit(etl_date))).select("GLCC","TRANSACTION_CODE","AMOUNT","DATE")
    path = f"{output_path}/datalake/"
    res = create_path_if_not_exists(spark,path)
    print(res)
    print(date_obj)
    
    transformed_df.write.format("delta").mode("overwrite").option(
                "replaceWhere", f"DATE = '{etl_date}'"
            ).save(path)
    print(f"+++++++++++++++++++++++++++++++++++++Success++++++++++++A+++++++++++B++++++++++++{date_str}")

if __name__ == "__main__":
   
    BASE_HDFS = get_hdfs_base()
    spark = create_spark_session("tRA PIPELINE", BASE_HDFS)
    today = get_etl_date(spark)
    posting_date_str = today.strftime("%d-%m-%Y")
    dates = [posting_date_str, posting_date_str]

    date_format = "%d-%m-%Y"
    # dates = ["01-06-2026", "01-06-2026"]   #[From_Date, To_Date]
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



































