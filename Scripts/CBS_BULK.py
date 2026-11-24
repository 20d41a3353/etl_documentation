import datetime 
from pyspark.sql import SparkSession , functions as F
import pyspark.sql.types as T

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import  to_date,expr, round, lower, col,when, length, trim,count, substring,trim, concat, lit, create_map, to_timestamp, sum,broadcast, coalesce,current_timestamp, first, input_file_name,create_map, to_timestamp, sum,broadcast, coalesce,current_timestamp, first, input_file_name,substring_index
from pyspark.sql.types import DecimalType, StringType, StructType, StructField, LongType, DateType, TimestampType
from datetime import date, timedelta
from pyspark.sql.functions import desc
import sys
from py4j.java_gateway import java_import
from delta.tables import DeltaTable 
import concurrent.futures
import builtins
from pyspark.sql import Row
from pyspark.sql.functions import expr, lpad, floor, concat_ws
from pyspark.sql.functions import ltrim, col

import re   
# =========== Common Methods =============================
from common.createSpark import create_spark_session
from common.fileUtil import get_hdfs_base
from datetime import datetime,date
from common.read_write_oracle import read_oracle
from common.create_path_if_not_exists import create_path_if_not_exists
from pyspark.sql.functions import col, trim, substring,regexp_replace
from collections import defaultdict
from common.pattern import read_patterns
from common.properties import get_oracle_properties
from common.dateUtil import get_etl_date

from common.log_etl_modified import log_etl
from common.constants import Process
from common.processRun import get_run_id
from common.processRun import get_process_run_id
from common.logger import setup_logger 

def parse_financial_amount(col_name, type_category):
    """
    Parses amounts based on a FIXED type category (e.g., 'INV', 'BOR').
    type_category is a Python string, NOT a DataFrame column.
    """
    try:

        amount_col = F.col(col_name)


        base_val = F.regexp_replace(amount_col, r"[-+]$", "").cast("decimal(25,4)")
        is_negative = F.substring(amount_col, -1, 1) == "-"

        signed_val = F.when(amount_col.isNull(), None) \
                      .when(is_negative, base_val * -1) \
                      .otherwise(base_val)


        if type_category in ["INV", "BOR"]:
            divisor = 1000
        else:
            divisor = 10000

        scaled_val = signed_val / divisor

    
        if type_category in ["BOR", "GLCCGEN", "GLCCCTA", "GLCCINV"]:

            final_val = scaled_val * -1

        else:
        
            final_val = scaled_val

        return final_val.cast("decimal(25,4)")
    except Exception as e:
        print(f"Error {e} occoured while parsing financial amounts.")

def main(type,spark,BASE_HDFS,etl_date,args,process_run_id):
    try:

        print(f"{type} Started Exexution")

        date_str = etl_date
        date_obj = datetime.strptime(date_str, "%d-%m-%Y")    
        year = date_obj.strftime("%Y")
        month = date_obj.strftime("%b").upper()
        day = date_obj.strftime("%d")

        posting_date_str = date_obj.strftime("%Y-%m-%d")

        output_date = date_obj.strftime("%d-%b-%y").upper()

        input_path = f"{BASE_HDFS}/CBS-FILES/{date_str}/BANCS24"
        output_path =  f"{BASE_HDFS}/druid-data-lake/{year}/{month}/{day}/BANCS24"

        print(f"====={year}============{month}==============={day}===============> is the date")

        log_etl(spark, process_run_id, f"CBS_Bulk_pipeline_started_{type}",1)


        query = f"""(select DISTINCT(FILE_NAME),HDFS_ROW_COUNT from controll_file_master where ETL_DATE = '{output_date}' and FILE_TYPE ='CBS'  and FILE_NAME like '{type}%' and HDFS_ROW_COUNT > 0) e""" 
        control_file_master_df = read_oracle(spark, query)

    
        # Extract the values into a list of Row objects, then pull the specific field
        my_list = [row['FILE_NAME'] for row in control_file_master_df.select('FILE_NAME').collect()]

        if  len(my_list)==0:
            log_etl(spark, process_run_id, f"contorol_file_missing_{type}",1)
            print(f"------------->Stopping Pipeline because control file master has no data for {type} selected date - {output_date} .<--------------")
            log_etl(spark, process_run_id, f"contorol_file_missing_{type}",3,f"Stopping Pipeline because control file master has no data for  {type}  selected date - {output_date}.")
            spark.stop()
            sys.exit(1)

        print(my_list)
        pattern = r'^(.*?)(\d{8})_([a-zA-Z]).*$'
        grouped = defaultdict(list)
        for fname in my_list:

            match = re.match(pattern, fname)
            if match:
                stream = match.group(3)
                grouped[stream].append(fname)
        all_paths = []
        for stream, files in grouped.items():
            paths = [f"{input_path}/{stream}/{ft}.gz" for ft in files]
            all_paths.extend(paths)

        found_paths,unfound_paths = read_patterns(spark,all_paths)

        if len(unfound_paths)>0:
            log_etl(spark, process_run_id, f"Found_Invalid_Paths_in_HDFS_{type}",1)
            print(f"Stopping pipeline Due to Unfound paths {unfound_paths}")
            log_etl(spark, process_run_id, f"Found_Invalid_Paths_in_HDFS_{type}",3, "Stopping pipeline due to unfound paths in HDFS", 0 , unfound_paths)
            spark.stop()
            sys.exit(1)
        
        log_etl(spark, process_run_id, f"CBS_Bulk_pipeline_started_{type}",2)

    except Exception as e:
        log_etl(spark, process_run_id, f"CBS_Bulk_pipeline_started_{type}",3)
        print(f"Error {e} occured while listing paths to read")
        spark.stop()
        sys.exit(1)


    try:
        log_etl(spark, process_run_id, f"Reading_Files_{type}",1)
        print(f"File reading Started for {type}.")
        df_raw = spark.read.format("text").load(found_paths).withColumn("FILE_NAME", regexp_replace(substring_index(input_file_name(), "/",-1), r"\.gz$",""))
        log_etl(spark, process_run_id, f"Reading_Files_{type}",2)
    except Exception as e:
        log_etl(spark, process_run_id, f"Reading_Files_{type}",3 ,f"Error {e} occured while reading files")
        print(f"Error {e} occured while reading the files")
        spark.stop()
        sys.exit(1)
  
    
    try:
        log_etl(spark, process_run_id, f"column_configuration_{type}",1)
        query_parm = f"""(select * from FILE_DATA_PARAMETERS where FILE_TYPE ='{type}') e"""
        parameters = read_oracle(spark, query_parm)
        metadata = parameters.collect()
        columns = ["FILE_NAME"]
        transformed_df = df_raw
        for row in metadata:
           if row["TO_BE_INCLUDED"] == "yes":
               column_name = row["COLUMN_NAME"]
               columns.append(column_name)
               start_pos = int(row["START_POSITION"])
               end_pos = int(row["END_POSITION"]) -1
               length = end_pos - start_pos + 1
               transformed_df = transformed_df.withColumn(
                   column_name,
                   trim(
                       substring(
                           col("value"),
                           start_pos,
                           length
                       )
                   )
               )
        transformed_df =transformed_df.drop("value")   
        log_etl(spark, process_run_id, f"column_configuration_{type}",2)
    except Exception as e:
        log_etl(spark, process_run_id, f"column_configuration_{type}",3,f"Error {e} occured while constructing Columns")
        print(f"Error {e} occured while parsing the raw data")
        spark.stop()
        sys.exit(1)
    
    try:
        log_etl(spark, process_run_id, f"column_parsing_{type}",1)
        if type == "INV":
            cols_to_convert=["BALANCE","INT_AVAILABLE","INT_ADJUSTMENT","OD_DR_INT_AVAIL","DR_INT_ADJUSTMENT","OD_PENALTY_INT_ACCR","OD_PENALTY_INT_ADJ","OD_DR_INT_INCR","INT_INCR"]
        if type == "BOR":
            print("BOR")
            cols_to_convert=["BALANCE","LOAN_TERM","APP_AMT","ADV_VAL","COLLECTION_AMT","INT_ACCR","BPI_ACCR","CI_ACCR","INT_ADJUSTMENT","ARR_INT_ACCR","ARR_INT_ADJUSTMENT"]
        if type in ("GLCCCTA","GLCCGEN"):
            cols_to_convert=["BALANCE"]
            transformed_df = transformed_df.withColumn("ACCOUNT",lit("NA"))
        if type == "GLCCINV":
            cols_to_convert=["BALANCE"]


        select_exprs = [
        parse_financial_amount(c, type).alias(c) if c in cols_to_convert else F.col(c)
        for c in transformed_df.columns
        ]   

        transformed_df = transformed_df.select(*select_exprs)

        transformed_df = transformed_df.withColumn("ACCOUNT", regexp_replace(col("ACCOUNT"), r"^0+", ""))   


        if type in ("GLCCCTA","GLCCGEN"):
            transformed_df= transformed_df.select(*columns,"ACCOUNT").withColumn("TRANS_DATE", to_date(lit(posting_date_str)))
        else:
            transformed_df= transformed_df.select(*columns).withColumn("TRANS_DATE", to_date(lit(posting_date_str)))
        log_etl(spark, process_run_id, f"column_parsing_{type}",2)
    except Exception as e:
        log_etl(spark, process_run_id, f"column_parsing_{type}",3,f"Error {e} occured")
        print(f"Error {e} occured while transformation of data")
        spark.stop()
        sys.exit(1)


    try:
        log_etl(spark, process_run_id, f"writing_to_datalake_{type}",1)
        table_path = f'{output_path}/{type}'
        res = create_path_if_not_exists(spark,table_path)
        print(res)

        transformed_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(table_path)
        print(f"[{type}] DATALAKE Writing Successfull.")
        log_etl(spark, process_run_id, f"writing_to_datalake_{type}",2)
    except Exception as e:
        log_etl(spark, process_run_id, f"writing_to_datalake_{type}",3, f"Error {e} occured.")
        print(f"Error {e} occured while writing to datalake table")
        spark.stop()
        sys.exit(1)

    # ==========================================================================================================================================
    # ==========================================================================================================================================
    try:
        
        df_raw_datalake = spark.read.format("delta").load(table_path)
        file_count_df = df_raw_datalake.groupBy("FILE_NAME").agg(count("*").alias("ROW_COUNT"))
        rows = file_count_df.agg(F.sum("ROW_COUNT")).collect()[0][0]

        comparison_df = (
            control_file_master_df.alias("c")
            .join(
                file_count_df.alias("f"),
                ["FILE_NAME"],
                "full_outer"
            )
            .select(
                col("FILE_NAME"),
                col("c.HDFS_ROW_COUNT").cast("decimal(25)"),
                col("f.ROW_COUNT").alias("DATA_LAKE_COUNT")
            )
        )



        comparison_df =comparison_df.withColumn("DATA_LAKE_STATUS",
                                                F.when(col("HDFS_ROW_COUNT")==col("DATA_LAKE_COUNT"),"YES")
                                                )
        existing_files_count = comparison_df.filter(col("DATA_LAKE_COUNT").isNotNull() & col("DATA_LAKE_STATUS").isNotNull())

        unmatched_files = comparison_df.filter(col("DATA_LAKE_COUNT").isNull() | col("DATA_LAKE_STATUS").isNull() | col("DATA_LAKE_STATUS").isNull())
        unmatch_count = unmatched_files.count()
        missing_unmatch = unmatched_files._jdf.showString(2000, 40, False)

        # =======================================
        try:
            oracle_properties = get_oracle_properties()
            conn = spark._sc._gateway.jvm.java.sql.DriverManager.getConnection(
                oracle_properties["url"],
                oracle_properties["user"],
                oracle_properties["password"]
            )
            conn.setAutoCommit(False)
            update_sql = """
            UPDATE CONTROLL_FILE_MASTER
            SET DATA_LAKE_COUNT = ?,
                DATA_LAKE_STATUS = ?
            WHERE FILE_NAME = ?
            AND ETL_DATE = ?
            """

            stmt = conn.prepareStatement(update_sql)
            log_etl(spark, process_run_id, f"updating_oracle_controll_file_master_{type}",1)
            for row in existing_files_count.collect():
                stmt.setInt(1, int(row["DATA_LAKE_COUNT"]))
                stmt.setString(2, row["DATA_LAKE_STATUS"])
                stmt.setString(3, row["FILE_NAME"])
                stmt.setString(4, output_date)
                stmt.addBatch()
            stmt.executeBatch()
            conn.commit()
            log_etl(spark, process_run_id, f"updating_oracle_controll_file_master_{type}",2)
            stmt.close()
            conn.close()
        except Exception as e:
            log_etl(spark, process_run_id, f"updating_oracle_controll_file_master_{type}",3)


        log_etl(spark, process_run_id, f"writing_SUMMARY_{type}",1)

        java_import(spark._jvm, "org.apache.hadoop.fs.FileSystem")
        java_import(spark._jvm, "org.apache.hadoop.fs.Path")
        fs = spark._jvm.FileSystem.get(spark._jsc.hadoopConfiguration())
        hdfs_dir = f"{output_path}/info/"    
        manifest_filename = f"{type}_MissPath_{len(unfound_paths)}_RowDiff_{unmatch_count}_Total_{rows}.txt"
        dest_file_path = spark._jvm.Path(f"{hdfs_dir}/{manifest_filename}")
        hdfs_dir_obj = spark._jvm.Path(hdfs_dir)
        if not fs.exists(hdfs_dir_obj):
            fs.mkdirs(hdfs_dir_obj)
        out_stream = fs.create(dest_file_path, True)
        unfound_paths= ", ".join([f"'{x}'" for x in unfound_paths])

        text_file = unfound_paths +"\n"+ missing_unmatch

        out_stream.write(bytearray(text_file, "utf-8"))
        out_stream.close()
        print(f"[{type}] updated oracle Successfull.")

        if unmatch_count > 0 or len(unfound_paths) > 0:
            if unmatch_count > 0:
                reason = "MissMatch RowCount"
            else:
                reason = "Unfound paths"

            print(f"Stopping Pipeline Due to {reason}. Check error file at path {dest_file_path}")
            print(text_file)
            spark.stop()
            sys.exit(1)
        log_etl(spark, process_run_id, f"writing_SUMMARY_{type}",2)
    except Exception as e:
        log_etl(spark, process_run_id, f"writing_SUMMARY_{type}",3, f"Error {e} occured while writing SUMMARY file.")        
        print(f"Error {e} occured while writing to oracle.")
        spark.stop()
        sys.exit(1)
 



if __name__ == "__main__":
    try:
         BASE_HDFS = get_hdfs_base()
         spark = create_spark_session("CBS - Bulk Data PIPELINE", BASE_HDFS)
         spark.conf.set("spark.sql.debug.maxToStringFields", "10000")   
         types = ["INV","BOR","GLCCGEN", "GLCCCTA", "GLCCINV"]   #  "INV","BOR","GLCCGEN", "GLCCCTA", "GLCCINV"
 
         date_format = "%d-%m-%Y"
         today = get_etl_date(spark)
         spark_date = today.strftime("%Y-%m-%d")
         posting_date_str = today.strftime("%d-%m-%Y")
         dates = [posting_date_str, posting_date_str]
         # dates = ["12-03-2026","12-03-2026"] 
         # dates = ["30-05-2026","30-05-2026"] 
         start_date = datetime.strptime(dates[0], date_format)
         end_date = datetime.strptime(dates[1], date_format)
         generated_dates = []
         current_date = start_date
         while current_date <= end_date:       
             generated_dates.append(current_date.strftime(date_format))        
             current_date += timedelta(days=1)
         print(generated_dates)
 
         PROCESS_ID= Process.CBS_ALL_DATA        
         RUN_ID = get_run_id(spark,spark_date,PROCESS_ID) 
         process_run_id = sys.argv[1] if len(sys.argv) > 1 else get_process_run_id(spark,PROCESS_ID,spark_date)
 
         print(f"Process Run ID for RUN {RUN_ID} is == > {process_run_id}")
 
 
         for etl_date in generated_dates:
             with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
              futures = [executor.submit(main, type, spark, BASE_HDFS,etl_date,sys.argv,process_run_id) for type in types]
              for future in concurrent.futures.as_completed(futures):
                  try:
                      future.result()
                  except Exception as e:
                      print(f"Job failed: {e}")
                      spark.stop()
                      sys.exit(1)
         spark.stop()
         sys.exit(0)             
    except Exception as e:
        print(f"Error {e} occured.")
        spark.stop()
        sys.exit(1)







































































# import datetime 
# from pyspark.sql import SparkSession , functions as F
# import pyspark.sql.types as T

# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
# from pyspark.sql.functions import  to_date,expr, round, lower, col,when, length, trim,count, substring,trim, concat, lit, create_map, to_timestamp, sum,broadcast, coalesce,current_timestamp, first, input_file_name,create_map, to_timestamp, sum,broadcast, coalesce,current_timestamp, first, input_file_name,substring_index
# from pyspark.sql.types import DecimalType, StringType, StructType, StructField, LongType, DateType, TimestampType
# from datetime import date, timedelta
# from pyspark.sql.functions import desc
# import sys
# from py4j.java_gateway import java_import
# from delta.tables import DeltaTable 
# import concurrent.futures
# import builtins
# from pyspark.sql import Row
# from pyspark.sql.functions import expr, lpad, floor, concat_ws
# from pyspark.sql.functions import ltrim, col

# import re   
# # =========== Common Methods =============================
# from common.createSpark import create_spark_session
# from common.fileUtil import get_hdfs_base
# from datetime import datetime,date
# from common.read_write_oracle import read_oracle
# from common.create_path_if_not_exists import create_path_if_not_exists
# from pyspark.sql.functions import col, trim, substring,regexp_replace
# from collections import defaultdict
# from common.pattern import read_patterns
# from common.properties import get_oracle_properties
# from common.dateUtil import get_etl_date



# def parse_financial_amount(col_name, type_category):
#     """
#     Parses amounts based on a FIXED type category (e.g., 'INV', 'BOR').
#     type_category is a Python string, NOT a DataFrame column.
#     """
#     try:

#         amount_col = F.col(col_name)


#         base_val = F.regexp_replace(amount_col, r"[-+]$", "").cast("decimal(25,4)")
#         is_negative = F.substring(amount_col, -1, 1) == "-"

#         signed_val = F.when(amount_col.isNull(), None) \
#                       .when(is_negative, base_val * -1) \
#                       .otherwise(base_val)


#         if type_category in ["INV", "BOR"]:
#             divisor = 1000
#         else:
#             divisor = 10000

#         scaled_val = signed_val / divisor

    
#         if type_category in ["BOR", "GLCCGEN", "GLCCCTA", "GLCCINV"]:

#             final_val = scaled_val * -1

#         else:
        
#             final_val = scaled_val

#         return final_val.cast("decimal(25,4)")
#     except Exception as e:
#         print(f"Error {e} occoured while parsing financial amounts.")

# def main(type,spark,BASE_HDFS,etl_date):
#     try:

#         print(f"{type} Started Exexution")

#         date_str = etl_date
#         date_obj = datetime.strptime(date_str, "%d-%m-%Y")    
#         year = date_obj.strftime("%Y")
#         month = date_obj.strftime("%b").upper()
#         day = date_obj.strftime("%d")

#         posting_date_str = date_obj.strftime("%Y-%m-%d")

#         output_date = date_obj.strftime("%d-%b-%y").upper()

#         input_path = f"{BASE_HDFS}/CBS-FILES/{date_str}/BANCS24"
#         output_path =  f"{BASE_HDFS}/druid-data-lake/{year}/{month}/{day}/BANCS24"

#         print(f"====={year}============{month}==============={day}===============> is the date")



#         query = f"""(select DISTINCT(FILE_NAME),HDFS_ROW_COUNT from controll_file_master where ETL_DATE = '{output_date}' and FILE_TYPE ='CBS'  and FILE_NAME like '{type}%' and HDFS_ROW_COUNT > 0) e""" 
#         control_file_master_df = read_oracle(spark, query)

    
#         # Extract the values into a list of Row objects, then pull the specific field
#         my_list = [row['FILE_NAME'] for row in control_file_master_df.select('FILE_NAME').collect()]

#         if  len(my_list)==0:
#             print(f"------------->Stopping Pipeline because control file master has no data for {type} type for the selected date - {output_date} .<--------------")
#             spark.stop()
#             sys.exit(1)

#         print(my_list)
#         pattern = r'^(.*?)(\d{8})_([a-zA-Z]).*$'
#         grouped = defaultdict(list)
#         for fname in my_list:

#             match = re.match(pattern, fname)
#             if match:
#                 stream = match.group(3)
#                 grouped[stream].append(fname)
#         all_paths = []
#         for stream, files in grouped.items():
#             paths = [f"{input_path}/{stream}/{ft}.gz" for ft in files]
#             all_paths.extend(paths)

#         found_paths,unfound_paths = read_patterns(spark,all_paths)

#         if len(unfound_paths)>0:
#             print(f"Stopping pipeline Due to Unfound paths {unfound_paths}")
#             return []
#     except Exception as e:
#         print(f"Error {e} occured while listing paths to read")


#     try:
#         print(f"File reading Started for {type}.")
#         df_raw = spark.read.format("text").load(found_paths).withColumn("FILE_NAME", regexp_replace(substring_index(input_file_name(), "/",-1), r"\.gz$",""))

#     except Exception as e:
#         print(f"Error {e} occured while reading the files")
  
    
#     try:

#         query_parm = f"""(select * from FILE_DATA_PARAMETERS where FILE_TYPE ='{type}') e"""
#         parameters = read_oracle(spark, query_parm)
#         metadata = parameters.collect()
#         columns = ["FILE_NAME"]
#         transformed_df = df_raw
#         for row in metadata:
#            if row["TO_BE_INCLUDED"] == "yes":
#                column_name = row["COLUMN_NAME"]
#                columns.append(column_name)
#                start_pos = int(row["START_POSITION"])
#                end_pos = int(row["END_POSITION"]) -1
#                length = end_pos - start_pos + 1
#                transformed_df = transformed_df.withColumn(
#                    column_name,
#                    trim(
#                        substring(
#                            col("value"),
#                            start_pos,
#                            length
#                        )
#                    )
#                )
#         transformed_df =transformed_df.drop("value")   
#     except Exception as e:
#         print(f"Error {e} occured while parsing the raw data")
    
#     try:

#         if type == "INV":
#             cols_to_convert=["BALANCE","INT_AVAILABLE","INT_ADJUSTMENT","OD_DR_INT_AVAIL","DR_INT_ADJUSTMENT","OD_PENALTY_INT_ACCR","OD_PENALTY_INT_ADJ","OD_DR_INT_INCR","INT_INCR"]
#         if type == "BOR":
#             print("BOR")
#             cols_to_convert=["BALANCE","LOAN_TERM","APP_AMT","ADV_VAL","COLLECTION_AMT","INT_ACCR","BPI_ACCR","CI_ACCR","INT_ADJUSTMENT","ARR_INT_ACCR","ARR_INT_ADJUSTMENT"]
#         if type in ("GLCCCTA","GLCCGEN"):
#             cols_to_convert=["BALANCE"]
#             transformed_df = transformed_df.withColumn("ACCOUNT",lit("NA"))
#         if type == "GLCCINV":
#             cols_to_convert=["BALANCE"]


#         select_exprs = [
#         parse_financial_amount(c, type).alias(c) if c in cols_to_convert else F.col(c)
#         for c in transformed_df.columns
#         ]   

#         transformed_df = transformed_df.select(*select_exprs)

#         transformed_df = transformed_df.withColumn("ACCOUNT", regexp_replace(col("ACCOUNT"), r"^0+", ""))   


#         if type in ("GLCCCTA","GLCCGEN"):
#             transformed_df= transformed_df.select(*columns,"ACCOUNT").withColumn("TRANS_DATE", to_date(lit(posting_date_str)))
#         else:
#             transformed_df= transformed_df.select(*columns).withColumn("TRANS_DATE", to_date(lit(posting_date_str)))

#     except Exception as e:
#         print(f"Error {e} occured while transformation of data")


#     try:

#         table_path = f'{output_path}/{type}'
#         res = create_path_if_not_exists(spark,table_path)
#         print(res)

#         transformed_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(table_path)
#         print(f"[{type}] DATALAKE Writing Successfull.")
#     except Exception as e:
#         print(f"Error {e} occured while writing to datalake table")

#     # ==========================================================================================================================================
#     # ==========================================================================================================================================
#     try:

#         df_raw_datalake = spark.read.format("delta").load(table_path)
#         file_count_df = df_raw_datalake.groupBy("FILE_NAME").agg(count("*").alias("ROW_COUNT"))
#         rows = file_count_df.agg(F.sum("ROW_COUNT")).collect()[0][0]

#         comparison_df = (
#             control_file_master_df.alias("c")
#             .join(
#                 file_count_df.alias("f"),
#                 ["FILE_NAME"],
#                 "full_outer"
#             )
#             .select(
#                 col("FILE_NAME"),
#                 col("c.HDFS_ROW_COUNT").cast("decimal(25)"),
#                 col("f.ROW_COUNT").alias("DATA_LAKE_COUNT")
#             )
#         )



#         comparison_df =comparison_df.withColumn("DATA_LAKE_STATUS",
#                                                 F.when(col("HDFS_ROW_COUNT")==col("DATA_LAKE_COUNT"),"YES")
#                                                 )
#         existing_files_count = comparison_df.filter(col("DATA_LAKE_COUNT").isNotNull() & col("DATA_LAKE_STATUS").isNotNull())

#         unmatched_files = comparison_df.filter(col("DATA_LAKE_COUNT").isNull() | col("DATA_LAKE_STATUS").isNull() | col("DATA_LAKE_STATUS").isNull())
#         unmatch_count = unmatched_files.count()
#         missing_unmatch = unmatched_files._jdf.showString(2000, 40, False)

#         # =======================================
#         oracle_properties = get_oracle_properties()
#         conn = spark._sc._gateway.jvm.java.sql.DriverManager.getConnection(
#             oracle_properties["url"],
#             oracle_properties["user"],
#             oracle_properties["password"]
#         )
#         conn.setAutoCommit(False)
#         update_sql = """
#         UPDATE CONTROLL_FILE_MASTER
#         SET DATA_LAKE_COUNT = ?,
#             DATA_LAKE_STATUS = ?
#         WHERE FILE_NAME = ?
#         AND ETL_DATE = ?
#         """

#         stmt = conn.prepareStatement(update_sql)
#         for row in existing_files_count.collect():
#             stmt.setInt(1, int(row["DATA_LAKE_COUNT"]))
#             stmt.setString(2, row["DATA_LAKE_STATUS"])
#             stmt.setString(3, row["FILE_NAME"])
#             stmt.setString(4, output_date)
#             stmt.addBatch()
#         stmt.executeBatch()
#         conn.commit()
#         stmt.close()
#         conn.close()

#         java_import(spark._jvm, "org.apache.hadoop.fs.FileSystem")
#         java_import(spark._jvm, "org.apache.hadoop.fs.Path")
#         fs = spark._jvm.FileSystem.get(spark._jsc.hadoopConfiguration())
#         hdfs_dir = f"{output_path}/info/"    
#         manifest_filename = f"{type}_MissPath_{len(unfound_paths)}_RowDiff_{unmatch_count}_Total_{rows}.txt"
#         dest_file_path = spark._jvm.Path(f"{hdfs_dir}/{manifest_filename}")
#         hdfs_dir_obj = spark._jvm.Path(hdfs_dir)
#         if not fs.exists(hdfs_dir_obj):
#             fs.mkdirs(hdfs_dir_obj)
#         out_stream = fs.create(dest_file_path, True)
#         unfound_paths= ", ".join([f"'{x}'" for x in unfound_paths])

#         text_file = unfound_paths +"\n"+ missing_unmatch

#         out_stream.write(bytearray(text_file, "utf-8"))
#         out_stream.close()
#         print(f"[{type}] updated oracle Successfull.")

#         if unmatch_count > 0 or len(unfound_paths) > 0:
#             if unmatch_count > 0:
#                 reason = "MissMatch RowCount"
#             else:
#                 reason = "Unfound paths"

#             print(f"Stopping Pipeline Due to {reason}. Check error file at path {dest_file_path}")
#             print(text_file)
#             spark.stop()
#             sys.exit(1)

#     except Exception as e:
#         print(f"Error {e} occured while writing to oracle.")
 



# if __name__ == "__main__":
    
#    BASE_HDFS = get_hdfs_base()
#    spark = create_spark_session("CBS - Bulk Data PIPELINE", BASE_HDFS)
#    spark.conf.set("spark.sql.debug.maxToStringFields", "10000")   
#    types = ["INV","BOR","GLCCGEN", "GLCCCTA", "GLCCINV"]   #  "INV","BOR","GLCCGEN", "GLCCCTA", "GLCCINV"
   
#    date_format = "%d-%m-%Y"
#    today = get_etl_date(spark)
#    posting_date_str = today.strftime("%d-%m-%Y")
#    dates = [posting_date_str, posting_date_str]
#    # dates = ["12-03-2026","12-03-2026"] 
# #    dates = ["30-05-2026","30-05-2026"] 
#    start_date = datetime.strptime(dates[0], date_format)
#    end_date = datetime.strptime(dates[1], date_format)
#    generated_dates = []
#    current_date = start_date
#    while current_date <= end_date:       
#        generated_dates.append(current_date.strftime(date_format))        
#        current_date += timedelta(days=1)
#    print(generated_dates)


#    for etl_date in generated_dates:
#        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
#         futures = [executor.submit(main, type, spark, BASE_HDFS,etl_date) for type in types]
#         for future in concurrent.futures.as_completed(futures):
#             try:
#                 future.result()
#             except Exception as e:
#                 print(f"Job failed: {e}")