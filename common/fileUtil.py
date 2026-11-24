


from common.dateUtil import get_etl_date
from common.properties import get_jdbc_url
from common.properties import get_oracle_properties

from common.properties import get_hdfs_base
from datetime import datetime
from py4j.java_gateway import java_import
HDFS_BASE = get_hdfs_base()

# TYPE For  GLIF -> glif for CBS -> bancs24
def files_to_read(spark,type):
    jdbc_url= get_jdbc_url()
    oracle_properties = get_oracle_properties()
    query = f"""
    (
    SELECT FILE_PATTERN, STREAM
    FROM FILE_TYPE_MASTER
    WHERE INCLUDE = 'Y' and type = '{type}'
    ) t
    """
    df = spark.read.jdbc(jdbc_url, query, properties=oracle_properties)
    if type.lower() == "glif":    
        stream_files = df.filter(df.STREAM == 'Y').select("FILE_PATTERN").rdd.flatMap(lambda x: x).collect()
        non_stream_files = df.filter(df.STREAM == 'N').select("FILE_PATTERN").rdd.flatMap(lambda x: x).collect()
        return {"stream_files" :stream_files,"non_stream_files":non_stream_files}
    elif type.lower() == "bancs24":
        files = df.select("FILE_PATTERN").rdd.flatMap(lambda x: x).collect()
        return files
    else :
        return []
    
    
def paths_for_read(type,date,today_str):
    today_str=today_str
    HDFS_BASE = get_hdfs_base()
    manifest_base=f"{HDFS_BASE}/ManifestFolder"
    
    if type.lower() == "glif":        
        return{
        "HDFS_BASE" : HDFS_BASE,
        "HDFS_PATH" : f"{HDFS_BASE}",
        "GL_DATALAKE_PATH" : f"{HDFS_BASE}/data-lake/Fincore/Data-Lake/GLIF",
        "manifest_filename" : f"{type}_{today_str}_glif_balance_manifest.txt",
        "spark_stream_path" : f"{HDFS_BASE}/DATA-LAKE/MANIFEST-FILES",
        "PRECHECK_DATALAKE_PATH":f"{HDFS_BASE}/data-lake/Fincore/Data-Lake/GLIF",
        #"PRECHECK_DATALAKE_PATH":f"{HDFS_BASE}/Precheck/Datalake/GLIF",
        "MANIFEST_BASE_PATH":manifest_base, 
        }
    if type.lower() == "bancs24":
        return{
        "HDFS_BASE" : HDFS_BASE,
        "HDFS_PATH" : f"{HDFS_BASE}/CBS-FILES/31-12-2025/BANCS24",
        "CBS_BALANCE_DATALAKE_PATH" : f"{HDFS_BASE}/data-lake/Fincore/Data-Lake/BANCS24",
        "manifest_filename" : f"{type}_{today_str}_cbs_balance_manifest.txt",
        "spark_stream_path" : f"{HDFS_BASE}/DATA-LAKE/MANIFEST-FILES/"
        }
    if type.lower() == "cbs":
        return{
        "HDFS_BASE" : HDFS_BASE,
        "HDFS_PATH" : f"{HDFS_BASE}/CBS-FILES/31-12-2025/BANCS24",
        "CBS_BALANCE_DATALAKE_PATH" : f"{HDFS_BASE}/data-lake/Fincore/Data-Lake/BANCS24",
        "manifest_filename" : f"{type}_{today_str}_cbs_balance_manifest.txt",
        "spark_stream_path" : f"{HDFS_BASE}/DATA-LAKE/MANIFEST-FILES/",
        "MANIFEST_BASE_PATH":manifest_base, 
        }
    if type.lower() == "difference":        
        return{
        "DIFFERENCE_DATALAKE_PATH" : f"{HDFS_BASE}/data-lake/Fincore/Data-Lake/DIFFERENCE_DATALAKE",
        "CBS_DATALAKE_PATH" : f"{HDFS_BASE}/data-lake/Fincore/Data-Lake/BANCS24",
        "GL_DATALAKE_PATH" : f"{HDFS_BASE}/data-lake/Fincore/Data-Lake/GLIF",
        "PRECHECK_DATALAKE_PATH":f"{HDFS_BASE}/data-lake/Fincore/Data-Lake/DIFFERENCE_DATALAKE",
        #"PRECHECK_DATALAKE_PATH":f"{HDFS_BASE}/Precheck/Datalake/DIFFERENCE",
        "REPORTS":f"{HDFS_BASE}/reports/diffrencereport",
        "MANIFEST_BASE_PATH":manifest_base,
        }
    if type.lower() == "controll":   
        clean_date = today_str.replace("-", "")
        formatedDate = datetime.strptime(clean_date, '%Y%m%d').strftime('%d-%m-%Y')     
        return{
        "HDFS_BASE" : HDFS_BASE,     
        "HDFS_PATH" : f"{HDFS_BASE}/CONTROL/{formatedDate}",
        "manifest_filename" : f"{type}_{today_str}_controll_manifest.txt",
        "spark_stream_path" : f"{HDFS_BASE}/DATA-LAKE/MANIFEST-FILES",
        "checkpt": f"{HDFS_BASE}/checkpoint/{today_str}/controll-chk/checkpoint",
        }
    if type.lower() == "gliftotalfiles": 
        clean_date = today_str.replace("-", "")
        formatedDate = datetime.strptime(clean_date, '%Y%m%d').strftime('%d-%m-%Y')       
        return{
        "HDFS_BASE" : HDFS_BASE,     
        "HDFS_PATH" : f"{HDFS_BASE}/CBS-FILES/{formatedDate}/GLIF",
        "checkpt": f"{HDFS_BASE}/checkpoint/{today_str}/glif-chk/checkpoint",
        "manifest_filename" : f"{type}_{today_str}_controll_manifest.txt",
        "spark_stream_path" : f"{HDFS_BASE}/DATA-LAKE/MANIFEST-FILES"
        }
    if type.lower() == "cbstotalfiles": 
        clean_date = today_str.replace("-", "")
        formatedDate = datetime.strptime(clean_date, '%Y%m%d').strftime('%d-%m-%Y')       
        return{
        "HDFS_BASE" : HDFS_BASE,     
        "HDFS_PATH" : f"{HDFS_BASE}/CBS-FILES/{formatedDate}/BANCS24",
        "checkpt": f"{HDFS_BASE}/checkpoint/{today_str}/cbs-chk/checkpoint",
        "manifest_filename" : f"{type}_{today_str}_controll_manifest.txt",
        "spark_stream_path" : f"{HDFS_BASE}/DATA-LAKE/MANIFEST-FILES"
        }
    else:
        return {}   

def tables_to_read(type):
    if(type.lower() == "glif"):
        return{
            "TRANSACTIONS" : "GL_TRANSACTIONS",
            "BALANCE" : "GL_BALANCE",
            "INVALID" : "GL_INVALID",
            "CGLS" : "CGL_MASTER",
            "CURRENCY" : "CURRENCY_MASTER",
            "BRANCH" : "BRANCH_MASTER",
            "CIRCLE" : "CIRCLE_MASTER"
        }
    if type.lower() == "bancs24":
        return{
            "BALANCE" : "CBS_BALANCE"
        }
    if type.lower() == "difference":
        return{
            "DIFFERENCE" : "DIFFERENCE",
            "GL_BALANCE" : "GL_BALANCE",
            "CBS_BALANCE" : "CBS_BALANCE"
        }
    if type.lower() == "controll":
        return{
            "CONTROLL_FILE_MASTER" : "CONTROLL_FILE_MASTER"
        }
        
def get_delta_path_by_file_type(file_type,etl_date_str):
    """
    Returns delta lake path based on FILE TYPE.
    """

    HDFS_BASE = get_hdfs_base()
    file_type = file_type.upper()

    if file_type == "GLIF":
        return f"{HDFS_BASE}/data-lake/bulk-data/{etl_date_str}/GLIF"

    elif file_type == "INV":
        return f"{HDFS_BASE}/data-lake/bulk-data/{etl_date_str}/CBS/INVTOGL"

    elif file_type == "BOR":
        return f"{HDFS_BASE}/data-lake/bulk-data/{etl_date_str}/CBS/BORTOGL"

    elif file_type == "CTA":
        return f"{HDFS_BASE}/data-lake/bulk-data/{etl_date_str}/CBS/GLCCCTA"

    elif file_type == "GEN":
        return f"{HDFS_BASE}/data-lake/bulk-data/{etl_date_str}/CBS/GLCCGEN"
    
    elif file_type == "GINV":
        return f"{HDFS_BASE}/data-lake/bulk-data/{etl_date_str}/CBS/GLCCINV"
    
    elif file_type == "RERUNCBS":
        return f"{HDFS_BASE}/data-lake/bulk-data/{etl_date_str}/CBS"
    else:
        raise Exception(f"Unsupported FILE TYPE: {file_type}")
        





       
def file_processing_paths(spark,type):
    HDFS_BASE = get_hdfs_base()
    today = get_etl_date(spark)
    date_str = today.strftime("%d-%m-%Y")
    etl_date = today.strftime("%Y-%m-%d")
    date_obj = datetime.strptime(date_str, "%d-%m-%Y")   
    year = date_obj.strftime("%Y")
    month = date_obj.strftime("%b").upper()
    day = date_obj.strftime("%d")

    if type.lower() == "glif_processing": 
               
        return{
        "HDFS_BASE" : HDFS_BASE,     
        "HDFS_FILES_PATH" : f"{HDFS_BASE}/CBS-FILES/{date_str}/GLIF",
        "GLIF_TXNS_DATALAKE" : f"{HDFS_BASE}/druid-data-lake/{year}/{month}/{day}/GLIF"

        }
    if type.lower() == "cbs_processing": 
               
        return{
        "HDFS_BASE" : HDFS_BASE,     
        "HDFS_FILES_PATH" : f"{HDFS_BASE}/CBS-FILES/{etl_date}/BANCS24",
        "BANCS_DATALAKE" : f"{HDFS_BASE}/druid-data-lake/{year}/{month}/{day}/BANCS24"

        }
    

def write_text_with_filename(df, output_dir, filename,  mode="overwrite"):

   temp_dir = f"{output_dir}/_tmp_{filename}"
   
   (df.coalesce(1).write.mode(mode).text(temp_dir))
   sc = df.sparkSession.sparkContext
   java_import(sc._jvm, "org.apache.hadoop.fs.Path")
   fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
   temp_path = sc._jvm.Path(temp_dir)
   final_path = sc._jvm.Path(f"{output_dir}/{filename}")   
   if fs.exists(final_path):
       fs.delete(final_path, False)   
   for status in fs.listStatus(temp_path):
       name = status.getPath().getName()
       if name.startswith("part-"):
           fs.rename(status.getPath(), final_path)
           break
   
   fs.delete(temp_path, True)

def write_csv_with_filename(df, output_dir, filename, header=True, mode="overwrite", sep=",", **options):
    """Writes a single partitioned file with a custom output filename into HDFS."""
    temp_dir = f"{output_dir}/_tmp_{filename}"
    final_file_path = f"{output_dir}/{filename}"
    
    (df.coalesce(1)
     .write
     .mode(mode)
     .option("header", str(header).lower())
     .option("sep", sep)
     .options(**options)
     .csv(temp_dir))
    
    sc = df.sparkSession.sparkContext
    Path = sc._jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
    
    fs = FileSystem.get(sc._jsc.hadoopConfiguration())
    temp_path = Path(temp_dir)
    final_path = Path(final_file_path)
    
    if fs.exists(final_path):
        fs.delete(final_path, True)
        
    for status in fs.listStatus(temp_path):
        name = status.getPath().getName()
        if name.startswith("part-"):
            fs.rename(status.getPath(), final_path)
            break
            
    fs.delete(temp_path, True)

def write_multiple_csv_with_filename(df, output_dir, filename, header=True, mode="overwrite", max_rows=1000000):

    temp_dir = f"{output_dir}/_tmp_{filename}"

    # 1. Write to temporary directory using maxRecordsPerFile

    (df.coalesce(1)

     .write

     .mode(mode)

     .option("header", str(header).lower())

     .option("maxRecordsPerFile", max_rows)

     .csv(temp_dir))

    sc = df.sparkSession.sparkContext

    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())

    temp_path = sc._jvm.Path(temp_dir)

    # 2. Extract base filename and extension (e.g., 'data.csv' -> 'data' and '.csv')

    if "." in filename:

        base_name, ext = filename.rsplit(".", 1)

        ext = "." + ext

    else:

        base_name, ext = filename, ""

    # 3. Collect and sort all generated part files sequentially

    part_files = [

        status.getPath() 

        for status in fs.listStatus(temp_path) 

        if status.getPath().getName().startswith("part-")

    ]

    part_files.sort(key=lambda p: p.getName())

    # 4. Rename files sequentially

    if len(part_files) == 1:

        # If total rows <= 10 lakh, save with the original filename

        final_path = sc._jvm.Path(f"{output_dir}/{filename}")

        if fs.exists(final_path):

            fs.delete(final_path, False)

        fs.rename(part_files[0], final_path)

    else:

        # If > 10 lakh rows, create filename_1.csv, filename_2.csv, etc.

        for idx, part_path in enumerate(part_files, start=1):

            seq_filename = f"{base_name}_{idx}{ext}"

            final_path = sc._jvm.Path(f"{output_dir}/{seq_filename}")

            if fs.exists(final_path):

                fs.delete(final_path, False)

            fs.rename(part_path, final_path)

    # 5. Clean up temporary directory

    fs.delete(temp_path, True)
 
