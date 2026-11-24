# def createDir(spark,path):
#     try:
#         hadoop_conf = spark._jsc.hadoopConfiguration()
#         fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    
#         Path = spark._jvm.org.apache.hadoop.fs.Path
    
#         etl_path_obj = Path(etl_folder)
#         filetype_path_obj = Path(target_delta_path)
    
#         # creating ETL folder
#         if not fs.exists(etl_path_obj):
#             fs.mkdirs(etl_path_obj)
#             print(f"Created ETL folder: {etl_folder}")
#         else:
#             print(f"ETL folder already exists: {etl_folder}")
#     except Exception as e:
#         return spark.stop()
import logging

def create_hdfs_dir(spark, hdfs_path: str, logger=None) -> bool:
    """
    Production-grade HDFS directory creator.
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    try:
        # Access the gateway via sparkContext
        sc = spark.sparkContext
        gateway = sc._gateway
        
        # Get Hadoop Configuration and Java classes
        hadoop_conf = sc._jsc.hadoopConfiguration()
        Path = gateway.jvm.org.apache.hadoop.fs.Path
        FileSystem = gateway.jvm.org.apache.hadoop.fs.FileSystem
        
        fs = FileSystem.get(hadoop_conf)
        dir_obj = Path(hdfs_path)

        if not fs.exists(dir_obj):
            # Success is a boolean returned by mkdirs
            success = fs.mkdirs(dir_obj)
            if success:
                logger.info(f"HDFS directory created: {hdfs_path}")
            else:
                logger.error(f"HDFS mkdirs failed for: {hdfs_path}")
            return success
        else:
            logger.info(f"HDFS directory already exists: {hdfs_path}")
            return True

    except Exception as e:
        logger.error(f"Critical error managing HDFS directory: {str(e)}")
        return False
