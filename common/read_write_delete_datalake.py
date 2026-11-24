def delete_hdfs_path(spark, hdfs_path):
   """
   Deletes an HDFS file or directory recursively using the Hadoop FileSystem API.
   Args:
       spark (SparkSession): The active Spark session.
       hdfs_path (str): The HDFS URI to delete (e.g., 'hdfs://namenode:8020/user/data/path').
   Returns:
       bool: True if deleted successfully or didn't exist, False if deletion failed.
   """
   # 1. Access the Hadoop Path and FileSystem classes via PySpark's JVM
   Path = spark._jvm.org.apache.hadoop.fs.Path
   # 2. Get the Hadoop configuration from the current SparkContext
   conf = spark.sparkContext._jsc.hadoopConfiguration()
   # 3. Create a Path object for the target URI
   path_obj = Path(hdfs_path)
   # 4. Get the correct FileSystem instance for this path
   fs = path_obj.getFileSystem(conf)
   # 5. Check if the path exists before attempting deletion
   if fs.exists(path_obj):
       # The second argument (True) enables recursive deletion.
       # This is what allows it to delete directories and their contents.
       success = fs.delete(path_obj, True)
       if success:
           print(f"Successfully deleted: {hdfs_path}")
           return True
       else:
           print(f"Failed to delete: {hdfs_path}")
           return False
   else:
       print(f"Path does not exist (skipping): {hdfs_path}")
       return True
# Example Usage:
# delete_hdfs_path(spark, "/user/hive/warehouse/my_table_data")