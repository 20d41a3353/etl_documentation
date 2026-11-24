from py4j.java_gateway import java_import

def create_path_if_not_exists(spark, path , permission="777"):
    """
    Checks if the given HDFS/local path exists.
    If not, creates the directory structure.
    """
    sc = spark.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()
    java_import(sc._gateway.jvm, "org.apache.hadoop.fs.Path")
    java_import(sc._gateway.jvm, "org.apache.hadoop.fs.FileSystem")
    java_import(sc._gateway.jvm, "org.apache.hadoop.fs.permission.FsPermission")
    fs = sc._gateway.jvm.FileSystem.get(hadoop_conf)
    hdfs_path = sc._gateway.jvm.Path(path)
    if not fs.exists(hdfs_path):
        fs.mkdirs(hdfs_path)
        fs.setPermission(hdfs_path,sc._gateway.jvm.FsPermission(permission))
        print(f"Created path: {path}")
    else:
        print(f"Path already exists: {path}")