from pyspark.sql import SparkSession

def create_spark_session(app_name, hdfs_url=None):

    name_space = "fincore"

    return (
        SparkSession.builder.appName(app_name)
        # Prefix all Hadoop HA configurations with spark.hadoop.
        .config("spark.hadoop.fs.defaultFS", f"hdfs://{name_space}")
        .config(f"spark.hadoop.dfs.nameservices", name_space)
        .config(f"spark.hadoop.dfs.ha.namenodes.{name_space}", "nn1,nn2,nn3,nn4,nn5")
        # Unique RPC address for each NameNode
        .config(f"spark.hadoop.dfs.namenode.rpc-address.{name_space}.nn1", "10.190.224.102:8022")
        .config(f"spark.hadoop.dfs.namenode.rpc-address.{name_space}.nn2", "10.190.224.103:8022")
        .config(f"spark.hadoop.dfs.namenode.rpc-address.{name_space}.nn3", "10.190.224.104:8022")
        .config(f"spark.hadoop.dfs.namenode.rpc-address.{name_space}.nn4", "10.190.224.105:8022")
        .config(f"spark.hadoop.dfs.namenode.rpc-address.{name_space}.nn5", "10.190.224.106:8022")
        # Proxy provider that automatically detects and routes to the Active NameNode
        .config(
            f"spark.hadoop.dfs.client.failover.proxy.provider.{name_space}",
            "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
        )
        # Delta and Spark configs
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.scheduler.mode", "FAIR")
        .getOrCreate()
    )
 
 
 
 # from pyspark.sql import SparkSession

# # ============================ SPARK SESSION ============================

# def create_spark_session(app_name, hdfs_url):

#     name_space= "fincore"
#     return (
#         SparkSession.builder.appName(app_name)
#         .config(f"fs.defaultFS", f"hdfs://{name_space}") \
#         .config(f"dfs.nameservices", f"{name_space}") \
#         .config(f"dfs.ha.namenodes.{name_space}", "nn1,nn2,nn2,nn3,nn4,nn5,nn6") \
#         .config(f"dfs.namenode.rpc-address.{name_space}.nn1", "10.190.224.102:8022") \
#         .config(f"dfs.namenode.rpc-address.{name_space}.nn2", "10.190.224.103:8022") \
#         .config(f"dfs.namenode.rpc-address.{name_space}.nn3", "10.190.224.104:8022") \
#         .config(f"dfs.namenode.rpc-address.{name_space}.nn3", "10.190.224.105:8022") \
#         .config(f"dfs.namenode.rpc-address.{name_space}.nn3", "10.190.224.106:8022") \
#         .config(f"dfs.client.failover.proxy.provider.{name_space}", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider") \
#         .config(f"spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
#         .config(f"spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
#         .config(f"spark.scheduler.mode","FAIR")
#         .getOrCreate()
#     )

#     # return (
#     #     SparkSession.builder.appName(app_name)
#     #     .config("spark.hadoop.fs.defaultFS", hdfs_url)
#     #     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
#     #     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
#     #     .getOrCreate()
#     # )

