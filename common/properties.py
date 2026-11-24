
# from pyspark.sql import SparkSession
# from delta import configure_spark_with_delta_pip
# import os

# kafkalistener = os.getenv("kafkalistener") 
# oracle_url = os.getenv("oracle_url") 
# oracle_user = os.getenv("oracle_user") 
# oracle_password = os.getenv("oracle_password") 
# oracle_driver = os.getenv("oracle_driver")
# # environment = os.getenv("fincore_environment")

# # oracle_user = "fincore"
# # oracle_password = "Password#1234"
# # oracle_driver = "oracle.jdbc.driver.OracleDriver"
# # oracle_url = "jdbc:oracle:thin:@//10.177.103.192:1523/fincorepdb1" 
# # kafkalistener = "kafka.be-test.svc.cluster.local:9092"
# environment = "dev"

# # ---------------------------
# # Spark Setup
# # ---------------------------

# builder = SparkSession.builder \
#     .appName("DeltaLakeOperation") \
#     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
#     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

# spark = configure_spark_with_delta_pip(builder).getOrCreate()

# # ---------------------------
# # ENV CONFIG
# # ---------------------------

# def get_hdfs_properties(environment):
#     return {
#         "dev": ["10.177.103.199", "10.177.103.192", "10.177.103.196"],
#         "sit": ["10.177.179.59", "10.177.179.60", "10.177.179.61"],
#         "uat": ["10.177.179.99", "10.177.179.100", "10.177.179.101"],
#         "preprod": ["10.177.179.158", "10.177.179.159", "10.177.179.160"],
#         "prod": ["10.177.224.102", "10.177.224.103", "10.177.224.104"]
#     }.get(environment, [])


# # ---------------------------
# #  ACTIVE NN RESOLVER (FINAL)
# # ---------------------------

# def get_active_nn(nn_list, port="8022", timeout=3):
#     sc = spark.sparkContext
#     conf = sc._jsc.hadoopConfiguration()
#     gateway = sc._gateway

#     for ip in nn_list:
#         address = f"{ip}:{port}"
#         try:

#             uri = gateway.jvm.java.net.URI(f"hdfs://{address}")
#             fs = gateway.jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)

#             # lightweight call
#             fs.exists(gateway.jvm.org.apache.hadoop.fs.Path("/"))

#             return f"hdfs://{address}"

#         except Exception as e:
#             continue

#     raise Exception(" No Active NameNode found")


# # ---------------------------
# # MAIN FUNCTION
# # ---------------------------

# def get_hdfs_base():
#     nodes = get_hdfs_properties(environment)

#     if not nodes:
#         raise Exception("No HDFS nodes configured")

#     return get_active_nn(nodes)


# def get_oracle_properties():
#     oracle_properties={
#         "url":oracle_url,
#         "user":oracle_user,
#         "password":oracle_password,
#         "driver":oracle_driver
#     }    
#     return oracle_properties

# def get_jdbc_url():
#     return oracle_url

# def get_kafka_ip():
#     if(kafkalistener):
#         return kafkalistener
 
# dsfd = get_kafka_ip()

# # ---------------------------
# # USAGE
# # ---------------------------
# try:
#     HDFS_BASE = get_hdfs_base()

#     # Example read
#     # df = spark.read.parquet(f"{HDFS_BASE}/path")

# except Exception as e:


















import os

# kafkalistener = os.getenv("kafkalistener") 
# oracle_url = os.getenv("oracle_url") 
# oracle_user = os.getenv("oracle_user") 
# oracle_password = os.getenv("oracle_password") 
# oracle_driver = os.getenv("oracle_driver") 19:43

region = "DEV"
oracle_user = "fincore"
oracle_password = "Password#1234"
oracle_driver = "oracle.jdbc.driver.OracleDriver"

if(region.upper() == "ST"):
     HDFS_BASE = "hdfs://10.177.179.61:8022" # ST
     oracle_url = "jdbc:oracle:thin:@//10.177.179.46:1523/fincorepdb1" #ST DB
     kafkalistener = "kafka.cbops.svc.cluster.local:9092"
if(region.upper()  == "DEV"):
    HDFS_BASE = "hdfs://10.177.103.199:8022" #Dev
    oracle_url = "jdbc:oracle:thin:@//10.177.103.192:1523/fincorepdb1" # Dev
    kafkalistener = "kafka.be-test.svc.cluster.local:9092"
if(region.upper()  == "ABHI"):
    HDFS_BASE = "hdfs://10.0.17.76:8020" #Dev
    oracle_url = "jdbc:oracle:thin:@//10.177.103.192:1523/fincorepdb1" # Dev
    kafkalistener = "localhost:9092"
if(region.upper()  == "UAT"):
    HDFS_BASE = "hdfs://10.177.179.99:8022" # UAT
    oracle_url = "jdbc:oracle:thin:@//10.177.179.85:1523/fincorepdb1"
    kafkalistener =  "kafka.uat-cbops1.svc.cluster.local:9092"
if(region == "PRODDR"):
    HDFS_BASE = "hdfs://10.177.196.149:8022"
    oracle_url = "jdbc:oracle:thin:@//10.177.196.126:1523/fincorepdb1"
    kafkalistener = "kafka.backend.svc.cluster.local:9092"
if(region.upper()  == "PRODPR"):
    HDFS_BASE = "hdfs://fincore"
    oracle_url = "jdbc:oracle:thin:@//10.190.224.79:1523/fincorepdb1"
    kafkalistener = "kafka.backend.svc.cluster.local:9092"




def get_oracle_properties():
    oracle_properties={
        "url":oracle_url,
        "user":oracle_user,
        "password":oracle_password,
        "driver":oracle_driver
    }    
    return oracle_properties

def get_jdbc_url():
    return oracle_url

def get_hdfs_base():
    return HDFS_BASE

def get_kafka_ip():
    if(kafkalistener):
        return kafkalistener
 
dsfd = get_kafka_ip()

