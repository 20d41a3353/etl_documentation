from common.properties import get_jdbc_url,get_oracle_properties

oracle_properties=get_oracle_properties()

oracle_url = oracle_properties["url"]   
Db =oracle_url.split("@//")[1].split("/")[0]

def get_etl_date(spark):
    print("Fetching From - DB ",Db)
    etl_date = get_fincore_date(spark,"ETL_DATE")
    return etl_date

def get_user_date(spark):
    user_date = get_fincore_date(spark,"USERS_DATE")
    return user_date
    

def get_fincore_date(spark,field_name):
    db_table="FINCORE_DATE"
    jdbc_url = get_jdbc_url()
    oracle_properties = get_oracle_properties()
    df = spark.read.jdbc(url=jdbc_url, table=db_table, properties=oracle_properties)
    date = df.select(field_name).first()[0]    
    return date