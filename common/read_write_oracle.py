
from common.properties import get_oracle_properties

oracle_properties=get_oracle_properties()


oracle_url = oracle_properties["url"]
oracle_user = oracle_properties["user"]
oracle_password = oracle_properties["password"]
oracle_driver = oracle_properties["driver"]


def read_oracle(spark,query):
    try:
        read_df = spark.read.format("jdbc") \
        .option("url", oracle_url) \
        .option("dbtable", query) \
        .option("user", oracle_user) \
        .option("password", oracle_password) \
        .option("driver", oracle_driver) \
        .load()
        return read_df
    except Exception as e:
        print("Oracle read error:", str(e))
        raise
        

def write_oracle(dataframe,table):
    try:
        dataframe.write \
            .format("jdbc") \
            .option("url", oracle_url) \
            .option("dbtable", table) \
            .option("user", oracle_user) \
            .option("password", oracle_password) \
            .option("driver", oracle_driver) \
            .mode("append") \
            .save()
        return f"successfully written to {table} table."    
    except Exception as e:
        return f"Error {e} while writting to db."        
    
def overwrite_oracle(dataframe,table):
    try:
        dataframe.write \
            .format("jdbc") \
            .option("url", oracle_url) \
            .option("dbtable", table) \
            .option("user", oracle_user) \
            .option("password", oracle_password) \
            .option("driver", oracle_driver) \
            .option("truncate","true") \
            .mode("overwrite") \
            .save()
        return f"successfully written to {table} table."    
    except Exception as e:
        return f"Error {e} while writting to db."  
    



def update_oracle(spark, query):
    try:
        jvm = spark._jvm

        connection = jvm.java.sql.DriverManager.getConnection(
            oracle_url,
            oracle_user,
            oracle_password
        )

        statement = connection.createStatement()
        effected_rows = statement.executeUpdate(query)

        print(f"Total no.of rows Effected {effected_rows}. while executing {query}")



        statement.close()
        connection.close()

        print("Oracle update successful.")

    except Exception as e:
        print("Oracle update error:", str(e))
        raise



def delete_table_data_by_query(spark,sql_query):
    conn = None
    stmt = None
    try:
        oracle_properties=get_oracle_properties()
        oracle_url = oracle_properties["url"]
        oracle_user = oracle_properties["user"]
        oracle_password = oracle_properties["password"]        
        conn = spark._sc._gateway.jvm.java.sql.DriverManager.getConnection(oracle_url,oracle_user,oracle_password)

        conn.setAutoCommit(False)             
        statement = conn.createStatement()
        print(f"Executing the Query in Oracle DB ==> {sql_query}")
        delete_count = statement.executeUpdate(sql_query)
        conn.commit()
        print(f"X X X X X X X X X X X X X X X X >  Sucessfully Deleted {delete_count} rows for {sql_query}  < X X X X X X X X X X X X X X X X X X X X ")
    except Exception as e:
        print(f"Exception {e} occoured while executing {sql_query} Query.")
        if conn:
            conn.rollback()
        spark.stop() 
        raise
    finally:
        if stmt:
            stmt.close()
        if conn:
            conn.close()

def execute_oracle_procedures(spark,procedure):
    try:
        jdbc_url = oracle_url
        props = {
            "user": oracle_user,
            "password": oracle_password,
            "driver": oracle_driver,

            # Corrected Block: Declares variables for the 3 OUT parameters
            "sessionInitStatement": f"{procedure}"
        }

        # Execute by reading a dummy table (ensures single execution)
        df = spark.read.jdbc(
            url=jdbc_url,
            table="(SELECT 1 FROM DUAL) t",
            properties=props
        )

        return df
    except Exception as e:
        print(f"Error {e} occured while executing procedure.") 