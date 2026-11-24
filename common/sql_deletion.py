from common.properties import get_oracle_properties

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