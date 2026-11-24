from common.read_write_oracle import read_oracle
def get_batch_id(spark):
    try:
        query = f"""(SELECT get_next_batch_id AS BATCH_ID_VAL FROM DUAL) t"""
        df_batch_id = read_oracle(spark,query)     
        batch_id_value = df_batch_id.collect()[0]["BATCH_ID_VAL"]       
        BATCH_ID_LITERAL = int(batch_id_value)
        
        print(f"=== Fetched BATCH_ID: {BATCH_ID_LITERAL} ===")
        return BATCH_ID_LITERAL
    except Exception as e:
        
        print(f"Error fetching BATCH_SEQ via JDBC in gl : {e}")
        spark.stop()
        exit()
        
def get_journal_id(spark):
    try:
        query = f"""(SELECT get_next_journal_id AS JOURNAL_ID_VAL FROM DUAL) t"""
        df_journal_id = read_oracle(spark,query)

        journal_id_value = df_journal_id.collect()[0]["JOURNAL_ID_VAL"]
        JOURNAL_ID_LITERAL = int(journal_id_value)
        print(f"=== Fetched JOURNAL_ID: {JOURNAL_ID_LITERAL} ===")
        return JOURNAL_ID_LITERAL
    except Exception as e:
        print(f"error {e} when fetching journal id")
        spark.stop()
        exit()