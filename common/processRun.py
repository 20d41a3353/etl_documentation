import os 
import logging
from datetime import datetime
from common.properties import get_oracle_properties,get_jdbc_url

def normalize_date(date_str: str) -> str:
    """
    Accepts DD-MM-YYYY or YYYY-MM-DD
    Returns DD-MM-YYYY
    """
    for fmt in ("%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%d-%m-%Y")
        except ValueError:
            pass
    raise ValueError(
        f"Invalid date format: {date_str}. Expected DD-MM-YYYY or YYYY-MM-DD"
    )


def get_run_id(spark,date,process_id):
    date = normalize_date(date)
    query = f"""
(
 SELECT COALESCE(MAX(RUN_ID), 1) AS RUN_ID
 FROM process_run
 where process_id={process_id} and etl_date=TO_DATE('{date}', 'DD-MM-YYYY')
) t
"""
    jdbc_url = get_jdbc_url()
    oracle_properties = get_oracle_properties()
    df = spark.read.jdbc(url=jdbc_url, table=query, properties=oracle_properties)
    #df= df.select(col("RUN_ID").cast("int"))
    run_id =  int(df.first()[0])
    return run_id

def get_process_run_id(spark,process_id,date):  

    date = normalize_date(date)
    query= f"""
        (
            select PROCESS_RUN_ID from process_run where process_id={process_id} and etl_date=TO_DATE('{date}', 'DD-MM-YYYY') order by run_id desc FETCH FIRST 1 ROWS ONLY
        ) t
    """
    jdbc_url = get_jdbc_url()
    oracle_properties = get_oracle_properties()
    df = spark.read.jdbc(url=jdbc_url, table=query, properties=oracle_properties)
    df.show()
    rows = df.head(1) 
    
    if len(rows) > 0:
        # If record exists, get the value and ensure it's not None
        val = rows[0][0]
        return int(val) if val is not None else 1
    
    return  1
    # process_run_id = df.first()[0]
    # if process_run_id is None:
    #     # No stage records for this date – start from 1
    #     return 1
    # return int(process_run_id)
    