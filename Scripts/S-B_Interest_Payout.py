import logging
from datetime import date, timedelta, datetime

# =========== Common Methods =============================

from common.dateUtil import get_etl_date
from common.fileUtil import get_hdfs_base
from common.read_write_oracle import read_oracle
from common.createSpark import create_spark_session
from common.fileUtil import write_csv_with_filename
from common.create_path_if_not_exists import create_path_if_not_exists



def main(spark,BASE_HDFS,etl_date):
 
    # date_str=date(2026,3,31).strftime("%Y-%m-%d")
    date_str=etl_date.strftime("%Y-%m-%d")
    date_var=etl_date.strftime("%d-%m-%Y")
    print(date_str)
    date_obj = datetime.strptime(date_var, "%d-%m-%Y")   
    print(date_obj)
    output_date = date_obj.strftime("%d-%b-%y").upper()
    print(output_date)



    print(date_str)

    output_path =  f"{BASE_HDFS}/reports/{date_str}/"
    query = f"""

        (WITH valid_cgl AS (
                SELECT DISTINCT cgl
                FROM rb_parsed_template
                WHERE head_code = '10051'
                  AND run_id = 'pnl_report'
            ),
            base AS ( 
                SELECT
                    gt.branch_code,
                    gt.currency,
                    gt.cgl,
                    SUM(gt.debit_amount) AS amount
                FROM gl_transactions gt
                JOIN valid_cgl v ON gt.cgl = v.cgl
                WHERE gt.transaction_date = '{output_date}' 
                and NARRATION like 'CBS%'
                GROUP BY
                    gt.branch_code,
                    gt.currency,
                    gt.cgl
            ),
            circle_base AS (
                SELECT 
                    b.code AS branch_code,
                    b.circle_code,
                    c.circle_name
                FROM branch_master b
                JOIN circle_master c ON b.circle_code = c.circle_code
            ),
            cgl_desc AS (

                SELECT 
                    cgl_number, 
                    description
                FROM cgl_master
            )
            SELECT
                cb.circle_code as circle,
                cb.circle_name ,
                b.cgl as acct,
                cm.description as acct_name, 
                b.currency,
                SUM(b.amount) AS amount
            FROM base b
            JOIN circle_base cb ON b.branch_code = cb.branch_code
            JOIN cgl_desc cm ON b.cgl = cm.cgl_number 
            GROUP BY
                cb.circle_code,
                cb.circle_name,
                b.cgl,
                cm.description,
                b.currency 
                ) e

            """
    records = read_oracle(spark,query)
    print(output_path)
    print(f"No of rows retrived from query ========== > {records.count()}")
    # records.show(10)
    # output_path = f'{BASE_HDFS}/S_B_Interest_Payout/'
    filename = f"S-B_I_Payout_report_{etl_date.strftime('%Y%m%d')}.psv"
    logging.info("csv file creation started.")
    create_path_if_not_exists(spark,output_path)
    write_csv_with_filename(records,output_path,filename,sep= "|")
    


if __name__ == "__main__":
   
    BASE_HDFS = get_hdfs_base()
    
    spark = create_spark_session("S-B Interest Payout", BASE_HDFS)
    etl_date = get_etl_date(spark)
    # etl_date = datetime(2026,6,25).date()
    main(spark,BASE_HDFS,etl_date)
