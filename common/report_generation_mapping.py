from common.read_write_oracle import read_oracle
from pyspark.sql.functions import from_json, col





from pyspark.sql.types import ArrayType, StringType
def get_nwsa_bsa_mapping(spark):
    query="""

                    (    with base as (select
            coalesce(a.cgl, b.cgl) as CGL,
            a.rpt as NWSA_AVAILABLE,
            b.rpt as BSA_AVAILABLE,
            a.HEAD_CODE as nwsa_code,
            b.head_code as bsa_code
        from
            (
                select
                    CGL,
                    'n' as rpt,
                    max(HEAD_CODE) || '/' || min(HEAD_CODE) as HEAD_CODE
                from
                    rb_parsed_template
                where
                    run_id in ('nwsa_report')
                    and (
                        HEAD_CODE like 'A%'
                        or HEAD_CODE like 'L%'
                    )
                group by
                    CGL
                having
                    count(1) > 1
                union all
                select
                    CGL,
                    'n' as rpt,
                    max(HEAD_CODE) as HEAD_CODE
                from
                    rb_parsed_template
                where
                    run_id in ('nwsa_report')
                    and (
                        HEAD_CODE like 'A%'
                        or HEAD_CODE like 'L%'
                    )
                group by
                    CGL
                having
                    count(1) = 1
            ) a
            full outer join (
                select
                    CGL,
                    substr (run_id, 1, 1) rpt,
                    min(HEAD_CODE) as HEAD_CODE
                from
                    rb_parsed_template
                where
                    run_id in ('pnl_report', 'ysa_report')
                group by
                    substr (run_id, 1, 1),
                    CGL
                having
                    count(1) = 1
                union all
                select
                    CGL,
                    'y' rpt,
                    min(HEAD_CODE) || '/' || max(HEAD_CODE) as HEAD_CODE
                from
                    rb_parsed_template
                where
                    run_id in ('ysa_report')
                group by
                    CGL
                having
                    count(1) > 1
            ) b on a.cgl = b.cgl)
        select * from base) e
            
    

    
    """
    
    nwsa_bsa_mapping = read_oracle(spark,query)
    
    return nwsa_bsa_mapping

def get_nwsa_bsa_mapping_list(spark):
    query = """(WITH base AS
                (
                   SELECT
                       COALESCE(n.cgl, y.cgl, p.cgl) AS cgl,
                       n.rpt AS nwsa_available,
                       y.rpt AS ysa_available,
                       p.rpt AS pnl_available,
                       n.nwsa_headcode,
                       y.ysa_headcode,
                       p.pnl_headcode
                   FROM
                   (
                       SELECT
                           cgl,
                           'n' AS rpt,
                           JSON_ARRAYAGG(head_code ORDER BY head_code) AS nwsa_headcode
                       FROM rb_parsed_template
                       WHERE run_id = 'nwsa_report'
                         AND (head_code LIKE 'A%' OR head_code LIKE 'L%')
                         AND head_code NOT LIKE 'S%'
                         AND head_code NOT LIKE 'N%'
                       GROUP BY cgl
                   ) n
                   FULL OUTER JOIN
                   (
                       SELECT
                           cgl,
                           'y' AS rpt,
                           JSON_ARRAYAGG(head_code ORDER BY head_code) AS ysa_headcode
                       FROM rb_parsed_template
                       WHERE run_id = 'ysa_report'
                       GROUP BY cgl
                   ) y
                       ON n.cgl = y.cgl
                   FULL OUTER JOIN
                   (
                       SELECT
                           cgl,
                           'p' AS rpt,
                           JSON_ARRAYAGG(head_code ORDER BY head_code) AS pnl_headcode
                       FROM rb_parsed_template
                       WHERE run_id = 'pnl_report'
                       GROUP BY cgl
                   ) p
                       ON COALESCE(n.cgl, y.cgl) = p.cgl
                )
                SELECT *
                FROM base) e"""    

    base_df = read_oracle(spark,query)

    casted_df = (
                base_df.withColumn(
                    "NWSA_HEADCODE",
                    from_json(col("NWSA_HEADCODE"), ArrayType(StringType()))
                )
                .withColumn(
                    "YSA_HEADCODE",
                    from_json(col("YSA_HEADCODE"), ArrayType(StringType()))
                ).withColumn(
                    "PNL_HEADCODE",
                    from_json(col("PNL_HEADCODE"), ArrayType(StringType()))
                )
                )
    casted_df.printSchema()
    return casted_df


