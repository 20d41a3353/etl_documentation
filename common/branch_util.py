from common.read_write_oracle import read_oracle
def get_drill_down_branch_mapping(spark):
    branch_mapping_query = """

            (       SELECT
    b.code AS BRANCH_CODE,
    b.circle_code AS CIRCLE_CODE,
    c.circle_name AS CIRCLE_NAME,
    b.nmr_code AS NMR_CODE,
    CASE 
        WHEN b.circle_code = '021' THEN 'F'
        ELSE 'D'
    END AS BRANCH_TYPE
FROM
    branch_master b
JOIN
    circle_master c ON b.circle_code = c.circle_code  ) t
        """
    branch_df = read_oracle(spark,branch_mapping_query)
    return branch_df