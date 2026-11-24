from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.functions import to_json, struct, col
from datetime import datetime, timedelta
from common.read_write_oracle import write_oracle
from  common.properties import get_kafka_ip
# Oracle table name for process run stages
PROCESS_RUN_STAGE_TABLE = "PROCESS_RUN_STAGE"

STAGE_IN_PROGRESS=1
STAGE_SUCCESS=2
STAGE_FAILURE=3


def log_etl(spark,process_run_id,stage, status, remarks="", flag=1, summary=""):
    """
    Tracks ETL stage progress by:
      - On start (status=1): Inserts a record into Oracle PROCESS_RUN_STAGE,
        then sends a Kafka INSERT event.
      - On end (status=2/3): Sends a Kafka UPDATE event (no direct Oracle update).

    :param spark: SparkSession
    :param pipeline_name: name of the pipeline (unused, kept for compatibility)
    :param run_id: process run ID (from Airflow)
    :param remarks: description of the stage (optional)
    :param stage: stage number (matches PROCESS_STAGE_MASTER.STAGE_ID)
    :param status: 1 = start, 2 = success, 3 = failure
    :param startTime: datetime when the stage started
    """
    kafka_bootstrap_servers = get_kafka_ip()
    # kafka_topic = "spark-events"
    kafka_topic = "spark-events"
    startTime= datetime.now() 
    startTime = startTime +  timedelta(hours=5, minutes=30)
    # startTime = startTime.strftime("%I:%M %p")

    if ((status == STAGE_IN_PROGRESS)) or flag!=1 :  # Stage start
        # ---------- 1. Insert into Oracle ----------
        insert_data = [(
            int(process_run_id),
            str(stage),
            status,                       # status = running
            "spark",                  # source
            remarks or "",             # remarks
            summary,                        
            startTime
        )]

        insert_columns = [
            "PROCESS_RUN_ID",
            "STAGE_ID",
            "STATUS",
            "SOURCE",
            "REMARKS",
            "SUMMARY",
            "START_TIME"
        ]

        insert_df = spark.createDataFrame(insert_data, insert_columns)
        insert_df.show()
        try:
            write_oracle(insert_df, PROCESS_RUN_STAGE_TABLE)
            print(f"Oracle INSERT successful for stage {stage}, process_run_id {process_run_id}")
        except Exception as e:
            print(f"ERROR: Oracle insert failed for stage {stage}, process_run_id {process_run_id}: {e}")
            raise

        # ---------- 2. Sending Kafka INSERT event ----------
        try:
            kafka_insert_data = [(
                int(process_run_id),
                str(stage),
                status,
                "spark",
                remarks or "",
                summary,
                startTime,
                "INSERT"
            )]

            kafka_insert_columns = [
                "processRunId",
                "stageId",
                "status",
                "source",
                "remarks",
                "summary",
                "startTime",
                "action"
            ]

            kafka_insert_df = spark.createDataFrame(kafka_insert_data, kafka_insert_columns)
            kafka_df = kafka_insert_df.select(
                F.lit(f"{process_run_id}_{stage}").alias("key"),
                to_json(struct(*kafka_insert_columns)).alias("value")
            )

            kafka_df.write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
                .option("topic", kafka_topic) \
                .save()

            print(f"Kafka INSERT event sent for stage {stage}, process_run_id {process_run_id}")
        except Exception as e:
            print(f"ERROR: Kafka insert failed for stage {stage}, process_run_id {process_run_id}: {e}")
            

    else:  # Stage end (status = 2 or 3)
        # ---------- Sending Kafka UPDATE event only ----------
        update_data = [(
            int(process_run_id),
            str(stage),
            int(status),      # final status
            remarks or "",
            "UPDATE"
        )]

        update_columns = [
            "processRunId",
            "stageId",
            "status",
            "remarks",
            "action"
        ]

        update_df = spark.createDataFrame(update_data, update_columns)
        try:
            kafka_update_df = update_df.select(
                F.lit(f"{process_run_id}_{stage}").alias("key"),
                to_json(struct(*update_columns)).alias("value")
            )

            kafka_update_df.write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
                .option("topic", kafka_topic) \
                .save()

            print(f"Kafka UPDATE event sent for stage {stage}, process_run_id {process_run_id}, status {status}")
        except Exception as e:
            print(f"ERROR: Kafka update failed for stage {stage}, process_run_id {process_run_id}: {e}")




























# import time
# import datetime
# from pyspark.sql import functions as F
# from pyspark.sql.functions import to_json, struct, col
# from common.properties import get_oracle_properties

# # Global dictionary to store generated processRunStageId for each (run_id, stage)
# _stage_record_ids = {}

# def log_etl(spark, pipeline_name, run_id, remarks, stage, status, startTime,summary=""):
#     """
#     Sends Kafka events to track ETL stage progress.
    
#     :param spark: SparkSession
#     :param pipeline_name: name of the pipeline (unused, kept for compatibility)
#     :param run_id: process run ID (from Airflow)
#     :param remarks: description of the stage (optional)
#     :param stage: stage number (matches PROCESS_STAGE_MASTER.STAGE_ID)
#     :param status: 1 = start, 2 = success, 3 = failure
#     :param startTime: datetime when the stage started
#     """
#     global _stage_record_ids
#     key = (run_id, stage)

#     # Kafka configuration
#     kafka_bootstrap_servers = "10.0.17.242:9092"
#     kafka_topic = "spark-events"

#     if status == 1:  # Stage start
#         # ---------- INSERT event ----------
#         insert_data = [(
#             int(run_id),
#             int(stage),
#             1,                       # status = running
#             "spark",                  # source
#             remarks or "",             # remarks (optional)
#             summary,                        # summary (optional)
#             startTime,
#             "INSERT"
#         )]

#         insert_columns = [
#             "processRunId",
#             "stageId",
#             "status",
#             "source",
#             "remarks",
#             "summary",
#             "startTime",
#             "action"
#         ]

#         insert_df = spark.createDataFrame(insert_data, insert_columns)
#         kafka_insert_df = insert_df.select(
#             # Using a constant key (e.g., stage) or empty; the Java service will handle it.
#             F.lit(str(stage)).alias("key"),
#             to_json(struct(*insert_columns)).alias("value")
#         )

#         kafka_insert_df.write \
#             .format("kafka") \
#             .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
#             .option("topic", kafka_topic) \
#             .save()

#         print(f"INSERT event sent for stage {stage}, run_id {run_id}")

#         # Allowing time for the Java service to insert the row and commit
#         time.sleep(3)

#         # # ---------- Retrieving generated processRunStageId from Oracle ----------
#         # oracle_props = get_oracle_properties()
#         # oracle_url = oracle_props["url"]
#         # oracle_user = oracle_props["user"]
#         # oracle_password = oracle_props["password"]
#         # oracle_driver = oracle_props["driver"]

#         # # Formatting startTime for Oracle 
#         # start_time_str = startTime.strftime("%Y-%m-%d %H:%M:%S")

#         # query = f"""
#         #     (SELECT PROCESS_RUN_STAGE_ID 
#         #      FROM PROCESS_RUN_STAGE 
#         #      WHERE PROCESS_RUN_ID = {int(run_id)}
#         #        AND STAGE_ID = {int(stage)}
#         #        AND START_TIME = TO_TIMESTAMP('{start_time_str}', 'YYYY-MM-DD HH24:MI:SS')
#         #     ) T1
#         # """

#         # try:
#             id_df = spark.read.format("jdbc") \
#                 .option("url", oracle_url) \
#                 .option("dbtable", query) \
#                 .option("user", oracle_user) \
#                 .option("password", oracle_password) \
#                 .option("driver", oracle_driver) \
#                 .load()

#             if id_df.count() > 0:
#                 stage_record_id = id_df.collect()[0][0]
#                 _stage_record_ids[key] = stage_record_id
#                 print(f"Retrieved processRunStageId = {stage_record_id} for stage {stage}")
#             else:
#                 print(f"WARNING: No row found for stage {stage}, run_id {run_id} after INSERT")
#         except Exception as e:
#             print(f"ERROR: Could not retrieve processRunStageId: {e}")

#     else:  # Stage end (status = 2 or 3)
#         # ---------- Retrieving stored ID ----------
#         if key not in _stage_record_ids:
#             print(f"ERROR: No stored processRunStageId for stage {stage}, run_id {run_id}. Cannot send UPDATE.")
#             return

#         stage_record_id = _stage_record_ids.pop(key)
        

#         # ---------- UPDATE event ----------
#         update_data = [(
#             int(stage_record_id),
#             int(run_id),
#             int(stage),
#             int(status),      # final status (2 = success, 3 = failure)
#             remarks or "",
#             "UPDATE"
#         )]

#         update_columns = [
#             "processRunStageId",
#             "processRunId",
#             "stageId",
#             "status",
#             "remarks",
#             "action"
#         ]

#         update_df = spark.createDataFrame(update_data, update_columns)
#         kafka_update_df = update_df.select(
#             col("processRunStageId").cast("string").alias("key"),
#             to_json(struct(*update_columns)).alias("value")
#         )

#         kafka_update_df.write \
#             .format("kafka") \
#             .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
#             .option("topic", kafka_topic) \
#             .save()

#         print(f"UPDATE event sent for stage {stage}, run_id {run_id}, status {status}")