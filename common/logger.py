import logging
import os
from datetime import datetime

def setup_logger(app_name,date,run_id):
   log_dir = "/app/logs" # add hdfs path to store logs
   os.makedirs(log_dir, exist_ok=True)
   pod_name= os.getenv("HOSTNAME","FINCORE")
   log_file = f"{log_dir}/{app_name}_{date}_RUN_{run_id}_{pod_name}.log"
   logging.basicConfig(
       level=logging.INFO,
       format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
       handlers=[
           logging.FileHandler(log_file),
           logging.StreamHandler()  # still show in console
       ]
   )
   
   return logging.getLogger(app_name)
   