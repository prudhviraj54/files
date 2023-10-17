# Databricks notebook source
import json
import os
import sys

# COMMAND ----------

path = os.getcwd() 
parent = os.path.dirname(path)
new_path = parent + '/src'
sys.path.insert(0, new_path)
import NPS_Processor_Utility as nps
from Parameters_Processing import pre_processing

# COMMAND ----------

# Example Parameters for medallia
# json_string = '{"frequency": "", "start_date": "2023-01-02", "end_date": "2023-04-30", "input_path": "abfss://dl-raw-medallia-pii-delta@cdncacprodopsdl.dfs.core.windows.net/api_extract", "output_path": "abfss://aalab-mlworkspace-opspii@cacaadatalakeproddl.dfs.core.windows.net/GACA/DEV_IIC/pipeline/nps", "language": 1, "business_unit": "IIC", "mode": "Medallia", "status": 1}'

# Example Parameters for confirmit
# json_string = '{"frequency": "", "start_date": "2022-10-01", "end_date": "2022-10-15", "input_path": "abfss://tnps-pii@cdncacprodopsdl.dfs.core.windows.net/nps_voice_of_the_customer/changes", "output_path": "abfss://aalab-mlworkspace-opspii@cacaadatalakeproddl.dfs.core.windows.net/GACA/AWS_TM_Output/test/nps", "language": 9, "business_unit": "GB", "mode": "Confirmit", "status": "complete"}'

json_string = dbutils.widgets.get('json_string')

try:
    jobs = {"Runs": dbutils.widgets.get('run_id'), "Jobs": dbutils.widgets.get('job_id')}
except:
    jobs = {"Runs": "Manual Trigger"}

print(jobs)

# COMMAND ----------

# Read Parameters 
pp = pre_processing()
dates, input_path, output_path, language, business_unit, mode, status = pp.extract_parameters(json_string)

# COMMAND ----------

# get output dataframe
nps_processor = nps.NPSProcessor()
output_results = nps_processor.nps_processor_results(dates, mode, input_path, output_path, business_unit, language, status, jobs)

# COMMAND ----------

display(output_results)
