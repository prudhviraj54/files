# Databricks notebook source
import os
import time
import datetime
import json
import sys

# COMMAND ----------

path = os.getcwd() 
parent = os.path.dirname(path)
new_path = parent + '/src'
sys.path.insert(0, new_path)
import NPS_Processor_Utility as nps

# COMMAND ----------

# parameters for medallia
json_string = '{"if_today": "False", "start_date": "2023-01-01", "end_date": "2023-01-06", "input_path": "abfss://dl-raw-medallia-pii-delta@cdncacprodopsdl.dfs.core.windows.net/api_extract", "output_path": "abfss://aalab-mlworkspace-opspii@cacaadatalakeproddl.dfs.core.windows.net/GACA/AWS_GB_TM_Output/NPS", "language": 1, "business_unit": "GB", "mode": "Medallia", "status": 1}'

# COMMAND ----------

# parameters for confirmit
json_string1 = '{"if_today": "False", "start_date": "2022-10-01", "end_date": "2022-10-10", "input_path": "abfss://tnps-pii@cdncacprodopsdl.dfs.core.windows.net/nps_voice_of_the_customer/changes", "output_path": "abfss://aalab-mlworkspace-opspii@cacaadatalakeproddl.dfs.core.windows.net/GACA/AWS_GB_TM_Output/NPS", "language": 9, "business_unit": "GB", "mode": "Confirmit", "status": "complete"}'

# COMMAND ----------

params = json.loads(json_string)
if_today = params['if_today']
start_date = params['start_date']
end_date = params['end_date']
mode = params['mode']
input_path = params['input_path']
output_path = params['output_path']
business_unit = params['business_unit']
language = params['language']
status = params['status']

# COMMAND ----------

if if_today == 'True':
    date = datetime.date.today()
    dates = [date]
else:
    sdate = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    edate = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    dates = [(sdate + datetime.timedelta(days=x)) for x in range((edate - sdate).days + 1)]

# COMMAND ----------

# get output dataframe
nps_processor = nps.NPSProcessor()
output_results = nps_processor.nps_processor_results(dates, mode, input_path, output_path, business_unit, language, status)

# COMMAND ----------

display(output_results)
