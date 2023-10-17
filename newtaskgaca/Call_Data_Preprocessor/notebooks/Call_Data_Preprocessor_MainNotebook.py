# Databricks notebook source
import json
import os 
import sys

# COMMAND ----------

path = os.getcwd() 
parent = os.path.dirname(path)
new_path = parent
sys.path.insert(0, new_path)
from src.Call_Data_Preprocessor_Utility import AmazonPreprocessor
from src.Parameters_Processing import pre_processing

# COMMAND ----------

json_string = '{"frequency": "", "start_date": "2022-10-01", "end_date": "2022-10-05", "input_path": "abfss://amazon-connect-stg-nonpii@cdncacprodopsdl.dfs.core.windows.net", "output_path": "abfss://aalab-mlworkspace-opspii@cacaadatalakeproddl.dfs.core.windows.net/GACA/AWS_TM_Output/test/pre_processor", "mode": "not_test", "business_unit": "GB", "trans_keys": {"Contact_ID": ["CustomerMetadata", "ContactId"], "trans_Transcript": ["Transcript", ""], "Matched_Categories": ["Categories", "MatchedCategories"]}, "ctr_keys": {"Contact_ID": ["ContactId", ""], "Call_Start_Time": ["ConnectedToSystemTimestamp", ""], "Call_End_Time": ["DisconnectTimestamp", ""], "Call_Direction": ["InitiationMethod", ""], "Disconnect_Reason": ["DisconnectReason", ""], "Agent_ID": ["Agent", "Username"], "Call_Duration": ["Agent", "AgentInteractionDuration"], "Hold_Duration": ["Agent", "CustomerHoldDuration"], "Number_Holds": ["Agent", "NumberOfHolds"], "Queue_Name": ["Queue", "Name"], "Queue_Duration": ["Queue", "Duration"], "Caller_Spoken_Intent": ["Attributes", "SpokenCallerIntent"], "Sponsor": ["Attributes", "Sponsor"], "SponsorCount": ["Attributes", "SponsorCount"], "SponsorFound": ["Attributes", "SponsorFound"], "SponsorNumber": ["Attributes", "SponsorNumber"], "BU": ["Attributes", "BU"], "Product": ["Attributes", "Product"], "Language": ["Attributes", "Language__c"], "Customer_Type": ["Attributes", "CustomerType"], "SF_Case_ID": ["Attributes", "sfCaseID"], "Last_Update_Time": ["LastUpdateTimestamp", ""]}, "params_sf": {"container_url": ["abfss://dl-raw-sfcase-adgbnonstaff-pii@cdncacprodopsdl.dfs.core.windows.net/Changes/", "abfss://dl-raw-sfcase-cnlgbnonstaff-pii@cdncacprodopsdl.dfs.core.windows.net/Changes/", "abfss://dl-raw-sfcase-smogbnonstaff-pii@cdncacprodopsdl.dfs.core.windows.net/Changes/", "abfss://dl-raw-sfcase-outreachgbnonstaff-pii@cdncacprodopsdl.dfs.core.windows.net/Changes/"], "container_id": ["ad", "cnl", "smo", "out"], "sf_keys": {"SF_Case_ID": "Id", "SF_Case_CreatedDate": "CreatedDate", "SF_Case_Number": "CaseNumber", "SF_Inquiry_Type": "Inquiry_Type__c", "SF_Business_Unit": "Business_Unit__c", "SF_Type": "Type__c", "SF_Case_Subtype": "Case_Subtype__c", "SF_Reason": "Reason", "SF_Case_Reason": "Case_Reason__c", "SF_IVR_Caller_Intent": "IVR_Caller_Intent__c"}}}'

# Read Parameters 
pp = pre_processing()
dates, input_path, output_path, business_unit, trans_keys, ctr_keys, params_sf, mode = pp.extract_parameters(json_string)

# COMMAND ----------

# Amazon Preprocessor
ap = AmazonPreprocessor(dates, input_path, output_path, business_unit, trans_keys, ctr_keys, params_sf, mode)
jobs = {"Runs": "Manual Trigger"}
res = ap.generate_results(jobs)

# COMMAND ----------

res.count()

# COMMAND ----------

display(res)

# COMMAND ----------
