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

# GB Example
# json_string = '{"frequency": "", "start_date": "2022-10-01", "end_date": "2022-10-01", "input_path": "abfss://amazon-connect-stg-nonpii@cdncacprodopsdl.dfs.core.windows.net", "output_path": "abfss://aalab-mlworkspace-opspii@cacaadatalakeproddl.dfs.core.windows.net/GACA/AWS_TM_Output/test/pre_processor", "mode": "not_test", "business_unit": "GB", "trans_keys": {"Contact_ID": ["CustomerMetadata", "ContactId"], "trans_Transcript": ["Transcript", ""], "Matched_Categories": ["Categories", "MatchedCategories"]}, "ctr_keys": {"Contact_ID": ["ContactId", ""], "Call_Start_Time": ["ConnectedToSystemTimestamp", ""], "Call_End_Time": ["DisconnectTimestamp", ""], "Call_Direction": ["InitiationMethod", ""], "Disconnect_Reason": ["DisconnectReason", ""], "Agent_ID": ["Agent", "Username"], "Call_Duration": ["Agent", "AgentInteractionDuration"], "Hold_Duration": ["Agent", "CustomerHoldDuration"], "Number_Holds": ["Agent", "NumberOfHolds"], "Queue_Name": ["Queue", "Name"], "Queue_Duration": ["Queue", "Duration"], "Caller_Spoken_Intent": ["Attributes", "SpokenCallerIntent"], "Sponsor": ["Attributes", "Sponsor"], "SponsorCount": ["Attributes", "SponsorCount"], "SponsorFound": ["Attributes", "SponsorFound"], "SponsorNumber": ["Attributes", "SponsorNumber"], "BU": ["Attributes", "BU"], "Product": ["Attributes", "Product"], "Language": ["Attributes", "Language__c"], "SF_Case_ID": ["Attributes", "sfCaseID"], "Customer_Type": ["Attributes", "CustomerType"], "Last_Update_Time": ["LastUpdateTimestamp", ""]}, "params_sf": {"container_url": ["abfss://dl-raw-sfcase-adgbnonstaff-pii@cdncacprodopsdl.dfs.core.windows.net/Changes/", "abfss://dl-raw-sfcase-cnlgbnonstaff-pii@cdncacprodopsdl.dfs.core.windows.net/Changes/", "abfss://dl-raw-sfcase-smogbnonstaff-pii@cdncacprodopsdl.dfs.core.windows.net/Changes/", "abfss://dl-raw-sfcase-outreachgbnonstaff-pii@cdncacprodopsdl.dfs.core.windows.net/Changes/"], "container_id": ["ad", "cnl", "smo", "out"], "sf_keys": {"SF_Case_ID": "Id", "SF_Case_CreatedDate": "CreatedDate", "SF_Case_Number": "CaseNumber", "SF_Inquiry_Type": "Inquiry_Type__c", "SF_Business_Unit": "Business_Unit__c", "SF_Type": "Type__c", "SF_Case_Subtype": "Case_Subtype__c", "SF_Reason": "Reason", "SF_Case_Reason": "Case_Reason__c", "SF_IVR_Caller_Intent": "IVR_Caller_Intent__c"}}}'

# IIC Example
# json_string = '{"frequency": "", "start_date": "2023-02-10", "end_date": "2023-03-05", "input_path": "abfss://amazon-connect-stg-nonpii@cdncacprodopsdl.dfs.core.windows.net", "output_path": "abfss://aalab-mlworkspace-opspii@cacaadatalakeproddl.dfs.core.windows.net/GACA/DEV_IIC/pipeline/pre_processor", "mode": "not_test", "business_unit": "IIC", "trans_keys": {"Contact_ID": ["CustomerMetadata", "ContactId"], "trans_Transcript": ["Transcript", ""], "Matched_Categories": ["Categories", "MatchedCategories"]}, "ctr_keys": {"Contact_ID": ["ContactId", ""], "Call_Start_Time": ["ConnectedToSystemTimestamp", ""], "Call_End_Time": ["DisconnectTimestamp", ""], "Call_Direction": ["InitiationMethod", ""], "Disconnect_Reason": ["DisconnectReason", ""], "Agent_ID": ["Agent", "Username"], "Call_Duration": ["Agent", "AgentInteractionDuration"], "Hold_Duration": ["Agent", "CustomerHoldDuration"], "Number_Holds": ["Agent", "NumberOfHolds"], "Queue_Name": ["Queue", "Name"], "Queue_Duration": ["Queue", "Duration"], "Caller_Spoken_Intent": ["Attributes", "SpokenCallerIntent"], "Sponsor": ["Attributes", "Sponsor"], "SponsorCount": ["Attributes", "SponsorCount"], "SponsorFound": ["Attributes", "SponsorFound"], "SponsorNumber": ["Attributes", "SponsorNumber"], "BU": ["Attributes", "BU"], "Product": ["Attributes", "Product"], "Language": ["Attributes", "Language__c"], "SF_Case_ID": ["Attributes", "sfCaseID"], "Customer_Type": ["Attributes", "CustomerType"], "Last_Update_Time": ["LastUpdateTimestamp", ""]}, "params_sf": {"container_url": ["abfss://dl-raw-sfcase-adiic-pii@cdncacprodopsdl.dfs.core.windows.net/Changes/", "abfss://dl-raw-sfcase-cnliic-pii@cdncacprodopsdl.dfs.core.windows.net/Changes/", "abfss://dl-raw-sfcase-smoiic-pii@cdncacprodopsdl.dfs.core.windows.net/Changes/", "abfss://dl-raw-sfcase-outreachiic-pii@cdncacprodopsdl.dfs.core.windows.net/Changes/"], "container_id": ["ad", "cnl", "smo", "out"], "sf_keys": {"SF_Case_ID": "Id", "SF_Case_CreatedDate": "CreatedDate", "SF_Case_Number": "CaseNumber", "SF_Inquiry_Type": "Inquiry_Type__c", "SF_Business_Unit": "Business_Unit__c", "SF_Type": "Type__c", "SF_Case_Subtype": "Case_Subtype__c", "SF_Contact_Type": "Contact_Type__c", "SF_AssetId": "AssetId", "SF_Age": "Age__c", "SF_Product_Family": "Product_Family__c", "SF_Selling_Code": "Selling_Code__c"}}}'

json_string = dbutils.widgets.get('json_string')

try:
    jobs = {"Runs": dbutils.widgets.get('run_id'), "Jobs": dbutils.widgets.get('job_id')}
except:
    jobs = {"Runs": "Manual Trigger"}

print(jobs)

# COMMAND ----------

# Read Parameters 
pp = pre_processing()
dates, input_path, output_path, business_unit, trans_keys, ctr_keys, params_sf, mode = pp.extract_parameters(json_string)

# Amazon Preprocessor
ap = AmazonPreprocessor(dates, input_path, output_path, business_unit, trans_keys, ctr_keys, params_sf, mode)
res = ap.generate_results(job=jobs)

# COMMAND ----------

res.count()

# COMMAND ----------

display(res)

# COMMAND ----------
