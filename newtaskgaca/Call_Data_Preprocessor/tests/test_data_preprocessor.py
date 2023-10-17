import pytest
import pyspark
from pyspark.sql import SparkSession

import pandas as pd
import datetime

import src.Call_Data_Preprocessor_Utility as util

ctr_keys = {'Contact_ID': ['ContactId', ''],
            'Call_Start_Time': ['ConnectedToSystemTimestamp', ''],
            'Call_End_Time': ['DisconnectTimestamp', ''],
            'Call_Direction': ['InitiationMethod', ''],
            'Disconnect_Reason': ['DisconnectReason', ''],
            'Agent_ID': ['Agent', 'Username'],
            'Call_Duration': ['Agent', 'AgentInteractionDuration'],
            'Hold_Duration': ['Agent', 'CustomerHoldDuration'],
            'Number_Holds': ['Agent', 'NumberOfHolds'],
            'Queue_Name': ['Queue', 'Name'],
            'Queue_Duration': ['Queue', 'Duration'],
            'Caller_Spoken_Intent': ['Attributes', 'SpokenCallerIntent'],
            'Sponsor': ['Attributes', 'Sponsor'], 
            'SponsorCount': ['Attributes', 'SponsorCount'], 
            'SponsorFound': ['Attributes', 'SponsorFound'], 
            'SponsorNumber': ['Attributes', 'SponsorNumber'], 
            'BU': ['Attributes', 'BU'],
            'Product': ['Attributes', 'Product'],
            'Language': ['Attributes', 'Language__c'],
            "Customer_Type": ["Attributes", "CustomerType"],
            'SF_Case_ID': ['Attributes', 'sfCaseID'],
            'Last_Update_Time': ['LastUpdateTimestamp', '']}

trans_keys = {'Contact_ID': ['CustomerMetadata', 'ContactId'],
              'trans_Transcript': ['Transcript', ''],
              'Matched_Categories': ['Categories', 'MatchedCategories']}

params_sf = {"container_url": ["abfss://dl-raw-sfcase-adiic-pii@cdncacprodopsdl.dfs.core.windows.net/Changes/", "abfss://dl-raw-sfcase-cnliic-pii@cdncacprodopsdl.dfs.core.windows.net/Changes/", "abfss://dl-raw-sfcase-smoiic-pii@cdncacprodopsdl.dfs.core.windows.net/Changes/", "abfss://dl-raw-sfcase-outreachiic-pii@cdncacprodopsdl.dfs.core.windows.net/Changes/"], "container_id": ["ad", "cnl", "smo", "out"], "sf_keys": {"SF_Case_ID": "Id", "SF_Case_CreatedDate": "CreatedDate", "SF_Case_Number": "CaseNumber", "SF_Inquiry_Type": "Inquiry_Type__c", "SF_Business_Unit": "Business_Unit__c", "SF_Type": "Type__c", "SF_Case_Subtype": "Case_Subtype__c", "SF_Reason": "Reason", "SF_Case_Reason": "Case_Reason__c", "SF_IVR_Caller_Intent": "IVR_Caller_Intent__c"}}

@pytest.mark.parametrize("start_date, end_date, input_path, output_path, business_unit, ctr_keys, trans_keys, params_sf, mode, expected_df",
                         [("2022-10-01", "2022-10-01", "abfss://aalab-mlworkspace-opspii@cacaadatalakeproddl.dfs.core.windows.net/GACA/AWS_GB_TM_Output_Delta/test_pre_processor/input", "abfss://aalab-mlworkspace-opspii@cacaadatalakeproddl.dfs.core.windows.net/GACA/AWS_GB_TM_Output_Delta/test_pre_processor/output", "GB", ctr_keys, trans_keys, params_sf, "test", "abfss://aalab-mlworkspace-opspii@cacaadatalakeproddl.dfs.core.windows.net/GACA/AWS_GB_TM_Output_Delta/test_pre_processor/expected_output")])

# Test data preprocessor
def test_data_preprocessor(start_date, end_date, input_path, output_path, business_unit, ctr_keys, trans_keys, params_sf, mode, expected_df):
    # ASSEMBLE
    spark = SparkSession.builder.getOrCreate()
    start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    dates = [(start_date + datetime.timedelta(days=x)) for x in range((end_date - start_date).days + 1)]
    ap = util.AmazonPreprocessor(dates, input_path, output_path, business_unit, trans_keys, ctr_keys, params_sf, mode)
    job = {"Runs": "Manual Trigger - Data Preprocessor Unit Test"}
    test_data = ap.generate_results(job)
    test_data = test_data.sort("Contact_ID")
    output_df_as_pd = test_data.toPandas()
    expected_output_df = spark.read.format('parquet').load(expected_df)
    expected_output_df = expected_output_df.sort("Contact_ID")
    expected_output_df_as_pd = expected_output_df.toPandas()
    pd.testing.assert_frame_equal(left=expected_output_df_as_pd,right=output_df_as_pd, check_exact=True)
