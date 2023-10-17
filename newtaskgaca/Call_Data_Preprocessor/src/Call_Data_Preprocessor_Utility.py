"""This file contains all helper functions that need to be used in the raw data preprocessor code."""
import datetime
import json
import os

import pyspark.sql.functions as f
from pyspark.sql.functions import udf
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, ArrayType, MapType, StringType

from src.TM_Logging import delta_table_crud, notebook_logging

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)


class AmazonPreprocessor():
    """
    Class to transform raw Amazon Connect data into defined columns stored as parquet in daily folders in user defined ADLS container.
    
    :param list[datetime.date] dates: a list of dates based on given start date and end date
    :param str input_path: the path of raw input data
    :param str output_path: the path of preprocessed output data
    :param str business_unit: the business unit code required
    :param dict{str: list[str]} trans_keys: the keys for transcript related attributes
    :param dict{str: list[str]} ctr_keys: the keys for ctr related attributes 
    :param dict{str: any} params_sf: the parameters related to Salesforce data
    :param str mode: if test or not
    """
       
    def __init__(self, dates, input_path, output_path, business_unit, trans_keys, ctr_keys, params_sf, mode):
        self.dates = dates
        self.input_path = input_path
        self.output_path = output_path
        self.bu = business_unit
        self.trans_keys = trans_keys
        self.ctr_keys = ctr_keys
        self.params_sf = params_sf
        self.mode = mode
    
    def read_files_from_path(self, path, logging, date):
        """
        Read all json files into spark df. Used for both contact records and transcipts folders.
        
        :param str path: the path to read data from
        :param datetime.date date: the date of the path/folder
        :return: Raw data in a dataframe from Amazon Connect
        :rtype: pyspark.sql.dataframe.DataFrame
        """
        try:
            spark_df = spark.read.option("recursiveFileLookup", "true").text(path)
            # change to proper json format
            clean_df = spark_df.filter(f.length(f.col('value')) > 1).withColumn('json_value', f.expr('substring(value, 2)')).drop('value')
            return clean_df.distinct()
    
        except:
            print('The path (' +  path + ') does not exist')
            logging.fill_source({'id': date, 'Error_Message': 'The path does not exist'})
            return spark.createDataFrame([], StructType([]))
    
    @staticmethod
    @udf('string')
    def extract_attributes(x, key1, key2):
        """
        User Defined Function (UDF) to extract desired attribute from json string. 
        
        :param str x: raw data in json string format
        :param str key1: level 1 key value for the desired attribute
        :param str key2: level 2 key value for the desired attribute           
        :return: Value from json string for the desired attribute
        :rtype: str
        """
        converted_dict = json.loads(x)
        
        if (key1 in converted_dict) and (converted_dict[key1] is not None):
            if key2 == '':
                return converted_dict[key1]
            elif key2 in converted_dict[key1]:
                return converted_dict[key1][key2]
            else:
                return None
        
        else:
            return None
    
    @staticmethod
    @udf(ArrayType(MapType(StringType(), StringType())))
    def get_conversations(lst):
        """
        User Defined Function (UDF) to extract required attributes from speaker fragments/utterances for each sentence.
            
        :param list[dict{str: str}] lst: raw transcript data in speaker fragments/utterances, where each utterance contains related attributes
        :return: Restructured utterances containing Caller Type, Text, and Sentimet
        :rtype: list[dict{str: str}]
        """
        if lst is None:
            return None
       
        else:
            Conversations = []
            for ind, val in enumerate(lst):
                Conversations.append({'Caller_Type': val['ParticipantId'], 'Text': val['Content'], 'Sentiment': val['Sentiment']})
            return Conversations
    
    def get_transcript(self, df, keys):
        """
        Generate spark df with call transcript related attributes. 
            
        :param pyspark.sql.dataframe.DataFrame df: Raw data generated from reading the transcript data source from Amazon Connect
        :param dict{str: list[str]} keys: keys for selected attributes
        :return: A dataframe with the desired call transcript related attributes
        :rtype: pyspark.sql.dataframe.DataFrame
        """   
        for k, v in keys.items():
            df = df.withColumn(k, self.extract_attributes(f.col('json_value'), f.lit(v[0]), f.lit(v[1])))
        
        df_distinct = df.distinct()   
        
        df_distinct = df_distinct.withColumn('Conversations', self.get_conversations(f.col('trans_Transcript'))).drop('trans_Transcript')  
        
        return df_distinct.drop('json_value') 
     
    def get_ctr(self, df, keys):
        """
        Generate spark df with call contact records related attributes. 
            
        :param pyspark.sql.dataframe.DataFrame df: Raw data generated from reading the transcript data source from Amazon Connect
        :param dict{str: list[str]} keys: keys for selected attributes
        :return: A dataframe with the desired contact records related attributes
        :rtype: pyspark.sql.dataframe.DataFrame
        """  
        for k, v in keys.items():
            df = df.withColumn(k, self.extract_attributes(f.col('json_value'), f.lit(v[0]), f.lit(v[1])))
            
        return df.drop('json_value').distinct() 

    def generate_results(self, job):
        """
        Retrieves Amazon Connect data from transcript and ctr folders, transforms the data by selecting the required attributes, then join with Salesforce data.
        
        :return: A dataframe with the desired Amazon Connect and Salesforce data.
        :rtype: pyspark.sql.dataframe.DataFrame
        """
        df_res = spark.createDataFrame([], StructType([]))
        
        sf = SalesforceProcessor()
        df_case = sf.read_salesforce(self.mode, self.params_sf)
        
        for d in self.dates:
            logging_path = os.path.dirname(self.output_path)
            logging = notebook_logging(path = f'{logging_path}/logs/pre_processor')
            logging.fill_source({'Date': d, 'Job_ID': job, 
                                 'Parameters': {'run_dates': [date.strftime("%Y-%m-%d") for date in self.dates], 
                                                'input_path': self.input_path, 
                                                'output_path': self.output_path, 
                                                'business_unit': self.bu, 
                                                'trans_keys': self.trans_keys, 
                                                'ctr_keys': self.ctr_keys, 
                                                'params_sf': self.params_sf, 
                                                'mode':self.mode}})
            
            date_str = d.strftime("%Y/%m/%d/")
            print('Running for ' + date_str)
            
            trans_raw = self.read_files_from_path(self.input_path + '/transcripts/' + date_str, logging, d)
            ctr_raw = self.read_files_from_path(self.input_path + '/ctr/' + date_str, logging, d)
            
            if (trans_raw.count() > 0):
                try:
                    trans = self.get_transcript(trans_raw, self.trans_keys)
                    ctr = self.get_ctr(ctr_raw, self.ctr_keys)
                    w = Window.partitionBy('Contact_ID').orderBy(f.desc('Last_Update_Time'))
                    ctr_update = ctr.withColumn('seq', f.row_number().over(w)).filter(f.col('seq') == 1).drop('seq').drop('Last_Update_Time')
                except Exception as e: 
                    logging.fill_source({'Date': d, 'Error_Message': f'Could not create trans table: {e}'})
                    raise e
                    
                try:
                    df_sf = sf.get_salesforce(df_case, d, self.params_sf['sf_keys'])
                    df_aws = trans.join(ctr_update, trans.Contact_ID == ctr_update.Contact_ID, 'left').drop(ctr_update.Contact_ID)
                    df_full = df_aws.join(df_sf, df_aws.SF_Case_ID == df_sf.SF_Case_ID, 'left').drop(df_sf.SF_Case_ID)
                except Exception as e:
                    logging.fill_source({'Date': d, 'Error_Message': f'Could not load salesforce table: {e}'})
                    raise e
                
                print("\t{} rows {} columns".format(df_full.count(), len(df_full.columns)))
                
                df_bu = df_full.filter(f.col('BU') == self.bu)
                print("\t{} rows".format(df_bu.count()))
                    
                if (df_res.count() > 0):
                    df_res = df_res.unionByName(df_bu)
                else:
                    df_res = df_bu
                    
                try:
                    yyyymmdd = d.strftime('%Y%m%d')
                    df_bu.coalesce(1).write.mode('append').format('parquet').save(self.output_path + '/' + yyyymmdd)
                except Exception as e:
                    logging.fill_source({'Date': d, 'Error_Message': f'Issues saving table: {e}'})
                    raise e
                
                logging.fill_source({'Date': d,'Rows_Entered': df_bu.count(), "Columns_Entered": len(df_bu.columns)})
                print('\tSucessfully export the results')
            
            else:
                continue
        
        return df_res

class SalesforceProcessor():
    """ Class to read Salesforce data and extract data in required time range"""
    
    def read_salesforce(self, mode, params_sf):
        """
        Read Salesforce data.
            
        :param str mode: if test or not
        :param dict{str: any} params_sf: the parameters related to Salesforce data
        :return: A dataframe of all Salesforce data
        :rtype: pyspark.sql.dataframe.DataFrame
        """
        if mode == 'not_test':
            df = spark.read.option('mergeSchema', 'true').parquet(params_sf['container_url'][0])
            df = df.withColumn('source_container', f.lit(params_sf['container_id'][0]))
            for ind, val in enumerate(params_sf['container_url'][1:]):
                df_temp = spark.read.option('mergeSchema', 'true').parquet(val)
                df_temp = df_temp.withColumn('source_container', f.lit(params_sf['container_id'][ind+1]))
                df = df.unionByName(df_temp)                 
            return df
        
        else:
            df = spark.read.format('parquet').load('abfss://aalab-mlworkspace-opspii@cacaadatalakeproddl.dfs.core.windows.net/GACA/AWS_GB_TM_Output_Delta/test_pre_processor/input/salesforce/20221001')
            return df
    
    def get_salesforce(self, df, date, sf_keys):
        """
        Extract related Salesforce data based on Last Modified Date
            
        :param pyspark.sql.dataframe.DataFrame df: A dataframe of all Salesforce data
        :param str date: required date
        :param dict{str: str} sf_keys: the keys for related attributes
        :return: A dataframe of Salesforce data at the specific date
        :rtype: pyspark.sql.dataframe.DataFrame
        """
        # Filter case created on date and the date before (calls crrossing midnight)
        df_filter = df.filter(f.col('CreatedDate').cast('date').between(date-datetime.timedelta(days=1), date))
        # Get the last modified records per ID
        w = Window.partitionBy('Id').orderBy(f.desc('LastModifiedDate'))
        df_update = df_filter.withColumn('seq', f.row_number().over(w)).filter(f.col('seq') == 1).drop('seq')
        # Select attributes
        if len(sf_keys) > 0:
            df_update = df_update.select(*[f.col(v).alias(k) for k, v in sf_keys.items()])
    
        return df_update.distinct()
    
