import warnings
import datetime
import os
from TM_Logging import delta_table_crud, notebook_logging

import pyspark.sql.functions as f
from pyspark.sql.functions import lit, to_date, to_timestamp
from pyspark.sql.types import StringType, BooleanType, StructType
from pyspark.sql.window import Window

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
warnings.filterwarnings("ignore")    
spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")

class NPSProcessor():
    """The class inputs the nps data from both the sources (Medallia and confirmit, and extracts the required attributes and renames them accordingly. """
    
    def check_nps_processor_path(self, mode, path):
        """
        Checks whether the input path exists. 
        
        :param str mode: whether the surveys should be read from Medallia or Confirmit
        :param str path: the path containing the nps data
        :return: True/False depending on whether the path exists in parquet or not
        :rtype: boolean
        """
        try:
            if mode == 'Medallia':
                df = spark.read.format('delta').load(path)
            else:
                df = spark.read.format('parquet').load(path)
            return True
        except:
            print('The path (' +  path + ') does not exist')
            return False
    
    def read_nps_data(self, mode, path, business_unit, language, status, logging, date): #change to input data
        """
        Read the NPS data and filters it by Language, Business Unit, and status of survey
        :param str mode: whether the surveys should be read from Medallia or Confirmit
        :param str path: the path containing the nps data
        :param str business_unit: the business unit code for the data
        :param str language: the language code for the data
        :param status: the status of the survey - completed or not
        :return: a dataframe containing the pre-processed input filtered by the business unit, status and language
        :rtype: pyspark.sql.dataframe.DataFrame
        """
        try:
            if mode == 'Medallia':
                df = spark.read.format('delta').load(path)
                # Filter out the surveys by status, business unit and language
                df_clean = df.filter((f.col('a_status_enum') == status) & (f.col('e_manulife_nps_business_unit_distinct_auto') == business_unit) & (f.col('e_manulife_nps_language_enum') == language))
                df_clean = df_clean.select(f.col('e_bp_cc_interaction_id_txt').alias('NPS_Contact_Id'), 
                                           f.col('k_manulife_cc_interaction_date').alias('NPS_Call_Date'), 
                                           f.col('a_surveyid').alias('NPS_Survey_Id'), 
                                           f.col('e_manulife_nps_case_number_txt').alias('NPS_Case_Number'),
                                           f.col('e_manulife_nps_language_enum').alias('NPS_Language'), 
                                           f.col('e_manulife_nps_business_unit_distinct_auto').alias('NPS_Business_Unit'), 
                                           f.col('q_manulife_cc_ltr_sc10').alias('NPS_LTR'), 
                                           f.col('q_manulife_cc_ltr_reason_cmt').alias('NPS_LTRWhy'), 
                                           f.col('k_manulife_cc_contact_type_for_reporting').alias('NPS_Customer_Type'), 
                                           f.col('e_manulife_nps_edate_txt').alias('NPS_Email_Date'),
                                           f.col('e_responsedate').alias('NPS_Response_Date'),
                                           f.col('a_status_enum').alias('NPS_Status'))
                return df_clean.distinct()
            else:
                df = spark.read.format('parquet').load(path)
                w = Window.partitionBy('responseid').orderBy(f.desc('last_touched'))
                df = df.withColumn('seq', f.row_number().over(w)).filter(f.col('seq') == 1).drop('seq').drop('last_touched')
                # Filter out the surveys by status, business unit and language
                df_clean = df.filter((f.col('status') == status) & (f.col('BusinessUnit') == business_unit) & (f.col('l') == language))
                df_clean = df_clean.select(f.col('CallDate').alias('NPS_Call_Date'), 
                                           f.col('responseid').alias('NPS_Survey_Id'), 
                                           f.col('InquiryKey').alias('NPS_Case_Number'),
                                           f.col('l').alias('NPS_Language'), 
                                           f.col('BusinessUnit').alias('NPS_Business_Unit'), 
                                           f.col('LTR').alias('NPS_LTR'), 
                                           f.col('LTRWhy').alias('NPS_LTRWhy'), 
                                           f.col('interview_start').alias('NPS_Email_Date'),
                                           f.col('interview_end').alias('NPS_Response_Date'),
                                           f.col('status').alias('NPS_Status'))
                return df_clean.distinct()
        except:
            print('The path (' +  path + ') does not exist')
            logging.fill_source({'id': date, 'Error_Message': 'The NPS data path does not exist'})
            return spark.createDataFrame([], StructType([]))

    def nps_processor_results(self, dates, mode, input_path, output_path, business_unit, language, status, job):
        """
        Read the filtered NPS data and saves output results according to the date of call
        :param list[str] dates: the list of dates for which we want to do the analysis
        :param str mode: whether the surveys should be read from Medallia or Confirmit
        :param str input_path: the path containing the nps data
        :param str output_path: the path where we want to store the survey data
        :param str business_unit: the business unit code for the data
        :param str language: the language code for the data
        :param status: the status of the survey - completed or not
        :return: a dataframe containing the survey data according to the date
        :rtype: pyspark.sql.dataframe.DataFrame
        """
        df_res = spark.createDataFrame([], StructType([]))
           
        for d in dates:
            logging_path = os.path.dirname(output_path)
            logging = notebook_logging(path = f'{logging_path}/logs/nps')
            logging.fill_source({'Date': d, 
                                 'Job_ID': job, 
                                 'Parameters': {'run_dates': [date.strftime("%Y-%m-%d") for date in dates], 
                                                'input_path': input_path, 
                                                'output_path': output_path, 
                                                'business_unit': business_unit, 
                                                'language': language, 
                                                'mode': mode, 
                                                'status': status}})
            
            date_str = d.strftime("%Y-%m-%d")
            print('Running for ' + date_str)
            
            df_nps = self.read_nps_data(mode, input_path, business_unit, language, status, logging, d)
            try:
                if mode=='Medallia':
                    df_nps = df_nps.withColumn('NPS_Source', lit('Medallia'))
                else:
                    df_nps = df_nps.withColumn('NPS_Source', lit('Confirmit'))
                    df_nps = df_nps.withColumn('NPS_Customer_Type', lit(None).cast('string'))
                    df_nps = df_nps.withColumn('NPS_Contact_Id', lit(None).cast('string'))
            except Exception as e:
                logging.fill_source({'Date': d, 'Error_Message': f'Could not process NPS data: {e}'})
                raise e
            
            date_of_running = datetime.date.today()
            if (date_of_running - d).days > 21:
                try:
                    df_nps = df_nps.withColumn('NPS_Call_Date', to_timestamp('NPS_Call_Date').cast('date').cast('string'))
                    df_full = df_nps.filter((f.col('NPS_Call_Date') == date_str))
                    
                    if df_full.count() > 0:
                        print("\t{} rows {} columns".format(df_full.count(), len(df_full.columns)))
                        
                        if (df_res.count() > 0):
                            df_res = df_res.unionByName(df_full)
                        else:
                            df_res = df_full
                        
                        yyyymmdd = d.strftime('%Y%m%d')
                        df_full.coalesce(1).write.mode('append').format('parquet').save(output_path + '/' + yyyymmdd)
                        print('\tSucessfully export the results')
                        logging.fill_source({'Date': d,'Rows_Entered': df_full.count(), "Columns_Entered": len(df_full.columns)})
                
                    else:
                        yyyymmdd = d.strftime('%Y%m%d')
                        print("No survey results for ", yyyymmdd)
                        logging.fill_source({'Date': d, 'Note': f'No survey results for :{yyyymmdd}'})
                        
                except Exception as e:
                    logging.fill_source({'Date': d, 'Error_Message': f'Could not filter NPS data by call date: {e}'})
                    raise e
                       
            else:
                print("Not enough time has passed to backfill the survey results.")
                logging.fill_source({'Date': d, 'Error_Message': 'Not enough time has passed to backfill the survey results'})
                continue
        
        return df_res
