from delta.tables import * 
from pyspark.sql.functions import *
import logging,json
from datetime import datetime


class watermark:
    def __init__(self,spark,var_param,platform):
        self.spark = spark
        self.deltapath = None
        self.platform = platform        
        self.var_param = var_param

    def setfilePath(self,path):
        try:
            self.deltapath=path
        except Exception as e:
            raise


    def default_val(self,checkcoltype):
        # read file
        #json_data = spark.read.format("json").option("multiline","true").option("encoding", "UTF-8").load(json_path)
        if checkcoltype.lower() == 'int' or checkcoltype.lower() == 'integer':
            try:
                return str(self.var_param['DEFAULT'])
            except:
                return 0
        elif checkcoltype.lower() == 'timestamp' or checkcoltype.lower() == 'ts':
            try:
                return str(self.var_param['DEFAULT'])
            except:
                return '1800-01-01 00:00:00'
        elif checkcoltype.lower() == 'date' or checkcoltype.lower() == 'dt':
            try:
                return str(self.var_param['DEFAULT'])
            except:
                return '1800-01-01'
        else:
            return ''
        
    def end_val_ret(self,checkcoltype):
        if checkcoltype.lower() == 'timestamp' or checkcoltype.lower() == 'ts':
            today_ts=datetime.utcnow().strftime(self.var_param['timestampFmt'])
            return today_ts
        elif checkcoltype.lower() == 'date' or checkcoltype.lower() == 'dt':
            today_dt=datetime.utcnow().strftime(self.var_param['timestampFmt'])
            return today_dt
        else:
            return ''
            
    def Select_WatermarkValue(self,checkcoltype):
        try :
            if self.platform == "databricks":
             from databricks.sdk.runtime import dbutils
             files = dbutils.fs.ls(self.deltapath)
            else:
             from notebookutils import mssparkutils 
             files = mssparkutils.fs.ls(self.deltapath) 
            if len(files) == 0:
                return self.default_val(checkcoltype, self.var_param)
            else:
                df=self.spark.read.format("delta").option("multiline","true").option("encoding", "UTF-8").load(self.deltapath)
                return df.filter(lower(col("Status"))== "complete").agg({"WatermarkValue_end_ts":"max"}).collect()[0][0]
        except Exception as e:
            raise
    
    def write_WatermarkValue(self,pipelinerunid,pipelinerunname,TableName,SourceName,ParamPathName,CheckColumn,CheckColumnType,WatermarkValue_start_ts,WatermarkValue_end_ts,Status,RunTime,Count,LogOutput,Error_Txt,CreateTs):
        try:
            New_Data_df = self.spark.createDataFrame(
            [
                (pipelinerunid,pipelinerunname,TableName,SourceName,ParamPathName,CheckColumn,CheckColumnType,WatermarkValue_start_ts,WatermarkValue_end_ts,Status,RunTime,Count,LogOutput,Error_Txt,CreateTs)
            ],
            ["pipelinerunid","pipelinerunname","TableName","SourceName","ParamPathName","CheckColumn","CheckColumnType","WatermarkValue_start_ts","WatermarkValue_end_ts","Status","RunTime","Count","LogOutput","Error_Txt","CreateTs"])
            
            DeltaTable.forPath(self.spark,self.deltapath).alias("basetbl").merge(New_Data_df.alias("deltatbl"),'basetbl.pipelinerunid=deltatbl.pipelinerunid') \
            .whenMatchedUpdate (set=
            {
                "WatermarkValue_start_ts":"deltatbl.WatermarkValue_start_ts",
                "WatermarkValue_end_ts":"deltatbl.WatermarkValue_end_ts",
                "Status":"deltatbl.Status",
                "RunTime":"deltatbl.RunTime",
                "Count":"deltatbl.Count",
                "LogOutput":"deltatbl.LogOutput",
                "Error_Txt":"deltatbl.Error_Txt",
                "CreateTs":"deltatbl.CreateTs"
            }
            ) \
            .whenNotMatchedInsert(values={
                "PipelineRunId":"deltatbl.PipelineRunId",
                "PipelineRunName":"deltatbl.PipelineRunName",
                "TableName":"deltatbl.TableName",
                "SourceName":"deltatbl.SourceName",
                "ParamPathName":"deltatbl.ParamPathName",
                "CheckColumn":"deltatbl.CheckColumn",
                "CheckColumnType":"deltatbl.CheckColumnType",
                "WatermarkValue_start_ts":"deltatbl.WatermarkValue_start_ts",
                "WatermarkValue_end_ts":"deltatbl.WatermarkValue_end_ts",
                "Status":"deltatbl.Status",
                "RunTime":"deltatbl.RunTime",
                "Count":"deltatbl.Count",
                "LogOutput":"deltatbl.LogOutput",
                "Error_Txt":"deltatbl.Error_Txt",
                "CreateTs":"deltatbl.CreateTs"
            }) \
            .execute()
            return 'Delta table got merged'
        except Exception as e:
            if 'is not a Delta table' in str(e):
                New_Data_df = self.spark.createDataFrame(
                [
                    (pipelinerunid,pipelinerunname,TableName,SourceName,ParamPathName,CheckColumn,CheckColumnType,WatermarkValue_start_ts,WatermarkValue_end_ts,Status,RunTime,Count,LogOutput,Error_Txt,CreateTs)
                ],
                ["pipelinerunid","pipelinerunname","TableName","SourceName","ParamPathName","CheckColumn","CheckColumnType","WatermarkValue_start_ts","WatermarkValue_end_ts","Status","RunTime","Count","LogOutput","Error_Txt","CreateTs"]   )
                
                New_Data_df.write.format("delta").option("header","true").option("encoding", "UTF-8").save(self.deltapath)
                return 'Delta table got created'
            else:
                raise 
            
    def execute_WatermarkValue(self,pipeline_id,pipeline_name,read_write_ind,result_json):
        try:
           out={}
           if read_write_ind=='R':
            out["pipeline_id"]=str(pipeline_id)
            out["incremental_col_nm"]=str(self.var_param['incremental_col_nm'])
            out["timestampFmt"]=self.var_param['timestampFmt']
            out["timestampSlice"]=self.var_param['timestampSlice']

            if self.var_param['override_start'] and self.var_param['override_end']:
                start=self.var_param['override_start'] 
                end=self.var_param['override_end']
            else:
                start=self.Select_WatermarkValue(self.var_param['incremental_checkcoltype'])
                end=self.end_val_ret(self.var_param['incremental_checkcoltype'])
            
            self.write_WatermarkValue(str(pipeline_id),str(pipeline_name),str(self.var_param['target_path']),str(self.var_param['source_path']),str(self.var_param),str(self.var_param['incremental_col_nm']),str(self.var_param['incremental_checkcoltype']),str(start),str(end),'starting','0',0,'Execution started','',str(datetime.utcnow().strftime(self.var_param['timestampFmt'])))
            out["startTimestamp"]=str(start)
            out["endTimestamp"]=str(end)
            
           elif read_write_ind=='W':
            result_data=json.loads(result_json)
            pipeline_id_eval=result_data['pipeline_id']
            status='complete'

            if result_data['exec_errormsg']:
                status='error'
            self.write_WatermarkValue(str(pipeline_id_eval),str(pipeline_name),str(self.var_param['target_path']),str(self.var_param['source_path']),str(self.var_param),str(self.var_param['incremental_col_nm']),str(self.var_param['incremental_checkcoltype']),str(result_data['WatermarkValue_start_ts']),str(result_data['WatermarkValue_end_ts']),str(status),str(result_data['exec_time_seconds']),str(result_data['op_recs_cnt']),str(result_data['exec_logmsg']),str(result_data['exec_errormsg']),str(datetime.utcnow().strftime(self.var_param['timestampFmt'])))
            logging.info('Watermark loading is done................')
           else:
            logging.error('Invalid option passed.................')
           return out 
        except Exception as e:
            self.write_WatermarkValue(str(pipeline_id),str(pipeline_name),str(self.var_param['target_path']),str(self.var_param['source_path']),str(self.var_param),str(self.var_param['incremental_col_nm']),str(self.var_param['incremental_checkcoltype']),'','','error',str(datetime.now().strftime(self.var_param['timestampFmt'])),0,'Exception occured',str(e),str(datetime.utcnow().strftime(self.var_param['timestampFmt'])))
            logging.error(str(e))
            return "{error:"+str(e)+"}"     
