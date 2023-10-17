import uuid
from pyspark.sql.types import *
from delta.tables import * 
from datetime import datetime,date
from pyspark.sql import functions as f

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")

class delta_table_crud():
    def __init__(self,**kwargs):
        '''
            Constructor for delta merge. This loads columns dynamically, stores columns, stores schema, and creates blank source table
            Input:  columns: use following format involving structtype as the key, and name of the colum in a list eg. {"StringType()":['number','value'],"IntegerType()":["this","that"]}
                    path: location of delta table
            Note that PK must be listed as 'id'
        '''
        if kwargs is not None:
            if kwargs["path"] is not None:
                self.path=kwargs["path"] 
            if kwargs["columns"] is not None:
                col=dict(kwargs["columns"])
                strfields=[]
                intfields=[]
                dtfields=[]
                blfields=[]
                tstampfileds=[]
                cols=[]
                for k,v in col.items():
                    if k=="StringType()":
                        strfields=[StructField(str(i),StringType()) for i in v]
                    if k=="IntegerType()":
                        intfields=[StructField(str(i),IntegerType()) for i in v]
                    if k=="DateType()":
                        dtfields=[StructField(str(i),DateType()) for i in v]
                    if k=="BooleanType()":
                        blfields=[StructField(str(i),BooleanType()) for i in v]
                    if k=="TimestampType()":
                        tstampfields=[StructField(str(i),TimestampType()) for i in v]
                    for i in v:
                        cols.append(i)
                
                self.schema=StructType(dtfields+tstampfields+blfields+strfields+intfields)
                self.cols=cols
                self.source=spark.createDataFrame(data=[],schema=self.schema)
                
    def _load_target(self):
        output_path=self.path
        
        if DeltaTable.isDeltaTable(spark,f'{output_path}')==False: #see if delta exists at this location
 
            self.source.coalesce(1).write.mode('append').format('delta').save(output_path)
            
        self.target=DeltaTable.forPath(spark,f'{output_path}')
    
    def insert_target(self,df):
        ''' insert or update data into delta table, data is user defined through a dict '''
        self._load_target() #get target table
        target=self.target
        #if dict do this,
        if type(df)==dict: #data tags. create datasource and json strings for insertion and updates
            df_row=[]
            for col in self.cols:
                if col in df:
                    df_row.append(df[col])
                else:
                    df_row.append(None)
        
            source=spark.createDataFrame(data=[df_row],schema=self.schema)
            update_string={} #prepare update and insert string
            for key in df:
                update_string[key]="source."+key
            insert_string=update_string
        else: #data frame inserted. create datasource and json strings for insertion and updates
            source=df
            update_string={} #prepare update and insert string
            for key in df.columns:
                update_string[key]="source."+key
            insert_string=update_string
        
                
        target.alias('target').merge(source.alias('source'),'target.id=source.id')\
            .whenMatchedUpdate(set=update_string)\
            .whenNotMatchedInsert(values = insert_string)\
            .execute()
 
    def read_data(self,cols,filtered=None): #return a dataframe for selected cols filtered by id
        ''' want to be able to find data from the delta table. Takes in desired columns, converts to dataframe, returns columns in df. There is an option to filter (NOT WORKED ON HERE) '''
        self._load_target()
        target=self.target
        df=target.toDF()
        if cols is None: #get all columns if not enetered
            cols=self.columns
        df=df.select(*cols)
        if filtered is not None:
            df=df.filter(f.col(filtered[k])==filtered[v])
        return df
 
 
class notebook_logging(): #only responsible for creating data labels to populate data frame
    def __init__(self,**kwargs):
        if kwargs is not None:
            if kwargs["path"] is not None:
                path=kwargs["path"]
                struct={
                    "DateType()":["Date"],
                    "TimestampType()":["Created_Date"],
                    "BooleanType()":["Success"],
                    "StringType()":["id","Parameters","Folder","Job_ID","Error_Message", "Note"],
                    "IntegerType()":["Rows_Entered","Columns_Entered","Rows_Updated"]
                }
                dtu=delta_table_crud(path=path,columns=struct)
                
                cols=[]
                
                for k,v in struct.items():
                    for i in v:
                        cols.append(i)
                
                self.path=path
                self.dtu=dtu
                self.cols=cols
                self.add_data=True
                self.id=str(uuid.uuid4())
                
    def fill_source(self, data_dict): #fills source dataframe 
        #add to data_dict
        data_dict=self.create_data(data_dict)
        dtu=self.dtu
        dtu.insert_target(data_dict)       
    
    def create_data(self,data_dict):
        ''' On first use, populate source table with additional information, based on new entry or not '''
        #need to pass in data set. Only run on initialization (ie when status is set to false)
        #will look to create data for folders, retries, created date
        
        data_dict["id"]=self.id
        
        if self.add_data==True:
            self.add_data=False
            current_timestamp=datetime.now()
            data_dict["Folder"]=self.path
            data_dict["Created_Date"]=current_timestamp
            data_dict["Success"]=False
        elif "Error_Message" not in data_dict and self.add_data==False:
            data_dict["Success"]=True
        else:
            data_dict['Success']=False
            
            
        return data_dict
