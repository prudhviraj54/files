import json
import sys
import os

from delta.tables import *
from pyspark.sql import functions as f
from datetime import datetime,date,timedelta
from dateutil.easter import easter
from dateutil.relativedelta import MO, SU, SA
from dateutil.relativedelta import relativedelta as rd

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)


class pre_processing():
    
    def flatten_json_string(self,params,linear_params):
        '''
            recursive function to flatten json string into single level hierarchy
        '''

        for k,v in params.items():
            if type(v)==dict:
                if k=="ctr_keys" or k=="trans_keys" or k=='params_sf': #exception for ctr and trans_keys
                    linear_params[k]=v
                else: #continue flattening
                    self.flatten_json_string(v,linear_params)
            else:
                linear_params[k]=v
            
        return linear_params

    def pre_process_json_string(self,params):
        '''
            remove escape characters as necessary and flatten json string into single level hierarchy
        '''
        json_string=params
        json_string="".join(json_string.split("\\"))
        json_file = json.loads(json_string)
        linear_params={}
        linear_params=self.flatten_json_string(json_file,linear_params)
        return linear_params    
    
    def extract_parameters(self,params):
        '''
            returns required parameters
        '''
        #TODO, exceptions in case the hard coded variables are not present 
        #TODO how to handle across different modules (ie not all values here will be present in other modules such as ctr_keys)
        #(how to handle required fields across libraries??)
        
        params=self.pre_process_json_string(params)
        
        if 'input_path' in params:
            input_path=params['input_path'] 
        if 'output_path' in params:
            output_path=params['output_path']
        if 'ctr_keys' in params:
            ctr_keys=params['ctr_keys']
        if 'trans_keys' in params:
            trans_keys=params['trans_keys']
        if 'mode' in params:
            mode=params['mode']
        if 'params_sf' in params:
            params_sf=params['params_sf']
        if 'business_unit' in params:
            business_unit=params['business_unit']
        
        
        st,ed=self.dates_get(params=params)
        dates=self.dates_list(st,ed)
        
        return dates, input_path, output_path, business_unit, trans_keys, ctr_keys, params_sf, mode
    
    def dates_get(self,params):
        '''
            gets start and end dates based on frequency. If frequency not present, then by start and end date
        '''
        limit=2 #can change this later on
        if "frequency" in params:
            frequency=params['frequency']
        if "start_date" in params:
            start_date=params["start_date"]
        if "end_date" in params:
            end_date=params["end_date"]
        if "output_path" in params:
            output_path = params["output_path"]
            path = os.path.dirname(output_path)

        if frequency=="" or frequency is None: #use start date until end date
            try:
                sdate = datetime.strptime(start_date, "%Y-%m-%d").date()
                edate = datetime.strptime(end_date, "%Y-%m-%d").date()

            except Exception as e:
                print(f"A start and end date needs to be provided to continue: {e}")
                #dbutils.notebook.exit(f'A start and end date needs to be provided to continue: {e}')
            
            return sdate,edate   
            
        else:
            try:           
                target=DeltaTable.forPath(spark,f'{path}/logs/pre_processor')
                df=target.toDF()
                if len(df.filter(f.col("Success")==True).limit(1).rdd.flatMap(lambda x:x).collect())==0: #no successful entries
                    new_date=df.orderBy(f.col("Date"),ascending=False).limit(1).select("Date").rdd.map(lambda x:x[0]).collect()[0]
                    new_date=new_date+timedelta(days=-1)
                else:
                    new_date=(df.filter(f.col("Success")==True).orderBy(f.col("Date"),ascending=False).limit(1).\
                              select("Date").rdd.map(lambda x:x[0]).collect())[0] #note that if there are no successes, then there is no new date returned
                last_date=(df.orderBy(f.col("Date"),ascending=False).limit(1).\
                   select("Date").rdd.map(lambda x:x[0]).collect())[0]
                sdate=new_date+timedelta(days=1)
            
                if frequency=="Weekly": #updates one week at a time
                    edate=last_date+timedelta(days=7)    
                else: #assume daily
                    current_date=date.today()
                    edate=current_date-timedelta(days=1) #running this the next day to get current date
                    if edate-new_date>timedelta(days=limit):
                        edate=sdate+timedelta(days=limit)
                
                return sdate,edate
            
        
            except Exception as e: 
                print(f"A start date needs to be provided no logging information is available: {e}")
                #dbutils.notebook.exit(f"A start date needs to be provided no logging information is available: {e}")
                sys.exit()
                

    def dates_list(self,start_date,end_date):
        '''
            derive a dates list using the start and end date derived from dates_get
        '''

        new_date=start_date
        list_dates=[]
        while True:
        
            list_dates.append(new_date)
            new_date+=timedelta(days=1)
            if new_date==(end_date+timedelta(days=1)):
                break
        return list_dates
        #return self.check_lists(list_dates)
    
    def is_weekend(self,*args):
        MON,TUE,WED,THU,FRI,SAT,SUN=range(7)
        WEEKEND=(SAT,SUN)
        dt=args[0] if len(args)==1 else date(*args)
        return dt.weekday() in WEEKEND

    def populate_holidays(self,year):
        holidays=[]
        name="New Year's Day"
        d=date(year,1,1)
        if self.is_weekend(d):
            d=d+rd(weekday=MO)
        holidays.append([d,name])
    
        name="Family Day"
        d=date(year,2,1)+rd(weekday=MO(+3))
        holidays.append([d,name])

        easter_date=easter(year)
        name="Good Friday"
        d=easter_date+rd(days=-2)
        holidays.append([d,name])
        name="Easter Monday"
        d=easter_date+rd(days=1)
        holidays.append([d,name])
    
        name="Victoria Day"
        d=date(year,5,24)+rd(weekday=MO(-1))
        holidays.append([d,name])
    
        name="Canada Day"
        d=date(year,7,1)
        if self.is_weekend(d):
            d=d+rd(weekday=MO)
        holidays.append([d,name])
    
        name="Civic Day"
        d=date(year,8,1)+rd(weekday=MO)
        holidays.append([d,name])
    
        name="Labour Day"
        d=date(year,9,1)+rd(weekday=MO)
        holidays.append([d,name])
    
        name="Thanksgiving"
        d=date(year,10,1)+rd(weekday=MO(+2))
        holidays.append([d,name])
    
        name="Christmas"
        d=date(year,12,25)
        if self.is_weekend(d):
            d=d+rd(days=+2)
        holidays.append([d,name])
    
        name="Boxing Day"
        d=date(year,7,1)
        if self.is_weekend(d):
            d=d+rd(days=+2)
        holidays.append([d,name])
    
        return holidays

    def populate_sundays(self,year):
    
        weekends=[]
        d=date(year,1,1)+rd(weekday=SU)
        while d.year==year:
            weekends.append([d,"Sunday"])
            d=d+rd(days=+7)
        return weekends
    
    def merge_lists(self,year):
        lst1=self.populate_sundays(year)
        lst2=self.populate_holidays(year)
        lst=lst1+lst2
        df=spark.createDataFrame(data=lst,schema=["Date","Name"])
        df=df.select(f.col("Date"))
        return df

    def check_lists(self,daterange):
        print('in check_lists')
        if daterange[0].year != daterange[-1].year: #get two calendar years
            pass
        df=self.merge_lists(daterange[0].year)
        date_list=df.select(f.collect_list("Date")).first()[0]
        return [i for i in daterange if i not in date_list]    
