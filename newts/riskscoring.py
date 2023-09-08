# Databricks notebook source
!pip install opencv-python
!pip uninstall fitz
!pip install pymupdf
!pip install frontend

# COMMAND ----------

import os
from pathlib import Path
import sys
import json
import csv
import pandas as pd
import re
import numpy as np
from pathlib import Path
#path_to_module = '../content/fd_spark/'
#sys.path.append(path_to_module)
sys.path.insert(0, os.path.abspath(os.path.join('..')))
from computer_version import ComputerVersionRules
from Form_Recognizer import DocRead
from rules import FraudDocumentAnalytics
from pyspark.sql import functions as F
from configparser import ConfigParser
import ast
import pickle
from datetime import datetime,date

# COMMAND ----------

#Get the configparser object
config_object = ConfigParser()

config_object.read("config.ini")

#Get the password
config_data = config_object["DATA"]
processed = ast.literal_eval(config_data["processed_date"])


# COMMAND ----------

processed

# COMMAND ----------

scored = ['2023-04-01', '2023-04-02', '2023-04-03', '2023-04-04', '2023-04-05', '2023-04-06', '2023-04-07', '2023-04-08', '2023-04-09', '2023-04-10', '2023-04-11', '2023-04-12', '2023-04-13', '2023-04-14', '2023-04-15', '2023-04-16', '2023-04-17', '2023-04-18', '2023-03-01']

# COMMAND ----------

df = spark.read.parquet("/dbfs/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_doc_analytics/fraud_result/2023-03-13.parquet")

# COMMAND ----------

for date in processed:
    if date not in scored:
        df_tmp = spark.read.parquet(f"/dbfs/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_doc_analytics/fraud_result/"+date+".parquet")
        df = df.union(df_tmp)
        scored.append(date)

# COMMAND ----------

types = ['paramedical_invoice','other_receipt']
df = df.filter(df.page_label.isin(types))
print(df.count())

# COMMAND ----------

model_name = f'risk_scoring_LR_2023-08-24'
model_path = "./%s"%(model_name)
model = pickle.load(open(model_path, 'rb'))

# COMMAND ----------

rs = FraudDocumentAnalytics()

# COMMAND ----------

df = rs.paid_by_cash(df)
df = rs.has_dollar_symbol(df)
df = rs.handwritten_check(df,0.3)
df = rs.invoice_num_check(df)
df = rs.get_payment(df)
df = rs.address_check(df)
df = rs.get_reg_num(df)
df = rs.bulk_treatment(df)

df = df.withColumn('payment_above_200', F.when(F.col('payment_amount')>200, True).otherwise(False))
df = df.withColumn('payment_above_400', F.when(F.col('payment_amount')>400, True).otherwise(False))
df = df.withColumn('payment_above_500', F.when(F.col('payment_amount')>500, True).otherwise(False))
df = df.withColumn('invalid_addr', F.when((F.col('handwritten_percentage')<=0.3) & (F.col('has_invalid_addr')==True) , True).otherwise(False))

df_to_rank =df.select(['obj_id','has_stamp','paid_by_cash','no_dollar_symbol','above_handwritten_threshold','invalid_invoice_nbr','invalid_addr','payment_above_200','payment_above_400','payment_above_500','has_bulk_treatment'])

to_int_list = ['has_stamp','paid_by_cash','no_dollar_symbol','above_handwritten_threshold','invalid_invoice_nbr','invalid_addr','payment_above_200','payment_above_400','payment_above_500','has_bulk_treatment']

df_to_rank = df_to_rank.select(['obj_id']+[F.col(c).cast("integer") for c in to_int_list])


df_to_rank =df_to_rank.withColumn('paid_by_cash&payment_above', F.col('paid_by_cash')*F.col('payment_above_200'))
df_to_rank =df_to_rank.withColumn('has_stamp&payment_above', F.col('has_stamp')*F.col('payment_above_500'))
df_to_rank =df_to_rank.withColumn('no_dollar_symbol&payment_above', F.col('no_dollar_symbol')*F.col('payment_above_500'))
df_to_rank =df_to_rank.withColumn('above_handwritten_threshold&payment_above', F.col('above_handwritten_threshold')*F.col('payment_above_500'))
df_to_rank =df_to_rank.withColumn('invalid_invoice_nbr&payment_above', F.col('invalid_invoice_nbr')*F.col('payment_above_500'))
df_to_rank =df_to_rank.withColumn('invalid_addr&payment_above', F.col('invalid_addr')*F.col('payment_above_400'))

df_to_rank = df_to_rank.withColumn('paid_by_cash&payment_below', F.when((F.col('paid_by_cash')==1) & (F.col('paid_by_cash&payment_above')==0) , 1).otherwise(0))
df_to_rank = df_to_rank.withColumn('has_stamp&payment_below', F.when((F.col('has_stamp')==1) & (F.col('has_stamp&payment_above')==0) , 1).otherwise(0))
df_to_rank = df_to_rank.withColumn('no_dollar_symbol&payment_below', F.when((F.col('no_dollar_symbol')==1) \
    & (F.col('no_dollar_symbol&payment_above')==0) , 1).otherwise(0))
df_to_rank = df_to_rank.withColumn('above_handwritten_threshold&payment_below', \
    F.when((F.col('above_handwritten_threshold')==1) & (F.col('above_handwritten_threshold&payment_above')==0) , 1).otherwise(0))
df_to_rank = df_to_rank.withColumn('invalid_invoice_nbr&payment_below', \
    F.when((F.col('invalid_invoice_nbr')==1) & (F.col('invalid_invoice_nbr&payment_above')==0) , 1).otherwise(0))
df_to_rank = df_to_rank.withColumn('invalid_addr&payment_below', F.when((F.col('invalid_addr')==1) \
    & (F.col('invalid_addr&payment_above')==0) , 1).otherwise(0))


drop_cols = ['payment_above_200','payment_above_400','payment_above_500','paid_by_cash','has_stamp','no_dollar_symbol','above_handwritten_threshold','invalid_invoice_nbr','invalid_addr']

df_to_rank = df_to_rank.drop(*drop_cols)


ordered_cols = ['obj_id','paid_by_cash&payment_below','has_stamp&payment_below','no_dollar_symbol&payment_below','above_handwritten_threshold&payment_below','invalid_invoice_nbr&payment_below','invalid_addr&payment_below','paid_by_cash&payment_above','has_stamp&payment_above','no_dollar_symbol&payment_above','above_handwritten_threshold&payment_above','invalid_invoice_nbr&payment_above','invalid_addr&payment_above','has_bulk_treatment']
df_to_rank = df_to_rank.select(*ordered_cols)

df_to_rank = df_to_rank.toPandas()

# COMMAND ----------

df_navr = df_to_rank[['obj_id']]
df_score = df_to_rank.drop(columns=['obj_id'])

# COMMAND ----------

prob = []
for row in model.predict_proba(df_score):
    prob.append(round(row[1],3))

# COMMAND ----------

df_to_rank['risk_score']=prob

# COMMAND ----------

# df.filter(df.obj_id=='2023-03-15_0050067523292_Step-_Element-_023e9e88-c3bc-45fd-9751-ae5c5efb6411_disabilitytaxcreditreciept.pdf').display()
df.display()

# COMMAND ----------

df_to_rank.sort_values(by='risk_score', ascending=False).head(50)

# COMMAND ----------
