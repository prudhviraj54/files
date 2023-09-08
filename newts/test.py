# Databricks notebook source
from pyspark.sql import SparkSession
#from azure.storage.blob import BlobServiceClient
import os
from pyspark.sql.functions import col, when

#spark = SparkSession.builder.getOrCreate()

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
from rules import FraudDocumentAnalytics
from pyspark.sql import functions as F

# COMMAND ----------

df = spark.read.parquet("/dbfs/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_doc_analytics/fraud_result/2023-03-13.parquet")

# COMMAND ----------

# processed =  ['2023-04-01', '2023-04-02', '2023-04-03', '2023-04-04', '2023-04-05', '2023-04-06', '2023-04-07', '2023-04-08']

# i = 0
# for date in processed:
#     if i == 0:
#         df= spark.read.parquet("/dbfs/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_doc_analytics/fraud_result/"+date+".parquet")
#     else:
#         df_tmp = spark.read.parquet("/dbfs/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_doc_analytics/fraud_result/"+date+".parquet")
#         df = df.union(df_tmp)

#     i += 1

# COMMAND ----------

rs = FraudDocumentAnalytics()

# COMMAND ----------

invoice = ['paramedical_invoice','other_receipt']
df = df.filter(df.page_label.isin(invoice))

# COMMAND ----------

df.count()

# COMMAND ----------


# df = rs.paid_by_cash(df)
# df = rs.has_dollar_symbol(df)
# df = rs.handwritten_check(df,0.3)
# df = rs.invoice_num_check(df)
# df = rs.get_payment(df)
# df = rs.address_check(df)
# df = rs.get_reg_num(df)
# df = rs.bulk_treatment(df)
df = rs.get_phone(df)
df = rs.get_fax(df)
df = rs.get_email(df)

# COMMAND ----------

df.display()

# COMMAND ----------

boolean_cols = ['has_stamp','paid_by_cash','no_dollar_symbol','above_handwritten_threshold','invalid_invoice_nbr','has_invalid_addr']

df = df.withColumn('true_count', sum(F.when(F.col(col_name)==True, 1).otherwise(0) for col_name in boolean_cols))

df.filter(F.col('true_count')>2).display()

# COMMAND ----------

df.filter(df.has_stamp==True).display()

# COMMAND ----------

df = df.withColumn('payment_above_200', F.when(F.col('payment_amount')>200, True).otherwise(False))
df = df.withColumn('payment_above_400', F.when(F.col('payment_amount')>400, True).otherwise(False))
df = df.withColumn('payment_above_500', F.when(F.col('payment_amount')>500, True).otherwise(False))
df = df.withColumn('invalid_addr', F.when((F.col('handwritten_percentage')<=0.3) & (F.col('has_invalid_addr')==True) , True).otherwise(False))

df_to_rank =df.select(['obj_id','has_stamp','paid_by_cash','no_dollar_symbol','above_handwritten_threshold','invalid_invoice_nbr','invalid_addr','payment_above_200','payment_above_400','payment_above_500'])

to_int_list = ['has_stamp','paid_by_cash','no_dollar_symbol','above_handwritten_threshold','invalid_invoice_nbr','invalid_addr','payment_above_200','payment_above_400','payment_above_500']

df_to_rank = df_to_rank.select(['obj_id']+[col(c).cast("integer") for c in to_int_list])


# COMMAND ----------

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

# COMMAND ----------

ordered_cols = ['obj_id','paid_by_cash&payment_below','has_stamp&payment_below','no_dollar_symbol&payment_below','above_handwritten_threshold&payment_below','invalid_invoice_nbr&payment_below','invalid_addr&payment_below','paid_by_cash&payment_above','has_stamp&payment_above','no_dollar_symbol&payment_above','above_handwritten_threshold&payment_above','invalid_invoice_nbr&payment_above','invalid_addr&payment_above']
df_to_rank = df_to_rank.select(*ordered_cols)

# COMMAND ----------

df_to_rank.display()

# COMMAND ----------

weights = [0.0475,0.095,0.005,0.06,0.025,0.0175,0.38,0.19,0.02,0.24, 0.1, 0.07]

# COMMAND ----------

df_to_rank = df_to_rank.withColumn('risk_score',F.col('paid_by_cash&payment_below')*weights[0]\
    +F.col('has_stamp&payment_below')*weights[1]+F.col('no_dollar_symbol&payment_below')*weights[2]+\
        F.col('above_handwritten_threshold&payment_below')*weights[3]+\
        F.col('invalid_invoice_nbr&payment_below')*weights[4]+\
            F.col('invalid_addr&payment_below')*weights[5]+F.col('paid_by_cash&payment_above')*weights[6]\
    +F.col('has_stamp&payment_above')*weights[7]+F.col('no_dollar_symbol&payment_above')*weights[8]+\
        F.col('above_handwritten_threshold&payment_above')*weights[9]+\
        F.col('invalid_invoice_nbr&payment_above')*weights[10]+\
            F.col('invalid_addr&payment_above')*weights[11])


# COMMAND ----------

df_to_rank.sort(F.col("risk_score").desc()).display()

# COMMAND ----------
