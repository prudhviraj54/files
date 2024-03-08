# Databricks notebook source
!pip install opencv-python
!pip install azure-ai-formrecognizer
!pip install ctgan
#!pip install fitz
!pip install frontend
!pip install tools
!pip install PyMuPDF

# COMMAND ----------

#!pip uninstall fitz --yes
#!pip install PyPDF2

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
#sys.path.insert(0, os.path.abspath(os.path.join('..')))
#os.makedirs('static')
from computer_version import ComputerVersionRules
from Form_Recognizer import DocRead
from rules import FraudDocumentAnalytics
from pyspark.sql import functions as F
from configparser import ConfigParser
import ast
import pickle
from datetime import datetime,date
from ctgan import CTGAN
from pyspark.sql.functions import regexp_extract,regexp_replace,when,col

# COMMAND ----------

import cv2
print(cv2.__version__)

# COMMAND ----------

cv = ComputerVersionRules()
fr = DocRead()

# COMMAND ----------

from azure.core.credentials import AzureKeyCredential


# COMMAND ----------

storage_account = "cacaadatalakeproddl"
spn = "e5ea9a15-f79c-428b-9445-1bd3a5a33483"
#https://cac-aa-datalakeprod.vault.azure.net/
spn_secret = dbutils.secrets.get(scope="cacaadatalakeprod",key="cdo-aa-terraform-prod-spn")
spark.conf.set("fs.azure.account.auth.type.{}.dfs.core.windows.net".format(storage_account),"OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.{}.dfs.core.windows.net".format(storage_account),
                "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.{}.dfs.core.windows.net".format(storage_account),
            spn)
spark.conf.set("fs.azure.account.oauth2.client.secret.{}.dfs.core.windows.net".format(storage_account),
                spn_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.{}.dfs.core.windows.net".format(storage_account), "https://login.microsoftonline.com/5d3e2773-e07f-4432-a630-1a0f68a28a05/oauth2/token")


# COMMAND ----------

# dbutils.fs.cp("abfss://aalab-mlworkspace-gbpii@cacaadatalakeproddl.dfs.core.windows.net/cdn-aa-gb-fraud-doc-analytics/Fraud_Invoices/true_cases_jan/","dbfs:/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_doc_analytics/test_folder/")
# dbutils.fs.cp("abfss://aalab-mlworkspace-gbpii@cacaadatalakeproddl.dfs.core.windows.net/cdn-aa-gb-fraud-doc-analytics/Fraud_Invoices/true_cases_jan/","dbfs:/FileStore/test_folder/",recurse = True)

# COMMAND ----------

root_path = f"abfss://aalab-mlworkspace-gbpii@cacaadatalakeproddl.dfs.core.windows.net/cdn-aa-gb-fraud-doc-analytics/Fraud_Invoices/true_cases_jan/"
date_name = 'true_cases'
#dbfs_path = "abfss://aalab-mlworkspace-gbpii@cacaadatalakeproddl.dfs.core.windows.net/cdn-aa-gb-fraud-doc-analytics/Fraud_Invoices/true_cases_jan/"
dbfs_path="dbfs:/FileStore/test_folder/"
#dbfs_path = "dbfs:/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_invoice_samples/True_cases_nov"
files_with_stamp = cv.detect_stamp(date_name,dbfs_path)
#files_with_stamp = files_with_stamp.filter(F.col('if_stamp')==True).select('obj_id').collect()
files_with_stamp_list = files_with_stamp.filter(F.col('if_stamp')==True).rdd.map(lambda x:x.obj_id).collect()
#files_with_stamp_list = [re.sub(r'\-|\_', '', file) for file in files_with_stamp_list]
out_df  = fr.read_document(date_name,root_path)
out_df  = fr.sort_document(out_df)
out_df = fr.handwritten_per(out_df)
out_df = fr.check_stamp(out_df,files_with_stamp_list)
columns = ['obj_id','img_path','FR_result','content','word_confidence','page_label','handwritten_percentage','has_stamp']
out_df = out_df.select(*columns)
#out_df.write.option("header", "true").mode("overwrite").parquet("/dbfs/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_doc_analytics/fraud_result/"+date_name+".parquet")

# COMMAND ----------

files_with_stamp.groupBy('if_stamp').count().display()

# COMMAND ----------

out_df.groupBy('has_stamp').count().display()

# COMMAND ----------

out_df.display()

# COMMAND ----------

#root_path = f"abfss://aalab-mlworkspace-gbpii@cacaadatalakeproddl.dfs.core.windows.net/cdn-aa-gb-fraud-doc-analytics/Fraud_Invoices/true_cases/"
#date_name = 'true_cases'
#dbfs_path = "dbfs:/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_invoice_samples/red_flag_param_invoices"
# files_with_stamp = cv.detect_stamp(date_name,dbfs_path)
#files_with_stamp = files_with_stamp.filter(F.col('if_stamp')==True).select('obj_id').collect()
# files_with_stamp_list = files_with_stamp.filter(F.col('if_stamp')==True).rdd.map(lambda x:x.obj_id).collect()
# files_with_stamp_list = [re.sub(r'\-|\_', '', file) for file in files_with_stamp_list]
#out_df  = fr.read_document(date_name,root_path)

# COMMAND ----------

# stamp_files = ["true_cases_Advanced Health Rehabilitation.pdf","true_cases_Alexander Ianovsky.pdf","true_cases_Dina Imaeva.pdf","true_cases_Expert Health Centre.pdf","true_cases_Zoya Bondarchuck (2050363).pdf","true_cases_Weidong Yin.pdf","true_cases_Screenshot 2024-02-06 at 9.56.15 AM.png","true_cases_Screenshot 2024-02-06 at 9.57.34 AM.png","true_cases_Screenshot 2024-02-06 at 9.58.08 AM.png","true_cases_Screenshot 2024-02-06 at 9.58.37 AM.png","true_cases_Screenshot 2024-02-06 at 9.59.07 AM.png","true_cases_Screenshot 2024-02-06 at 9.59.42 AM.png","true_cases_Screenshot 2024-02-06 at 10.00.10 AM.png","true_cases_Screenshot 2024-02-06 at 10.00.36 AM.png","true_cases_Screenshot 2024-02-06 at 10.01.11 AM.png","true_cases_Screenshot 2024-02-06 at 10.02.00 AM.png","true_cases_Screenshot 2024-02-06 at 10.02.25 AM.png","true_cases_Screenshot 2024-02-06 at 10.03.29 AM.png","true_cases_Screenshot 2024-02-06 at 10.06.08 AM.png","true_cases_Screenshot 2024-02-06 at 10.06.08 AM.png","true_cases_Screenshot 2024-02-06 at 10.08.34 AM.png","true_cases_Screenshot 2024-02-06 at 10.09.10 AM.png","true_cases_Screenshot 2024-02-06 at 10.11.21 AM.png","true_cases_Screenshot 2024-02-06 at 10.19.23 AM.png","true_cases_Screenshot 2024-02-06 at 10.19.56 AM.png","true_cases_Screenshot 2024-02-06 at 10.20.35 AM.png","true_cases_205005-1272006 - stockings.pdf"]
    
# out_df = out_df.withColumn("has_stamp", \
#               F.when(out_df["obj_id"].isin(stamp_files), True).otherwise(out_df["has_stamp"]))

# COMMAND ----------

out_df = out_df.withColumn('fraud_label', F.lit(1))

# COMMAND ----------

#out_df.write.option("header", "true").mode("overwrite").parquet("/dbfs/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_doc_analytics/model_training_date/true_fraud.parquet")
out_df.write.option("header", "true").mode("overwrite").parquet("abfss://aalab-mlworkspace-gbpii@cacaadatalakeproddl.dfs.core.windows.net/cdn-aa-gb-fraud-doc-analytics/model_training_data/true_fraud.parquet")

# COMMAND ----------

#df_true = spark.read.option("header", "true").parquet("/dbfs/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_doc_analytics/model_training_date/true_fraud.parquet")
df_true = spark.read.option("header", "true").parquet("abfss://aalab-mlworkspace-gbpii@cacaadatalakeproddl.dfs.core.windows.net/cdn-aa-gb-fraud-doc-analytics/model_training_data/true_fraud.parquet")

# COMMAND ----------

df_true.count()

# COMMAND ----------

# MAGIC %md 
# MAGIC #### False Fraud Data

# COMMAND ----------

#df_false = spark.read.option("header", "true").parquet("/dbfs/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_doc_analytics/fraud_result/2023-04-14.parquet")
df_false = spark.read.option("header", "true").csv("abfss://aalab-mlworkspace-gbpii@cacaadatalakeproddl.dfs.core.windows.net/cdn-aa-gb-fraud-doc-analytics/Fraud_results/2023-04-14.csv")

# COMMAND ----------

types = ['paramedical_invoice','other_receipt']
df_false = df_false.filter(df_false.page_label.isin(types)).sample(0.07,seed=443)
df_false = df_false.withColumn('fraud_label', F.lit(0))

# COMMAND ----------

df_false.count()

# COMMAND ----------

from pyspark.sql.types import DoubleType,BooleanType
df_false = df_false.withColumn("handwritten_percentage", col("handwritten_percentage").cast(DoubleType())) \
                    .withColumn("has_stamp", col("has_stamp").cast(BooleanType()))

# COMMAND ----------

#df_false.write.option("header", "true").mode("overwrite").parquet("/dbfs/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_doc_analytics/model_training_date/false_fraud.parquet")
df_false.write.option("header", "true").mode("overwrite").parquet("abfss://aalab-mlworkspace-gbpii@cacaadatalakeproddl.dfs.core.windows.net/cdn-aa-gb-fraud-doc-analytics/model_training_data/false_fraud.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### All Training Data
# MAGIC

# COMMAND ----------

df_true.columns

# COMMAND ----------

df_false.columns

# COMMAND ----------

df_true.dtypes

# COMMAND ----------

df_false.dtypes

# COMMAND ----------

df_true=df_true.drop("FR_result")

# COMMAND ----------

df_all = df_true.union(df_false)

# COMMAND ----------

df_all = df_all.orderBy(F.rand())

# COMMAND ----------

#df_all.write.option("header", "true").mode("overwrite").parquet("/dbfs/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_doc_analytics/model_training_data/all_set_initial.parquet")
df_all.write.option("header", "true").mode("overwrite").parquet("abfss://aalab-mlworkspace-gbpii@cacaadatalakeproddl.dfs.core.windows.net/cdn-aa-gb-fraud-doc-analytics/model_training_data/all_set_initial.parquet")

# COMMAND ----------

#df_all = spark.read.option("header", "true").parquet("/dbfs/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_doc_analytics/model_training_data/all_set_initial.parquet")
df_all = spark.read.option("header", "true").parquet("abfss://aalab-mlworkspace-gbpii@cacaadatalakeproddl.dfs.core.windows.net/cdn-aa-gb-fraud-doc-analytics/model_training_data/all_set_initial.parquet")

# COMMAND ----------

df_all.display()

# COMMAND ----------

df_all.filter(col("content").isNull()).display()

# COMMAND ----------

df_all = df_all.fillna("No data found",subset=['content'])

# COMMAND ----------

#val=df_all.groupBy("no_dollar_symbol").count().orderBy("no_dollar_symbol")
#val.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ### Pre-process Data

# COMMAND ----------

rs = FraudDocumentAnalytics()

# COMMAND ----------

df_all = rs.paid_by_cash(df_all)
df_all = rs.has_dollar_symbol(df_all)
df_all = rs.handwritten_check(df_all,0.3)
df_all = rs.invoice_num_check(df_all)
df_all = rs.get_payment(df_all)
df_all = rs.address_check(df_all)
df_all = rs.get_reg_num(df_all)
df_all = rs.bulk_treatment(df_all)

# COMMAND ----------

#df_all = rs.get_phone(df_all)
#df_all = rs.invoice_num(df_all)

# COMMAND ----------

#invoice_pattern = r"(?:invoice #|rx |Inovice:|Invoice # |invoice NO.|invoice:|Registration #)(\d+)"
#df_all = df_all.withColumn("invoice_number", regexp_extract("content", invoice_pattern, 1))

# COMMAND ----------

#from pyspark.sql.functions import col
#val=df_all.groupBy("has_bulk_treatment").count().orderBy("has_bulk_treatment")
val_count=df_all.groupBy("payment_amount").count().filter(col("count")>1)
val_count.display()

# COMMAND ----------

#contains_rx = df_all.filter(col('content').like('%rx%'))
#contains_rx.display()

# COMMAND ----------

df_all = df_all.withColumn('payment_amount',
                    when((df_all['fraud_label'] == 1) & (df_all['payment_amount'] <= 100), 800)
                    .when((df_all['fraud_label'] == 1) & (df_all['payment_amount'] < 300) & (df_all['payment_amount'] > 100), 900)
                    .when((df_all['fraud_label'] == 1) & (df_all['payment_amount'] == 'No Amount'), 500)
                     .otherwise(df_all['payment_amount'])
)

# COMMAND ----------

df_all.groupby('payment_amount').count().display()

# COMMAND ----------

df_all.agg(F.max('payment_amount')).collect()[0][0]

# COMMAND ----------

df.display()

# COMMAND ----------

df_all = df_all.withColumn('payment_above_300', F.when(F.col('payment_amount')>300, True).otherwise(False))
df_all = df_all.withColumn('payment_above_400', F.when(F.col('payment_amount')>400, True).otherwise(False))
df_all = df_all.withColumn('payment_above_500', F.when(F.col('payment_amount')>500, True).otherwise(False))
df_all = df_all.withColumn('invalid_addr', F.when((F.col('handwritten_percentage')<=0.3) & (F.col('has_invalid_addr')==True) , True).otherwise(False))

df_to_rank =df_all.select(['obj_id','has_stamp','paid_by_cash','no_dollar_symbol','above_handwritten_threshold','invalid_invoice_nbr','invalid_addr','payment_amount','payment_above_300','payment_above_400','payment_above_500','has_bulk_treatment','fraud_label'])

to_int_list = ['has_stamp','paid_by_cash','no_dollar_symbol','above_handwritten_threshold','invalid_invoice_nbr','invalid_addr','payment_amount','payment_above_300','payment_above_400','payment_above_500','has_bulk_treatment','fraud_label']

df_to_rank = df_to_rank.select(['obj_id']+[F.col(c).cast("integer") for c in to_int_list])


# COMMAND ----------

#don't run this cell next time
#df_to_rank = df_to_rank.toPandas()
#df_new = df_to_rank.drop(columns=['obj_id'])
#print(df_new.shape)
#df_new.corr()

# COMMAND ----------

df_to_rank =df_to_rank.withColumn('paid_by_cash&payment_above', F.col('paid_by_cash')*F.col('payment_above_300'))
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


drop_cols = ['payment_above_300','payment_above_400','payment_above_500','paid_by_cash','has_stamp','no_dollar_symbol','above_handwritten_threshold','invalid_invoice_nbr','invalid_addr']

df_to_rank = df_to_rank.drop(*drop_cols)

# COMMAND ----------

ordered_cols = ['obj_id','payment_amount','paid_by_cash&payment_below','has_stamp&payment_below','no_dollar_symbol&payment_below','above_handwritten_threshold&payment_below','invalid_invoice_nbr&payment_below','invalid_addr&payment_below','paid_by_cash&payment_above','has_stamp&payment_above','no_dollar_symbol&payment_above','above_handwritten_threshold&payment_above','invalid_invoice_nbr&payment_above','invalid_addr&payment_above','has_bulk_treatment','fraud_label']
df_to_rank = df_to_rank.select(*ordered_cols)

# COMMAND ----------

df_to_rank = df_to_rank.toPandas()

# COMMAND ----------

df_to_rank.shape

# COMMAND ----------

#Removig duplicates which has same invoice numbers
#mask = df_to_rank[df_to_rank.invoice_number.isna()]
#df_to_rank.dropna(inplace=True)
#df_to_rank.drop_duplicates(subset=['invoice_number'], inplace=True)
#df_to_rank=pd.concat([df_to_rank, mask], axis=0)
#df_to_rank= df_to_rank.reset_index(drop=True)
#df.drop(columns=["index"], inplace=True)
#df_to_rank.drop(columns=["invoice_number"],inplace=True)
#df_to_rank.shape

# COMMAND ----------

#df_to_rank
dicretae_columns =['obj_id','fraud_label']
ctgan = CTGAN(epochs=10)
ctgan.fit(df_to_rank,dicretae_columns)
syn_data=ctgan.sample(5000)

# COMMAND ----------

print(syn_data.shape)
syn_data.fraud_label.value_counts()

# COMMAND ----------

df_to_rank.head()

# COMMAND ----------

df_to_rank.isnull().sum()

# COMMAND ----------

df_to_rank[df_to_rank.payment_amount.isnull()==True]

# COMMAND ----------

df_to_rank.payment_amount.fillna(0,inplace=True)

# COMMAND ----------

from sklearn.model_selection import train_test_split
train_df, test_df = train_test_split(df_to_rank, test_size = 0.2, random_state = 42)
#train_df, test_df = train_test_split(syn_data, test_size = 0.2, random_state = 42)

train_df.reset_index(drop = True, inplace = True)
test_df.reset_index(drop = True, inplace = True)

# COMMAND ----------

#train_df.to_csv("/dbfs/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_doc_analytics/model_training_data/training_set.csv",index=False)
#test_df.to_csv("/dbfs/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_doc_analytics/model_training_data/test_set.csv",index=False)

spark_train_df = spark.createDataFrame(train_df)
spark_train_df.write.option("header", "true").mode("overwrite").csv("abfss://aalab-mlworkspace-gbpii@cacaadatalakeproddl.dfs.core.windows.net/cdn-aa-gb-fraud-doc-analytics/model_training_data/training_set_2024_01_17.csv")

spark_test_df = spark.createDataFrame(test_df)
spark_test_df.write.option("header","true").mode("overwrite").csv("abfss://aalab-mlworkspace-gbpii@cacaadatalakeproddl.dfs.core.windows.net/cdn-aa-gb-fraud-doc-analytics/model_training_data/test_set_2024_01_17.csv")

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ### Model Selection

# COMMAND ----------

from sklearn.model_selection import cross_val_score
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.svm import SVC
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import confusion_matrix
from sklearn.tree import DecisionTreeClassifier

# COMMAND ----------

#train_df = pd.read_csv('/dbfs/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_doc_analytics/model_training_data/training_set.csv')
#test_df = pd.read_csv('/dbfs/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_doc_analytics/model_training_data/test_set.csv')
train_df = spark.read.option("header", "true").csv("abfss://aalab-mlworkspace-gbpii@cacaadatalakeproddl.dfs.core.windows.net/cdn-aa-gb-fraud-doc-analytics/model_training_data/training_set_2024_01_17.csv")
train_df = train_df.toPandas()
test_df = spark.read.option("header", "true").csv("abfss://aalab-mlworkspace-gbpii@cacaadatalakeproddl.dfs.core.windows.net/cdn-aa-gb-fraud-doc-analytics/model_training_data/test_set_2024_01_17.csv")
test_df = test_df.toPandas()
train_df.shape,test_df.shape

# COMMAND ----------

#invalid_addr&payment_below
#no_dollar_symbol&payment_below
#has_stamp&payment_below
#train_df = train_df[['obj_id','invalid_addr&payment_below','no_dollar_symbol&payment_below','has_stamp&payment_below','fraud_label']]
#test_df = test_df[['obj_id','invalid_addr&payment_below','no_dollar_symbol&payment_below','has_stamp&payment_below','fraud_label']]

# COMMAND ----------

X_train = train_df.drop(columns=['obj_id','fraud_label'])
X_test = test_df.drop(columns=['obj_id','fraud_label'])
y_train = train_df[['fraud_label']]
y_test = test_df[['fraud_label']]

# COMMAND ----------

#from databricks import dbutils
#workspace_id = dbutils.entry_point.getDbutils().notebook().getContext().api_url().split('/')[6]
#print(workspace_id)

# COMMAND ----------

spark.conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId")

# COMMAND ----------

models = [
    ('Logistic Regression', LogisticRegression()),
    ('Random Forest', RandomForestClassifier()),
    ('Decision Tree',DecisionTreeClassifier())
    #('SVM', SVC()),
    #('Gradient Boosting', GradientBoostingClassifier())
]

results = []
for name, model in models:
    scores = cross_val_score(model, X_train, y_train, cv=5, scoring = 'accuracy')
    results.append((name, scores.mean(), scores.std()))

for name, mean_score, std_score in results:
    print(f'{name}: Mean Accuracy = {mean_score:.4f}|Std = {std_score: .4f}')

for name, model in models:
    model.fit(X_train, y_train)
    test_accuracy = model.score(X_test, y_test)
    print(f'\n{name}: Test Accuracy = {test_accuracy: .4f}')
    y_pred = model.predict(X_test)
    cm = confusion_matrix(y_test, y_pred)
    print('Confusion Matrix:')
    print(cm)

#Compare models on the test set
best_model_name = max(results, key=lambda x: x[1])[0]
best_model = [model for name, model in models if name==best_model_name][0]
best_model.fit(X_train, y_train)
test_accuracy = best_model.score(X_test, y_test)
print(f'\nBest Model ({best_model_name}) Test Accuracy = {test_accuracy: .4f}')
print(results)

# COMMAND ----------

best_model

# COMMAND ----------

results

# COMMAND ----------

y_pred = best_model.predict(X_test)

# COMMAND ----------

cm = confusion_matrix(y_test, y_pred)
print(cm)

# COMMAND ----------

# MAGIC %md
# MAGIC ###USING SMOTE TECHNIQUE

# COMMAND ----------

!pip install imblearn

# COMMAND ----------

from imblearn.over_sampling import SMOTE
sm = SMOTE(random_state = 42)
X_train_res, y_train_res = sm.fit_resample(X_train, y_train)

# COMMAND ----------

y_train_res.fraud_label.value_counts()

# COMMAND ----------

models = [
    ('Logistic Regression', LogisticRegression()),
    ('Random Forest', RandomForestClassifier()),
    ('SVM', SVC()),
    ('Gradient Boosting', GradientBoostingClassifier())
]

results = []
for name, model in models:
    scores = cross_val_score(model, X_train_res, y_train_res, cv=5, scoring = 'accuracy')
    results.append((name, scores.mean(), scores.std()))

for name, mean_score, std_score in results:
    print(f'{name}: Mean Accuracy = {mean_score:.4f}|Std = {std_score: .4f}')

for name, model in models:
    model.fit(X_train_res, y_train_res)
    test_accuracy = model.score(X_test, y_test)
    print(f'\n{name}: Test Accuracy = {test_accuracy: .4f}')
    y_pred = model.predict(X_test)
    cm = confusion_matrix(y_test, y_pred)
    print('Confusion Matrix:')
    print(cm)

#Compare models on the test set
best_model_name = max(results, key=lambda x: x[1])[0]
best_model = [model for name, model in models if name==best_model_name][0]
best_model.fit(X_train_res, y_train_res)
test_accuracy = best_model.score(X_test, y_test)
print(f'\nBest Model ({best_model_name}) Test Accuracy = {test_accuracy: .4f}')
print(results)

# COMMAND ----------

best_model

# COMMAND ----------

results

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Model Training

# COMMAND ----------

model = best_model
model

# COMMAND ----------

model = best_model
#model = LogisticRegression()
#model = RandomForestClassifier()
#model = GradientBoostingClassifier()
#model = SVC(DecisionTreeClassifier())
#model = DecisionTreeClassifier()
X_train = df_to_rank.drop(columns=['obj_id','fraud_label'])
y_train = df_to_rank[['fraud_label']]


model.fit(X_train, y_train)


# COMMAND ----------

#model = SVC(gamma='auto',probability=True)
#model = LogisticRegression()
#model.fit(X_train_res, y_train_res)

# COMMAND ----------

model_name = f'risk_scoring_LR1_{str(date.today())}'
model_path = "./%s"%(model_name)
pickle.dump(model,open(model_path, 'wb'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Parquet files for test data

# COMMAND ----------

root_path = f"abfss://aalab-mlworkspace-gbpii@cacaadatalakeproddl.dfs.core.windows.net/cdn-aa-gb-fraud-doc-analytics/OCS_Images/2023-10-28/"
date_name = '2023-10-28'
dbfs_path = "dbfs:/FileStore/testfiles_parquet_folder/"
# dbfs_path = "abfss://aalab-mlworkspace-gbpii@cacaadatalakeproddl.dfs.core.windows.net/cdn-aa-gb-fraud-doc-analytics/OCS_Images/2023-10-27/"
#dbfs_path = "dbfs:/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_invoice_samples/True_cases_nov"
files_with_stamp = cv.detect_stamp(date_name,dbfs_path)
#files_with_stamp = files_with_stamp.filter(F.col('if_stamp')==True).select('obj_id').collect()
files_with_stamp_list = files_with_stamp.filter(F.col('if_stamp')==True).rdd.map(lambda x:x.obj_id).collect()
#files_with_stamp_list = [re.sub(r'\-|\_', '', file) for file in files_with_stamp_list]
out_df = fr.read_document(date_name,root_path)
out_df = fr.sort_document(out_df)
out_df = fr.handwritten_per(out_df)
out_df = fr.check_stamp(out_df,files_with_stamp_list)
columns = ['obj_id','img_path','FR_result','content','word_confidence','page_label','handwritten_percentage','has_stamp']
out_df = out_df.select(*columns)
out_df.write.option("header", "true").mode("overwrite").parquet("abfss://aalab-mlworkspace-gbpii@cacaadatalakeproddl.dfs.core.windows.net/cdn-aa-gb-fraud-doc-analytics/Fraud_results/test_parquet_files/"+date_name+".parquet")
#out_df.write.option("header", "true").mode("overwrite").parquet("/dbfs/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_doc_analytics/fraud_result/"+date_name+".parquet")

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ### Model testing

# COMMAND ----------

#model_name = f'risk_scoring_LR1_{str(date.today())}'
model_path_pickle = "/dbfs/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_doc_analytics/fraud_result/LR1_08_Mar_2024.pkl"
#model_path_pickle = f"abfss://aalab-mlworkspace-gbpii@cacaadatalakeproddl.dfs.core.windows.net/cdn-aa-gb-fraud-doc-analytics/Fraud_results/LR_{str(date.today())}.pkl"
pickle.dump(model,open(model_path_pickle, 'wb'))
#dbutils.fs.cp("/dbfs/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_doc_analytics/fraud_result/LR1_17_Jan_2024.pkl","abfss://aalab-mlworkspace-gbpii@cacaadatalakeproddl.dfs.core.windows.net/cdn-aa-gb-fraud-doc-analytics/Fraud_results/")

# COMMAND ----------

df = spark.read.option("header", "true").parquet("abfss://aalab-mlworkspace-gbpii@cacaadatalakeproddl.dfs.core.windows.net/cdn-aa-gb-fraud-doc-analytics/Fraud_results/test_parquet_files/truecases.parquet")
df.display()

# COMMAND ----------

#df = spark.read.option("header", "true").parquet("/dbfs/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_doc_analytics/fraud_result/2023-10-10.parquet")
#df = spark.read.option("header", "true").parquet("abfss://aalab-mlworkspace-gbpii@cacaadatalakeproddl.dfs.core.windows.net/cdn-aa-gb-fraud-doc-analytics/Fraud_results/test_parquet_files/2023-10-06.parquet")
df = spark.read.option("header", "true").parquet("abfss://aalab-mlworkspace-gbpii@cacaadatalakeproddl.dfs.core.windows.net/cdn-aa-gb-fraud-doc-analytics/model_training_data/true_fraud.parquet")
types = ['paramedical_invoice','other_receipt']
#df = df.filter(df.page_label.isin(types))
print(df.count())
df.display()

# COMMAND ----------

#df.filter(col("page_label")).count()
df.groupBy("page_label").count().display()

# COMMAND ----------

# contains_rx = df.filter(col('obj_id').like('%2023-10-03_0050074397594%'))
# contains_rx.display()

# COMMAND ----------

#df1.filter(col("obj_id")=="2023-04-23_0050068918283_Step-_Element-_29ec8654-3af0-4581-9c1d-7932f6777c5f_nexgen.jpg.jpg").display()

# COMMAND ----------

model_path_pickle = "/dbfs/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_doc_analytics/fraud_result/LR1_14_Feb_2024.pkl"
model = pickle.load(open(model_path_pickle, 'rb'))

# COMMAND ----------

# model_name = f'risk_scoring_LR1_2023-12-12'
# model_path = "./%s"%(model_name)
# model = pickle.load(open(model_path, 'rb'))

# COMMAND ----------

df = rs.paid_by_cash(df)
df = rs.has_dollar_symbol(df)
df = rs.handwritten_check(df,0.3)
df = rs.invoice_num_check(df)
df = rs.get_payment(df)
df = rs.address_check(df)
df = rs.get_reg_num(df)
df = rs.bulk_treatment(df)

df = df.withColumn('payment_above_300', F.when(F.col('payment_amount')>300, True).otherwise(False))
df = df.withColumn('payment_above_400', F.when(F.col('payment_amount')>400, True).otherwise(False))
df = df.withColumn('payment_above_500', F.when(F.col('payment_amount')>500, True).otherwise(False))
df = df.withColumn('invalid_addr', F.when((F.col('handwritten_percentage')<=0.3) & (F.col('has_invalid_addr')==True) , True).otherwise(False))

#invoice_pattern = r"(?:invoice #|rx |Inovice:|Invoice # |invoice #: cni|invoice #: |invoice NO.|invoice:|receipt #|receipt#: )(\d+)"
#df = df.withColumn("invoice_number", regexp_extract("content", invoice_pattern, 1))

df_to_rank1 =df.select(['obj_id','content','has_stamp','paid_by_cash','no_dollar_symbol','above_handwritten_threshold','invalid_invoice_nbr','invalid_addr','payment_amount','payment_above_300','payment_above_400','payment_above_500','has_bulk_treatment'])

to_int_list = ['has_stamp','paid_by_cash','no_dollar_symbol','above_handwritten_threshold','invalid_invoice_nbr','invalid_addr','payment_amount','payment_above_300','payment_above_400','payment_above_500','has_bulk_treatment']

df_to_rank1 = df_to_rank1.select(['obj_id','content']+[F.col(c).cast("integer") for c in to_int_list])


df_to_rank1 =df_to_rank1.withColumn('paid_by_cash&payment_above', F.col('paid_by_cash')*F.col('payment_above_300'))
df_to_rank1 =df_to_rank1.withColumn('has_stamp&payment_above', F.col('has_stamp')*F.col('payment_above_500'))
df_to_rank1 =df_to_rank1.withColumn('no_dollar_symbol&payment_above', F.col('no_dollar_symbol')*F.col('payment_above_500'))
df_to_rank1 =df_to_rank1.withColumn('above_handwritten_threshold&payment_above', F.col('above_handwritten_threshold')*F.col('payment_above_500'))
df_to_rank1 =df_to_rank1.withColumn('invalid_invoice_nbr&payment_above', F.col('invalid_invoice_nbr')*F.col('payment_above_500'))
df_to_rank1 =df_to_rank1.withColumn('invalid_addr&payment_above', F.col('invalid_addr')*F.col('payment_above_400'))

df_to_rank1 = df_to_rank1.withColumn('paid_by_cash&payment_below', F.when((F.col('paid_by_cash')==1) & (F.col('paid_by_cash&payment_above')==0) , 1).otherwise(0))
df_to_rank1 = df_to_rank1.withColumn('has_stamp&payment_below', F.when((F.col('has_stamp')==1) & (F.col('has_stamp&payment_above')==0) , 1).otherwise(0))
df_to_rank1 = df_to_rank1.withColumn('no_dollar_symbol&payment_below', F.when((F.col('no_dollar_symbol')==1) \
    & (F.col('no_dollar_symbol&payment_above')==0) , 1).otherwise(0))
df_to_rank1 = df_to_rank1.withColumn('above_handwritten_threshold&payment_below', \
    F.when((F.col('above_handwritten_threshold')==1) & (F.col('above_handwritten_threshold&payment_above')==0) , 1).otherwise(0))
df_to_rank1 = df_to_rank1.withColumn('invalid_invoice_nbr&payment_below', \
    F.when((F.col('invalid_invoice_nbr')==1) & (F.col('invalid_invoice_nbr&payment_above')==0) , 1).otherwise(0))
df_to_rank1 = df_to_rank1.withColumn('invalid_addr&payment_below', F.when((F.col('invalid_addr')==1) \
    & (F.col('invalid_addr&payment_above')==0) , 1).otherwise(0))


drop_cols = ['payment_above_300','payment_above_400','payment_above_500','paid_by_cash','has_stamp','no_dollar_symbol','above_handwritten_threshold','invalid_invoice_nbr','invalid_addr']

df_to_rank1 = df_to_rank1.drop(*drop_cols)


ordered_cols = ['obj_id','content','payment_amount','paid_by_cash&payment_below','has_stamp&payment_below','no_dollar_symbol&payment_below','above_handwritten_threshold&payment_below','invalid_invoice_nbr&payment_below','invalid_addr&payment_below','paid_by_cash&payment_above','has_stamp&payment_above','no_dollar_symbol&payment_above','above_handwritten_threshold&payment_above','invalid_invoice_nbr&payment_above','invalid_addr&payment_above','has_bulk_treatment']
df_to_rank1 = df_to_rank1.select(*ordered_cols)

df_to_rank1 = df_to_rank1.toPandas()

# COMMAND ----------

df_to_rank1.shape

# COMMAND ----------

df2=df_to_rank1[df_to_rank1.obj_id=="true_cases_112994-100006528 invoice.pdf"]
df2.display()

# COMMAND ----------

#Removig duplicates which has same invoice numbers
#mask = df_to_rank1[df_to_rank1.invoice_number.isna()]
#df_to_rank1.dropna(inplace=True)
df_to_rank1 = df_to_rank1[~df_to_rank1['content'].str.contains("explanation of benefits",case=False)]
df_to_rank1.drop_duplicates(subset=['content'], inplace=True)
#df_to_rank1=pd.concat([df_to_rank1, mask], axis=0)
#df_to_rank1= df_to_rank1.reset_index(drop=True)
#df.drop(columns=["index"], inplace=True)
df_to_rank1.drop(columns=["content"],inplace=True)
df_to_rank1.shape

# COMMAND ----------

df_to_rank1[df_to_rank1.payment_amount.isnull()==True]

# COMMAND ----------

df_to_rank1.isnull().sum()

# COMMAND ----------

df_to_rank1.payment_amount.fillna(0,inplace=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Percentgae rules applied

# COMMAND ----------

# cols = ['paid_by_cash','no_dollar_symbol','above_handwritten_threshold',         
#              'invalid_invoice_nbr','invalid_addr','has_bulk_treatment']
# df_to_rank3 = df_to_rank1[['obj_id','has_stamp','payment_amount','payment_above_300','payment_above_400','payment_above_500']+ cols]
# df_to_rank3["true_count"] = df_to_rank3[cols].sum(axis=1)
# df_to_rank3["percentage_rules_applied"] = (((df_to_rank3["true_count"]+1)/7)*100).astype(int)
# df_to_rank3.head(5)

# COMMAND ----------

cols = ['paid_by_cash&payment_below',
       'has_stamp&payment_below', 'no_dollar_symbol&payment_below',
       'above_handwritten_threshold&payment_below',
       'invalid_invoice_nbr&payment_below', 'invalid_addr&payment_below',
       'paid_by_cash&payment_above', 'has_stamp&payment_above',
       'no_dollar_symbol&payment_above',
       'above_handwritten_threshold&payment_above',
       'invalid_invoice_nbr&payment_above', 'invalid_addr&payment_above',
       'has_bulk_treatment']
df_to_rank3 = df_to_rank1[['obj_id', 'payment_amount', 'paid_by_cash&payment_below',
       'has_stamp&payment_below', 'no_dollar_symbol&payment_below',
       'above_handwritten_threshold&payment_below',
       'invalid_invoice_nbr&payment_below', 'invalid_addr&payment_below',
       'paid_by_cash&payment_above', 'has_stamp&payment_above',
       'no_dollar_symbol&payment_above',
       'above_handwritten_threshold&payment_above',
       'invalid_invoice_nbr&payment_above', 'invalid_addr&payment_above',
       'has_bulk_treatment',]]# 'risk_score']]
df_to_rank3["true_count"] = df_to_rank3[cols].sum(axis=1)
#df_to_rank3["percentage_rules_applied"] = (((df_to_rank3["true_count"]+1)/7)*100).astype(int)
df_to_rank3.head(5)

# COMMAND ----------

df_to_rank3.display()

# COMMAND ----------

df_to_rank3[df_to_rank3.obj_id==""]

# COMMAND ----------

l1 = ["2023-10-04_0050074437795_Step-_Element-_c4cb75e5-d5c3-49e3-81f9-ce0cafad8a27_CameraPhoto.jpg.jpg",

"2023-10-04_0050074435561_Step-_Element-_54b2bd91-b3b3-4adc-b794-770f1fa4f4d6_F919D8F8-2A60-4E96-BF00-18E86E4492A8.png.jpg",

"2023-10-04_0050074438608_Step-_Element-_5261c385-c5b3-4630-83d7-94a2baa1ba9f_IMG_4016.jpg.jpg",

"2023-10-04_0050074439307_Step-_Element-_8c54df99-d4a6-4f68-be47-11ca3f2dbfe7_Payment_Receipt_for_Fall_2023.pdf",

"2023-10-04_0050074440042_Step-_Element-_5818773d-75d2-4b05-9362-577c715daa3f_LeslieMayManulife.pdf",

"2023-10-04_0050074440490_Step-_Element-_47d1c532-3234-4097-87e7-d46b69beba07_230828001.jpg.jpg",

"2023-10-04_0050074443518_Step-_Element-_462e2aa9-42d1-4d36-8ded-12072d39370a_16964297376996447099620971003507.jpg.jpg",

"2023-10-04_0050074444060_Step-_Element-_d776dc14-b47b-482b-9191-65b8fd4eb712_Invoice_Sep_30_th_,_2023.pdf",

"2023-10-04_0050074445644_Step-_Element-_21d81ea3-718e-4f29-863b-9c7ede431eca_Anjo_Soria_Flynn___08_08_2023___ORD-000451900.pdf",

"2023-10-04_0050074448694_Step-_Element-_dff2ada4-de35-44e7-b81b-7f6732ffa84a_Orthotics_Sept_2023.pdf",

"2023-10-04_0050074457401_Step-_Element-_385bdd41-83a1-43ea-87bc-7c83bd54413a_orthotics_2..jpg.jpg",

"2023-10-04_0050074458762_Step-_Element-_33899647-2d2b-4bae-a707-cc6f8503d217_Deepanshi_Chiro.pdf",


"2023-10-04_0050074458794_Step-_Element-_ee3a3826-c1fe-473b-bdde-e14d3f728639_Invoice_8748_(1)_(1).pdf",

"2023-10-04_0050074458828_Step-_Element-_ba2772e9-3d26-4b46-a943-62f4becb0555_Saraf_Rahman_-_August_2023.pdf",

"2023-10-04_0050074458906_Step-_Element-_c4a4bd6a-36b9-4742-9760-5fe0673cde1c_Deepanshi_Massage.pdf",

"2023-10-04_0050074459702_Step-_Element-_cad8675b-803b-41a0-b788-247f8ee1a2b4_20231004164616552.pdf",

"2023-10-04_0050074461577_Step-_Element-_9154ea67-fbef-44cc-9686-9df01c37f13b_CameraPhoto.jpg.jpg",

"2023-10-04_0050074462343_Step-_Element-_efc69c32-f0b8-42d7-a235-1e513e49cfa8_20231004_165746.jpg.jpg",

"2023-10-04_0050074462902_Step-_Element-_78dd42f4-da76-49ee-91c0-c5b45a9f9c72_20230831_Orthotics.pdf",

"2023-10-04_0050074472190_Step-_Element-_fd76b7b7-2652-4367-a1e2-a61bca7b81fa_Khalisah_-_orthotics_Oct_2023.pdf",


"2023-10-04_0050074472256_Step-_Element-_5c12188c-ac7f-4fd7-bc1b-236a371c2de1_Mikail_-_orthotics_prescription_Oct_2023.pdf",

"2023-10-04_0050074473075_Step-_Element-_c921a523-a8ac-44e9-8915-5e23148ea166_16964684750214550187651236762624.jpg.jpg"]

rules_applied_files = df_to_rank3[df_to_rank3.obj_id.isin(l1)]
rules_applied_files.head()

# COMMAND ----------

#rules_applied_files.to_csv("rules_applied.csv")
#/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_doc_analytis
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("sample").getOrCreate()
spark_df = spark.createDataFrame(rules_applied_files)
spark_df.display()

# COMMAND ----------

Invoice_paid_by_cash = df_to_rank3[df_to_rank3.paid_by_cash==1]
print(Invoice_paid_by_cash.shape)
Invoice_paid_by_cash.head()

# COMMAND ----------

Invoice_paid_by_cash.shape

# COMMAND ----------

Invoice_paid_by_cash_above300 = Invoice_paid_by_cash[Invoice_paid_by_cash.payment_amount>=300]
print(Invoice_paid_by_cash_above300.shape)
Invoice_paid_by_cash_above300.head(10)

# COMMAND ----------

#need to execute
df_navr = df_to_rank1[['obj_id']]
df_score = df_to_rank1.drop(columns=['obj_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checking Score with weights

# COMMAND ----------

df_score.corr()

# COMMAND ----------

weights = [0.0475,0.095,0.005,0.06,0.025,0.0175,0.38,0.19,0.02,0.24, 0.1, 0.07,0.02]

# COMMAND ----------

score = df_score.mul(weights).sum(axis=1)
top_1 = score.quantile(0.99)
top_2 = score.quantile(0.95)

# COMMAND ----------

df_to_rank1['risk_score'] = score

df_to_rank1['risk_level'] = 'Not Suspicious'
df_to_rank1.loc[df_to_rank1.risk_score>=top_2,'risk_level'] = 'Should Investigate'
df_to_rank1.loc[df_to_rank1.risk_score>=top_1,'risk_level'] = 'Must Investigate'

# COMMAND ----------

df_to_rank1.risk_level.value_counts()

# COMMAND ----------

df_to_rank1.sort_values(by='risk_score', ascending=False).head(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prediction using Model

# COMMAND ----------

prob = []
for row in model.predict_proba(df_score):
    prob.append(round(row[1],3))

# COMMAND ----------

df_to_rank1['risk_score']=prob

# COMMAND ----------

#origiunal values when ran by Baito
df_to_rank1.risk_score.value_counts()

# COMMAND ----------

 df_to_rank2[df_to_rank2.obj_id == '2023-10-05_0050074490583_Step-_Element-_30cec2f9-a300-4420-9605-9e63f3bbcdfc_20231005_121354.jpg.jpg']

# COMMAND ----------

df_to_rank2['file_name'] = df_to_rank1['obj_id'].str.replace("2023-10-06_","",1)
file_list = df_to_rank2["file_name"].to_list()
#file_list = []
#for i in range(len(lt1)):
#    temp = lt1[i].split("2023-10-06_")[1]
#    file_list.append(temp)

# COMMAND ----------

file_list[0:5]

# COMMAND ----------

file_list.remove("0050074543474_Step-_Element-_73e87b08-93cd-4388-a640-d65ea46df181_20231006_165430[1].jpg.jpg")

# COMMAND ----------

len(file_list)

# COMMAND ----------

str1="abfss://aalab-mlworkspace-gbpii@cacaadatalakeproddl.dfs.core.windows.net/cdn-aa-gb-fraud-doc-analytics/OCS_Images/2023-10-06/0050074541750_Step-_Element-_b0840d3f-7e61-44b3-ae47-a60451600702_IMG_0049[1].JPG.jpg"
str1[201]

# COMMAND ----------

source_path = "abfss://aalab-mlworkspace-gbpii@cacaadatalakeproddl.dfs.core.windows.net/cdn-aa-gb-fraud-doc-analytics/OCS_Images/2023-10-06/"
destination_path ="abfss://aalab-mlworkspace-gbpii@cacaadatalakeproddl.dfs.core.windows.net/cdn-aa-gb-fraud-doc-analytics/Final_output/2023-10-06/"
for item in file_list:
    source_path_file=f"{source_path}{item}"
    destination_path_file=f"{destination_path}"
    dbutils.fs.cp(source_path_file,destination_path_file)


# COMMAND ----------

path="abfss://aalab-mlworkspace-gbpii@cacaadatalakeproddl.dfs.core.windows.net/cdn-aa-gb-fraud-doc-analytics/Final_output/2023-10-06/"
files_ct=dbutils.fs.ls(path)
len(files_ct)

# COMMAND ----------

df_to_rank2 = df_to_rank1[df_to_rank1.payment_amount>300]
print(df_to_rank2.shape)
df_to_rank2.sort_values(by='risk_score', ascending=False).head(50)

# COMMAND ----------

df_to_rank2 = df_to_rank1[df_to_rank1.payment_amount>300]
print(df_to_rank2.shape)
df_to_rank2.sort_values(by='risk_score', ascending=False).head(50)

# COMMAND ----------

df_to_rank1.sort_values(by='risk_score', ascending=False).head(50)

# COMMAND ----------



# COMMAND ----------

fraud_check = df_to_rank1.sort_values(by='risk_score', ascending=False).head(50)
fraud_check.to_csv("fraud_check_file.csv")

# COMMAND ----------

dbfs_path = "dbfs:/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_invoice_samples/red_flag_param_invoices"

root_path = f"abfss://aalab-mlworkspace-gbpii@cacaadatalakeproddl.dfs.core.windows.net/cdn-aa-gb-fraud-doc-analytics/OCS_Images/"

dbutils.fs.cp(root_path, dbfs_path, recurse = True)

