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

# MAGIC %md 
# MAGIC ### Data Prepartion 

# COMMAND ----------

# MAGIC %md 
# MAGIC #### True Fraud Data

# COMMAND ----------

cv = ComputerVersionRules
fr = DocRead()

# COMMAND ----------

root_path = f"abfss://aalab-mlworkspace-gbpii@cacaadatalakeproddl.dfs.core.windows.net/cdn-aa-gb-fraud-doc-analytics/Fraud_Invoices/true_cases/"

date_name = 'true_cases'
dbfs_path = "dbfs:/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_invoice_samples/red_flag_param_invoices"
files_with_stamp = cv.detect_stamp(date_name,dbfs_path)
files_with_stamp_list = files_with_stamp.filter(F.col('if_stamp')==True).rdd.map(lambda x:x.obj_id).collect()
files_with_stamp_list = [re.sub(r'\-|\_', '', file) for file in files_with_stamp_list]
out_df  = fr.read_document(date_name,root_path)
out_df  = fr.sort_document(out_df)
out_df = fr.handwritten_per(out_df)
out_df = fr.check_stamp(out_df,files_with_stamp_list)
columns = ['obj_id','img_path','FR_result','content','word_confidence','page_label','handwritten_percentage','has_stamp']
out_df = out_df.select(*columns)
# out_df.write.option("header", "true").mode("overwrite").parquet("/dbfs/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_doc_analytics/fraud_result/"+date_name+".parquet")



# COMMAND ----------

out_df.display()

# COMMAND ----------

stamp_files = ['true_cases_Advanced Health Rehabilitation.pdf','true_cases_Alexander Ianovsky.pdf','true_cases_Dina Imaeva.pdf','true_cases_Expert Health Centre.pdf','true_cases_Zoya Bondarchuck (2050363).pdf','true_cases_Weidong Yin.pdf']

out_df = out_df.withColumn("has_stamp", \
              F.when(out_df["obj_id"].isin(stamp_files), True).otherwise(out_df["has_stamp"]))

# COMMAND ----------

out_df = out_df.withColumn('fraud_label', F.lit(1))

# COMMAND ----------

out_df.display()

# COMMAND ----------

out_df.write.option("header", "true").mode("overwrite").parquet("/dbfs/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_doc_analytics/model_training_date/true_fraud.parquet")

# COMMAND ----------

df_true = spark.read.option("header", "true").parquet("/dbfs/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_doc_analytics/model_training_date/true_fraud.parquet")

# COMMAND ----------

df_true.count()

# COMMAND ----------

# MAGIC %md 
# MAGIC #### False Fraud Data

# COMMAND ----------

df_false = spark.read.option("header", "true").parquet("/dbfs/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_doc_analytics/fraud_result/2023-04-12.parquet")

# COMMAND ----------

types = ['paramedical_invoice','other_receipt']
df_false = df_false.filter(df_false.page_label.isin(types)).sample(0.02,seed=443)
df_false = df_false.withColumn('fraud_label', F.lit(0))

# COMMAND ----------

df_false.count()

# COMMAND ----------

df_false.display()

# COMMAND ----------

df_false.write.option("header", "true").mode("overwrite").parquet("/dbfs/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_doc_analytics/model_training_date/false_fraud.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### All Training Data
# MAGIC

# COMMAND ----------

df_all = df_true.union(df_false)

# COMMAND ----------

df_all = df_all.orderBy(F.rand())

# COMMAND ----------

df_all.write.option("header", "true").mode("overwrite").parquet("/dbfs/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_doc_analytics/model_training_data/all_set_initial.parquet")

# COMMAND ----------

df_all = spark.read.option("header", "true").parquet("/dbfs/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_doc_analytics/model_training_data/all_set_initial.parquet")

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

df_all.display()

# COMMAND ----------

df_all = df_all.withColumn('payment_above_200', F.when(F.col('payment_amount')>200, True).otherwise(False))
df_all = df_all.withColumn('payment_above_400', F.when(F.col('payment_amount')>400, True).otherwise(False))
df_all = df_all.withColumn('payment_above_500', F.when(F.col('payment_amount')>500, True).otherwise(False))
df_all = df_all.withColumn('invalid_addr', F.when((F.col('handwritten_percentage')<=0.3) & (F.col('has_invalid_addr')==True) , True).otherwise(False))

df_to_rank =df_all.select(['obj_id','has_stamp','paid_by_cash','no_dollar_symbol','above_handwritten_threshold','invalid_invoice_nbr','invalid_addr','payment_above_200','payment_above_400','payment_above_500','has_bulk_treatment','fraud_label'])

to_int_list = ['has_stamp','paid_by_cash','no_dollar_symbol','above_handwritten_threshold','invalid_invoice_nbr','invalid_addr','payment_above_200','payment_above_400','payment_above_500','has_bulk_treatment','fraud_label']

df_to_rank = df_to_rank.select(['obj_id']+[F.col(c).cast("integer") for c in to_int_list])


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

df_to_rank.display()

# COMMAND ----------

ordered_cols = ['obj_id','paid_by_cash&payment_below','has_stamp&payment_below','no_dollar_symbol&payment_below','above_handwritten_threshold&payment_below','invalid_invoice_nbr&payment_below','invalid_addr&payment_below','paid_by_cash&payment_above','has_stamp&payment_above','no_dollar_symbol&payment_above','above_handwritten_threshold&payment_above','invalid_invoice_nbr&payment_above','invalid_addr&payment_above','has_bulk_treatment','fraud_label']
df_to_rank = df_to_rank.select(*ordered_cols)

# COMMAND ----------

df_to_rank = df_to_rank.toPandas()

# COMMAND ----------

train_df.head()

# COMMAND ----------

from sklearn.model_selection import train_test_split
train_df, test_df = train_test_split(df_to_rank, test_size = 0.2, random_state = 42)

train_df.reset_index(drop = True, inplace = True)
test_df.reset_index(drop = True, inplace = True)

# COMMAND ----------

train_df.to_csv("/dbfs/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_doc_analytics/model_training_data/training_set.csv",index=False)
test_df.to_csv("/dbfs/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_doc_analytics/model_training_data/test_set.csv",index=False)

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

# COMMAND ----------

train_df = pd.read_csv('/dbfs/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_doc_analytics/model_training_data/training_set.csv')
test_df = pd.read_csv('/dbfs/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_doc_analytics/model_training_data/test_set.csv')

# COMMAND ----------

X_train = train_df.drop(columns=['obj_id','fraud_label'])
X_test = test_df.drop(columns=['obj_id','fraud_label'])
y_train = train_df[['fraud_label']]
y_test = test_df[['fraud_label']]

# COMMAND ----------

models = [
    ('Logistic Regression', LogisticRegression()),
    ('Random Forest', RandomForestClassifier()),
    ('SVM', SVC()),
    ('Gradient Boosting', GradientBoostingClassifier())
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

# COMMAND ----------

best_model

# COMMAND ----------

y_pred = best_model.predict(X_test)

# COMMAND ----------

cm = confusion_matrix(y_test, y_pred)
print(cm)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Model Training

# COMMAND ----------

model = LogisticRegression()

X_train = df_to_rank.drop(columns=['obj_id','fraud_label'])
y_train = df_to_rank[['fraud_label']]


model.fit(X_train, y_train)



# COMMAND ----------

model_name = f'risk_scoring_LR_{str(date.today())}'
model_path = "./%s"%(model_name)
pickle.dump(model,open(model_path, 'wb'))

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ### Model testing

# COMMAND ----------

df = spark.read.option("header", "true").parquet("/dbfs/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_doc_analytics/fraud_result/2023-04-12.parquet")
types = ['paramedical_invoice','other_receipt']
df = df.filter(df.page_label.isin(types))
print(df.count())

# COMMAND ----------

model_name = f'risk_scoring_LR_2023-08-24'
model_path = "./%s"%(model_name)
model = pickle.load(open(model_path, 'rb'))

# COMMAND ----------

# df = rs.bulk_treatment(df)

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

df_to_rank.sort_values(by='risk_score', ascending=False).head(50)

# COMMAND ----------

df_to_rank.shape

# COMMAND ----------
