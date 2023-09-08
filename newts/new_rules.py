# Databricks notebook source
!pip install pyspark
!pip install opencv-python
!pip install azure.core
!pip install azure.ai.formrecognizer
!pip install pdf2image

# COMMAND ----------

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

from pyspark.sql import SparkSession
#from azure.storage.blob import BlobServiceClient
import os
from pyspark.sql.functions import col, when

#spark = SparkSession.builder.getOrCreate()

from computer_version import ComputerVersionRules
from Form_Recognizer import DocRead
from rules import FraudDocumentAnalytics

# COMMAND ----------

cv = ComputerVersionRules()
fr = DocRead()

# COMMAND ----------

from datetime import datetime, timedelta

def get_toronto_time():
    from datetime import datetime
    import pytz

    timestamp = datetime.now()
    toronto_time = timestamp.astimezone(pytz.timezone('US/Eastern')).strftime("%H:%M:%S")

    return toronto_time

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_doc_analytics/fraud_tmp/fruad_training")

# COMMAND ----------

root_path = f"abfss://aalab-mlworkspace-gbpii@cacaadatalakeproddl.dfs.core.windows.net/cdn-aa-gb-fraud-doc-analytics/Fraud_Invoices/"

for date in dbutils.fs.ls(root_path):
    date_name = date.name[:-1]
    print(date_name)

    date_path = root_path+date_name+"/"
    dbfs_path = "dbfs:/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_doc_analytics/fraud_tmp/fruad_training"
    # print('Start moving folders from ADLS to DBFS at {}'.format(get_toronto_time()))
    # dbutils.fs.cp(date_path, dbfs_path, recurse = True)
    # print('Finish moving folders from ADLS to DBFS at {}'.format(get_toronto_time()))
    files_with_stamp = cv.detect_stamp(date_name,dbfs_path)
    files_with_stamp = files_with_stamp.filter(F.col('if_stamp')==True).rdd.map(lambda x:x.obj_id).collect()
    print(files_with_stamp)
    # print('Start read document content at {}'.format(get_toronto_time()))
    # out_df  = fr.read_document(date_name,date_path)
    # print('Finish read document content at {}'.format(get_toronto_time()))
    # out_df  = fr.sort_document(out_df)
    # out_df = fr.handwritten_per(out_df)
    # out_df = fr.check_stamp(out_df,files_with_stamp)
    # columns = ['obj_id','img_path','content','word_confidence','page_label','handwritten_percentage','has_stamp']
    # out_df = out_df.select(*columns)
    # out_df.write.option("header", "true").mode("overwrite").parquet("/dbfs/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_doc_analytics/fraud_result/fraud_training.parquet")
    
    # print('Successfully save document reading result on {} at {}'.format(date_name, get_toronto_time()))



# COMMAND ----------

import cv2
import matplotlib.pyplot as plt
def detect_stamp_page(filename):
    orb = cv2.SIFT_create()
    train_image = cv2.imread("Capture.png")
    train_image = cv2.cvtColor(train_image, cv2.COLOR_BGR2RGB)
    train_image_blur = cv2.GaussianBlur(train_image.copy(), (15, 15), 0)
    kp1, des1 = orb.detectAndCompute(train_image_blur, None)

    try:                   
        img = cv2.imread(filename)
        img_to_match = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
        img_to_match_blur = cv2.GaussianBlur(img_to_match.copy(), (21, 21), cv2.BORDER_DEFAULT)
        kp2, des2 = orb.detectAndCompute(img_to_match_blur, None)

        if des2 is None:
            return False

        bf = cv2.BFMatcher()
        matches = bf.knnMatch(des1, des2, k=2)
        print(matches)
        good = []

        match_distance = 0.9
        for m, n in matches:
            if m.distance < match_distance * n.distance:
                good.append([m])
        print(len(good))

        match_image = cv2.drawMatchesKnn(train_image,kp1,img_to_match,kp2,good[:100],None,flags=2)

        fig = plt.figure(figsize=(15,15))
        ax = fig.add_subplot(1,1,1)
        ax.imshow(match_image)
        plt.show()
        if len(good) > 5:
            return True
        else:
            return False
    except:
        return False


# COMMAND ----------

detect_stamp_page("/dbfs/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_doc_analytics/fraud_tmp/fruad_training/Dina_Imaeva_page_1.png")

# COMMAND ----------

# MAGIC %pip install poppler-utils

# COMMAND ----------

import cv2,os
import logging
from pdf2image import convert_from_path


# pages = convert_from_path(r"sample.pdf",poppler_path='')
pages = convert_from_path(r"sample.pdf",poppler_path='/usr/bin')

img = np.array(pages[0])

print(img)

# COMMAND ----------

import fitz
doc = fitz.open('sample.pdf')
for i,page in enumerate(doc):
    pix = page.get_pixmap()  # render page to an image
    pix.save(f"page_{i}.png")

    

# COMMAND ----------

tmp = cv2.imread("page_0.png")
print(tmp)

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_doc_analytics/fraud_tmp/fruad_training/sample.pdf")

# COMMAND ----------

out_df.display()

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

df = spark.read.parquet("/dbfs/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_doc_analytics/fraud_result/fraud_training.parquet")
invoice = ['paramedical_invoice','other_receipt']
df = df.filter(df.page_label.isin(invoice))

# COMMAND ----------

rs = FraudDocumentAnalytics()

# COMMAND ----------

df= rs.bulk_treatment(df)

# COMMAND ----------

df.display()

# COMMAND ----------
