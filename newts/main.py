
# Databricks notebook source
# MAGIC %md
# MAGIC # PYSPARK Code

# COMMAND ----------

#!pip install pyspark
#!pip install opencv-python
#!pip install azure.core
#!pip install azure.ai.formrecognizer

# COMMAND ----------

from collections import namedtuple
from pyspark.sql import SparkSession
#from azure.storage.blob import BlobServiceClient
import os
from pyspark.sql.functions import col, when

#spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
import argparse
import os
from pathlib import Path
import sys
import json
import csv
import pandas as pd
import re
import numpy as np
from pathlib import Path

from computer_version import ComputerVersionRules
from Form_Recognizer import DocRead
from rules import FraudDocumentAnalytics
from src.databricks.ocs_images.config import spark_main

input_path = spark_main.input_path
local_output_path = spark_main.output_path

cv = ComputerVersionRules(input_path,local_output_path)
fr = DocRead()
date_name = spark_main.date_name
batch_name = spark_main.batch_name
batch_path =  spark_main.batch_path
files_with_stamp = cv.detect_stamp(date_name,batch_name,input_path)
files_with_stamp = files_with_stamp.collect()
test2  = fr.read_document(date_name,batch_name,batch_path)
test2  = fr.sort_document(batch_name, test2)
# test2  = fr.parse_metadata(batch_name, input_path,local_output_path, test2)
test2 = fr.handwritten_per(batch_name,test2)
test2 = fr.check_stamp(test2,files_with_stamp)

test2.show()

rs = FraudDocumentAnalytics()

test2 = rs.paid_by_cash(test2)
test2 = rs.has_dollar_symbol(test2)
test2 = rs.handwritten_check(test2,0.3)
test2 = rs.invoice_num_check(test2)
test2 = rs.get_payment(test2)
test2 = rs.address_check(test2)
test2 = rs.get_reg_num(test2)

test2.show()
