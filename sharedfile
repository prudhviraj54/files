
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license.

print('************************** Project Started **************************')

# from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime

# from azureml.core import Run
# run = Run.get_context()
# run.log('Loss', 1.2)
# run.log('Loss', 1.8)
# run.log('Loss', 0.9)
# print(run.input_datasets)

# from pathlib import Path
spark = SparkSession.builder.getOrCreate()
# dbutils = DBUtils(spark)

import argparse
import os

parser = argparse.ArgumentParser("extract")

parser.add_argument("--myparam", type=str, help="input_extract data")
parser.add_argument("--myparam2", type=str, help="output_extract directory")
parser.add_argument("--input", type=str, help="output_extract directory")
parser.add_argument("--output", type=str, help="output_extract directory")

parser.add_argument("--AZUREML_RUN_TOKEN", type=str, help="output_extract directory")
parser.add_argument("--AZUREML_ARM_SUBSCRIPTION", type=str, help="output_extract directory")
parser.add_argument("--AZUREML_ARM_RESOURCEGROUP", type=str, help="output_extract directory")
parser.add_argument("--AZUREML_ARM_WORKSPACE_NAME", type=str, help="output_extract directory")

parser.add_argument("--AZUREML_RUN_ID", type=str, help="output_extract directory")
parser.add_argument("--AZUREML_ARM_PROJECT_NAME", type=str, help="output_extract directory")
parser.add_argument("--AZUREML_RUN_TOKEN_EXPIRY", type=str, help="output_extract directory")

# parser.add_argument("--titanic_input_dataset", type=str, help="output_extract directory")

args, unknown = parser.parse_known_args()

# args = parser.parse_args()

print('args here:')
print(args)
print('args end:')

print(f'args.input: {args.input}')
print(f'args.output: {args.output}')

print("In train.py")
print("As a data scientist, this is where I use my training code.")

i = args.input
o = args.output


n = i + "/testdata.txt"
df = spark.read.csv(n)

df.show()

data = [('value1', 'value2')]
df2 = spark.createDataFrame(data)

z = o + "/output.txt"
df2.write.csv(z)

print('**Token Section')

from azureml.core.authentication import AzureMLTokenAuthentication
from azureml.core import Workspace

# print(f'args.AZUREML_RUN_TOKEN: {args.AZUREML_RUN_TOKEN}')
# print(f'args.AZUREML_ARM_SUBSCRIPTION: {args.AZUREML_ARM_SUBSCRIPTION}')
# print(f'args.AZUREML_ARM_RESOURCEGROUP: {args.AZUREML_ARM_RESOURCEGROUP}')
# print(f'args.AZUREML_ARM_WORKSPACE_NAME: {args.AZUREML_ARM_WORKSPACE_NAME}')
# 
# print(f'args.AZUREML_RUN_ID: {args.AZUREML_RUN_ID}')
# print(f'args.AZUREML_ARM_PROJECT_NAME: {args.AZUREML_ARM_PROJECT_NAME}')
# 
# print(f'args.AZUREML_RUN_TOKEN_EXPIRY: {args.AZUREML_RUN_TOKEN_EXPIRY}')

# token_auth = AzureMLTokenAuthentication.create(azureml_access_token=args.AZUREML_RUN_TOKEN, 
#                                                expiry_time=datetime.fromtimestamp(int(args.AZUREML_RUN_TOKEN_EXPIRY)),
#                                                host='Databricks',
#                                                subscription_id=args.AZUREML_ARM_SUBSCRIPTION,
#                                                resource_group_name=args.AZUREML_ARM_RESOURCEGROUP,
#                                                workspace_name=args.AZUREML_ARM_WORKSPACE_NAME,
#                                                experiment_name=args.AZUREML_ARM_PROJECT_NAME,
#                                                run_id=args.AZUREML_RUN_ID,
#                                                user_email="HOSSEIN.SARSHAR@GMAIL.COM")
# 
# 
# print('token_auth')
# print(token_auth)
# 
# t_header = token_auth.get_authentication_header()
# print('t_header')
# print(t_header)
# 
# ws = Workspace(
#     subscription_id=args.AZUREML_ARM_SUBSCRIPTION,
#     resource_group=args.AZUREML_ARM_RESOURCEGROUP,
#     workspace_name=args.AZUREML_ARM_WORKSPACE_NAME,
#     auth=token_auth
#     )

# print("Found workspace {} at location {}".format(ws.name, ws.location))

print('************************** Project Ended **************************')
