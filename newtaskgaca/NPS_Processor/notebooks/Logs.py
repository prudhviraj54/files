# Databricks notebook source
from delta.tables import *

# COMMAND ----------

# MAGIC %md
# MAGIC Log used for monitoring when packages run as pipeline

# COMMAND ----------

target=DeltaTable.forPath(spark,'abfss://aalab-mlworkspace-opspii@cacaadatalakeproddl.dfs.core.windows.net/GACA/AWS_TM_Output/test/logs/nps')
df=target.toDF()
df.display()

# COMMAND ----------
