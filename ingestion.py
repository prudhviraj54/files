import argparse
import logging
import os
import sys
import json
from datetime import date, timedelta, datetime
import pyodbc
from pytz import timezone
import re

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import ClientSecretCredential

# UDFs
@F.udf
def mask_show_first_n(col, n):
    if col is None:
        return col
    else:
        col = str(col)
        n = int(n)
        col_show = col[:n]
        col_hide = col[n:]
        col_hide = re.sub(r"([a-zA-Z]{1})", "X", col_hide)
        col_hide = re.sub(r"([0-9]{1})", "n", col_hide)
        return col_show + col_hide

@F.udf
def mask_date(col):
    if col is None:
        return col
    else:
        col = str(col)
        year_month_day = col[:7] + "-01"
        return year_month_day
        # return datetime.strptime(year_month_day, "%Y-%m-%d").date()

class Ingest:
    def __init__(self, yacht, **kwargs):
        self.logging_level = None
        self.log_container_name = None
        self.log_dir = None
        self.log_storage_acc = None
        self.spark = SparkSession.builder.getOrCreate()
        self.key_id = None
        self.batch_id = None
        self.load_mode = None
        self.table_type = None
        self.initial_load_flag = None
        self.last_load_empty = True
        self.spn_id = None
        self.secret_scope = None
        self.secret_key = None
        self.tenant_id = None
        self.dir_metadata = None
        self.task_arr = []
        self.storage_account = None
        self.landing_container = None
        self.landing_dir = None
        self.table_schema = None
        self.landing_table_name = None
        self.stg_container = None
        self.stg_container_mask = None
        self.stg_dir = None
        self.snapshot_container = None
        self.snapshot_container_mask = None
        self.snapshot_table_name = None
        self.hist_container = None
        self.hist_container_mask = None
        self.change_container = None
        self.change_container_mask = None
        self.delta_cols = None
        self.masking_format = None
        self.__dict__.update(kwargs)
        self.tz = 'America/Toronto'
        self.etl_run_dt = datetime.now(tz=timezone(self.tz)).strftime("%Y-%m-%d %H-%M-%S")
        self.dbutils = self._get_dbutils()
        self.logger = self._get_logger()
        self.load_task = None
        self.yachty = self._get_yacht(yacht)
        self.adls_service_client = None

    def _validate(self):
        """
        Validating if all the required parameters are passed in from ADF
        :return:
        """

        self.load_mode = self.load_mode.lower()
        self.table_type = self.table_type.lower()
        
        # masking format is made optional, so change the masking_format to empty string in case it was not passed in as parameter
        if self.masking_format is None:
            self.masking_format = ""

        # checking configurations
        valid_modes = ["cdc", "delta"]
        valid_table_types = ["dimension", "transaction"]

        if self.load_mode not in valid_modes:
            raise ValueError("valid options are {}. Actual value: {}".format(valid_modes, self.load_mode))

        if self.table_type not in valid_table_types:
            raise ValueError("valid table types are {}. Actual value: {}".format(valid_table_types, self.table_type))

        if None in [self.snapshot_table_name, self.landing_container, self.landing_dir, self.snapshot_container, self.hist_container, self.change_container, self.batch_id,
                    self.yachty, self.initial_load_flag, self.spn_id, self.secret_scope, self.secret_key, self.tenant_id, self.dir_metadata, self.storage_account, self.table_schema]:
            raise ValueError("missing required values")

    def _get_dbutils(self):
        """
        Get dbutils object
        :return: DBUtils
        """
        from pyspark.dbutils import DBUtils
        return DBUtils(self.spark)

    def _get_logger(self):
        """
        Create logger and logging directory if it doesn't already exist.
        Will also mount the logging container if not mounted already
        :return: logger
        """
        VALID_LEVELS = ['WARNING', 'INFO', 'ERROR', 'DEBUG']
        if self.logging_level not in VALID_LEVELS:
            self.logging_level = "INFO"

        level_lookup = {
            'WARNING': logging.WARNING,
            'INFO': logging.INFO,
            'ERROR': logging.ERROR,
            'DEBUG': logging.DEBUG
        }
        logging.Formatter.converter = lambda *args: datetime.now(
            tz=timezone(self.tz)).timetuple()

        logging.basicConfig(level=level_lookup[self.logging_level],
                            format="[%(levelname)s] %(asctime)s - %(message)s")

        stream_handler = logging.StreamHandler(sys.stdout)

        logger = logging.getLogger("ingestion")
        logger.addHandler(stream_handler)

        # check if the path is mounted
        mount_point = "/mnt/{}".format(self.log_container_name)

        if not any(mount.mountPoint == mount_point for mount in self.dbutils.fs.mounts()):
            configs = {"fs.azure.account.auth.type": "OAuth",
                       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                       "fs.azure.account.oauth2.client.id": self.spn_id,
                       "fs.azure.account.oauth2.client.secret": self.dbutils.secrets.get(scope=self.secret_scope, key=self.secret_key),
                       "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/{}/oauth2/token".format(self.tenant_id)}

            self.dbutils.fs.mount(
                source="abfss://{}@{}.dfs.core.windows.net/".format(self.log_container_name, self.log_storage_acc),
                mount_point=mount_point,
                extra_configs=configs)

        # only add filehandler when the log dir is provided, otherwise, only add stream handler
        if self.log_dir is not None:
            # if path doesn't exist, create
            log_dir = "/dbfs/mnt/{}/{}/Databricks/{}/".format(self.log_container_name, self.log_dir,
                                                              datetime.strptime(self.etl_run_dt, "%Y-%m-%d %H-%M-%S").strftime("%Y-%m-%d"))
            if not os.path.exists(log_dir):
                os.makedirs(log_dir, exist_ok=True)

            now = datetime.now(tz=timezone('America/Toronto'))
            now = now.strftime("%H-%M-%S")

            log_file = log_dir + "ingestion_{}_{}.log".format(self.snapshot_table_name, now)

            file_handler = logging.FileHandler(log_file, mode='a')
            file_handler.setFormatter(logging.Formatter("[%(levelname)s] %(asctime)s - %(message)s"))
            logger.addHandler(file_handler)

        return logger

    def _get_yacht(self, yt):
        """
        Adding some attributes to the Yacht object
        :param yt:
        :return: yt
        """
        yt.dbutils = self.dbutils
        yt.logger = self.logger
        yt.targettablename = self.snapshot_table_name
        return yt

    def _storage_reader(self, area, task=None):
        """
        handles reading from the storage areas: Landing, Staging, History, Current, Changes
        to hide complexity

        :param area: Landing, Staging, History, Current, Changes
        :return: pyspark.sql.DataFrame
        """
        input_file = None
        valid_read_areas = ["Landing", "Staging", "History", "Current", "Changes"]
        if area not in valid_read_areas:
            raise ValueError("valid areas to read from are {}".format(valid_read_areas))

        if area == "Landing" and task is None:
            raise ValueError("a batch date is required to specify the directory to read data from but missing, please check directory metadata")

        if area == "Landing":
            input_file = "abfss://{}@{}.dfs.core.windows.net/{}/{}/{}/{}/".format(self.landing_container,
                                                                                  self.storage_account,
                                                                                  self.landing_dir,
                                                                                  self.table_schema,
                                                                                  self.landing_table_name,
                                                                                  task)

        elif area == "Staging":
            input_file = "abfss://{}@{}.dfs.core.windows.net/{}/{}/staging.parquet".format(self.stg_container,
                                                                                           self.storage_account,
                                                                                           self.table_schema,
                                                                                           self.snapshot_table_name)
        elif area == "History":
            input_file = "abfss://{}@{}.dfs.core.windows.net/{}/{}/History".format(self.hist_container,
                                                                                   self.storage_account,
                                                                                   self.table_schema,
                                                                                   self.snapshot_table_name)
        elif area == "Current":
            input_file = "abfss://{}@{}.dfs.core.windows.net/{}/{}/Current".format(self.snapshot_container,
                                                                                   self.storage_account,
                                                                                   self.table_schema,
                                                                                   self.snapshot_table_name)

        elif area == "Changes":
            input_file = "abfss://{}@{}.dfs.core.windows.net/{}/{}/Changes".format(self.change_container,
                                                                                   self.storage_account,
                                                                                   self.table_schema,
                                                                                   self.snapshot_table_name)

        self.logger.info("reading df from input area {}: {}".format(area, input_file))
        return self.spark.read.parquet(input_file)

    def _storage_writer(self, area, df, task=None, masking=False):
        """
        Handles writing to Staging, History, Current, Changes
        :param area: Staging, History, Current
        :return:
        """
        valid_write_areas = ["Staging", "History", "Current", "Changes"]
        if area not in valid_write_areas:
            raise ValueError("Valid areas to write to are {}".format(valid_write_areas))

        # reference df_temp to df in case no masking is applied. df_temp which is df will be written
        df_temp = df

        if area == "Staging":
            if masking:
                stg_file_location = "abfss://{}@{}.dfs.core.windows.net/{}/{}/staging.parquet".format(self.stg_container_mask,
                                                                                                    self.storage_account,
                                                                                                    self.table_schema,
                                                                                                    self.snapshot_table_name)
                df_temp = self.mask(self.masking_format , df)
            else:
                stg_file_location = "abfss://{}@{}.dfs.core.windows.net/{}/{}/staging.parquet".format(self.stg_container,
                                                                                                    self.storage_account,
                                                                                                    self.table_schema,
                                                                                                    self.snapshot_table_name)
            df_temp.write.mode("overwrite").parquet(stg_file_location)

        elif area == "History":
            if masking:
                hist_file_location = "abfss://{}@{}.dfs.core.windows.net/{}/{}/History".format(self.hist_container_mask,
                                                                                            self.storage_account,
                                                                                            self.table_schema,
                                                                                            self.snapshot_table_name)
                df_temp = self.mask(self.masking_format, df)
            else:
                hist_file_location = "abfss://{}@{}.dfs.core.windows.net/{}/{}/History".format(self.hist_container,
                                                                                            self.storage_account,
                                                                                            self.table_schema,
                                                                                            self.snapshot_table_name)
            self.logger.info("writing df to history: {}".format(hist_file_location))
            df_temp.write.mode("overwrite").parquet(hist_file_location)

        elif area == "Current":
            if masking:
                pub_file_location = "abfss://{}@{}.dfs.core.windows.net/{}/{}/Current".format(self.snapshot_container_mask,
                                                                                            self.storage_account,
                                                                                            self.table_schema,
                                                                                            self.snapshot_table_name)
                df_temp = self.mask(self.masking_format, df)
            else:
                pub_file_location = "abfss://{}@{}.dfs.core.windows.net/{}/{}/Current".format(self.snapshot_container,
                                                                                            self.storage_account,
                                                                                            self.table_schema,
                                                                                            self.snapshot_table_name)
            self.logger.info("writing df to snapshot: {}".format(pub_file_location))
            df_temp.write.mode("overwrite").parquet(pub_file_location)

        elif area == "Changes":
            if masking:
                change_file_location = "abfss://{}@{}.dfs.core.windows.net/{}/{}/Changes".format(self.change_container_mask,
                                                                                                self.storage_account,
                                                                                                self.table_schema,
                                                                                                self.snapshot_table_name)
                df_temp = self.mask(self.masking_format, df)
            else:
                change_file_location = "abfss://{}@{}.dfs.core.windows.net/{}/{}/Changes".format(self.change_container,
                                                                                                self.storage_account,
                                                                                                self.table_schema,
                                                                                                self.snapshot_table_name)
            self.logger.info("writing df to changes directory: {}".format(change_file_location))
            # only overwrite if an initial load is requested and this is the earliest snapshot available
            if self.initial_load_flag.lower() in ["y", "yes"] and self.last_load_empty:
                df_temp.write.partitionBy("DL_ETL_EFF_DT").mode("overwrite").parquet(change_file_location)
            else:
                # equivalent to append if new partition, overwrite if old partition
                df_temp.coalesce(5).write.option("partitionOverwriteMode", "dynamic").partitionBy("DL_ETL_EFF_DT").mode("overwrite").parquet(change_file_location)

    def _truncate_load(self, task):
        """
        Read from landing, and write to Changes,  history, and create materialized snapshot table

        interactions with different storage areas
        landing: read
        staging: N/A
        history:  overwrite
        snapshot: overwrite
        change:   overwrite
        """

        # read from landing
        raw_file = self._storage_reader("Landing", task)
        src_ct = raw_file.count()
        self.logger.info("Landing file row count: {}".format(src_ct))

        # adding operation type for cdc_dimension, cdc_transaction, delta_dimension
        if self.load_task != "delta_transaction" and self.key_id != "":
            raw_file = raw_file.withColumn('DL_ETL_OPS_IND', F.lit('I'))

        task_dt = datetime.strptime(task, '%Y-%m-%dT%H-%M-%S').strftime('%Y-%m-%d')
        # write to history (publish)
        raw_file = raw_file.withColumn('DL_ETL_EFF_DT', F.lit(task_dt).cast("date"))\
            .withColumn("DL_ETL_END_DT", F.lit("9999-12-31").cast("date"))\
            .withColumn("DL_ETL_RUN_DT", F.to_timestamp(F.lit(self.etl_run_dt), "yyyy-MM-dd HH-mm-ss"))\
            .withColumn("DL_ETL_BATCH_ID", F.lit(self.batch_id).cast("int"))

        chg_file = raw_file.drop("DL_ETL_END_DT")
        chg_ct = src_ct
        self.logger.info("Change file count is: {}".format(chg_ct))

        # tables with no primary keys, regardless of the type of table or load will not have changes file
        if self.key_id != "":
            self._journal_yacht(journal_type="opening", task=task, task_name="Changes")
            try:
                if self.load_task != "delta_transaction":
                    # write to change (publish)
                    self._storage_writer("Changes", df=chg_file, task=task)
                    if self.masking_format != "":
                        self._storage_writer("Changes", df=chg_file, task=task, masking=True)
                    chg_file = chg_file.drop("DL_ETL_OPS_IND")
                else:
                    self._storage_writer("Changes", df=chg_file, task=task)
                    if self.masking_format != "":
                        self._storage_writer("Changes", df=chg_file, task=task, masking=True)
            except Exception as e:
                self.logger.exception(e)
                self._journal_yacht(journal_type="closing", task=task,
                                    task_status="FAILED", task_name="Changes")
                self._journal_yacht(journal_type="indicator", task_status="FAILED")
                sys.exit(1)

            self._journal_yacht(journal_type="closing", task=task,
                                task_status="SUCCESS", task_name="Changes")

        if str(src_ct) != "0" and self.last_load_empty and self.initial_load_flag.lower() == "y":
            self.last_load_empty = False

        # only load the history if the load has primary key and is NOT transaction tables
        if self.key_id != "" and self.table_type != "transaction":
            self._journal_yacht(journal_type="opening", task=task, task_name="History")
            try:
                self._storage_writer("History", df=raw_file, task=task)
                if self.masking_format != "":
                    self._storage_writer("History", df=raw_file, task=task, masking=True)
            except Exception as e:
                self.logger.exception(e)
                self._journal_yacht(journal_type="closing", task=task,
                                    task_status="FAILED", task_name="History")
                self._journal_yacht(journal_type="indicator", task_status="FAILED")
                sys.exit(1)

            self._journal_yacht(journal_type="closing", task=task,
                                task_status="SUCCESS", task_name="History")

        self._journal_yacht(journal_type="opening", task=task, task_name="Current")
        try:
            self._storage_writer("Current", chg_file)
            if self.masking_format != "":
                self._storage_writer("Current", chg_file, masking=True)
        except Exception as e:
            self.logger.exception(e)
            self._journal_yacht(journal_type="closing", task=task,
                                task_status="FAILED", task_name="Current")
            self._journal_yacht(journal_type="indicator", task_status="FAILED")
            sys.exit(1)

        self._journal_yacht(journal_type="closing", task=task,
                            task_status="SUCCESS", task_name="Current")

    def _update_change(self, change_history, key_cols, window_spec, task):
        """
        Updating Changes file
        :param change_history: current Changes file loaded into a dataframe
        :param key_cols:       columns used to identify distinct records for comparison
        :param window_spec:    the window used to find row number
        :param task:           name of folder in landing that contains the parquet file, usually a timestamp
        :return:
        """
        landing_file = self._storage_reader("Landing", task)
        src_ct = landing_file.count()
        self.logger.info("Landing file count is: {}".format(src_ct))
        self._journal_yacht(journal_type="opening", task=task, task_name="Changes")
        task_dt = datetime.strptime(task, '%Y-%m-%dT%H-%M-%S').strftime('%Y-%m-%d')
        try:
            # if it is delta load, we cannot get rid of the last load since data comes in increnmentally. Instead we merge the two and get distinct records only
            task_change_history = change_history \
                .where(F.col("DL_ETL_EFF_DT") == task_dt) \
                .drop("DL_ETL_OPS_IND", "DL_ETL_EFF_DT",
                      "DL_ETL_RUN_DT", "DL_ETL_BATCH_ID")

            # for the case of delta load for transaction tables, the changes file is direct load and append from landing
            if self.load_task != "delta_transaction":
                """
                CDC process to find the difference of two dataframes and append to changes file
                """
                # window to find the latest history snapshot, the '< task' is to protect the integrity of the change file when the same latest load was already loaded
                latest_change_history_snapshot = change_history\
                    .where(F.col("DL_ETL_EFF_DT") < task_dt)\
                    .withColumn("row_num", F.row_number().over(window_spec)) \
                    .where(F.col("row_num") == "1") \
                    .where(F.col("DL_ETL_OPS_IND") != "D").drop("row_num")

                if self.load_task == "delta_dimension":
                    # if the same record in the landing file, keep landing record of the two, else, retain what was loaded last time
                    landing_file = landing_file.unionByName(task_change_history.join(landing_file, key_cols, "left_anti"))

                latest_change_history_snapshot = latest_change_history_snapshot.drop("DL_ETL_OPS_IND", "DL_ETL_EFF_DT",
                                                                                     "DL_ETL_RUN_DT", "DL_ETL_BATCH_ID")
                cols = [x.lower() for x in latest_change_history_snapshot.columns]

                # hashing row records for comparison
                landing_file = landing_file.withColumn('hash_id', F.md5(
                    F.concat(*[F.coalesce(F.trim(F.upper(F.col(c))), F.lit('*')) for c in cols])))
                latest_change_history_snapshot = latest_change_history_snapshot.withColumn('hash_id', F.md5(
                    F.concat(*[F.coalesce(F.trim(F.upper(F.col(c))), F.lit('*')) for c in cols])))

                # new or updated records
                """Spark has some bug on left anti join for some reason, need to check, this is temp fix"""
                #new_id = landing_file.join(latest_change_history_snapshot, on="hash_id", how="left_anti").select(*key_cols)
                new_id = landing_file.alias("a").join(latest_change_history_snapshot.alias("b"), on="hash_id", how="left")\
                    .where(F.col("b.hash_id").isNull())\
                    .select("a.*")\
                    .select(*key_cols)
                # deleted or updated records
                """Spark has some bug on left anti join for some reason, need to check, this is temp fix"""
                #old_id = latest_change_history_snapshot.join(landing_file, on="hash_id", how="left_anti").select(*key_cols)
                old_id = latest_change_history_snapshot.alias("a").join(landing_file.alias("b"), on="hash_id", how="left")\
                    .where(F.col("b.hash_id").isNull())\
                    .select("a.*")\
                    .select(*key_cols)
                # insert ID
                insert_id = new_id.join(old_id, on=key_cols, how="left_anti")
                
                # update ID
                update_id = old_id.join(new_id, on=key_cols, how="inner")

                # For the Performance Optimization purpose, we have closed these operations - Count of inserted and updated records.
                # self.logger.info("Number of records to be inserted: {}".format(insert_id.count())) 
                # self.logger.info("Number of records to be updated: {}".format(update_id.count())) 

                # insert
                insert_records = landing_file.join(insert_id, on=key_cols, how="inner") \
                    .withColumn("DL_ETL_OPS_IND", F.lit("I")) \
                    .withColumn("DL_ETL_EFF_DT", F.lit(task_dt).cast("date")) \
                    .withColumn("DL_ETL_RUN_DT", F.to_timestamp(F.lit(self.etl_run_dt), "yyyy-MM-dd HH-mm-ss")) \
                    .withColumn("DL_ETL_BATCH_ID", F.lit(self.batch_id).cast("int")) \
                    .drop("hash_id")
                # soft update
                update_records = landing_file.join(update_id, on=key_cols, how="inner") \
                    .withColumn("DL_ETL_OPS_IND", F.lit("U")) \
                    .withColumn("DL_ETL_EFF_DT", F.lit(task_dt).cast("date")) \
                    .withColumn("DL_ETL_RUN_DT", F.to_timestamp(F.lit(self.etl_run_dt), "yyyy-MM-dd HH-mm-ss")) \
                    .withColumn("DL_ETL_BATCH_ID", F.lit(self.batch_id).cast("int")) \
                    .drop("hash_id")

                cdc_records = insert_records.unionByName(update_records)

                # if the case is cdc, there is "D" in ops_ind otherwise there isn't
                if self.load_mode == "cdc":
                    # deleted ID
                    delete_id = old_id.join(new_id, on=key_cols, how="left_anti")

                    # soft delete
                    delete_records = change_history.where(F.col("DL_ETL_EFF_DT") < task_dt)\
                        .join(delete_id, on=key_cols, how="inner") \
                        .withColumn("row_num", F.row_number().over(window_spec)).where(F.col("row_num") == "1").drop(
                        *["row_num"])
                    delete_records = delete_records.withColumn("DL_ETL_OPS_IND", F.lit("D")).withColumn("DL_ETL_EFF_DT",
                                                                                                        F.lit(task_dt).cast("date")) \
                        .withColumn("DL_ETL_BATCH_ID", F.lit(self.batch_id).cast("int")) \
                        .withColumn("DL_ETL_RUN_DT", F.to_timestamp(F.lit(self.etl_run_dt), "yyyy-MM-dd HH-mm-ss"))
                    
                    # For the Performance Optimization purpose, we have closed this operations - Count of Deleted records.
                    # self.logger.info("Number of records that are to be deleted: {}".format(delete_records.count()))

                    cdc_records = cdc_records.unionByName(delete_records)

            else:
                # case for delta transaction table. No ops ind, no comparison of dataframes
                # first merge landing with the task partition if the dates are the same
                landing_file = landing_file.unionByName(task_change_history.join(landing_file, key_cols, "left_anti"))
                cdc_records = landing_file.withColumn("DL_ETL_EFF_DT", F.lit(task_dt).cast("date"))\
                    .withColumn("DL_ETL_RUN_DT", F.to_timestamp(F.lit(self.etl_run_dt), "yyyy-MM-dd HH-mm-ss"))\
                    .withColumn("DL_ETL_BATCH_ID", F.lit(self.batch_id).cast("int"))

            # write to change history
            cdc_records.persist()
            
            # For the Performance Optimization purpose, we have closed this operations - Count records.
            # chg_ct = cdc_records.count()
            # self.logger.info("Total number of records for the change partition ({}) is: {}".format(task, chg_ct))

            if self.masking_format != "":
                self._storage_writer("Changes", df=cdc_records, task=task, masking=True)
            self._storage_writer("Changes", df=cdc_records, task=task)
            cdc_records.unpersist()
        except Exception as e:
            self.logger.exception(e)
            self._journal_yacht(journal_type="closing", task=task,
                                task_status="FAILED", task_name="Changes")
            self._journal_yacht(journal_type="indicator", task_status="FAILED")
            sys.exit(1)

        self._journal_yacht(journal_type="closing", task=task,
                            task_status="SUCCESS", task_name="Changes")

    def _update_snapshot(self, window_spec, task):
        """
        Updating Current file
        :param window_spec: window used to find the row number
        :param task:        name of folder in landing that contains the parquet file, usually a timestamp
        :return:
        """
        self._journal_yacht(journal_type="opening", task=task, task_name="Current")
        try:
            latest_change_history_updated = self._storage_reader("Changes")
            updated_history_snapshot = latest_change_history_updated.withColumn("row_num", F.row_number().over(window_spec)) \
                .where(F.col("row_num") == "1").drop("row_num")

            # same batch id for the snapshot through out
            updated_history_snapshot = updated_history_snapshot.withColumn("DL_ETL_BATCH_ID", F.lit(self.batch_id).cast("int"))\
                .withColumn("DL_ETL_RUN_DT", F.to_timestamp(F.lit(self.etl_run_dt), "yyyy-MM-dd HH-mm-ss"))

            # only cdc will have delete case
            # delta dimension : I, U only
            # delta transaction : no ops ind col
            if self.load_mode == "cdc":
                updated_history_snapshot = updated_history_snapshot.where(F.col("DL_ETL_OPS_IND") != "D").drop("DL_ETL_OPS_IND")
            elif self.load_task != "delta_transaction":
                updated_history_snapshot = updated_history_snapshot.drop("DL_ETL_OPS_IND")

            updated_history_snapshot.persist()

            # For the Performance Optimization purpose, we have closed this operations - Count records.
            # self.logger.info("Current file count is: {}".format(updated_history_snapshot.count()))
            if self.masking_format != "":
                self._storage_writer("Current", updated_history_snapshot, masking=True)
            self._storage_writer("Current", updated_history_snapshot)
            updated_history_snapshot.unpersist()

        except Exception as e:
            self.logger.exception(e)
            self._journal_yacht(journal_type="closing", task=task,
                                task_status="FAILED", task_name="Current")
            self._journal_yacht(journal_type="indicator", task_status="FAILED")
            sys.exit(1)

        self._journal_yacht(journal_type="closing", task=task,
                            task_status="SUCCESS", task_name="Current")

    def _update_history(self, key_cols, task):
        """
        Updating History file
        :param key_cols: primary keys used to identify distinct records
        :param task:     name of folder in landing that contains the parquet file, usually a timestamp
        :return:
        """
        self._journal_yacht(journal_type="opening", task=task, task_name="History")
        try:
            # history logic
            hist_file = self._storage_reader("History")
            chg_file = self._storage_reader("Changes")
            yyyy_mm_dd = "%Y-%m-%d"

            max_hist_eff_dt = str(hist_file.select(F.max("DL_ETL_EFF_DT")).head()[0])
            max_chg_eff_dt = str(chg_file.select(F.max("DL_ETL_EFF_DT")).head()[0])

            self.logger.info("Max history DL_ETL_EFF_DT is: {}".format(max_hist_eff_dt))
            self.logger.info("Max Change DL_ETL_EFF_DT is: {}".format(max_chg_eff_dt))

            # adjust the eff date to compare to the end date, minus 1 day back
            max_chg_eff_dt_adj = (datetime.strptime(max_chg_eff_dt, yyyy_mm_dd) - timedelta(days=1)).strftime(
                yyyy_mm_dd)
            max_hist_end_dt = str(hist_file.where(F.col("DL_ETL_END_DT") != "9999-12-31").select(F.max("DL_ETL_END_DT")).head()[0])

            # case where there is no deleted records. All end date will be 9999-12-31 which is filtered out
            if max_hist_end_dt != "None":
                # adjust the end date to the eff date for comparison
                max_hist_end_dt_adj = (datetime.strptime(max_hist_end_dt, yyyy_mm_dd) + timedelta(days=1)).strftime(
                    yyyy_mm_dd)

                # when history is ahead of change or rerun the latest change, roll back history
                if max_chg_eff_dt <= max_hist_eff_dt or max_chg_eff_dt_adj <= max_hist_end_dt:
                    # get rid of the insert and update
                    roll_back_hist_file = hist_file.where(F.col("DL_ETL_EFF_DT") < max_chg_eff_dt)
                    # only keep the records that were never deleted or it was deleted before the last load
                    roll_back_hist_file = roll_back_hist_file.where((F.col("DL_ETL_END_DT") < max_chg_eff_dt_adj) | (F.col("DL_ETL_END_DT") == "9999-12-31"))
                    deleted_record_l = hist_file.where(
                        (F.col("DL_ETL_END_DT") >= max_chg_eff_dt_adj) &
                        (F.col("DL_ETL_END_DT") != "9999-12-31"))\
                        .withColumn("DL_ETL_END_DT", F.lit("9999-12-31").cast("date"))
                    hist_file = roll_back_hist_file.unionByName(deleted_record_l)
                    cdc_records = chg_file.where(F.col("DL_ETL_EFF_DT") == max_chg_eff_dt)
                else:
                    # when changes is ahead of history
                    # get the later date, either end dt adj or eff dt
                    if max_hist_end_dt_adj > max_hist_eff_dt:
                        use_date = max_hist_end_dt_adj
                    else:
                        use_date = max_hist_eff_dt

                    cdc_records = chg_file.where(F.col("DL_ETL_EFF_DT") > use_date)
            else:
                # when history is ahead of change or case of re-run the latest change, roll back history
                if max_chg_eff_dt <= max_hist_eff_dt:
                    roll_back_hist_file = hist_file.where(F.col("DL_ETL_EFF_DT") < max_chg_eff_dt)
                    hist_file = roll_back_hist_file
                    cdc_records = chg_file.where(F.col("DL_ETL_EFF_DT") == max_chg_eff_dt)
                else:
                    # when changes is ahead of history
                    cdc_records = chg_file.where(F.col("DL_ETL_EFF_DT") > max_hist_eff_dt)

            cdc_dates = cdc_records.select("DL_ETL_EFF_DT").distinct().rdd.flatMap(lambda x: x).collect()
            cdc_dates.sort(reverse=False)

            # if there is change data captured
            if len(cdc_dates) != 0:
                for dates in cdc_dates:
                    dates_str = str(dates)
                    yesterday_dates_str = (datetime.strptime(dates_str, yyyy_mm_dd) - timedelta(days=1)).strftime(
                        yyyy_mm_dd)
                    cdc_records_day = cdc_records.where(F.col("DL_ETL_EFF_DT") == dates_str).withColumn("DL_ETL_END_DT",
                                                                                                        F.lit("9999-12-31").cast("date"))
                    hist_file_expired = hist_file.where(F.col("DL_ETL_END_DT") != "9999-12-31")
                    hist_file_active = hist_file.where(F.col("DL_ETL_END_DT") == "9999-12-31")

                    # insert records for the day
                    insert_records_day = cdc_records_day.where(F.col("DL_ETL_OPS_IND") == "I")

                    # updated and deleted records for the day
                    changed_records_day = cdc_records_day.where((F.col("DL_ETL_OPS_IND") == "U") | (F.col("DL_ETL_OPS_IND") == "D"))

                    # update active records that are updated or deleted
                    hist_file_active_update = hist_file_active.alias("a").join(changed_records_day.select(*key_cols),
                                                                               on=key_cols, how="inner") \
                        .withColumn("DL_ETL_END_DT", F.lit(yesterday_dates_str).cast("date"))

                    hist_file_active_remaining = hist_file_active.alias("a").join(
                        changed_records_day.select(*key_cols), on=key_cols, how="left_anti"
                    ).select("a.*")
                    hist_file_active = hist_file_active_update.unionByName(
                        hist_file_active_remaining)

                    # append insert records, update record and expired records
                    hist_file = hist_file_active.unionByName(hist_file_expired).unionByName(insert_records_day).unionByName(
                        changed_records_day.where(F.col("DL_ETL_OPS_IND") != "D"))

            hist_file.persist()
            if self.masking_format != "":
                self._storage_writer("History", df=hist_file, masking=True)
            self._storage_writer("History", df=hist_file)
            hist_file.unpersist()

        except Exception as e:
            self.logger.exception(e)
            self._journal_yacht(journal_type="closing", task=task,
                                task_status="FAILED", task_name="History")
            self._journal_yacht(journal_type="indicator", task_status="FAILED")
            sys.exit(1)

        self._journal_yacht(journal_type="closing", task=task,
                            task_status="SUCCESS", task_name="History")

    def _delta_cdc(self, task):
        """
        Cdc data comes in full dataset
        Delta data comes in incrementally

        When either delta or cdc is required, eg. when not initial load
        This method will call update_* methods to update Change, History, Current files

        interactions with different storage areas
        landing: read
        staging:  (None)
        history: write (overwrite)
        snapshot: write(overwrite)
        changes: write(append/overwrite)

        :param task:     name of folder in landing that contains the parquet file, usually a timestamp
        :return
        """

        if self.key_id == "":
            self.logger.info("Table {} does not have primary key, start truncate process to load current view".format(self.snapshot_table_name))
            self._truncate_load(task)
        else:
            change_history = self._storage_reader("Changes")
            key_cols = self.key_id.split(",")
            window_spec = Window.partitionBy(key_cols).orderBy(F.col("DL_ETL_EFF_DT").desc())
            task_dt = datetime.strptime(task, '%Y-%m-%dT%H-%M-%S').strftime('%Y-%m-%d')
            # in case of rerun, task can equal to the latest in the change, however, if it is earlier, throw error
            latest_last_load = str(change_history.select(F.max("DL_ETL_EFF_DT")).head()[0])
            self.logger.info("Max DL_ETL_EFF_DT from change file from last load was {}".format(latest_last_load))
            if task_dt < latest_last_load:
                raise Exception("Current batch {} is already been processed previously".format(task))

            self._update_change(change_history=change_history, key_cols=key_cols, window_spec=window_spec, task=task)

            if self.table_type != "transaction":
                self._update_history(key_cols=key_cols, task=task)

            # create snapshot, only if the task is the latest batch for the table
            if task == self.task_arr[-1]:
                self._update_snapshot(window_spec=window_spec, task=task)

    def _refresh_current(self, task):
        """
        This method is called when the delta table does not have any delta files for the day
        and the only task required for the table is simply refreshing the current view with
        updated batch id and run dt timestamp

        :param task: name of folder in landing that contains the parquet file, usually a timestamp
        :return:
        """
        if task == self.task_arr[-1]:
            self._journal_yacht(journal_type="opening", task=task, task_name="Current")
            try:
                df_current = self._storage_reader("Current")
                df_current = df_current.withColumn("DL_ETL_BATCH_ID", F.lit(self.batch_id).cast("int"))\
                    .withColumn("DL_ETL_RUN_DT", F.to_timestamp(F.lit(self.etl_run_dt), "yyyy-MM-dd HH-mm-ss"))

                df_current.persist()
                if self.masking_format != "":
                    self._storage_writer(area="Current", df=df_current, masking=True)
                self._storage_writer(area="Current", df=df_current)
                df_current.unpersist()
            except Exception as e:
                self.logger.exception(e)
                self._journal_yacht(journal_type="closing", task=task,
                                    task_status="FAILED", task_name="Current")
                self._journal_yacht(journal_type="indicator", task_status="FAILED")
                sys.exit(1)

            self._journal_yacht(journal_type="closing", task=task,
                                task_status="SUCCESS", task_name="Current")

    def _adls_connect(self):
        """
        Used to add azure authentication attributes to the SparkSession so that it can connect to storage
        containers without mounting them to Databricks workspace in the first place.

        Method also creates ADLS service client which can be used later to operate I/O functions to remove
        files and folders from Blob storage during clean up step
        :return:
        """

        secret = self.dbutils.secrets.get(scope=self.secret_scope, key=self.secret_key)

        self.spark.conf.set("fs.azure.account.auth.type.{}.dfs.core.windows.net".format(self.storage_account), "OAuth")
        self.spark.conf.set("fs.azure.account.oauth.provider.type.{}.dfs.core.windows.net".format(self.storage_account),
                            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
        self.spark.conf.set("fs.azure.account.oauth2.client.id.{}.dfs.core.windows.net".format(self.storage_account),
                            self.spn_id)
        self.spark.conf.set("fs.azure.account.oauth2.client.secret.{}.dfs.core.windows.net".format(self.storage_account),
                            secret)
        self.spark.conf.set("fs.azure.account.oauth2.client.endpoint.{}.dfs.core.windows.net".format(self.storage_account),
                            "https://login.microsoftonline.com/{}/oauth2/token".format(self.tenant_id))

        credential = ClientSecretCredential(self.tenant_id, self.spn_id, secret)
        self.adls_service_client = DataLakeServiceClient(account_url="https://{}.dfs.core.windows.net".format(self.storage_account), credential=credential)

    def _parse_task(self):
        """
        Parsing the landing folder metadata into array of tasks
        :return:
        """
        # parse the directory metadata to form tasks
        task_json = json.loads(self.dir_metadata)
        for task in task_json:
            if task["type"] == "Folder":
                self.task_arr.append(task["name"])

        self.task_arr.sort()

        self.logger.info("dl_etl_eff_dt to be loaded for today are: {}".format(self.task_arr))

    def _journal_yacht(self, journal_type, task=None, task_status=None, task_name=None):
        """
        To journal and Yacht records by utilizing methods from Yacht object
        :param journal_type: opening, closing, indicator
        :param task:         name of folder in landing that contains the parquet file, usually a timestamp
        :param task_status:  FAILED, SUCCESS
        :param task_name:    name of the task - Current, Changes, History
        :return:
        """
        if journal_type not in ["opening", "closing", "indicator"]:
            raise ValueError("journal type : {} is not support. valid values are 'opening', 'closing'".format(journal_type))

        source_location = "{}/{}/{}/{}".format(self.storage_account, self.landing_container,
                                               self.landing_dir, self.table_schema)

        target_location = "{}/{}/{}".format(self.storage_account, self.change_container, self.table_schema)

        target_location_mask = "{}/{}/{}".format(self.storage_account, self.change_container_mask, self.table_schema)

        if journal_type == "opening":
            self.yachty.upsert_task_entry(None, None, self.load_mode, "STARTED", 0, 0, 0, task_name, self.key_id,
                                          source_location, target_location, self.batch_id, task)
            # insert opening tasking record for masked table
            if self.masking_format != "":
                self.yachty.upsert_task_entry(None, None, self.load_mode, "STARTED", 0, 0, 0, task_name+"-MASK", self.key_id,
                                          source_location, target_location_mask, self.batch_id, task)

        elif journal_type == "closing":
            if task_status != "FAILED":
                landing_file = self._storage_reader("Landing", task=task)
                src_ct = landing_file.count()
                try:
                    target_file = self._storage_reader(task_name, task=task)

                    if task_name == "Changes":
                        target_file = target_file.where(F.col("DL_ETL_EFF_DT") == task)

                    trg_ct = target_file.count()
                except AnalysisException as e:
                    # this is the exception when we are doing initial load of empty dataframe where the change, snapshot and history are not created therefore no way to be read back for count
                    if self.initial_load_flag.lower() == "y":
                        trg_ct = 0
                    else:
                        self.logger.exception(e)

                """
                # for now we are not using the logic to figure out the max and min values of the delta values in the dataset
                if self.load_mode == "delta" and self.key_id != "":
                    delta_start = None
                    delta_end = None
                    # figure out the smallest delta start value and largest delta end value
                    for col in self.delta_cols:
                        local_delta_start = str(landing_file.select(F.min(col)).head()[0])
                        local_delta_end = str(landing_file.select(F.max(col)).head()[0])

                        if local_delta_start == "None":
                            local_delta_start = None

                        if local_delta_end == "None":
                            local_delta_end = None

                        if delta_start is None:
                            delta_start = local_delta_start
                        else:
                            if local_delta_start is not None and local_delta_start < delta_start:
                                delta_start = local_delta_start

                        if delta_end is None:
                            delta_end = local_delta_end
                        else:
                            if local_delta_end is not None and local_delta_end > delta_end:
                                delta_end = local_delta_end

                    self.yachty.upsert_task_entry(delta_start, delta_end, self.load_mode, task_status, src_ct,
                                                  trg_ct, 0, task_name, self.key_id, source_location,
                                                  target_location, self.batch_id, task)
                else:
                    self.yachty.upsert_task_entry(task, task, self.load_mode, task_status, src_ct,
                                                  trg_ct, 0, task_name, self.key_id, source_location,
                                                  target_location, self.batch_id, task)
                """
                self.yachty.upsert_task_entry(None, None, self.load_mode, task_status, src_ct,
                                              trg_ct, 0, task_name, self.key_id, source_location,
                                              target_location, self.batch_id, task)

                if self.masking_format != "":
                    self.yachty.upsert_task_entry(None, None, self.load_mode, task_status, src_ct,
                                              trg_ct, 0, task_name+"-MASK", self.key_id, source_location,
                                              target_location_mask, self.batch_id, task)
            else:
                """
                # for now we are not using the logic to figure out the max and min values of the delta values in the dataset
                self.yachty.upsert_task_entry(task, task, self.load_mode, "FAILED", 0, 0, 0, task_name, self.key_id,
                                              source_location, target_location, self.batch_id, task)
                """
                self.yachty.upsert_task_entry(None, None, self.load_mode, "FAILED", 0, 0, 0, task_name, self.key_id,
                                              source_location, target_location, self.batch_id, task)
                if self.masking_format != "":
                    self.yachty.upsert_task_entry(None, None, self.load_mode, "FAILED", 0, 0, 0, task_name+"-MASK", self.key_id,
                                                source_location, target_location_mask, self.batch_id, task)

        elif journal_type == "indicator":
            self.yachty.upsert_task_entry(None, None, self.load_mode, task_status, 0, 0, 0, "Indicator", self.key_id,
                                          source_location, target_location, self.batch_id, task)

    def _clean_up(self, task):
        """
        Cleaning up the ADLS directory for the task that was completed successfully
        :param task: name of folder in landing that contains the parquet file, usually a timestamp
        :return:
        """
        try:
            clean_dir = "{}/{}/{}/{}".format(self.landing_dir,
                                             self.table_schema,
                                             self.landing_table_name,
                                             task)
            self.logger.info("Cleaning up directory: {}/{}".format(self.landing_container, clean_dir))
            file_system_client = self.adls_service_client.get_file_system_client(file_system=self.landing_container)
            directory_client = file_system_client.get_directory_client(clean_dir)
            directory_client.delete_directory()
        except Exception as e:
            self._journal_yacht(journal_type="indicator", task_status="FAILED")
            self.logger.exception(e)

    @staticmethod
    def mask(masking_format, df):
        """
        mask targeted columns with other values
        :param masking_format: masking format string from the config
        :param df: input dataframe
        :return : masked dataframe
        """
        mask_cols_list = masking_format.split(sep=',')
        num_mask_col = len(mask_cols_list)
        num_col_total = len(df.columns)
        count_masked = 0

        # operating on the copy of the df instead of directly on the df itself, grab the schema info [{"colname": "col_type"}, {}...]
        df_new = df
        df_new_schema = list(map(lambda x: x.jsonValue(), df.schema))
        df_new_schema_dict = {}

        for item in df_new_schema:
            df_new_schema_dict[item["name"].upper()] = (item["name"], item["type"])

        for col in mask_cols_list:
            [mask_strategy, col_nm] = col.split(":")
            col_nm = col_nm.upper()
            if mask_strategy == "mask":
                df_new = df_new.withColumn(df_new_schema_dict[col_nm][0], F.regexp_replace(F.col(col_nm).cast("string"), r"([a-zA-Z]{1})", "X"))
                df_new = df_new.withColumn(df_new_schema_dict[col_nm][0], F.regexp_replace(F.col(col_nm).cast("string"), r"(\d{1})", "n"))
                count_masked += 1
            elif mask_strategy == "datemask":
                if df_new_schema_dict[col_nm][1] == "date":
                    df_new = df_new.withColumn(df_new_schema_dict[col_nm][0], mask_date(F.col(col_nm)).cast("date"))
                elif df_new_schema_dict[col_nm][1] == "timestamp":
                    df_new = df_new.withColumn(df_new_schema_dict[col_nm][0], F.date_trunc("mm", F.col(col_nm)))
                else:
                    raise ValueError("Column {} with type {} should not use datemask masking strategy".format(col_nm, df_new_schema_dict[col_nm][1]))
                count_masked += 1
            elif re.compile("^(mask_show_first_)[0-9]+$").match(mask_strategy):
                n = mask_strategy.split("_")[-1]
                df_new = df_new.withColumn(df_new_schema_dict[col_nm][0], mask_show_first_n(F.col(col_nm), F.lit(n)))
                count_masked += 1
            else:
                raise ValueError("masking pattern {} is not a valid pattern, valid patterns are: [mask, datemask, mask_first_n]".format(mask_strategy))
        
        # check if new columns introduced and if all expected columns are masked
        if num_mask_col != count_masked or num_col_total != len(df_new.columns):
            raise Exception ("Masking ERROR. Expected number columns to be masked: {}, actual: {}. Number of columns in the dataframe before masking: {}, after: {}. Hint: double check if column names in mask_cols parameter match the names from the landing zone".format(num_mask_col, count_masked, num_col_total, len(df.columns)))
        
        return df_new

    def _task_flow(self, task):
        """
        The steps to be done for each of the task job.
        They can be consisted of truncate load, delta/cdc or only refreshing the current depending on the type of the table and load type
        The process ends with cleaning up the landing directory for the task if it was loaded successfully
        """
        src_file_ct = self._storage_reader("Landing", task).count()
        self.logger.info("landing file count is: {}".format(src_file_ct))

        # initial load flag Y, treat ONLY first task as initial load
        if self.initial_load_flag.lower() in ["y", "yes"] and self.last_load_empty:
            self.logger.info("Start initial load process for table {}({})".format(self.landing_table_name, task))
            self._truncate_load(task)
        else:
            # not initial load, if the landing is not 0, or landing is 0 but cdc case
            if src_file_ct != 0 or (src_file_ct == 0 and self.load_mode == "cdc"):
                self.logger.info("Start delta/cdc process for table...")
                self._delta_cdc(task)
            else:
                # delta case, only if this is the last task in the task_arr, create the current snapshot
                if task == self.task_arr[-1]:
                    # in the case landing is empty, refresh the current snapshot with new batch id and batch date
                    self.logger.info(
                        "Landing file row count is 0, no new data added for today's batch, refreshing current view now...")
                    self._refresh_current(task)

        self._clean_up(task)

    def start(self):
        """
        Basically the main
        :return:
        """
        try:
            self.logger.info("Validating parameters..")
            self._validate()
            self.logger.info("Parameters validated")
            self.logger.info("Initial load flag: {}".format(self.initial_load_flag))
            self.logger.info("DL_ETL_RUN_DT is : {}".format(self.etl_run_dt))
            self.logger.info("Batch ID is : {}".format(self.batch_id))
            self.logger.info("Table name is : {}".format(self.snapshot_table_name))
            self.logger.info("key columns used to identify unique records are: '{}'".format(self.key_id))
            self.logger.info("Masking format: {}".format(self.masking_format))

            self.load_task = "{}_{}".format(self.load_mode, self.table_type)
            self.logger.info("Type of table loading combination: {}".format(self.load_task))

            self.logger.info("Adding ADLS details to SparkSession..")
            self._adls_connect()

            self.logger.info("Now parsing batches to be processed for today's run: {}".format(self.dir_metadata))
            self._parse_task()

            self.logger.info("Parsing delta columns '{}'".format(self.delta_cols))
            self.delta_cols = self.delta_cols.split(",")
            self.logger.info("Delta columns are: {}".format(self.delta_cols))

            if self.task_arr:
                for task in self.task_arr:
                    self.logger.info("Start loading batch for DL_ETL_EFF_DT: {}".format(task))
                    self._task_flow(task)
                self._journal_yacht(journal_type="indicator", task_status="SUCCESS")
            else:
                raise Exception("Databricks job should not have been triggered since there is no batch to be processed in landing")
        except Exception as e:
            self.logger.exception(e)
            sys.exit(1)


class Yacht:
    def __init__(self, **kwargs):
        self.dbutils = None
        self.logger = None
        self.yacht_secret_scope = None
        self.yacht_secret_key = None
        self.yacht_sql_id = None
        self.yacht_sql_server = None
        self.yacht_db = None
        self.project_abbr = None
        self.toolorpattern = None
        self.sourcesystemcd = None
        self.sourcetablename = None
        self.targetsystemcd = None
        self.targettablename = None
        self.etl_create_id = None
        self.__dict__.update(kwargs)
        self._validate()

    def _validate(self):
        """
        Validating if all the parameters are passed in correctly
        :return:
        """
        if None in [self.yacht_secret_scope, self.yacht_secret_key, self.yacht_sql_id, self.yacht_sql_server,
                    self.yacht_db, self.project_abbr, self.sourcesystemcd, self.sourcetablename,
                    self.targetsystemcd, self.toolorpattern, self.etl_create_id]:
            raise ValueError("configuration for yacht is incomplete")

    def _get_connection(self):
        """
        Getting the SQL connection to Yacht
        :return:
        """
        if self.dbutils is None:
            raise ValueError ("dbutils attribute is missing from object")

        pw = self.dbutils.secrets.get(scope=self.yacht_secret_scope, key=self.yacht_secret_key)

        cnxn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + self.yacht_sql_server + ';PORT=1433;DATABASE=' +
            self.yacht_db + ';UID=' + self.yacht_sql_id + ';PWD=' + pw + ';')

        return cnxn

    def _construct_payload(self, loadtype, taskstatus, sourcecnt, targetcnt, rejectcnt,
                           taskname, key_id, sourcedatabase, targetdatabase, task):  # NOSONAR
        yacht_taskname = "{}-{}-{}-{}".format(self.project_abbr, self.sourcesystemcd, self.targetsystemcd, taskname.upper())

        if taskname == "Indicator":
            src_tb_name = self.sourcetablename
        else:
            src_tb_name = self.sourcetablename + '/' + str(task)

        _payload = (
            "<Payload><TaskName SourceSystemCD=\"{0}\" SourceDatabase=\"{1}\" SourceTableName=\"{2}\" SourceTableKeyColumn=\"{3}\" TargetSystemCD=\"{4}\" TargetDatabase=\"{5}\" TargetTableName=\"{6}\" ToolOrPattern=\"{7}\" LoadType=\"{8}\" TaskStatus=\"{9}\" SourceCnt=\"{10}\" TargetCnt=\"{11}\" RejectCnt=\"{12}\">{13}</TaskName></Payload>"
            .format(self.sourcesystemcd, sourcedatabase, src_tb_name, key_id,
                    self.targetsystemcd, targetdatabase, self.targettablename, self.toolorpattern.upper(),
                    loadtype.upper(), taskstatus.upper(), sourcecnt, targetcnt, rejectcnt, yacht_taskname)
            )
        return _payload

    def upsert_task_entry(self, startdeltaparam, enddeltaparam, loadtype, taskstatus, sourcecnt,
                          targetcnt, rejectcnt, taskname, key_id, sourcedatabase, targetdatabase, batch_id, task):

        if startdeltaparam is None:
            startdeltaparam = 'null'
        else:
            startdeltaparam = "\'" + startdeltaparam + "\'"

        if enddeltaparam is None:
            enddeltaparam = 'null'
        else:
            enddeltaparam = "\'" + enddeltaparam + "\'"

        payload = self._construct_payload(
            loadtype=loadtype,
            taskstatus=taskstatus,
            sourcecnt=sourcecnt,
            targetcnt=targetcnt,
            rejectcnt=rejectcnt,
            taskname=taskname,
            key_id=key_id,
            sourcedatabase=sourcedatabase,
            targetdatabase=targetdatabase,
            task=task
        )
        sql = "EXEC etl.usp_UpsertTaskEntry @ETLControlBatchID={}, @StartDeltaParam={}, @EndDeltaParam={}, @ETLCreateID='{}', @Payload='{}'".format(
            batch_id, startdeltaparam, enddeltaparam, self.etl_create_id, payload
        )

        self.logger.info("Yacht SQL statement: {}".format(sql))

        cnxn = self._get_connection()
        cursor = cnxn.cursor()
        r = cursor.execute(sql).fetchone()
        cnxn.commit()
        cursor.close()
        cnxn.close()


if __name__ == "__main__":
    # common
    parser = argparse.ArgumentParser()

    ingest_conf_list = [
        {"name": "batch_id", "help": "YACHT batch id", "type": str, "required": True},
        {"name": "key_id", "help": "list of comma separated key columns", "type": str, "required": False},
        {"name": "load_mode", "help": "type of load: cdc, delta", "type": str, "required": True},
        {"name": "table_type", "help": "type of table: transaction, dimension", "type": str, "required": True},
        {"name": "initial_load_flag", "help": "whether or not the current load is initial load: Y/N", "type": str, "required": True},
        {"name": "spn_id", "help": "application id of SPN", "type": str, "required": True},
        {"name": "secret_scope", "help": "Databricks secret scope name", "type": str, "required": True},
        {"name": "secret_key", "help": "Databricks secret key name", "type": str, "required": True},
        {"name": "tenant_id", "help": "tenant id", "type": str, "required": True},
        {"name": "dir_metadata", "help": "childitem output from get metadata task on ADF", "type": str, "required": True},
        {"name": "logging_level", "help": "WARNING, INFO, ERRO', DEBUG", "type": str, "required": False},
        {"name": "log_container_name", "help": "logging container", "type": str, "required": False},
        {"name": "log_dir", "help": "logging directory", "type": str, "required": False},
        {"name": "log_storage_acc", "help": "logging directory", "type": str, "required": False},
        {"name": "delta_cols", "help": "columns used to do delta load", "type": str, "required": False},
        {"name": "storage_account", "help": "storage account id", "type": str, "required": True},
        {"name": "landing_container", "help": "", "type": str, "required":True},
        {"name": "landing_dir", "help": "", "type": str, "required":True},
        {"name": "table_schema", "help": "schema of the table such as dbo", "type": str, "required": True},
        {"name": "landing_table_name", "help": "name of the table in landing zone", "type": str, "required": True},
        {"name": "stg_container", "help": "", "type": str, "required": False},
        {"name": "stg_container_mask", "help": "", "type": str, "required": False},
        {"name": "stg_dir", "help": "", "type": str, "required": False},
        {"name": "snapshot_container", "help": "", "type": str, "required": True},
        {"name": "snapshot_container_mask", "help": "", "type": str, "required": False},
        {"name": "snapshot_table_name", "help": "", "type": str, "required": True},
        {"name": "hist_container", "help": "", "type": str, "required": True},
        {"name": "hist_container_mask", "help": "", "type": str, "required": False},
        {"name": "change_container", "help": "", "type": str, "required": True},
        {"name": "change_container_mask", "help": "", "type": str, "required": False},
        {"name": "masking_format", "help": "eg: mask:col_name,mask_show_first_2:col_name,datemask:col_name", "type": str, "required": False}
        
    ]
    yacht_conf_list = [
        {"name": "yacht_secret_scope", "help": "Databricks secret scope of Yacht sql id", "type": str, "required": True},
        {"name": "yacht_secret_key", "help": "Databricks secret key of Yacht sql id", "type": str, "required": True},
        {"name": "yacht_sql_id", "help": "", "type": str, "required": True},
        {"name": "yacht_sql_server", "help": "", "type": str, "required": True},
        {"name": "yacht_db", "help": "", "type": str, "required": True},
        {"name": "project_abbr", "help": "", "type": str, "required": True},
        {"name": "toolorpattern", "help": "", "type": str, "required": True},
        {"name": "sourcesystemcd", "help": "", "type": str, "required": True},
        {"name": "targetsystemcd", "help": "", "type": str, "required": True},
        {"name": "sourcetablename", "help": "", "type": str, "required": True},
        {"name": "etl_create_id", "help": "", "type": str, "required": True},
    ]

    for item in ingest_conf_list + yacht_conf_list:
        parser.add_argument("--{}".format(item["name"]), help=item["help"], type=item["type"], required=item["required"])

    # converting args to dictionary
    args = parser.parse_args()
    config = vars(args)

    ingest_conf = {}
    yacht_conf = {}

    # split config into ingest and yacht specific config
    for item in config.keys():
        if item in list(map(lambda x: x.get("name"), ingest_conf_list)):
            ingest_conf[item] = config[item]
        elif item in list(map(lambda x: x.get("name"), yacht_conf_list)):
            yacht_conf[item] = config[item]

    yacht = Yacht(**yacht_conf)
    Ingest(yacht, **ingest_conf).start()
