def get_dir_content(ls_path):
    for dir_path in dbutils.fs.ls(ls_path):
        if dir_path.isFile():
            yield dir_path.path
        elif dir_path.isDir() and ls_path != dir_path.path:
            yield from get_dir_content(dir_path.path)
    
list(get_dir_content(dir_path))



for file_location in list(get_dir_content(dir_path)):
    file_name = file_location.split(sep='/')[-1]
    table_name = file_name.split(sep='.')[0]
    if table_name =='column_headers':
        headers_df = spark.read.format("csv").option("header", "true").option("sep", "\t").load(file_location)
        header_columns = headers_df.schema.names
        #remove all special characters which are not allowed in a column name
        columns = ([re.sub("(\,|\;|\{|\}|\(|\)|\n|\t|\=)","",i) for i in header_columns])
    elif table_name =='hit_data':
        hit_data_df = spark.read.format("csv").option("header", "false").option("sep", "\t").load(file_location)
        #replace special characters with underscore from column names before writing into delta format
        hit_data_df_with_schema = hit_data_df.toDF(*[re.sub('[^\w]', '_', c) for c in columns]).withColumn("load_timestamp",current_timestamp()).withColumn("hit_date",F.to_date('date_time'))
        write_location = f"abfss://{deltaContainer}@{adls_name}.dfs.core.windows.net/{folder}/{table_name}/"
        hit_data_df_with_schema.write.format("delta").mode("append").partitionBy("hit_date").save(write_location)
    elif table_name not in ('hit_data','column_headers'):
        sch = StructType([
            StructField(f"{table_name}id", StringType()),
            StructField(f"{table_name}", StringType())
        ])
        df = spark.read.format("csv").schema(sch).option("header", "false").option("sep", "\t").load(file_location)
        write_location = f"abfss://{deltaContainer}@{adls_name}.dfs.core.windows.net/{folder}/{table_name}/"
        df.write.format("delta").mode("overwrite").save(write_location)
    else:
        print("File not found: file location "+file_location + "table_name" + table_name)
