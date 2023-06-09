from pyspark.sql import SparkSession
from azure.storage.blob import BlobServiceClient
import os
from pyspark.sql.functions import col, when

# Create SparkSession
spark = SparkSession.builder.getOrCreate()

# Specify the Azure Blob storage account details
storage_account_name = "<your_storage_account_name>"
storage_account_key = "<your_storage_account_key>"
container_name = "<your_container_name>"

blob_service_client = BlobServiceClient(account_url=f"https://{storage_account_name}.blob.core.windows.net", credential=storage_account_key)

input_mount_folder = "/mnt/input"
blob_service_client.get_blob_client(container=container_name, blob=input_name).download_to_path(input_mount_folder)
input_path = input_mount_folder
local_output_path = 'output/'

cv = computer_version_rules(input_path,local_output_path)


input_path = "/mnt/input/"  # Replace with the actual input path
files_stamp_detected = []
for datename in os.listdir(input_path):
    date_path = input_path + datename + '/'
    if not spark._jvm.java.nio.file.Files.exists(spark._jvm.java.nio.file.Paths.get(date_path)):
        continue
    for batchname in os.listdir(date_path):
        batch_path = date_path + batchname + '/'
        if not spark._jvm.java.nio.file.Files.exists(spark._jvm.java.nio.file.Paths.get(batch_path)):
            continue
        
        files = cv.detect_stamp(datename, batchname, batch_path)
        
        # Add detected stamp files to the list
        files_stamp_detected += files

# Continue with your code using the files_stamp_detected list
fr = doc_read(input_path,local_output_path)

date_name = 'Test'
batch_name = '111'
input_path = 'output/'


files_with_stamp = spark.sparkContext.parallelize([(date_name, batch_name, input_path)]) \
    .map(lambda x: cv.detect_stamp(x[0], x[1], x[2])) \
    .collect()[0]

test2 = spark.read.format("binaryFile").option("pathGlobFilter", "*.pdf").load(input_path)
test2 = test2.sort(batch_name)
test2 = test2.withColumn("handwritten_percentage", fr.handwritten_per(batch_name, test2))
test2 = fr.check_stamp(test2, files_with_stamp)

test2 = rules.paid_by_cash(test2)
test2 = rules.has_dollar_symbol(test2)
test2 = rules.handwritten_check(test2,0.3)
test2 = rules.invoice_num_check(test2)
test2 = rules.get_payment(test2)
test2 = rules.address_check(test2)
test2 = rules.get_reg_num(test2)

process_list = ['Oct-27-2022','Oct-31-2022']

input_path = 'output/'
process_list = ['datename1', 'datename2', 'datename3']  # Example process list

for datename in os.listdir(input_path):
    if datename in process_list:
        date_path = input_path + datename + '/'

        for batchname in os.listdir(date_path):
            if batchname in ['08000013-155028', '08000014-155028', '08000015-160242'] or datename == 'Oct-31-2022':
                print(datename)
                print(batchname)
                batch_path = date_path + batchname + '/'
                files_with_stamp = spark.sparkContext.parallelize([(datename, batchname, batch_path)]) \
                    .map(lambda x: cv.detect_stamp(x[0], x[1], x[2])) \
                    .collect()[0]

                df_out = spark.read.format("binaryFile").option("pathGlobFilter", "*.pdf").load(batch_path)
                df_out = df_out.sort(batchname)
                # df_out = fr.parse_metadata(batchname, batch_path, local_output_path, df_out)
                df_out = df_out.withColumn("handwritten_percentage", fr.handwritten_per(batchname, df_out))
                df_out = fr.check_stamp(df_out, files_with_stamp)

                # Write to a temporary path on the cluster
                temp_out_file_path = '/tmp/' + datename + '_' + batchname + '.csv'
                df_out.write.csv(temp_out_file_path, header=True, mode="overwrite")

                print('Read and label batch {} successfully, saved to {}'.format(batchname, temp_out_file_path))


input_path = 'output/'

for part in os.listdir(input_path):
    batch_path = input_path + part + '/'
    files_with_stamp = spark.sparkContext.parallelize([('OCS_files_2', part, batch_path)]) \
        .map(lambda x: cv.detect_stamp(x[0], x[1], x[2])) \
        .collect()[0]

    df_out = spark.read.format("binaryFile").option("pathGlobFilter", "*.pdf").load(batch_path)
    df_out = fr.sort_document(part, df_out)
    # df_out = fr.parse_metadata(batchname, batch_path, local_output_path, df_out)
    df_out = df_out.withColumn("handwritten_percentage", fr.handwritten_per(part, df_out))
    df_out = fr.check_stamp(df_out, files_with_stamp)

    # Write to a temporary path on the cluster
    temp_out_file_path = '/tmp/OCS_Test_' + part + '_02102023.csv'
    df_out.write.csv(temp_out_file_path, header=True, mode="overwrite")
    print('Read and label batch {} successfully, saved to {}'.format(part, temp_out_file_path))

df = spark.read.csv('output/Oct-25-2022_08000003-154055.csv', header=True, inferSchema=True)
df = fr.sort_document('111', df)
df.show()

df.write.csv('output/Oct-25-2022_08000003-154055.csv', header=True)
rules = fraud_document_analytics()
out_df = spark.createDataFrame([], schema=df.schema)

for file in os.listdir(local_output_path):
    if file.startswith('OCS') and file.endswith('02102023.csv'):
        doc_text_path = local_output_path + file
        print(doc_text_path)
        
        df = spark.read.csv(doc_text_path, header=True, inferSchema=True)
        
        invoices = ['paramedical invoice', 'other_receipt']
        df = df.filter(col('page_label').isin(invoices))
        df = rules.paid_by_cash(df)
        df = rules.has_dollar_symbol(df)
        df = rules.handwritten_check(df, 0.3)
        df = rules.invoice_num_check(df)
        df = rules.get_payment(df)
        df = rules.address_check(df)
        df = rules.get_reg_num(df)      
        out_df = out_df.union(df)

out_df = out_df.na.fill(False)
out_df = out_df.withColumn('has_stamp', 
                           when((col('has_stamp') == True) & (lower(col('content')).contains('costco')), False)
                           .otherwise(col('has_stamp')))

out_df.show()

out_df = out_df.withColumn('payment_above_200', when(col('payment_amount') >= 200, True).otherwise(False))
out_df = out_df.withColumn('payment_above_400', when(col('payment_amount') >= 400, True).otherwise(False))
out_df = out_df.withColumn('payment_above_500', when(col('payment_amount') >= 500, True).otherwise(False))
out_df = out_df.withColumn('has_invalid_addr', when(col('handwritten_percentage') > 0.3, False).otherwise(col('has_invalid_addr')))

df_to_rank = out_df.select('file_name', 'has_stamp', 'paid_by_cash', 'no_dollar_symbol', 'above_handwritten_threshold',
                           'invalid_invoice_nbr', 'has_invalid_addr', 'payment_above_200', 'payment_above_400',
                           'payment_above_500')

df_to_rank = df_to_rank.withColumn('has_stamp', col('has_stamp').cast('integer'))
df_to_rank = df_to_rank.withColumn('paid_by_cash', col('paid_by_cash').cast('integer'))
df_to_rank = df_to_rank.withColumn('no_dollar_symbol', col('no_dollar_symbol').cast('integer'))
df_to_rank = df_to_rank.withColumn('above_handwritten_threshold', col('above_handwritten_threshold').cast('integer'))
df_to_rank = df_to_rank.withColumn('invalid_invoice_nbr', col('invalid_invoice_nbr').cast('integer'))
df_to_rank = df_to_rank.withColumn('has_invalid_addr', col('has_invalid_addr').cast('integer'))
df_to_rank = df_to_rank.withColumn('payment_above_200', col('payment_above_200').cast('integer'))
df_to_rank = df_to_rank.withColumn('payment_above_400', col('payment_above_400').cast('integer'))
df_to_rank = df_to_rank.withColumn('payment_above_500', col('payment_above_500').cast('integer'))

df_to_rank = df_to_rank.withColumn('paid_by_cash&payment_above', col('paid_by_cash') * col('payment_above_200'))
df_to_rank = df_to_rank.withColumn('has_stamp&payment_above', col('has_stamp') * col('payment_above_500'))
df_to_rank = df_to_rank.withColumn('no_dollar_symbol&payment_above', col('no_dollar_symbol') * col('payment_above_500'))
df_to_rank = df_to_rank.withColumn('above_handwritten_threshold&payment_above', col('above_handwritten_threshold') * col('payment_above_500'))
df_to_rank = df_to_rank.withColumn('invalid_invoice_nbr&payment_above', col('invalid_invoice_nbr') * col('payment_above_500'))
df_to_rank = df_to_rank.withColumn('has_invalid_addr&payment_above', col('has_invalid_addr') * col('payment_above_400'))

df_to_rank = df_to_rank.withColumn('paid_by_cash', when(col('paid_by_cash&payment_above') == 1, 0).otherwise(col('paid_by_cash')))
df_to_rank = df_to_rank.withColumn('has_stamp', when(col('has_stamp&payment_above') == 1, 0).otherwise(col('has_stamp')))
df_to_rank = df_to_rank.withColumn('no_dollar_symbol', when(col('no_dollar_symbol&payment_above') == 1, 0).otherwise(col('no_dollar_symbol')))
df_to_rank = df_to_rank.withColumn('above_handwritten_threshold', when(col('above_handwritten_threshold&payment_above') == 1, 0).otherwise(col('above_handwritten_threshold')))
df_to_rank = df_to_rank.withColumn('invalid_invoice_nbr', when(col('invalid_invoice_nbr&payment_above') == 1, 0).otherwise(col('invalid_invoice_nbr')))
df_to_rank = df_to_rank.withColumn('has_invalid_addr', when(col('has_invalid_addr&payment_above') == 1, 0).otherwise(col('has_invalid_addr')))

df_to_rank = df_to_rank.drop('payment_above_200', 'payment_above_400', 'payment_above_500')

df_to_rank = df_to_rank.withColumnRenamed('paid_by_cash', 'paid_by_cash&payment_below')
df_to_rank = df_to_rank.withColumnRenamed('has_stamp', 'has_stamp&payment_below')
df_to_rank = df_to_rank.withColumnRenamed('no_dollar_symbol', 'no_dollar_symbol&payment_below')
df_to_rank = df_to_rank.withColumnRenamed('above_handwritten_threshold', 'above_handwritten_threshold&payment_below')
df_to_rank = df_to_rank.withColumnRenamed('invalid_invoice_nbr', 'invalid_invoice_nbr&payment_below')
df_to_rank = df_to_rank.withColumnRenamed('has_invalid_addr', 'has_invalid_addr&payment_below')
