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
