from pyspark.sql.functions import col, udf,array,lit,when,lower
from pyspark.sql.types import BooleanType,DoubleType,StructType, StructField, StringType
from pyspark.sql import SparkSession
import re,os
from pyspark.sql import functions as F
import logging
from azure.core.credentials import AzureKeyCredential
from azure.ai.formrecognizer import DocumentAnalysisClient
import json

spark = SparkSession.builder.appName('document_reader').getOrCreate()

class DocRead:
    def __init__(self):
        logger = logging.getLogger(__name__)
    
    @staticmethod
    def check_stamp(df, files_list):
        def if_has_stamp(file_name, content):
            if not content:
                return False
            elif re.sub(r'\-|\_', '', file_name) in files_list and 'costco' not in content.lower():
                return True
            else:
                return False
        if_has_stamp_udf = udf(if_has_stamp, BooleanType())
        df = df.withColumn('has_stamp', if_has_stamp_udf(col('obj_id'), col('content')))
        return df
    
    @staticmethod
    def handwritten_per(df):
     @udf(DoubleType())
     def handwritten_percentage_udf(styles, content):
        handwritten_len = 0
        
        if not styles or not content:
            return 0
        else:
            styles = json.loads(styles)
            for style in styles:
                spans = style['spans']
                for span in spans:
                 handwritten_len += span['length']

            content_length = len(content)
            return round(handwritten_len / content_length, 3) if content_length > 0 else 0

     df = df.withColumn('handwritten_percentage', when(col('styles').isNull(), 0).otherwise(handwritten_percentage_udf(df['styles'], df['content']))) 
     return df

    

    @staticmethod
    def read_document(datename, date_path):

        endpoint = "https://prodadvanalyticsformrecogonizer.cognitiveservices.azure.com/"
        key = "cb6b5519fd7d49a4924908c4afe2a6fd"

        credential = AzureKeyCredential(key)
        document_analysis_client = DocumentAnalysisClient(endpoint, credential)

        # Define the schema for the input DataFrame
        read_schema = StructType([
            StructField("obj_id", StringType()),
            StructField("img_path", StringType()),
        ])


        # Define the UDF that returns a PySpark DataFrame
        @udf(StringType())
        def call_read_model(img_path, document_analysis_client=document_analysis_client):
            result =  {"response": None, "error": None }
            try:
                result["response"] = document_analysis_client.begin_analyze_document_from_url("prebuilt-read", img_path).result().to_dict()
            except Exception as e:
                result["error"] = str(e)

            result_json = json.dumps(result)
            return result_json
        
        # Define the UDF that returns read result
        @udf()
        def get_content(fr_result):
            content = ''
            result = json.loads(fr_result)['response']
            if not result:
                return None 
            content = str(result['content'])

            return content
        
        @udf()
        def get_word_info(fr_result):
            word_info = {}
            result = json.loads(fr_result)['response']
            if not result:
                return None 
            for page in result['pages']:
                    for word in page['words']:
                        word_info[word['content']] = word['confidence']

            return word_info
        
        @udf()
        def get_styles(fr_result):
            styles = ''
            result = json.loads(fr_result)['response']
            if not result:
                return None 
            styles = result['styles']

            return json.dumps(styles)
        
        def get_dbutils(spark):
            try:
                from pyspark.dbutils import DBUtils
                dbutils = DBUtils(spark)
            except ImportError:
                import IPython
                dbutils = IPython.get_ipython().user_ns["dbutils"]
            return dbutils

        dbutils = get_dbutils(spark)
        
        doc_collection = []
        i = 1
        # print(date_path)
        for file in dbutils.fs.ls(date_path):
            # if i > 5000: break
            filename = file.name
            # print(filename)
            if filename.endswith(".jpg") or filename.endswith(".tif") or filename.endswith(".pdf") or filename.endswith(".png"):
                # print('check:'+filename)
                doc_collection.append([datename+'_'+filename,'https://cacaadatalakeproddl.blob.core.windows.net/aalab-mlworkspace-gbpii/cdn-aa-gb-fraud-doc-analytics/OCS_Images/'\
                +datename+'/'+filename])
                # path = 'https://cacaadatalakeproddl.blob.core.windows.net/aalab-mlworkspace-gbpii/cdn-aa-gb-fraud-doc-analytics/Fraud_Invoices/true_cases/'+filename
                # doc_collection.append([datename+'_'+filename,path])
                i += 1


        # Create the input DataFrame
        read_df = spark.createDataFrame(doc_collection, schema=read_schema)

        read_df = read_df.withColumn("FR_result", call_read_model("img_path"))
        read_df = read_df.withColumn("content", get_content("FR_result"))
        read_df = read_df.withColumn("word_confidence", get_word_info("FR_result"))
        read_df = read_df.withColumn("styles", get_styles("FR_result"))

        read_df = read_df.withColumn('content', F.regexp_replace('content', '\n', ' '))
        return read_df

    @staticmethod
    def sort_document(df):
        # Key words must be lower-case
        drug_clinc = ['shoppers drug mart']
        claim_form_keywords = ['group benefits retiree extended health care claim','group benefits extended health care claim', 'group benefits medical expense claim','group benefits assignment of paramedical','the manufacturers life insurance company','extended health care claim']
        claim_1_keywords = ['manulife','plan contract number','plan member certificate number','plan member name','birthday','plan member information', 'plan member address','spouse','dependants','patient information','date of birthday','page 1 of 2','complete for all expenses','use one line per patient','prescription drug expenses']
        claim_2_keywords = ['equipment and appliance','vision care expenses','claims confirmation','mailing instruction', 'banking information and email address','page 2 of 2','please mail your completed', 'total amount of all receipts','the manufacturers life insurance']
        claim_form_otip = ['otip']
        otip_claim_1_keywords = ['member basic personal information','patient information','perscription drug expenses','page 1 of 2','identification number','plan number']
        drug_keywords = ['pharmacy','drug', 'prescription', ' din ', ' rx ', ' mfr ', ' pays ',' dr ','pharmacies',' total ',' fee ',' cost ','receipt','medication record']
        other_receipt_keywords = ['receipt', 'invoice']
        other_receipt_keywords_2 = ['date','service','items','payments']
        other_receipt_exclusive_keywords = ['customer copy', 'claim']
        paramedical_keywords = ['clinic', 'treatment', 'therapy', 'massage', 'physiotherapist', 'chiropractor', 'psychologist', 'physiotherapy', 'chiropractic']
        paramedical_cross_keywords = ['invoice', 'receipt', 'bill', 'paid']
      
        # UDFs for labeling the document types
        def label_claim_page_one(text):
            if not text: 
                return None
            else:
                text = re.sub(r'[^\w\s]','',text)
            if any(word in text for word in claim_form_keywords):
                keywords_len = len(claim_1_keywords)
                count = 0
                for keyword in claim_1_keywords:
                    if text.find(keyword)>-1:
                        count += 1
                if count > 5:
                    return 'claim_page_one'
            elif any(word in text for word in claim_form_otip):
                count = 0
                for keyword in otip_claim_1_keywords:
                    if text.find(keyword)>-1:
                        count += 1
                if count > 3:
                    return 'claim_page_one'
            else:
                return None

        def label_claim_page_two(text):
                if not text: 
                    return None
                else:
                    text = re.sub(r'[^\w\s]','',text)
                count = 0
                for keyword in claim_2_keywords:
                    if text.find(keyword)>-1: count += 1
                if count >= 2:
                    return 'claim_page_two'
                else:
                    return None

        def label_drug_receipt(text):
            if not text: 
                return None
            else:
                text = re.sub(r'[^\w\s]','',text)
            count = 0

            #make sure there is payment amount 
            pattern = re.compile(r'[\s|$]*[\d{1,3},?]*\d{1,3}\.\d{2}')
            amount = re.search(pattern, text)

            for keyword in drug_keywords:
                if text.find(keyword)>-1: 
                    count += 1
            if text.find('official prescription receipt') > -1:
                return 'drug_receipt'           
            elif count > 3 and text.find('claim')==-1 and amount:
                return 'drug_receipt'
            else:
                return None
            


        def label_paramedical_invoice(text):
            if not text: 
                return None
            else:
                text = re.sub(r'[^\w\s]','',text)
            return 'paramedical_invoice' if any(keyword in text for keyword in paramedical_keywords) and any(keyword in text for keyword in paramedical_cross_keywords) else None

        def label_other_receipt(text):
            if not text: 
                return None
            else:
                text = re.sub(r'[^\w\s]','',text)
            if any(word in text for word in other_receipt_keywords) and \
                any(word in text for word in other_receipt_keywords_2) and \
                    not any(word in text for word in other_receipt_exclusive_keywords): # remove exception
                return 'other_receipt'
            else:
                return None

        def label_other_doc(text):
            return 'other_doc'
        
        label_claim_page_one_udf = udf(label_claim_page_one, StringType())
        label_claim_page_two_udf = udf(label_claim_page_two, StringType())
        label_drug_receipt_udf = udf(label_drug_receipt, StringType())
        label_paramedical_invoice_udf = udf(label_paramedical_invoice, StringType())
        label_other_receipt_udf = udf(label_other_receipt, StringType())
        label_other_doc_udf = udf(label_other_doc, StringType())
        
        df = df.withColumn('text',lower(col('content')))
        #df = df.withColumn('page_label', lit('other_doc').cast(StringType()))
        #df = df.withColumn('page_label', when(col('page_label').isNull(), label_claim_page_one_udf('text')).otherwise(col('page_label')))
        df = df.withColumn('page_label', label_claim_page_one_udf('text'))
        df = df.withColumn('page_label', when(col('page_label').isNull(), label_claim_page_two_udf('text')).otherwise(col('page_label')))
        df = df.withColumn('page_label', when(col('page_label').isNull(), label_paramedical_invoice_udf('text')).otherwise(col('page_label')))
        df = df.withColumn('page_label', when(col('page_label').isNull(), label_other_receipt_udf('text')).otherwise(col('page_label')))
        df = df.withColumn('page_label', when(col('page_label').isNull(), label_drug_receipt_udf('text')).otherwise(col('page_label')))
        df = df.withColumn('page_label', when(col('page_label').isNull(), label_other_doc_udf('text')).otherwise(col('page_label')))
        df = df.drop('text')
        
        #df = df.select('file_name', 'page_label')
        
        #print('Labelled batch {}'.format(batchname))
        return df
