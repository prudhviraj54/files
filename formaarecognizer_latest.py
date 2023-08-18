from pyspark.sql.functions import col, udf,array,lit,when,lower
from pyspark.sql.types import BooleanType,DoubleType,StructType, StructField, StringType
from pyspark.sql import SparkSession
import re,os
from pyspark.sql import functions as F
import logging
from azure.core.credentials import AzureKeyCredential
from azure.ai.formrecognizer import DocumentAnalysisClient

spark = SparkSession.builder.appName('document_reader').getOrCreate()

class DocRead:
    def __init__(self):
        logger = logging.getLogger(__name__)
    
    @staticmethod
    def check_stamp(df, files_list):
        def if_has_stamp(file_name, content):
            if file_name in files_list and 'costco' not in content.lower():
                return True
            else:
                return False
        if_has_stamp_udf = udf(if_has_stamp, BooleanType())
        df = df.withColumn('has_stamp', if_has_stamp_udf(col('file_name'), col('content')))
        return df
    
    @staticmethod
    def handwritten_per(batch_name, df):
     @udf(DoubleType())
     def handwritten_percentage_udf(styles, content):
        handwritten_len = 0
        if not styles:
            return 0
        else:
            for style in styles:
                spans = style.spans
                for span in spans:
                 handwritten_len += span.length

            content_length = len(content)
            return round(handwritten_len / content_length, 3) if content_length > 0 else 0

     df = df.withColumn('handwritten_percentage', when(col('styles').isNull(), 0).otherwise(handwritten_percentage_udf(df['styles'], df['content']))) 
     return df

    

    @staticmethod
    def read_document(date_name, batch_name, batch_path):
        endpoint = "https://prodadvanalyticsformrecogonizer.cognitiveservices.azure.com/"
        key = "cb6b5519fd7d49a4924908c4afe2a6fd"
        credential = AzureKeyCredential(key)
        document_analysis_client = DocumentAnalysisClient(endpoint, credential)
        out_df_rows = []
        for filename in os.listdir(batch_path):
            if filename.endswith(".jpg") or filename.endswith(".tif") or filename.endswith(".pdf"):
                word_info = {}
                file_path = os.path.join(batch_path, filename)
                #try:
                with open(file_path, 'rb') as file:
                    file_content = file.read()
                    poller = document_analysis_client.begin_analyze_document("prebuilt-read", document=file_content)
                result = poller.result().content
                if type(result) is not str:
                    result = str(result)
                for page in poller.result().pages:
                    for word in page.words:
                        word_info[word.content] = word.confidence
                filename = date_name + '_' + batch_name + '_' + filename
                styles = poller.result().styles
                out_df_rows.append((filename, result, word_info, styles))
                #except:
                 #   continue
        # Convert the list of rows to a DataFrame
        out_df = spark.createDataFrame(out_df_rows, ['file_name', 'content', 'word_confidence', 'styles'])
        # Clean the text
        out_df = out_df.withColumn('content', F.regexp_replace('content', '\n', ' '))
        return out_df

    @staticmethod
    def sort_document(batchname, df):
      # Key words must be lower-case
      drug_clinic = ['shoppers drug mart']
      claim_form_keywords = ['group benefits retiree extended health care claim', 'group benefits extended health care claim', 'group benefits medical expense claim', 'group benefits assignment of paramedical', 'the manufacturers life insurance company']
      claim_1_keywords = ['plan member information', '1 of 2']
      claim_2_keywords = ['equipment and appliance', 'vision care expenses', 'claims confirmation', 'mailing instruction', 'banking information and email address', '2 of 2']
      drug_keywords = ['pharmacy', 'drug mart', 'prescription']
      other_receipt_keywords = ['receipt', 'invoice']
      paramedical_keywords = ['clinic', 'treatment', 'therapy', 'massage', 'physiotherapist', 'chiropractor', 'psychologist', 'physiotherapy', 'chiropractic']
      paramedical_cross_keywords = ['invoice', 'receipt', 'bill', 'paid']
      
      # UDFs for labeling the document types
      def label_claim_page_one(text):
          return 'claim_page_one' if 'claim page one' in text else None

      def label_claim_page_two(text):
          return 'claim_page_two' if 'claim page two' in text else None

      def label_drug_receipt(text):
          return 'drug_receipt' if any(keyword in text for keyword in drug_keywords) else None

      def label_paramedical_invoice(text):
          return 'paramedical_invoice' if any(keyword in text for keyword in paramedical_keywords) else None

      def label_other_receipt(text):
          return 'other_receipt' if any(keyword in text for keyword in other_receipt_keywords) else None

      def label_other_doc(text):
          return 'other_doc'
      
      label_claim_page_one_udf = udf(label_claim_page_one, StringType())
      label_claim_page_two_udf = udf(label_claim_page_two, StringType())
      label_drug_receipt_udf = udf(label_drug_receipt, StringType())
      label_paramedical_invoice_udf = udf(label_paramedical_invoice, StringType())
      label_other_receipt_udf = udf(label_other_receipt, StringType())
      label_other_doc_udf = udf(label_other_doc, StringType())
      
      df = df.withColumn('text', lower(col('content')))
      #df = df.withColumn('page_label', lit('other_doc').cast(StringType()))
      #df = df.withColumn('page_label', when(col('page_label').isNull(), label_claim_page_one_udf('text')).otherwise(col('page_label')))
      df = df.withColumn('page_label', label_claim_page_one_udf('text'))
      df = df.withColumn('page_label', when(col('page_label').isNull(), label_claim_page_two_udf('text')).otherwise(col('page_label')))
      df = df.withColumn('page_label', when(col('page_label').isNull(), label_drug_receipt_udf('text')).otherwise(col('page_label')))
      df = df.withColumn('page_label', when(col('page_label').isNull(), label_paramedical_invoice_udf('text')).otherwise(col('page_label')))
      df = df.withColumn('page_label', when(col('page_label').isNull(), label_other_receipt_udf('text')).otherwise(col('page_label')))
      df = df.withColumn('page_label', when(col('page_label').isNull(), label_other_doc_udf('text')).otherwise(col('page_label')))
      df = df.drop('text')
      
      #df = df.select('file_name', 'page_label')
      
      #print('Labelled batch {}'.format(batchname))
      return df
