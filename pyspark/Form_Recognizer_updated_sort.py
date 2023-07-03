from pyspark.sql.functions import col, udf,array,lit,when,col, lower, regexp_replace, split, expr, array, locate, size
from pyspark.sql.types import BooleanType,DoubleType,StructType, StructField, StringType
from pyspark.sql import SparkSession
import re,os,textract
from pyspark.sql import functions as F
import logging


class doc_read:
    def __init__(self):
            self.spark = SparkSession.builder.appName('document_reader').getOrCreate()
            self.logger = logging.getLogger(__name__)

    def check_stamp(self,df, files_list):
        def if_has_stamp(file_name, content):
            if file_name in files_list and 'costco' not in content.lower():
                return True
            else:
                return False
        if_has_stamp_udf = udf(if_has_stamp, BooleanType())
        df = df.withColumn('has_stamp', if_has_stamp_udf(col('file_name'), col('content')))
        return df

    def handwritten_per(self, batch_name, df):
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
    

    def read_document(self, date_name, batch_name, batch_path):
        data = []
        idx = 0
        for filename in os.listdir(batch_path):
            if not (filename.endswith(".jpg") or filename.endswith(".tif") or filename.endswith(".pdf")):
                continue
            self.logger.info(filename)
            word_info = {}
            file_path = batch_path + '/' + filename
            try:
                text = textract.process(file_path).decode("utf-8")
                result = text.strip()

                for word in result.split():
                    word_info[word] = 1

                filename = date_name + '_' + batch_name + '_' + filename
                data.append((filename, result, word_info, None))
                idx += 1
            except Exception as e:
                self.logger.error(f"Error processing file: {filename}")
                self.logger.error(e)

        #headers = ['file_name', 'content', 'word_confidence', 'styles']
        schema = StructType([
        StructField('file_name', StringType(), nullable=True),
        StructField('content', StringType(), nullable=True),
        StructField('word_confidence', StringType(), nullable=True),
        StructField('styles', StringType(), nullable=True)
        ])
        df = self.spark.createDataFrame(data, schema)
        df = df.withColumn('content', F.regexp_replace('content', '\n', ' '))

        self.logger.info(f"Read {idx} images' text in batch {batch_name}")
        return df

    def sort_document(self,batchname, df):
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




















    

        # # Recognize types by keywords
        # filename_labels = []
        # for ind in range(df.count()):
        #     text = F.lower(F.col('content').getItem(ind))
        #     text = F.regexp_replace(text, r'[^\w\s]', '')
        #     text = F.regexp_replace(text, ' +', ' ')
        #     labelled = 0
        #     if F.expr(f"exists(array_contains(split('{text}', ' '), word), True)") and F.expr(f"exists(array_contains(split('{text}', ' '), word), True)").count() > 0:
        #         filename_labels.append('claim_page_one')
        #         labelled = 1
        #     # If text has two or more keywords of claim page two
        #     if labelled == 0:
        #         count = 0
        #         for keyword in claim_2_keywords:
        #             if F.expr(f"locate('{keyword}', '{text}') > 0").count() > 0:
        #                 count += 1
        #         if count >= 2:
        #             filename_labels.append('claim_page_two')
        #             labelled = 1
        #     if labelled == 0:
        #         for clinic in drug_clinic:
        #             if F.expr(f"locate('{clinic}', '{text}') > 0 and (locate('prescription receipt', '{text}') > 0 or locate('patient pays', '{text}') > 0)").count() > 0:
        #                 filename_labels.append('drug_receipt')
        #                 labelled = 1
        #                 break
        #     if labelled == 0:
        #         for drug_keyword in drug_keywords:
        #             if F.expr(f"locate('{drug_keyword}', '{text}') > 0 and (locate(' din', '{text}') > 0 or locate(' rx', '{text}') > 0)").count() > 0:
        #                 filename_labels.append('drug_receipt')
        #                 labelled = 1
        #                 break
        #     if labelled == 0:
        #         if F.expr(f"exists(array_contains(split('{text}', ' '), word), True)") and F.expr(f"exists(array_contains(split('{text}', ' '), word), True)").count() > 0:
        #             filename_labels.append('paramedical invoice')
        #             labelled = 1
        #     if labelled == 0:
        #         if F.expr(f"exists(array_contains(split('{text}', ' '), word), True)") and F.expr(f"exists(array_contains(split('{text}', ' '), word), True)").count() > 0 and F.expr(f"locate('claim', '{text}') < 0").count() > 0:
        #             filename_labels.append('other_receipt')
        #             labelled = 1
        #     if labelled == 0:
        #         filename_labels.append('other_doc')
        # df = df.withColumn('page_label', F.array(filename_labels))
        # print('Labelled batch {}'.format(batchname))
        # return df


