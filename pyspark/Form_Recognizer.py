from pyspark.sql.functions import col, udf
from pyspark.sql.types import BooleanType
from pyspark.sql import SparkSession
import re,os
from pyspark.sql import functions as F
from azure.core.credentials import AzureKeyCredential
from azure.ai.documentanalysis import DocumentAnalysisClient

class doc_read:
    def check_stamp(df, files_list):
        def if_has_stamp(file_name, content):
            if file_name in files_list and 'costco' not in content.lower():
                return True
            else:
                return False
        if_has_stamp_udf = udf(if_has_stamp, BooleanType())

        df = df.withColumn('has_stamp', if_has_stamp_udf(col('file_name'), col('content')))
        return df

    def calculate_handwritten_percentage(batch_name, df):
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
                return round(handwritten_len / content_length, 3)

        df = df.withColumn('handwritten_percentage', handwritten_percentage_udf(df['styles'], df['content']))
        return df

    def read_document(date_name, batch_name, batch_path):
        endpoint = "https://canadacentral.api.cognitive.microsoft.com/"
        key = ""
        credential = AzureKeyCredential(key)
        document_analysis_client = DocumentAnalysisClient(endpoint, credential)
        out_df_rows = []
        for filename in os.listdir(batch_path):
            if filename.endswith(".jpg") or filename.endswith(".tif"):
                word_info = {}
                file_path = os.path.join(batch_path, filename)
                try:
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
                except:
                    continue
        # Convert the list of rows to a DataFrame
        out_df = spark.createDataFrame(out_df_rows, ['file_name', 'content', 'word_confidence', 'styles'])
        # Clean the text
        out_df = out_df.withColumn('content', F.regexp_replace('content', '\n', ' '))
        return out_df

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
        # Recognize types by keywords
        filename_labels = []
        for ind in range(df.count()):
            text = F.lower(F.col('content').getItem(ind))
            text = F.regexp_replace(text, r'[^\w\s]', '')
            text = F.regexp_replace(text, ' +', ' ')
            labelled = 0
            if F.expr(f"exists(array_contains(split('{text}', ' '), word), True)") and F.expr(f"exists(array_contains(split('{text}', ' '), word), True)").count() > 0:
                filename_labels.append('claim_page_one')
                labelled = 1
            # If text has two or more keywords of claim page two
            if labelled == 0:
                count = 0
                for keyword in claim_2_keywords:
                    if F.expr(f"locate('{keyword}', '{text}') > 0").count() > 0:
                        count += 1
                if count >= 2:
                    filename_labels.append('claim_page_two')
                    labelled = 1
            if labelled == 0:
                for clinic in drug_clinic:
                    if F.expr(f"locate('{clinic}', '{text}') > 0 and (locate('prescription receipt', '{text}') > 0 or locate('patient pays', '{text}') > 0)").count() > 0:
                        filename_labels.append('drug_receipt')
                        labelled = 1
                        break
            if labelled == 0:
                for drug_keyword in drug_keywords:
                    if F.expr(f"locate('{drug_keyword}', '{text}') > 0 and (locate(' din', '{text}') > 0 or locate(' rx', '{text}') > 0)").count() > 0:
                        filename_labels.append('drug_receipt')
                        labelled = 1
                        break
            if labelled == 0:
                if F.expr(f"exists(array_contains(split('{text}', ' '), word), True)") and F.expr(f"exists(array_contains(split('{text}', ' '), word), True)").count() > 0:
                    filename_labels.append('paramedical invoice')
                    labelled = 1
            if labelled == 0:
                if F.expr(f"exists(array_contains(split('{text}', ' '), word), True)") and F.expr(f"exists(array_contains(split('{text}', ' '), word), True)").count() > 0 and F.expr(f"locate('claim', '{text}') < 0").count() > 0:
                    filename_labels.append('other_receipt')
                    labelled = 1
            if labelled == 0:
                filename_labels.append('other_doc')
        df = df.withColumn('page_label', F.array(filename_labels))
        print('Labelled batch {}'.format(batchname))
        return df


