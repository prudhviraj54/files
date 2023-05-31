#!/usr/bin/env python
# coding: utf-8

get_ipython().run_line_magic('pip', 'install azure-ai-formrecognizer --pre --upgrade')
get_ipython().run_line_magic('pip', 'install lxml')

import os
import json
import lxml
from bs4 import BeautifulSoup as bs
from pathlib import Path
import sys
import json
import csv
import pandas as pd
from azure.core.credentials import AzureKeyCredential
from azure.ai.formrecognizer import DocumentAnalysisClient
from azureml.core import Workspace, Dataset,Datastore
from azureml.data.datapath import DataPath
import ast
import re
from pathlib import Path
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize


class doc_read:
    
    def __init__(self, input_path,local_output_path):
        # self.datastore_name = "dl_aalab_mlworkspace_gbpii"
        # self.ws = Workspace.from_config()
        # self.datastore = Datastore(self.ws, self.datastore_name)
        # self.input_name = 'cdn-aa-gb-fraud-doc-analytics/Health-Claim-Transactions'
        # self.input_dataset = Dataset.File.from_files(path = [(self.datastore,self.input_name)])

        # self.input_mount_ctx = self.input_dataset.mount()   
        # self.input_mount_ctx.start()  
        # self.input_dataset_mount_folder = self.input_mount_ctx.mount_point
        # self.input_path = self.input_dataset_mount_folder+'/'
        
        self.input_path = input_path
        self.local_output_path = local_output_path


    def read_document(self,date_name,batch_name, batch_path):
        endpoint = "https://canadacentral.api.cognitive.microsoft.com/"
        key = ""
        credential = AzureKeyCredential(key)
        document_analysis_client = DocumentAnalysisClient(endpoint, credential)
        
        batch_name = batch_name 
        out_df = pd.DataFrame(columns=['file_name','content','word_confidence','styles'])
        idx = 0

        for filename in os.listdir(batch_path):
            if filename.endswith(".jpg") or filename.endswith(".tif") : 
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
                            word_info[word.content]=word.confidence

                        
            #         print(result)
                    filename = date_name+'_'+batch_name+'_'+filename
                    styles = poller.result().styles
                    # print(type(styles))
                    out_df.loc[len(out_df.index)] = [filename, result,word_info,styles]

                    # print(filename)
                    idx += 1
                except:
                    continue

        #clean the text
        out_df['content'] = out_df['content'].replace(regex = '\n', value = ' ')
        # out_df['content'] = out_df['content'].replace(regex = '*', value = ' ')
        # out_df['content'] = out_df['content'].replace(regex = '/', value = ' ')
        
        # # write to a tmp path on compute instance
        # temp_out_batch_path = self.local_output_path + batch_name + '/'
        # temp_out_file_path = temp_out_batch_path + batch_name + '_read_output.csv'
        # if not os.path.exists(temp_out_batch_path):
        #     os.makedirs(temp_out_batch_path)

        # out_df.to_csv(temp_out_file_path, index = False)

        print("Read {} images' text in batch {}".format(idx, batch_name))
        return out_df



    def sort_document(self,batchname, df):      
        # key words must be lower-case     
        drug_clinc = ['shoppers drug mart']
        claim_form_keywords = ['group benefits retiree extended health care claim','group benefits extended health care claim', 'group benefits medical expense claim','group benefits assignment of paramedical','the manufacturers life insurance company']
        claim_1_keywords = ['plan member information', '1 of 2']
        claim_2_keywords = ['equipment and appliance','vision care expenses','claims confirmation','mailing instruction', 'banking information and email address','2 of 2']
        drug_keywords = ['pharmacy','drug mart', 'prescription']
        other_receipt_keywords = ['receipt', 'invoice']
        paramedical_keywords = ['clinic', 'treatment', 'therapy','massage','physiotherapist','chiropractor','psychologist','physiotherapy','chiropractic']
        paramedical_cross_keywords = ['invoice', 'receipt', 'bill', 'paid']
        
        #Recognize types by keywords 
        filename_labels = []
        keyword_confidence = []
        for ind in df.index:
            # print(df['file_name'][ind])
            text = str(df['content'][ind]).lower() #add other data cleaning steps if required
            text = re.sub(r'[^\w\s]','',text)
            text = re.sub(' +', ' ', text)
            # print(text)
            word_info = df['word_confidence'][ind]
            # if type(word_info) is str:
            #     word_info = ast.literal_eval(word_info)
            # word_info = dict((re.sub(r'[^\w\s]','',k.lower()), v) for k, v in word_info.items())
            # print(word_info)
            labelled = 0
        

            if any(word in text for word in claim_form_keywords) and any(word in text for word in claim_1_keywords):
                filename_labels.append('claim_page_one')
                labelled = 1


            #if text has two or more keywords of claim page two
            if labelled == 0:
                count = 0
                for keyword in claim_2_keywords:
                    if text.find(keyword)>-1: count += 1
                if count >= 2:
                    filename_labels.append('claim_page_two')
                    labelled = 1

            
            if labelled == 0:
                for clinc in drug_clinc:
                    if text.find(clinc)>-1 and (text.find('prescription receipt')>-1 or text.find('patient pays')>-1):
                        filename_labels.append('drug_receipt')
                        labelled = 1
                        break

            if labelled == 0:
                for drug_keyword in drug_keywords:
                    if text.find(drug_keyword)>-1 and (text.find(' din') or text.find(' rx'))>-1 :
                        filename_labels.append('drug_receipt')
                        labelled = 1
                        break


            if labelled == 0:
                if any(word in text for word in paramedical_keywords) and any(word in text for word in paramedical_cross_keywords):
                    filename_labels.append('paramedical invoice')
                    labelled = 1

            if labelled == 0:
                if any(word in text for word in other_receipt_keywords) and text.find('date') > -1 and text.find('claim')<0:
                    filename_labels.append('other_receipt')
                    labelled = 1

            if labelled == 0:
                filename_labels.append('other_doc')
        
        # print(filename_labels)
        df['page_label'] = filename_labels

        print('Labelled batch {}'.format(batchname))

        return df


    def parse_metadata(self,batch_name, batch_path,local_output_path, df):       
        total_transactions = 0
        total_images = 0
        
        files = os.listdir(batch_path)
        

        oxi_file = [f for f in files if '.oxi' in f or '.desc' in f]

        if len(oxi_file)<1: 
            print('No oxi file in folder')
            return 

        oxi_filename = oxi_file[0]
        file_path = os.path.join(batch_path, oxi_filename)
        temp_out_batch_path = local_output_path + 'parsed_oxi/'
        output_file1 = temp_out_batch_path+ batch_name +'_parsed_oxi_by_transaction_'+batch_name+'.json'
        output_file2 = temp_out_batch_path+ batch_name +'_parsed_oxi_by_filename_'+batch_name+'.json'

        # Read oxi files
        with open(file_path, 'r') as fp:
            doc = fp.read()
        soup = bs(doc, 'lxml')
        meta_data = []
        filename_meta_data = {}
        
        transactions = soup.find_all('transaction')
        trans_list = []

        for i, transaction in enumerate(transactions):
            total_transactions += 1
            d = {}
            d['transaction_id'] = transaction['transactionid']
            d['pages'] = []
            pages = transaction.find_all('page')
            for j, page in enumerate(pages):
                p = {}
                p['batch_sequence'] = page['batchsequence']
                p['images'] = []
                images = page.find_all('image')
                for k, image in enumerate(images):
                    im = {}
                    if image['filename'] != "":
                        total_images += 1
                        im['filename'] = image['filename']
                        im['side'] = image['side']
                        p['images'].append(im)

                        filename_meta_data[image['filename']] = {'transaction_id': transaction['transactionid'], 
                                                                    'batch_sequence': page['batchsequence'], 'side': image['side']}
                        
                        trans_list.append(transaction['transactionid'])
                d['pages'].append(p)

            meta_data.append(d)
        # print(trans_list)
        df['transaction'] = trans_list

        # write to a tmp path on compute instance
        if not os.path.exists(temp_out_batch_path):
            os.makedirs(temp_out_batch_path) 

        with open(output_file1, 'w') as fp:
            json.dump(meta_data, fp, indent=True)
        with open(output_file2, 'w') as fp:
            json.dump(filename_meta_data, fp, indent=True)
            
        print("Parse oxi file in batch {} with total {} transcations and {} images and save locally".format(batch_name, total_transactions , total_images))
        return df

    def handwritten_per(self,batch_name,df):
        handwritten_percentage = []
        for ind in df.index:
            handwritten_len = 0
            if not df['styles'][ind]:
                handwritten_percentage.append(0)
            else:
                styles = df['styles'][ind]
                for style in styles:
                    spans = style.spans
                    for span in spans:
                        handwritten_len += span.length

                content_length = len(df['content'][ind])
                handwritten_percentage.append(round(handwritten_len/content_length, 3))
        
        df['handwritten_percentage'] = handwritten_percentage
        print("Calculating handwritten percentage of files in batch {}".format(batch_name))
        return df


    def check_stamp(self,df,files_list):
        if_stamp = []
        for ind in df.index:
            if df['file_name'][ind] in files_list and df['content'][ind].lower().find('costco')<0:
                if_stamp.append(True)
            else:
                if_stamp.append(False)

        df['has_stamp'] = if_stamp
        return df










