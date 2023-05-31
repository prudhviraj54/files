#!/usr/bin/env python
# coding: utf-8

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
from spellchecker import SpellChecker
from nltk.tokenize import word_tokenize


class fraud_document_analytics:
    def __init__(self):
        pass


    def paid_by_cash(self,read_res):
        cash_payment_invoice = []
        keywords = ['cash']
        payment_keywords = ['cash','money order','credit','visa', 'amex','debit','cheque','charge','mc','db','check','credit card','refund','roa','grant','master','cq']
        read_res['paid_by_cash'] = False
        for row in read_res.iterrows():
            text = row[1]['content'].lower() 
            cash_idx = text.find('cash') 
            cashier_idx = text.find('cashier')
            # remove 'cashier'
            if (cash_idx >-1 and cashier_idx < 0) or (cash_idx >-1 and cashier_idx > -1 and cash_idx != cashier_idx): 
                pay_method = 0
                # exclude invoices where have mulitple payments
                for payment in payment_keywords:
                    if text.find(payment) >-1:
                        pay_method += 1
                if pay_method < 2:
                    read_res.loc[row[0], 'paid_by_cash'] = True

        return read_res


    def has_dollar_symbol(self,read_res):
        read_res['no_dollar_symbol'] = False
        for row in read_res.iterrows():
            text = row[1]['content'].lower()      
            if text.find('$')<0:
                read_res.loc[row[0], 'no_dollar_symbol'] = True
        return read_res


    def address_check(self,read_res):
        # regex_addr_validated = r'\w+\s+(stn|east|west|north|south|avenue|ave|boulevard|blvd|circle|cir|court|ct|expressway|expy|freeway|fwy|lane|ln|parkway|pkwy|pky|road|rd|square|sq|street|st|driveway|dr|drive|highway|hwy|crescent|cres|trail|way).+(albert|ab|british columbia|bc|manitoba|mb|new brunswick|nb|newfoundland and labrador|nl|northwest territories|nt|nova scotia|ns|nunavut|nu|ontario|on|prince edward island|pe|quebec|qc|saskatchewan|sk|yukon|yt).+[a-zA-Z](\d|i|o|s)[a-zA-Z]\s?(\d|i|o|s)[a-zA-Z](\d|i|o)\s'
        regex_addr_validated = r'[a-zA-Z](\d|i|o|s)[a-zA-Z]\s?(\d|i|o|s)[a-zA-Z](\d|i|o)\s'
        read_res['has_invalid_addr'] = False
        road_name = ['avenue','ave','boulevard','blvd','circle','cir','court','ct','expressway','expy','freeway','fwy','lane','ln','parkway','pky','road','rd','square','sq','street','st','driveway','dr','drive','highway','hwy']
        province_name = ['albert','ab','british columbia','bc','manitoba','mb','new brunswick','nb','newfoundland and labrador', 'nl', 'northwest territories', 'nt', 'nova scotia', 'ns', 'nunavut', 'nu','ontario','on', 'prince edward island', 'pe', 'quebec', 'qc', 'saskatchewan', 'sk', 'yukon', 'yt']
        exception_text = ['manulife','image']
        read_res['verified_address'] = None 
        add_regex = re.compile(regex_addr_validated)
        sum = 0
        fail_nbr = 0
        
        for row in read_res.iterrows():
            # exclude handwritten invoices
            # if row[1]['handwritten_percentage'] > 0.25: continue 
            # verified_addr = []
            # print(row[1]['file_name'])
            text = row[1]['content'].lower()
            if_valid = True
            #case 1: have 'address' keywords
            start = 0
            text= text.replace("email address"," ")
            while text.find('address',start) > -1:
                idx = text.find('address',start)
                potential_add_text = text[idx: idx+80] if idx+80 < len(text) else text[idx:]
                sum += 1
                start = idx+1
                # print(potential_add_text)
                search_res = re.search(add_regex, potential_add_text)
                if search_res and any(substring in potential_add_text for substring in road_name) and any(substring in potential_add_text for substring in province_name) :
                    continue
                    # verified_addr.append(search_res.group())
                    # print('pass')
                else:
                    # exclude manulife pobox postal code and other special cases
                    # if text.find('n2j 4w1')<0 and not any(substring in potential_add_text for substring in exception_text) :
                    read_res.loc[row[0], 'has_invalid_addr'] = True
                    if_valid = False
                    fail_nbr += 1
                    # print(potential_add_text)
                    # print('failed failed')



            #case 2: postal code check
            p = re.compile(r'\s[a-zA-Z](\d|i|o)[a-zA-Z]\s?(\d|i|o)[a-zA-Z](\d|i|o)\s')
            for m in p.finditer(text):
                sum += 1
                potential_add_text = text[m.end()-80: m.end()+5] if m.end()-80>0 and m.end()+5 < len(text) else text[: m.end()+1]
                # print(potential_add_text)
                
                search_res = re.search(add_regex, potential_add_text)
                if any(substring in potential_add_text for substring in road_name) and any(substring in potential_add_text for substring in province_name):
                    continue
                    # verified_addr.append(search_res.group())
                    # print('pass')
                else:
                    # if text.find('n2j 4w1')<0 and not any(substring in potential_add_text for substring in exception_text):
                    # print(potential_add_text)
                    # print('failedd')
                    fail_nbr += 1
                    if if_valid == True:
                        read_res.loc[row[0], 'has_invalid_addr'] = True
                    else: continue

            # read_res.at[row[0], 'verified_address'] = verified_addr
        
        # print(fail_nbr/sum)
        return read_res

    def handwritten_check(self,read_res, threshold):
        read_res['above_handwritten_threshold'] = False
        for row in read_res.iterrows():
            handwritten_percentage = float(row[1]['handwritten_percentage'])
            if handwritten_percentage>threshold:
                # print(handwritten_percentage,threshold)
                read_res.loc[row[0], 'above_handwritten_threshold'] = True
        return read_res


    def invoice_num_check(self,read_res):
        read_res['invalid_invoice_nbr'] = False
        read_res['possible_invoice_nbr'] = None
        invoice_num_keywords = ['invoice number', 'invoice #', 'invoice num', 'invoice#']
        for row in read_res.iterrows():
            # print(row[1]['file_name'])
            # locate keyword 'invoice'
            text = row[1]['content'].lower()
            start = 0

            for keyword in invoice_num_keywords:
                idx = text.find(keyword)
                if idx == -1: continue
                potential_invoice_num = text[idx: idx+25] if idx+25 < len(text) else text[idx:]
                start = idx+1
                # print(potential_invoice_num)
                if potential_invoice_num.find('date')>-1: 
                    potential_invoice_num = potential_invoice_num[:potential_invoice_num.find('date')]

                text_list = potential_invoice_num.split(' ')
                for segment in text_list:
                    if any(c.isdigit() for c in segment):
                        if self._if_single_num(segment):
                            # print('suspicious single invoice number: ' + segment)
                            read_res.loc[row[0], 'invalid_invoice_nbr'] = True
                            read_res.loc[row[0], 'possible_invoice_nbr'] = segment
                            break
                        elif self._if_repeat_num(segment): 
                            # print('suspicious repeat invoice number: ' + segment)
                            read_res.loc[row[0], 'invalid_invoice_nbr'] = True
                            read_res.loc[row[0], 'possible_invoice_nbr'] = segment
                            break
                        elif self._if_consecutive_num(segment):
                            # print('suspicious consecutive invoice number: ' + segment)
                            read_res.loc[row[0], 'invalid_invoice_nbr'] = True
                            read_res.loc[row[0], 'possible_invoice_nbr'] = segment
                            break
                        else: 
                            read_res.loc[row[0], 'possible_invoice_nbr'] = segment
                            break
        return read_res


    def _if_repeat_num(self,segment:str):
        regex = '([1-9]+)\\1\\1\\1+'
        search_res = re.search(regex, segment)
        return True if search_res else False

    def _if_single_num(self,segment:str):
        return True if segment.isnumeric() and len(segment)==1 else False

    def _if_consecutive_num(self,segment:str):
        # Get all 2 or more consecutive Numbers
        lstNumbers = re.findall(r'\d{4}', segment)
        # Sort and convert list to Set to avoid duplicates
        setValues = set(sorted(lstNumbers))
        lstValues = list(setValues)
        for num in lstValues:
            if (str(num) in '0123456789'):
                return True

        return False

    def get_payment(self,read_res):
        payment_amount = []
        payment_keywords = ['total', 'payment due', 'payment owing', 'amount', 'payment amount', 'patient pays','fee charged','payment received']

        for row in read_res.iterrows():
            amount = None
            text = row[1]['content'].lower()
            potential_amount = re.findall(r'[$][\d{1,3},?]*\d{1,3}\.\d{2}', text)
            # print(potential_amount)
            if potential_amount:
                amount_list = [float(i[1:].replace(",","")) for i in potential_amount]
                amount = max(amount_list)
            if not amount:
                potential_amount_2 = re.findall(r'[\d{1,3},?]*\d{1,3}\.\d{2}', text)
                if potential_amount_2:
                    amount_list = [float(i.replace(",","")) for i in potential_amount_2]
                    amount = max(amount_list)
            payment_amount.append(amount)
            # print(amount)

        read_res['payment_amount'] = payment_amount

        return read_res


    def get_reg_num(self,read_res):
        read_res['register_num'] = None
        invoice_num_keywords = ['phn/reg', 'reg#', 'reg #', 'register#', 'register #', 'register number', 'reg no', 'register no']
        for row in read_res.iterrows():
            # print(row[1]['file_name'])
            # locate keyword 'invoice'
            text = row[1]['content'].lower()
            text = text.replace('.', ' ')
            text = text.replace(',', ' ')
            text = text.replace(':', ' ')

            start = 0
            for keyword in invoice_num_keywords:
                idx = text.find(keyword)
                if idx == -1: continue
                potential_invoice_num = text[idx: idx+25] if idx+25 < len(text) else text[idx:]
                start = idx+1
                # print(row[1]['file_name'])
                # print(potential_invoice_num)

                text_list = potential_invoice_num.split(' ')
                for segment in text_list:
                    if any(c.isdigit() for c in segment):
                        segment = segment.replace('#', '')
                        segment = segment.replace(' ', '')
                        read_res.loc[row[0], 'register_num'] = segment
                        break
        return read_res



