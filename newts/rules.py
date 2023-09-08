from pyspark.sql import SparkSession
import re
from pyspark.sql.functions import col, lower, udf, lit,when
from pyspark.sql import functions as F
import logging
from pyspark.sql.types import IntegerType, StringType, DoubleType, BooleanType,DoubleType
from datetime import datetime 
sc = SparkSession.builder.appName('example_spark').getOrCreate()

class FraudDocumentAnalytics:
    def __init__(self):
        self.spark = SparkSession.builder.appName('document_reader').getOrCreate()
        self.logger = logging.getLogger(__name__)
    
    #   @staticmethod
    def paid_by_cash(self,read_res):
        cash_payment_invoice = []
        keywords = ['cash']
        payment_keywords = ['cash', 'money order', 'credit', 'visa', 'amex', 'debit', 'cheque', 'charge', 'mc', 'db', 'check', 'credit card', 'refund', 'roa', 'grant', 'master', 'cq']
        read_res = read_res.withColumn("payment_method",sum(F.when(lower(col("content")).contains(pay_method), 1).otherwise(0)\
            for pay_method in payment_keywords))
        read_res = read_res.withColumn("paid_by_cash",lower(col("content")).contains("cash")&~(lower(col("content")).contains("cashier"))&(col("payment_method")<2)).drop('payment_method')
        return read_res
    
    #   @staticmethod
    def has_dollar_symbol(self,read_res):
        def check_no_dollar_symbol(content):
            return '$' not in content
        check_no_dollar_symbol_udf = udf(check_no_dollar_symbol, BooleanType())
        read_res = read_res.withColumn('no_dollar_symbol', check_no_dollar_symbol_udf('content'))
        return read_res
    
    #   @staticmethod
    def handwritten_check(self,read_res, threshold):
        read_res = read_res.withColumn('above_handwritten_threshold', F.when(F.col('handwritten_percentage') > threshold, True).otherwise(False))
        return read_res
    
    #   @staticmethod
    def get_payment(self,read_res):
        #payment_amount = []
        #payment_keywords = ['total', 'payment due', 'payment owing', 'amount', 'payment amount', 'patient pays','fee charged','payment received']
        #for row in read_res.iterrows():
        amount = "No Amount"
        def extract_amount(text):
            #text = read_res.lower()
            potential_amount = re.findall(r'[$][\d{1,3},?]*\d{1,3}\.\d{2}', text)
                # print(potential_amount)
            if potential_amount:
                amount_list = [float(i[1:].replace(",","")) for i in potential_amount]
                return max(amount_list)
            potential_amount_2 = re.findall(r'[\d{1,3},?]*\d{1,3}\.\d{2}', text)
            if potential_amount_2:
                amount_list = [float(i.replace(",","")) for i in potential_amount_2]
                return max(amount_list)
            return amount
        extract_amount_udf = F.udf(extract_amount, StringType())
        read_res = read_res.withColumn('content',F.lower(F.col('content')))
        read_res = read_res.withColumn("payment_amount",extract_amount_udf(F.col('content')))
        return read_res
    
    #   @staticmethod         
    def get_reg_num(self,read_res):
        read_res = read_res.withColumn("register_num", lit(None))
        invoice_num_keywords = ['phn/reg', 'reg#', 'reg #', 'register#', 'register #', 'register number', 'reg no', 'register no']
        register = "No Register Num"
        def extract_register_num(text):
            text = text.lower()
            text = text.replace('.', ' ')
            text = text.replace(',', ' ')
            text = text.replace(':', ' ')
            for keyword in invoice_num_keywords:
                    idx = text.find(keyword)
                    if idx == -1:
                        continue
                    potential_invoice_num = text[idx: idx+25] if idx+25 < len(text) else text[idx:]
                    text_list = potential_invoice_num.split(' ')
                    for segment in text_list:
                        if any(c.isdigit() for c in segment):
                            segment = segment.replace('#', '')
                            segment = segment.replace(' ', '')
                            return segment
            return register
        extract_register_num_udf = udf(extract_register_num, StringType())
        read_res = read_res.withColumn("register_num", extract_register_num_udf(col("content")))
        return read_res
    
    #   @staticmethod
    def repeat_num(self,segment):
        regex = r'([1-9]+)\1\1\1+'
        search_res = re.search(regex,segment)
        return True if search_res else False
    
    #   @staticmethod
    def single_num(self,segment):
        return True if segment.isnumeric() and len(segment)==1 else False
  
    #   @staticmethod
    def consecutive_num(self,segment):
        # Get all 2 or more consecutive Numbers
        lstnumbers = re.findall(r'\d{4}', segment)
        # Sort and convert list to Set to avoid duplicates
        setvalues = set(sorted(lstnumbers))
        lstvalues = list(setvalues)
        for num in lstvalues:
            if (str(num) in '0123456789'):
                return True
        return False
  
    #   @staticmethod
    def invoice_num_check(self,read_res):
        read_res = read_res.withColumn("invalid_invoice_nbr", lit(False))
        read_res = read_res.withColumn("possible_invoice_nbr", lit(None))
        def repeat_num(segment):
            regex = r'([1-9]+)\1\1\1+'
            search_res = re.search(regex,segment)
            return True if search_res else False

        def single_num(segment):
            return True if segment.isnumeric() and len(segment)==1 else False

        def consecutive_num(segment):
        # Get all 2 or more consecutive Numbers
            lstnumbers = re.findall(r'\d{4}', segment)
            # Sort and convert list to Set to avoid duplicates
            setvalues = set(sorted(lstnumbers))
            lstvalues = list(setvalues)
            for num in lstvalues:
                if (str(num) in '0123456789'):
                    return True
                return False
            
        def extract_invoice(text):
            text = text.lower()
            invoice_num_keywords = ['invoice number', 'invoice #', 'invoice num', 'invoice#']
            invoice = "None"

            for keyword in invoice_num_keywords:
                idx = text.find(keyword)
                if idx == -1: 
                    continue
                potential_invoice_num = text[idx: idx+25] if idx+25 < len(text) else text[idx:]
                if potential_invoice_num.find('date')>-1: 
                    potential_invoice_num = potential_invoice_num[:potential_invoice_num.find('date')]

                text_list = potential_invoice_num.split(' ')
                for segment in text_list:
                    if any(c.isdigit() for c in segment):
                        if single_num(segment):
                            return segment
                        elif repeat_num(segment):
                            segment_repeat = segment 
                            return segment_repeat
                        elif consecutive_num(segment):
                            segemnt_cons = segment
                            return segemnt_cons
                        else: 
                            #ead_res.loc[row[0], 'possible_invoice_nbr'] = segment
                            return segment
            return invoice
                            
        extract_invoice_udf = udf(extract_invoice, StringType())
        read_res = read_res.withColumn("possible_invoice_nbr", extract_invoice_udf(col("content")))
        return read_res  
    
#   @staticmethod
    def address_check(self,df):
        regex_addr_validated = r'[a-zA-Z](\d|i|o|s)[a-zA-Z]\s?(\d|i|o|s)[a-zA-Z](\d|i|o)\s'
        # road_name = ['avenue', 'ave', 'boulevard', 'blvd', 'circle', 'cir', 'court', 'ct', 'expressway', 'expy', 'freeway', 'fwy', 'lane', 'ln', 'parkway', 'pky', 'road', 'rd', 'square', 'sq', 'street', 'st', 'driveway', 'dr', 'drive', 'highway', 'hwy']
        # province_name = ['albert', 'ab', 'british columbia', 'bc', 'manitoba', 'mb', 'new brunswick', 'nb', 'newfoundland and labrador', 'nl', 'northwest territories', 'nt', 'nova scotia', 'ns', 'nunavut', 'nu', 'ontario', 'on', 'prince edward island', 'pe', 'quebec', 'qc', 'saskatchewan', 'sk', 'yukon', 'yt']
        # exception_text = ['manulife', 'image']
        # add_regex = regex_addr_validated


        # Create a UDF to check address validity
        def is_valid_address(text):
            text = text.lower()

            # # Case 1: Have 'address' keywords
            # start = 0
            # while text.find('address', start) > -1:
            #     idx = text.find('address', start)
            #     potential_add_text = text[idx: idx + 80] if idx + 80 < len(text) else text[idx:]

            #     if re.search(add_regex, potential_add_text) and any(substring in potential_add_text for substring in road_name) and any(substring in potential_add_text for substring in province_name):
            #         start = idx + 1
            #     else:
            #         return False

            # # Case 2: Postal code check
            # p = re.compile(r'\s[a-zA-Z](\d|i|o)[a-zA-Z]\s?(\d|i|o)[a-zA-Z](\d|i|o)\s')
            # for m in p.finditer(text):
            #     potential_add_text = text[m.end() - 80: m.end() + 5] if m.end() - 80 > 0 and m.end() + 5 < len(text) else text[:m.end() + 1]
                
            #     if any(substring in potential_add_text for substring in road_name) and any(substring in potential_add_text for substring in province_name):
            #         continue
            #     else:
            #         return False

            if re.search(regex_addr_validated, text):
                return True
            else:
                return False
        # Define the UDF
        is_valid_address_udf = udf(lambda text: is_valid_address(text), BooleanType())
        # Apply the UDF to check address validity
        df = df.withColumn('has_invalid_addr', ~is_valid_address_udf(lower(col('content'))))

        return df

    def bulk_treatment(self,df):

        def get_date_num(text):
            regex_date_format = [r'\w{3,4}[\s|-]\d+[,|\/]?[\s|-]?20\d{2}',r'\d+[\s|-]\w{3,4}[\s|-]20\d{2}',r'\d{2}\/\d{2}\/20\d{2}',r'20\d{2}\/\d{2}\/\d{2}', r'\d{2}-\w{3,4}-\d{2}\s',r'\w{3,4}[\.|\,|\/|\s]+\d+[\.|\,|\/|\s]+20\d{2}']
            dates = []

            for index, date_format in enumerate(regex_date_format):
                date_match = re.findall(date_format,text)
                for date in set(date_match):
                    date = re.sub(r'[^\w\s]',' ',date)
                    date = re.sub(r'\s+',' ',date)
                    date = date.strip()
                    try:
                        date = convert_date_format(date,index)
                        if date:
                            dates.append(date)
                    except ValueError:
                        pass

            dates = list(set(dates))
            return len(dates)
        
        def get_date(text):
            regex_date_format = [r'\w{3,4}[\s|-]\d+[,|\/]?[\s|-]?20\d{2}',r'\d+[\s|-]\w{3,4}[\s|-]20\d{2}',r'\d{2}\/\d{2}\/20\d{2}',r'20\d{2}\/\d{2}\/\d{2}', r'\d{2}-\w{3,4}-\d{2}\s',r'\w{3,4}[\.|\,|\/|\s]+\d+[\.|\,|\/|\s]+20\d{2}']
            dates = []

            for index, date_format in enumerate(regex_date_format):
                date_match = re.findall(date_format,text)
                for date in set(date_match):
                    date = re.sub(r'[^\w\s]',' ',date)
                    date = re.sub(r'\s+',' ',date)
                    date = date.strip()
                    try:
                        date = convert_date_format(date,index)
                        if date:
                            dates.append(date)
                    except ValueError:
                        pass

            dates = list(set(dates))
            return str(dates)
        
        def convert_date_format(date:str,idx:int):
            if idx == 0:
                d = datetime.strptime(date, '%b %d %Y').date()
            elif idx == 1:
                d = datetime.strptime(date, '%d %b %Y').date()
            elif idx ==2:
                d = datetime.strptime(date, '%m %d %Y').date()
            elif idx ==3:
                d = datetime.strptime(date, '%Y %m %d').date()
            elif idx ==4:
                date = date[:-2]+'20'+date[-2:]
                d = datetime.strptime(date, '%d %b %Y').date()
            else:
                d = None

            return d

        # Define the UDF
        treatment_num_udf = udf(lambda text: get_date_num(text), IntegerType())
        treatment_date_udf = udf(lambda text: get_date(text), StringType())
        # Apply the UDF to check if is bulk treatment
        df = df.withColumn('treatment_num', treatment_num_udf(lower(col('content'))))
        df = df.withColumn('treatment_dates', treatment_date_udf(lower(col('content'))))
        df = df.withColumn('has_bulk_treatment', when((col('treatment_num') > 2) & (col('treatment_num') < 10), True)\
            .otherwise(False)).drop('treatment_num')

        return df
    
    def get_phone(self,df):

        def find_phone(text):
            phone_fax_pattern = r'[(]?\d{3}[)|\-|\s|\.]\s?\d{3}[\-|\s|\.]\d{4}'


            phone_nbr = None
            res = re.search(phone_fax_pattern,text)

            # the first match as phone number
            if res:
                phone_nbr = res.group()
                phone_nbr= re.sub(r'[^\w\s]','',phone_nbr).replace(' ','')

            return phone_nbr

        # Define the UDF
        find_phone_udf = udf(lambda text: find_phone(text), StringType())
        # Apply the UDF to check address validity
        df = df.withColumn('phone_nbr', find_phone_udf(lower(col('content'))))

        return df
    

    def get_fax(self,df):

        def find_fax(text):
            phone_fax_pattern = r'[(]?\d{3}[)|\-|\s|\.]\s?\d{3}[\-|\s|\.]\d{4}'
            fax_keywords= ['fax:','fax','f:']
            fax_nbr = None

            for fax_keyword in fax_keywords:
                if text.find(fax_keyword)>-1:
                    idx = text.find(fax_keyword)
                    potential_fax_nbr = text[idx:idx+30] if idx+30 < len(text) else text[idx:len(text)-1]
                    # print(potential_fax_nbr)
                    res = re.search(phone_fax_pattern,potential_fax_nbr)

                    # the match would be fax number 
                    if res:
                        fax_nbr = res.group()
                        fax_nbr= re.sub(r'[^\w\s]','',fax_nbr).replace(' ','')
                        # print(fax_nbr)
                        break
            
            return fax_nbr
    

        # Define the UDF
        find_fax_udf = udf(lambda text: find_fax(text), StringType())
        # Apply the UDF to check address validity
        df = df.withColumn('fax_nbr', find_fax_udf(lower(col('content'))))

        return df
    

    def get_email(self,df):
        

        def find_email(text):
            email_pattern = r'([A-Za-z0-9]+[-_])*[A-Za-z0-9]+@[A-Za-z0-9-\s]+(\.\s*[A-Z|a-z]{2,})+'

            email = None
            email_addr = re.search(email_pattern, text)
            if email_addr:
                email = email_addr.group()
                # print(email)
            
            return email

        # Define the UDF
        find_email_udf = udf(lambda text: find_email(text), StringType())
        # Apply the UDF to check address validity
        df = df.withColumn('email_nbr', find_email_udf(lower(col('content'))))

        return df

    def get_first_words(self,df):

        def find_first_words(word_info):
        
            # word_info = row[1]['word_confidence']
            word_info = ast.literal_eval(word_info)
            word_info = dict((re.sub(r'[^\w\s]','',k.lower()), v) for k, v in word_info.items())

            words = list(word_info.keys())
            first_five_words = words[:5]

            return first_five_words

        # Define the UDF
        find_first_words_udf = udf(lambda text: find_first_words(text), StringType())
        # Apply the UDF to check address validity
        df = df.withColumn('first_words', find_first_words_udf(col('word_confidence')))

        return df
        
            
