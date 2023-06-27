from pyspark.sql import SparkSession
import re
from pyspark.sql.functions import col, lower, udf, lit,when
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, DoubleType, BooleanType,DoubleType
sc = SparkSession.builder.appName('example_spark').getOrCreate()

class fraud_document_analytics:
  def __init__(self):
    pass

  def paid_by_cash(self,read_res):
    cash_payment_invoice = []
    keywords = ['cash']
    payment_keywords = ['cash', 'money order', 'credit', 'visa', 'amex', 'debit', 'cheque', 'charge', 'mc', 'db', 'check', 'credit card', 'refund', 'roa', 'grant', 'master', 'cq']
    read_res = read_res.withColumn("paid_by_cash",lower(col("content")).contains("cash")&~(lower(col("content")).contains("cashier")))
    return read_res
  
  def has_dollar_symbol(self,read_res):
    def check_no_dollar_symbol(content):
      return '$' not in content
    check_no_dollar_symbol_udf = udf(check_no_dollar_symbol, BooleanType())
    read_res = read_res.withColumn('no_dollar_symbol', check_no_dollar_symbol_udf('content'))
    return read_res

  def handwritten_check(self,read_res, threshold):
    read_res = read_res.withColumn('above_handwritten_threshold', F.lit(False))
    #check_threshold = F.udf(lambda percentage: percentage > threshold, returnType=BooleanType())
    def check_threshold(hand,threshold):
      return hand>threshold
      #check_threshold = F.udf(lambda percentage: int(percentage > threshold), returnType=IntegerType())
   # read_res = read_res.withColumn('hand', read_res['hand'].cast('float'))
    read_res = read_res.withColumn('above_handwritten_threshold', check_threshold(read_res['handwritten_percentage'],threshold))
    return read_res

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
           
  def get_reg_num(self,read_res):
    read_res = read_res.withColumn("register_num", lit(None))
    invoice_num_keywords = ['phn/reg', 'reg#', 'reg #', 'register#', 'register #', 'register number', 'reg no', 'register no']
    Reg = "No Register Num"
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
      return Reg
    extract_register_num_udf = udf(extract_register_num, StringType())
    read_res = read_res.withColumn("register_num", extract_register_num_udf(col("content")))
    return read_res

  def repeat_num(self,segment):
    regex = r'([1-9]+)\1\1\1+'
    search_res = re.search(regex,segment)
    return True if search_res else False

  def single_num(self,segment):
    return True if segment.isnumeric() and len(segment)==1 else False

  def consecutive_num(self,segment):
    # Get all 2 or more consecutive Numbers
    lstNumbers = re.findall(r'\d{4}', segment)
    # Sort and convert list to Set to avoid duplicates
    setValues = set(sorted(lstNumbers))
    lstValues = list(setValues)
    for num in lstValues:
      if (str(num) in '0123456789'):
        return True
        return False
  
  def invoice_num_check(self,read_res):
    read_res = read_res.withColumn("invalid_invoice_nbr", lit(False))
    read_res = read_res.withColumn("possible_invoice_nbr", lit(None))
    def repeat_num(segment):
      regex = r'([1-9]+)\1\1\1+'
      search_res = re.search(regex,segment)
      return True if search_res else False

    def single_num(self,segment):
      return True if segment.isnumeric() and len(segment)==1 else False

    def consecutive_num(self,segment):
      # Get all 2 or more consecutive Numbers
      lstNumbers = re.findall(r'\d{4}', segment)
      # Sort and convert list to Set to avoid duplicates
      setValues = set(sorted(lstNumbers))
      lstValues = list(setValues)
      for num in lstValues:
        if (str(num) in '0123456789'):
          return True
        return False
    invoice_num_keywords = ['invoice number', 'invoice #', 'invoice num', 'invoice#']
    invoice = "None"
    def extract_invoice(text):
      text = text.lower()

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
                      return segment
                  elif consecutive_num(segment):
                      return segment
                  else: 
                      #read_res.loc[row[0], 'possible_invoice_nbr'] = segment
                    return segment
      return invoice
						
    extract_invoice_udf = udf(extract_invoice, StringType())
    read_res = read_res.withColumn("possible_invoice_nbr", extract_invoice_udf(col("content")))
    return read_res  
  

  def address_check(self,df):
    regex_addr_validated = r'[a-zA-Z](\d|i|o|s)[a-zA-Z]\s?(\d|i|o|s)[a-zA-Z](\d|i|o)\s'
    road_name = ['avenue', 'ave', 'boulevard', 'blvd', 'circle', 'cir', 'court', 'ct', 'expressway', 'expy', 'freeway', 'fwy', 'lane', 'ln', 'parkway', 'pky', 'road', 'rd', 'square', 'sq', 'street', 'st', 'driveway', 'dr', 'drive', 'highway', 'hwy']
    province_name = ['albert', 'ab', 'british columbia', 'bc', 'manitoba', 'mb', 'new brunswick', 'nb', 'newfoundland and labrador', 'nl', 'northwest territories', 'nt', 'nova scotia', 'ns', 'nunavut', 'nu', 'ontario', 'on', 'prince edward island', 'pe', 'quebec', 'qc', 'saskatchewan', 'sk', 'yukon', 'yt']
    exception_text = ['manulife', 'image']
    add_regex = regex_addr_validated

    # Create a UDF to check address validity
    def is_valid_address(text):
        text = text.lower()

        # Case 1: Have 'address' keywords
        start = 0
        while text.find('address', start) > -1:
            idx = text.find('address', start)
            potential_add_text = text[idx: idx + 80] if idx + 80 < len(text) else text[idx:]

            if re.search(add_regex, potential_add_text) and any(substring in potential_add_text for substring in road_name) and any(substring in potential_add_text for substring in province_name):
                start = idx + 1
            else:
                return False

        # Case 2: Postal code check
        p = re.compile(r'\s[a-zA-Z](\d|i|o)[a-zA-Z]\s?(\d|i|o)[a-zA-Z](\d|i|o)\s')
        for m in p.finditer(text):
            potential_add_text = text[m.end() - 80: m.end() + 5] if m.end() - 80 > 0 and m.end() + 5 < len(text) else text[:m.end() + 1]
            
            if any(substring in potential_add_text for substring in road_name) and any(substring in potential_add_text for substring in province_name):
                continue
            else:
                return False

        return True
     # Define the UDF
    is_valid_address_udf = udf(lambda text: is_valid_address(text), BooleanType())
    # Apply the UDF to check address validity
    df = df.withColumn('has_invalid_addr', ~is_valid_address_udf(lower(col('content'))))

    return df
  # def address_check(self,read_res):
  #       def if_valid_address(address_text):
  #         regex_addr_validated = r'[a-zA-Z](\d|i|o|s)[a-zA-Z]\s?(\d|i|o|s)[a-zA-Z](\d|i|o)\s'
  #         road_name = ['avenue','ave','boulevard','blvd','circle','cir','court','ct','expressway','expy','freeway','fwy','lane','ln','parkway','pky','road','rd','square','sq','street','st','driveway','dr','drive','highway','hwy']
  #         province_name = ['albert','ab','british columbia','bc','manitoba','mb','new brunswick','nb','newfoundland and labrador', 'nl', 'northwest territories', 'nt', 'nova scotia', 'ns', 'nunavut', 'nu','ontario','on', 'prince edward island', 'pe', 'quebec', 'qc', 'saskatchewan', 'sk', 'yukon', 'yt']
  #         exception_text = ['manulife','image']
  #         add_regex = re.compile(regex_addr_validated)
  #         search_res = re.search(add_regex, address_text)
  #         if search_res and any(substring in address_text for substring in road_name) and any(substring in address_text for substring in province_name):
  #           return True
  #         elif not any(substring in address_text for substring in exception_text):
  #           return False
  #         else:
  #           return True
  #       if_valid_address_udf = udf(if_valid_address, BooleanType())
  #       read_res = read_res.withColumn('has_invalid_addr', col('has_invalid_addr').cast(BooleanType()))
  #       read_res = read_res.withColumn('verified_address', col('verified_address').cast(StringType()))
  #       invoice_num_keywords = ['address']
  #       for keyword in invoice_num_keywords:
  #         read_res = read_res.withColumn('idx', col('content').lower().indexOf(keyword))
  #         read_res = read_res.withColumn('idx', col('idx') + 1)
  #         read_res = read_res.withColumn('potential_add_text', col('content').substr(col('idx'), 80))
  #         read_res = read_res.withColumn('potential_add_text', re.expr("IF(instr(potential_add_text, 'email address') > 0, substring(potential_add_text, 1, instr(potential_add_text, 'email address')-1), potential_add_text)"))
  #         read_res = read_res.withColumn('has_invalid_addr', if_valid_address_udf(col('potential_add_text')) | col('has_invalid_addr'))
  #       p = re.compile(r'\s[a-zA-Z](\d|i|o)[a-zA-Z]\s?(\d|i|o)[a-zA-Z](\d|i|o)\s')
  #       read_res = read_res.withColumn('text_matches', re.expr("transform_all(content, x -> regexp_extract_all(x, '\\s[a-zA-Z](\\d|i|o)[a-zA-Z]\\s?(\\d|i|o)[a-zA-Z](\\d|i|o)\\s', 0))"))
  #       read_res = read_res.withColumn('potential_add_text', re.expr("explode_outer(flatten(text_matches))"))
  #       read_res = read_res.withColumn('has_invalid_addr', if_valid_address_udf(col('potential_add_text')) | col('has_invalid_addr'))

  #       return read_res
