from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType, BooleanType,StructType,StructField
from pyspark.sql import SparkSession
import os
import cv2
import logging
import fitz

spark = SparkSession.builder.appName('computer_version_rules').getOrCreate()


class ComputerVersionRules:
  def __init__(self):
      logger = logging.getLogger(__name__)
  
  @staticmethod
  def detect_stamp(date_name, dbfs_path):
    detected_stamp_files = []

    def detect_stamp_file(filepath):
        if filepath.endswith('.jpg') or filepath.endswith('.tif') or filepath.endswith('.png'):
            return detect_stamp_page(filepath)
        elif filepath.endswith('pdf'):
            fileroot = '/'.join(filepath.split('/')[:-1])
            filename = filepath.split('/')[-1].split('.')[0]
            filename = filename.replace(' ','_')
            try:
                doc = fitz.open(filepath)
                for i,page in enumerate(doc):
                    pix = page.get_pixmap()  # render page to an image
                    save_path = fileroot+'/'+filename+f"_page_{i}.png"
                    pix.save(save_path)

                    if detect_stamp_page(save_path):
                        return True
                    
                return False
            except:
                return False
        else:
            return False



    def detect_stamp_page(filename):
        orb = cv2.SIFT_create()
        train_image = cv2.imread("Capture.png")
        train_image = cv2.cvtColor(train_image, cv2.COLOR_BGR2RGB)
        train_image_blur = cv2.GaussianBlur(train_image.copy(), (15, 15), 0)
        kp1, des1 = orb.detectAndCompute(train_image_blur, None)

        try:                   
            img = cv2.imread(filename)
            img_to_match = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
            img_to_match_blur = cv2.GaussianBlur(img_to_match.copy(), (21, 21), cv2.BORDER_DEFAULT)
            kp2, des2 = orb.detectAndCompute(img_to_match_blur, None)

            if des2 is None:
                return False

            bf = cv2.BFMatcher()
            matches = bf.knnMatch(des1, des2, k=2)
            good = []

            match_distance = 0.75
            for m, n in matches:
                if m.distance < match_distance * n.distance:
                    good.append([m])

            if len(good) > 5:
                return True
            else:
                return False
        except:
            print('error')
            return False


    
    def get_dbutils(spark):
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
        except ImportError:
            import IPython
            dbutils = IPython.get_ipython().user_ns["dbutils"]
        return dbutils

    dbutils = get_dbutils(spark)
    
    # Define the schema for the input DataFrame
    read_schema = StructType([
        StructField("obj_id", StringType()),
        StructField("img_path", StringType()),
    ])
    doc_collection = []
    for file in dbutils.fs.ls(dbfs_path):
        filename = file.name
        if (filename.endswith(".jpg") or filename.endswith(".tif") or filename.endswith(".pdf") or filename.endswith(".png")) and \
            (filename.find('page')==-1):
            doc_collection.append([date_name+'_'+filename,"/dbfs/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_doc_analytics/fraud_tmp/"\
                +date_name+'/'+filename])
            # img_path = "/dbfs/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_doc_analytics/fraud_tmp/fruad_training/"+filename
            # print(img_path)
            # path = "/dbfs/FileStore/shared_uploads/hubaith@mfcgd.com/fraud_invoice_samples/red_flag_param_invoices/"+filename
            # print(path)
            # doc_collection.append([date_name+'_'+filename,path])

    # Create the input DataFrame
    df = spark.createDataFrame(doc_collection, schema=read_schema)

    detect_stamp_udf = udf(detect_stamp_file,  BooleanType())
    # df = df.filter(detect_stamp_udf(df['img_path']))
    df = df.withColumn('if_stamp',detect_stamp_udf("img_path"))

    # return df.selectExpr("concat('{}_', substring_index(path, '/', -1)) as file_name".format(date_name))
    return df
