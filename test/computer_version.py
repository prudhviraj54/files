from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType, BooleanType
from pyspark.sql import SparkSession
import cv2,os

class ComputerVersionRules:
  def __init__(self, input_path, local_output_path):
    self.input_path = input_path
    self.local_output_path = local_output_path
    self.spark = SparkSession.builder.appName('computer_version_rules').getOrCreate()

  def detect_stamp(self, date_name, batch_name, batch_path):
    detected_stamp_files = []
    def detect_stamp_file(filename):
        file_ext = os.path.splitext(filename)[-1].lower()
        if file_ext == '.jpg' or file_ext == '.tif' or file_ext == '.pdf':
          orb = cv2.SIFT_create()
          train_image = cv2.imread("../content/fd_spark/Capture.png")
          train_image = cv2.cvtColor(train_image, cv2.COLOR_BGR2RGB)
          train_image_blur = cv2.GaussianBlur(train_image.copy(), (15, 15), 0)
          kp1, des1 = orb.detectAndCompute(train_image_blur, None)

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
              file = date_name + '_' + batch_name + '_' + filename
              detected_stamp_files.append(file)
              return True

        return False

    detect_stamp_udf = udf(detect_stamp_file,  BooleanType())
    df = self.spark.read.format('binaryFile').option('pathGlobFilter', '*.jpg').load(batch_path)
    df = df.filter(detect_stamp_udf(df['path']))

    return df.selectExpr("concat('{}_', '{}_', substring_index(path, '/', -1)) as file_name".format(date_name, batch_name))
