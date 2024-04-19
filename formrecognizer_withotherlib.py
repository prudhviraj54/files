import pytesseract
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, MapType
from PIL import Image
import os

# Define a UDF to apply OCR to each image file
def ocr_image(file_path):
    try:
        img = Image.open(file_path)
        text = pytesseract.image_to_string(img)
        return text
    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")
        return ""

# Register the UDF
ocr_udf = udf(ocr_image, StringType())

# Function to read documents using OCR
def read_documents(date_name, batch_name, batch_path):
    # Create DataFrame with file paths
    df = spark.createDataFrame([(f"{date_name}_{batch_name}_{filename}", os.path.join(batch_path, filename)) for filename in os.listdir(batch_path)], 
                               schema=["file_name", "file_path"])
    
    # Apply OCR using the UDF
    df = df.withColumn("content", ocr_udf("file_path"))
    
    return df

# Example usage
date_name = "2024-02-28"
batch_name = "batch1"
batch_path = "/dbfs/path/to/images"
df = read_documents(date_name, batch_name, batch_path)
df.show(truncate=False)
