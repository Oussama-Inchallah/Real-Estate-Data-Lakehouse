
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from awsglue.context import GlueContext
from awsglue.job import Job


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['S3_INPUT_PATH','S3_OUTPUT_PATH','JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# I/O Path
input_path= args['S3_INPUT_PATH']
output_path= args['S3_OUTPUT_PATH']

# Read data from S3 Bucket 
df = spark.read.parquet(input_path)

# Columns transformations
df = df.withColumn("Month", F.split(F.col("PERIOD_BEGIN"), "-")[1])
df = df.withColumn("Year", F.split(F.col("PERIOD_BEGIN"), "-")[0])
df = df.withColumn("AVG_SALE_TO_LIST", F.round(F.col("AVG_SALE_TO_LIST"), 2))
df = df.withColumn("LAST_UPDATED", F.to_date(F.col("LAST_UPDATED"), "yyyy-MM-dd"))

# Ordering columns
df = df.select("Year", "Month", "PERIOD_BEGIN", "PERIOD_END", "STATE", "PROPERTY_TYPE", "MEDIAN_SALE_PRICE", "MEDIAN_LIST_PRICE", "HOMES_SOLD", "PENDING_SALES", "NEW_LISTINGS", "INVENTORY", "MONTHS_OF_SUPPLY", "MEDIAN_DOM", "AVG_SALE_TO_LIST", "PARENT_METRO_REGION", "LAST_UPDATED")
df_v1 = df.drop("PERIOD_BEGIN","PERIOD_END","PARENT_METRO_REGION")

# Nulls and duplicates
df_v1= df_v1.dropDuplicates()
df_v2= df_v1.dropna()

# Renaming & Filtering
df_v2 = df_v2.withColumnRenamed("AVG_SALE_TO_LIST" ,"AVG_LIST_TO_SALE")
df_v3 = df_v2.select("Year", "Month", "STATE", "PROPERTY_TYPE", "MEDIAN_LIST_PRICE","MEDIAN_SALE_PRICE","AVG_LIST_TO_SALE", "HOMES_SOLD", "PENDING_SALES", "NEW_LISTINGS", "INVENTORY", "MONTHS_OF_SUPPLY", "MEDIAN_DOM",  "LAST_UPDATED")
df_final = df_v3.filter(F.col("Year") >= "2018")

# Save the DataFrame to Parquet format in S3 silver layer
df_final.write.mode("overwrite").parquet(output_path)

job.commit()