
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['S3_INPUT_PATH','S3_OUTPUT_PATH','JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# I/O Path for S3 bucket
input_path= args['S3_INPUT_PATH']
output_path= args['S3_OUTPUT_PATH']


# Read data from S3 Bucket 
data = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("sep", "\t") \
    .csv(input_path)

# Drop unnecessary columns
df = data.drop("_MOM", "IS_SEASONALLY_ADJUSTED","_YOY","CITY", "TABLE_ID", "REGION_TYPE_ID","MONTHS_OF_SUPPLY_MOM","MONTHS_OF_SUPPLY_YOY",
               "REGION_TYPE","REGION","STATE_CODE","MONTHS_OF_SUPPLY_MOM","MONTHS_OF_SUPPLY_YOY","PROPERTY_TYPE_ID","MEDIAN_SALE_PRICE_MOM",
               "MEDIAN_SALE_PRICE_YOY","MEDIAN_LIST_PRICE_MOM","MEDIAN_LIST_PRICE_YOY","MEDIAN_PPSF","MEDIAN_PPSF_MOM","MEDIAN_PPSF_YOY",
               "MEDIAN_LIST_PPSF","MEDIAN_LIST_PPSF_MOM","AVG_SALE_TO_LIST_MOM","AVG_SALE_TO_LIST_YOY","SOLD_ABOVE_LIST","SOLD_ABOVE_LIST_MOM",
               "SOLD_ABOVE_LIST_YOY","PRICE_DROPS","PRICE_DROPS_MOM","PRICE_DROPS_YOY","OFF_MARKET_IN_TWO_WEEKS","OFF_MARKET_IN_TWO_WEEKS_MOM",
               "OFF_MARKET_IN_TWO_WEEKS_YOY","MEDIAN_LIST_PPSF_YOY","PERIOD_DURATION","PARENT_METRO_REGION_METRO_CODE","HOMES_SOLD_MOM","MEDIAN_DOM_MOM",
               "MEDIAN_DOM_YOY","HOMES_SOLD_YOY","PENDING_SALES_MOM","PENDING_SALES_YOY","NEW_LISTINGS_MOM","NEW_LISTINGS_YOY","INVENTORY_MOM","INVENTORY_YOY")

# Save the DataFrame to Parquet format in S3 bronze layer
df.write.mode("overwrite").parquet(output_path)

job.commit()