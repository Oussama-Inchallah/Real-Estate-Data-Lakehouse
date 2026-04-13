
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from awsglue.context import GlueContext
from awsglue.job import Job


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['S3_INPUT_PATH','S3_OUTPUT_PATH_1','S3_OUTPUT_PATH_2','S3_OUTPUT_PATH_3','JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# I/O Path
input_path= args['S3_INPUT_PATH']
output_path_1= args['S3_OUTPUT_PATH_1']
output_path_2= args['S3_OUTPUT_PATH_2']
output_path_3= args['S3_OUTPUT_PATH_3']

# Read data from S3 silver layer 
data = spark.read.parquet(input_path)


### Gold Layer ### 

# 1 --- Market Overview by Year & State
df_Market_Overview_Year = data.groupby("Year", "STATE").agg({"HOMES_SOLD":"sum",
                                                             "MEDIAN_LIST_PRICE":"avg",
                                                             "MEDIAN_SALE_PRICE":"avg",
                                                             "AVG_LIST_TO_SALE":"avg",
                                                             "MEDIAN_DOM":"avg"}
                                                            ).orderBy("Year")

df_Market_Overview_Year = df_Market_Overview_Year.select("Year",
                                                         "STATE",
                                                         F.col("sum(HOMES_SOLD)").alias("total_homes_sold"),
                                                         F.round("avg(AVG_LIST_TO_SALE)", 2).alias("avg_sale_to_list"),
                                                         F.round("avg(MEDIAN_LIST_PRICE)", 2).alias("avg_list_price"),
                                                         F.round("avg(MEDIAN_SALE_PRICE)", 2).alias("avg_sale_price"),
                                                         F.round("avg(MEDIAN_DOM)", 1).alias("avg_dom"))

df_Market_Overview_Year.write.partitionBy("Year").mode("overwrite").parquet(output_path_1)


# 2 --- Seasonal Analysis: group by Year + State + Season
df_with_season = (data.withColumn("season",
                                  F.when(F.col("Month").cast("int").isin(12, 1, 2), "Winter")
                                  .when(F.col("Month").cast("int").isin(3, 4, 5), "Spring")
                                  .when(F.col("Month").cast("int").isin(6, 7, 8), "Summer")
                                  .otherwise("Fall")))

df_state_season_year = (df_with_season.groupBy("Year", "STATE", "season").agg(
                                                          F.sum("HOMES_SOLD").alias("total_homes_sold"),
                                                          F.round(F.avg("INVENTORY"), 2).alias("avg_inventory"),
                                                          F.round(F.sum("HOMES_SOLD") / F.sum("INVENTORY")*100, 2).alias("absorption_rate%"),
                                                          F.round(F.avg("MEDIAN_LIST_PRICE"), 2).alias("avg_list_price"),
                                                          F.round(F.avg("MEDIAN_SALE_PRICE"), 2).alias("avg_sale_price"),
                                                          F.round(F.avg("AVG_LIST_TO_SALE"), 2).alias("avg_sale_to_list"),
                                                          F.round(F.avg("MEDIAN_DOM"), 1).alias("avg_dom")
                                                         ).orderBy("Year", "STATE", "season"))

df_state_season_year.write.partitionBy("Year","season").mode("overwrite").parquet(output_path_2)


# 3 --- State Market Pressure by Year, State, Property Type
df_gold_market_pressure = (data.groupBy("Year", "STATE", "PROPERTY_TYPE").agg(
                                            F.sum("HOMES_SOLD").alias("total_homes_sold"),
                                            F.round(F.avg("INVENTORY"), 2).alias("avg_inventory"),
                                            F.round(F.avg("PENDING_SALES"), 2).alias("avg_pending_sales"),
                                            F.round(F.sum("HOMES_SOLD") / F.sum("INVENTORY")*100, 2).alias("absorption_rate_%"),
                                            F.round(F.avg("PENDING_SALES") / F.avg("INVENTORY")*100, 2).alias("pending_to_inventory_%"),
                                            F.round(F.avg("MONTHS_OF_SUPPLY"), 2).alias("avg_months_supply"),
                                            F.round(F.avg("AVG_LIST_TO_SALE"), 2).alias("avg_sale_to_list"),
                                            F.round(F.avg("MEDIAN_DOM"), 1).alias("avg_dom"),
                                        ).orderBy("Year", "STATE", "PROPERTY_TYPE"))

df_gold_market_pressure.write.partitionBy("Year").mode("overwrite").parquet(output_path_3)

job.commit()