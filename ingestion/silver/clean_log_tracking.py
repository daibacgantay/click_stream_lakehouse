import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

spark = SparkSession.builder \
    .appName("Silver_LogTracking") \
    .getOrCreate()

batch_date = sys.argv[1]  


df_bronze = spark.read \
    .format("delta") \
    .load("s3a://lakehouse/bronze/log_tracking")

schema = StructType([
    StructField("event_time",    TimestampType()),
    StructField("event_type",    StringType()),
    StructField("product_id",    IntegerType()),
    StructField("category_id",   StringType()),
    StructField("category_code", StringType()),
    StructField("brand",         StringType()),
    StructField("price",         DoubleType()),
    StructField("user_id",       IntegerType()),
    StructField("user_session",  StringType()),
])

df_parsed = df_bronze.select(
    F.from_json(F.col("content"), schema).alias("data")
).select("data.*") \
 .filter(
    (F.col("event_time") >= F.lit(batch_date).cast("timestamp")) &
    (F.col("event_time") < F.date_add(F.lit(batch_date).cast("date"), 7).cast("timestamp"))
)



df_clean = df_parsed \
    .fillna({"brand": "unknown", "category_code": "unknown"}) \
    .filter(F.col("price") > 0) \
    .dropDuplicates()

df_clean \
    .write \
    .format("delta") \
    .mode("append") \
    .save("s3a://lakehouse/silver/log_tracking")

spark.sql("CREATE DATABASE IF NOT EXISTS silver")
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.log_tracking
    USING delta
    LOCATION 's3a://lakehouse/silver/log_tracking'
""")


spark.stop()