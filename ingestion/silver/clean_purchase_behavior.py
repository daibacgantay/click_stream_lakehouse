import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, DateType

spark = SparkSession.builder \
    .appName("Silver_PurchaseBehavior") \
    .getOrCreate()

batch_date = sys.argv[1]


df_bronze = spark.read \
    .format("delta") \
    .load("s3a://lakehouse/bronze/purchase_behavior")


schema = StructType([
    StructField("user_id",            IntegerType()),
    StructField("event_time",         TimestampType()),
    StructField("event_type",         StringType()),
    StructField("product_id",         IntegerType()),
    StructField("category_id",        StringType()),
    StructField("category_code",      StringType()),
    StructField("brand",              StringType()),
    StructField("price",              DoubleType()),
    StructField("user_session",       StringType()),
    StructField("event_date",         DateType()),
    StructField("first_event_date",   DateType()),
    StructField("start_of_week",      DateType()),
    StructField("week_number",        IntegerType()),
    StructField("end_of_week",        DateType()),
    StructField("week_text",          StringType()),
    StructField("cohort_index_week",  StringType()),
    StructField("week_after",         IntegerType()),
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
    .save("s3a://lakehouse/silver/purchase_behavior")

spark.sql("CREATE DATABASE IF NOT EXISTS silver")
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.purchase_behavior
    USING delta
    LOCATION 's3a://lakehouse/silver/purchase_behavior'
""")


spark.stop()