import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Bronze_PurchaseBehavior") \
    .getOrCreate()

batch_date = sys.argv[1]

df_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/opt/spark/data/02-purchase-behavior.csv")

df_filtered = df_raw.filter(
    (F.col("event_time") >= F.lit(batch_date).cast("timestamp")) &
    (F.col("event_time") < F.date_add(F.lit(batch_date).cast("date"), 7).cast("timestamp"))
)

df_filtered.show()

df_bronze = df_filtered.select(
    F.lit("purchase_behavior").alias("datasource"),
    F.current_timestamp().alias("ingesttime"),
    F.to_json(F.struct("*")).alias("content")
)

df_bronze.write \
    .format("delta") \
    .mode("append") \
    .save("s3a://lakehouse/bronze/purchase_behavior")

spark.stop()