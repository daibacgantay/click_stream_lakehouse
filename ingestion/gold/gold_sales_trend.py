import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Gold_SalesTrend") \
    .getOrCreate()

batch_date = sys.argv[1]

df_gold = spark.sql(f"""
    SELECT
        '{batch_date}'              AS week_start,
        DATE(event_time)            AS event_date,
        event_type,
        COUNT(*)                    AS event_count,
        COUNT(DISTINCT user_id)     AS unique_users,
        ROUND(SUM(price), 2)        AS revenue
    FROM silver.log_tracking
    WHERE event_time >= '{batch_date}'
      AND event_time < date_add(cast('{batch_date}' AS date), 7)
    GROUP BY DATE(event_time), event_type
    ORDER BY event_date, event_type
""")


spark.sql("CREATE DATABASE IF NOT EXISTS gold")

df_gold \
    .write \
    .format("delta") \
    .mode("append") \
    .save("s3a://lakehouse/gold/sales_trend")

spark.sql("""
    CREATE TABLE IF NOT EXISTS gold.sales_trend
    USING delta
    LOCATION 's3a://lakehouse/gold/sales_trend'
""")

spark.stop()
