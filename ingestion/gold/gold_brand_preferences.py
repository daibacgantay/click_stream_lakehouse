import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Gold_BrandPreferences") \
    .getOrCreate()

batch_date = sys.argv[1]

df_gold = spark.sql(f"""
    SELECT
        '{batch_date}'              AS week_start,
        brand,
        COUNT(*)                    AS order_count,
        COUNT(DISTINCT user_id)     AS unique_buyers,
        ROUND(SUM(price), 2)        AS total_revenue,
        ROUND(AVG(price), 2)        AS avg_price
    FROM silver.log_tracking
    WHERE event_type = 'purchase'
      AND event_time >= '{batch_date}'
      AND event_time < date_add(cast('{batch_date}' AS date), 7)
    GROUP BY brand
    ORDER BY total_revenue DESC
""")


spark.sql("CREATE DATABASE IF NOT EXISTS gold")

df_gold \
    .write \
    .format("delta") \
    .mode("append") \
    .save("s3a://lakehouse/gold/brand_preferences")

spark.sql("""
    CREATE TABLE IF NOT EXISTS gold.brand_preferences
    USING delta
    LOCATION 's3a://lakehouse/gold/brand_preferences'
""")

spark.stop()
