import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Gold_CohortRetention") \
    .getOrCreate()

batch_date = sys.argv[1]

df_gold = spark.sql(f"""
    WITH cohort_size AS (
        SELECT
            cohort_index_week,
            COUNT(DISTINCT user_id) AS cohort_users
        FROM silver.purchase_behavior
        WHERE week_after = 0
        GROUP BY cohort_index_week
    ),
    weekly_retention AS (
        SELECT
            cohort_index_week,
            week_after,
            COUNT(DISTINCT user_id) AS retained_users
        FROM silver.purchase_behavior
        GROUP BY cohort_index_week, week_after
    )
    SELECT
        w.cohort_index_week,
        w.week_after,
        w.retained_users,
        c.cohort_users,
        ROUND(w.retained_users * 100.0 / c.cohort_users, 2) AS retention_rate
    FROM weekly_retention w
    JOIN cohort_size c ON w.cohort_index_week = c.cohort_index_week
    ORDER BY w.cohort_index_week, w.week_after
""")


spark.sql("CREATE DATABASE IF NOT EXISTS gold")

df_gold \
    .write \
    .format("delta") \
    .mode("overwrite") \
    .save("s3a://lakehouse/gold/cohort_retention")

spark.sql("""
    CREATE TABLE IF NOT EXISTS gold.cohort_retention
    USING delta
    LOCATION 's3a://lakehouse/gold/cohort_retention'
""")

spark.stop()
