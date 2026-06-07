import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Gold_RFMSegmentation") \
    .getOrCreate()

batch_date = sys.argv[1]

df_gold = spark.sql(f"""
    WITH purchases AS (
        SELECT
            user_id,
            MAX(DATE(event_time))   AS last_purchase_date,
            COUNT(*)                AS frequency,
            SUM(price)              AS monetary
        FROM silver.log_tracking
        WHERE event_type = 'purchase'
        GROUP BY user_id
    ),
    snapshot AS (
        SELECT MAX(DATE(event_time)) AS snapshot_date
        FROM silver.log_tracking
    ),
    rfm_raw AS (
        SELECT
            p.user_id,
            DATEDIFF(s.snapshot_date, p.last_purchase_date)    AS recency_days,
            p.frequency,
            ROUND(p.monetary, 2)                                AS monetary
        FROM purchases p
        CROSS JOIN snapshot s
    ),
    rfm_scored AS (
        SELECT
            user_id,
            recency_days,
            frequency,
            monetary,
            NTILE(5) OVER (ORDER BY recency_days DESC)  AS r_score,
            NTILE(5) OVER (ORDER BY frequency ASC)      AS f_score,
            NTILE(5) OVER (ORDER BY monetary ASC)       AS m_score
        FROM rfm_raw
    )
    SELECT
        user_id,
        recency_days,
        frequency,
        monetary,
        r_score,
        f_score,
        m_score,
        CASE
            WHEN r_score >= 4 AND f_score >= 4              THEN 'Champions'
            WHEN r_score >= 3 AND f_score >= 3              THEN 'Loyal'
            WHEN r_score >= 3 AND f_score < 3               THEN 'Potential'
            WHEN r_score < 3  AND f_score >= 3              THEN 'At Risk'
            ELSE                                                 'Lost'
        END AS segment
    FROM rfm_scored
    ORDER BY r_score DESC, f_score DESC
""")


spark.sql("CREATE DATABASE IF NOT EXISTS gold")

df_gold \
    .write \
    .format("delta") \
    .mode("overwrite") \
    .save("s3a://lakehouse/gold/rfm_segmentation")

spark.sql("""
    CREATE TABLE IF NOT EXISTS gold.rfm_segmentation
    USING delta
    LOCATION 's3a://lakehouse/gold/rfm_segmentation'
""")

spark.stop()
