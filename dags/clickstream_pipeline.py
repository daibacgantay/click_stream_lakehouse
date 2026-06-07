from datetime import datetime
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

BATCH_WEEKS = [
    "2019-10-28",   # W44
    "2019-11-04",   # W45
    "2019-11-11",   # W46
    "2019-11-18",   # W47
    "2019-11-25",   # W48
]

SUBMIT_KWARGS = dict(
    conn_id="spark_default",
    driver_memory="1g",
    executor_memory="2g",
    executor_cores=2,
    num_executors=1,
    verbose=False,
)

with DAG(
    dag_id="clickstream_lakehouse",
    start_date=datetime(2024, 1, 1),
    schedule=None,       
    catchup=False,
    tags=["lakehouse", "clickstream"],
) as dag:

    prev_month = None

    for batch_date in BATCH_WEEKS:
        tag = batch_date.replace("-", "_")   # "2019_10_28"

        with TaskGroup(group_id=f"week_{tag}") as month_group:

            with TaskGroup(group_id="bronze") as bronze:
                b_log = SparkSubmitOperator(
                    task_id="ingest_log_tracking",
                    application="/opt/airflow/jobs/bronze/ingest_log_tracking.py",
                    application_args=[batch_date],
                    **SUBMIT_KWARGS,
                )
                b_purchase = SparkSubmitOperator(
                    task_id="ingest_purchase_behavior",
                    application="/opt/airflow/jobs/bronze/ingest_purchase_behavior.py",
                    application_args=[batch_date],
                    **SUBMIT_KWARGS,
                )

            with TaskGroup(group_id="silver") as silver:
                s_log = SparkSubmitOperator(
                    task_id="clean_log_tracking",
                    application="/opt/airflow/jobs/silver/clean_log_tracking.py",
                    application_args=[batch_date],
                    **SUBMIT_KWARGS,
                )
                s_purchase = SparkSubmitOperator(
                    task_id="clean_purchase_behavior",
                    application="/opt/airflow/jobs/silver/clean_purchase_behavior.py",
                    application_args=[batch_date],
                    **SUBMIT_KWARGS,
                )

            with TaskGroup(group_id="gold") as gold:
                SparkSubmitOperator(
                    task_id="sales_trend",
                    application="/opt/airflow/jobs/gold/gold_sales_trend.py",
                    application_args=[batch_date],
                    **SUBMIT_KWARGS,
                )
                SparkSubmitOperator(
                    task_id="brand_preferences",
                    application="/opt/airflow/jobs/gold/gold_brand_preferences.py",
                    application_args=[batch_date],
                    **SUBMIT_KWARGS,
                )
                SparkSubmitOperator(
                    task_id="cohort_retention",
                    application="/opt/airflow/jobs/gold/gold_cohort_retention.py",
                    application_args=[batch_date],
                    **SUBMIT_KWARGS,
                )
                SparkSubmitOperator(
                    task_id="rfm_segmentation",
                    application="/opt/airflow/jobs/gold/gold_rfm_segmentation.py",
                    application_args=[batch_date],
                    **SUBMIT_KWARGS,
                )

            
            bronze >> silver >> gold

        # tuần trước xong mới chạy tuần tiếp theo
        if prev_month:
            prev_month >> month_group

        prev_month = month_group
