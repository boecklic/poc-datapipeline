"""
medallion_pipeline.py
─────────────────────────────────────────────────────────────────────────────
Medallion (Bronze → Silver → Gold) data pipeline DAG.

Airflow 3.x notes:
  - Imports come from `airflow.sdk` (the new stable public interface).
  - Deprecated context variables (execution_date, tomorrow_ds, etc.) removed.
  - `logical_date` replaces `execution_date` in task context.
  - `start_date` is still used for scheduled DAGs but `catchup` defaults
    to False in Airflow 3.x (explicitly set here for clarity).
  - `PythonOperator` now also accepts async callables (not used here but
    available if you want to add deferrable steps).

Flow:
  1. upload_sample_csv   – seed a sample CSV into MinIO raw-data bucket
  2. bronze_ingest       – Spark: CSV  → Iceberg bronze table
  3. silver_clean        – Spark: bronze → cleaned silver table
  4. gold_aggregate      – Spark: silver → aggregated gold table
  5. notify_kafka        – publish pipeline-complete event to Kafka

Environment variables (set in docker-compose.yml):
  MINIO_ENDPOINT         http://minio:9000
  ICEBERG_REST_URI       http://iceberg-rest:8181
  KAFKA_BOOTSTRAP        kafka:9092
  AWS_ACCESS_KEY_ID      minioadmin
  AWS_SECRET_ACCESS_KEY  minioadmin
"""

from __future__ import annotations

import json
import logging
import os
import textwrap
from datetime import datetime, timedelta

# Airflow 3.x: import dag and task from the stable `airflow.sdk` interface.
from airflow.sdk import dag, task
from botocore.vendored.six import u
from pyspark.sql import SparkSession

logger = logging.getLogger("airflow.task")

# ──────────────────────────────────────────────────────────────────────────────
# Config (mirrors docker-compose env vars)
# ──────────────────────────────────────────────────────────────────────────────
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
ICEBERG_REST_URI = os.getenv("ICEBERG_REST_URI", "http://iceberg-rest:8181")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

RAW_BUCKET = "raw-data"
RAW_KEY = "customers/customers.csv"
S3_RAW_PATH = f"s3a://{RAW_BUCKET}/{RAW_KEY}"

ICEBERG_CATALOG = "local"
ICEBERG_NS = "medallion"

# ──────────────────────────────────────────────────────────────────────────────
# Sample CSV data
# ──────────────────────────────────────────────────────────────────────────────
SAMPLE_CSV = textwrap.dedent("""\
    id,name,email,age,country,revenue,signup_date
    1,Alice Smith,alice@example.com,30,US,1200.50,2023-01-15
    2,Bob Jones,bob@example.com,,UK,850.00,2023-02-20
    3,Carol White,carol@example.com,25,DE,2300.75,2023-03-10
    4,Dave Brown,dave@example.com,45,US,,2023-04-05
    5,Eve Davis,eve@EXAMPLE.COM,32,FR,980.00,2023-04-22
    6,Frank Miller,frank@example.com,28,US,1500.00,2023-05-01
    7,Grace Wilson,,29,UK,670.25,2023-05-15
    8,Hank Moore,hank@example.com,55,DE,3200.00,2023-06-01
    9,Ivy Taylor,ivy@example.com,22,FR,420.00,2023-06-18
    10,Jack Anderson,jack@example.com,38,US,1875.50,2023-07-04
""")


# ──────────────────────────────────────────────────────────────────────────────
# Shared Spark session factory (called inside each PythonOperator)
# ──────────────────────────────────────────────────────────────────────────────
def _get_spark(app_name: str):
    """Build a SparkSession with Iceberg + S3 (MinIO) config."""
    from pyspark.sql import SparkSession

    packages = ",".join(
        [
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        ]
    )

    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", packages)
        # Iceberg catalog
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "rest")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.uri", ICEBERG_REST_URI)
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", "s3a://warehouse/")
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.s3.endpoint", MINIO_ENDPOINT)
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.s3.path-style-access", "true")
        # S3A / MinIO
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", AWS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        # Performance
        .config("spark.driver.memory", "1g")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ──────────────────────────────────────────────────────────────────────────────
# Pipeline entry point (empty function since logic is in individual tasks)
# ──────────────────────────────────────────────────────────────────────────────
# # ──────────────────────────────────────────────────────────────────────────────
# DAG definition
# ──────────────────────────────────────────────────────────────────────────────
default_args = {
    "owner": "data-engineering",
    "retries": 1,
    # Airflow 3.x: retry_exponential_backoff accepts a numeric multiplier.
    # Use 2.0 (was True in Airflow 2.x) or just a timedelta for fixed delay.
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}


@dag(
    dag_id="medallion_pipeline",
    description="CSV → Bronze → Silver → Gold (Iceberg) + Kafka notification",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["medallion", "iceberg", "spark", "kafka"],
)
def sample_data_pipeline(**_):

    # ──────────────────────────────────────────────────────────────────────────────
    # Task 1 – Upload sample CSV to MinIO
    # ──────────────────────────────────────────────────────────────────────────────
    @task(task_id="upload_sample_csv")
    def upload_sample_csv(**_):
        import boto3
        from botocore.config import Config

        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=AWS_KEY,
            aws_secret_access_key=AWS_SECRET,
            config=Config(signature_version="s3v4"),
            region_name="us-east-1",
        )

        existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
        if RAW_BUCKET not in existing:
            s3.create_bucket(Bucket=RAW_BUCKET)

        s3.put_object(Bucket=RAW_BUCKET, Key=RAW_KEY, Body=SAMPLE_CSV.encode())
        print(f"Uploaded sample CSV → s3://{RAW_BUCKET}/{RAW_KEY}")
        logger.info(
            f"TESTING LOGGING: Uploaded sample CSV → s3://{RAW_BUCKET}/{RAW_KEY}"
        )

    # ──────────────────────────────────────────────────────────────────────────────
    # Task 2 – Bronze: raw CSV → Iceberg bronze table (schema-on-read, no changes)
    # ──────────────────────────────────────────────────────────────────────────────
    @task.pyspark(
        task_id="bronze_ingest",
        config_kwargs={
            "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
            f"spark.sql.catalog.{ICEBERG_CATALOG}": "org.apache.iceberg.spark.SparkCatalog",
            f"spark.sql.catalog.{ICEBERG_CATALOG}.type": "rest",
            f"spark.sql.catalog.{ICEBERG_CATALOG}.uri": ICEBERG_REST_URI,
            f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse": "s3a://warehouse/",
            f"spark.sql.catalog.{ICEBERG_CATALOG}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            f"spark.sql.catalog.{ICEBERG_CATALOG}.s3.endpoint": MINIO_ENDPOINT,
            f"spark.sql.catalog.{ICEBERG_CATALOG}.s3.path-style-access": "true",
            "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
            "spark.hadoop.fs.s3a.access.key": AWS_KEY,
            "spark.hadoop.fs.s3a.secret.key": AWS_SECRET,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "spark.driver.memory": "1g",
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        },
    )
    def bronze_ingest(spark: SparkSession):
        # spark = _get_spark("bronze_ingest")
        try:
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.{ICEBERG_NS}")

            df = (
                spark.read.option("header", "true")
                .option("inferSchema", "true")
                .csv(S3_RAW_PATH)
            )

            from pyspark.sql import functions as F

            df = df.withColumn("_ingested_at", F.current_timestamp()).withColumn(
                "_source_file", F.lit(S3_RAW_PATH)
            )

            df.printSchema()
            print(f"Bronze row count: {df.count()}")

            (
                df.writeTo(f"{ICEBERG_CATALOG}.{ICEBERG_NS}.bronze_customers")
                .tableProperty("write.format.default", "parquet")
                .createOrReplace()
            )
            print("Bronze table written ✓")
        finally:
            spark.stop()

    # ──────────────────────────────────────────────────────────────────────────────
    # Task 3 – Silver: clean & standardise bronze data
    # ──────────────────────────────────────────────────────────────────────────────
    @task(task_id="silver_clean")
    def silver_clean(**_):
        spark = _get_spark("silver_clean")
        try:
            from pyspark.sql import functions as F
            from pyspark.sql.types import DoubleType, IntegerType

            df = spark.table(f"{ICEBERG_CATALOG}.{ICEBERG_NS}.bronze_customers")

            silver = (
                df.dropna(subset=["id", "name", "email"])
                .withColumn("email", F.lower(F.trim(F.col("email"))))
                .withColumn("age", F.col("age").cast(IntegerType()))
                .withColumn("revenue", F.col("revenue").cast(DoubleType()))
                .fillna({"revenue": 0.0})
                .withColumn("signup_year", F.year(F.to_date("signup_date")))
                .withColumn("signup_month", F.month(F.to_date("signup_date")))
                .drop("_ingested_at", "_source_file")
                .withColumn("_cleaned_at", F.current_timestamp())
            )

            silver.printSchema()
            print(f"Silver row count: {silver.count()}")

            (
                silver.writeTo(f"{ICEBERG_CATALOG}.{ICEBERG_NS}.silver_customers")
                .tableProperty("write.format.default", "parquet")
                .partitionedBy("signup_year", "signup_month")
                .createOrReplace()
            )
            print("Silver table written ✓")
        finally:
            spark.stop()

    # ──────────────────────────────────────────────────────────────────────────────
    # Task 4 – Gold: aggregate silver into a business-ready summary
    # ──────────────────────────────────────────────────────────────────────────────
    @task(task_id="gold_aggregate")
    def gold_aggregate(**_):
        spark = _get_spark("gold_aggregate")
        try:
            from pyspark.sql import functions as F

            df = spark.table(f"{ICEBERG_CATALOG}.{ICEBERG_NS}.silver_customers")

            gold = (
                df.groupBy("country", "signup_year")
                .agg(
                    F.count("id").alias("customer_count"),
                    F.sum("revenue").alias("total_revenue"),
                    F.avg("revenue").alias("avg_revenue"),
                    F.avg("age").alias("avg_age"),
                    F.max("revenue").alias("max_revenue"),
                    F.min("revenue").alias("min_revenue"),
                )
                .withColumn("_aggregated_at", F.current_timestamp())
                .orderBy("country", "signup_year")
            )

            gold.show(truncate=False)
            print(f"Gold row count: {gold.count()}")

            (
                gold.writeTo(f"{ICEBERG_CATALOG}.{ICEBERG_NS}.gold_customer_summary")
                .tableProperty("write.format.default", "parquet")
                .partitionedBy("country")
                .createOrReplace()
            )
            print("Gold table written ✓")
        finally:
            spark.stop()

    # ──────────────────────────────────────────────────────────────────────────────
    # Task 5 – Notify via Kafka
    # ──────────────────────────────────────────────────────────────────────────────
    @task(task_id="notify_kafka")
    def notify_kafka(**context):
        from kafka import KafkaProducer

        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
            retries=5,
            request_timeout_ms=30_000,
        )

        # Airflow 3.x: use `logical_date` instead of the removed `execution_date`.
        logical_date = context.get("logical_date") or context.get("data_interval_start")

        payload = {
            "event": "PIPELINE_COMPLETE",
            "dag_id": context["dag"].dag_id,
            "run_id": context["run_id"],
            "logical_date": logical_date.isoformat() if logical_date else None,
            "tables": {
                "bronze": f"{ICEBERG_CATALOG}.{ICEBERG_NS}.bronze_customers",
                "silver": f"{ICEBERG_CATALOG}.{ICEBERG_NS}.silver_customers",
                "gold": f"{ICEBERG_CATALOG}.{ICEBERG_NS}.gold_customer_summary",
            },
            "status": "success",
        }

        future = producer.send(
            topic="pipeline-events",
            key=context["dag"].dag_id,
            value=payload,
        )
        record_metadata = future.get(timeout=15)
        producer.flush()
        producer.close()

        print(
            f"Kafka event published → topic={record_metadata.topic} "
            f"partition={record_metadata.partition} "
            f"offset={record_metadata.offset}"
        )
        print(f"Payload: {json.dumps(payload, indent=2)}")

    t1 = upload_sample_csv()
    t2 = bronze_ingest()
    t3 = silver_clean()
    t4 = gold_aggregate()
    t5 = notify_kafka()

    # Linear dependency chain
    t1 >> t2 >> t3 >> t4 >> t5


sample_data_pipeline()

# ──────────────────────────────────────────────────────────────────────────────
# DAG definition
# ──────────────────────────────────────────────────────────────────────────────
# default_args = {
#     "owner": "data-engineering",
#     "retries": 1,
#     # Airflow 3.x: retry_exponential_backoff accepts a numeric multiplier.
#     # Use 2.0 (was True in Airflow 2.x) or just a timedelta for fixed delay.
#     "retry_delay": timedelta(minutes=2),
#     "email_on_failure": False,
# }

# # Airflow 3.x: `DAG` is imported from `airflow.sdk`.
# # `catchup` now defaults to False in 3.x; set explicitly for clarity.
# with DAG(
#     dag_id="medallion_pipeline",
#     description="CSV → Bronze → Silver → Gold (Iceberg) + Kafka notification",
#     start_date=datetime(2024, 1, 1),
#     schedule="@daily",
#     catchup=False,
#     default_args=default_args,
#     tags=["medallion", "iceberg", "spark", "kafka"],
# ) as dag:
#     t1 = PythonOperator(
#         task_id="upload_sample_csv",
#         python_callable=upload_sample_csv,
#         doc_md="Upload a sample customer CSV to MinIO (raw-data bucket).",
#     )

#     t2 = PythonOperator(
#         task_id="bronze_ingest",
#         python_callable=bronze_ingest,
#         doc_md="Read raw CSV from S3, add metadata columns, write to Iceberg bronze table.",
#     )

#     t3 = PythonOperator(
#         task_id="silver_clean",
#         python_callable=silver_clean,
#         doc_md="Read bronze, drop bad rows, normalise types/values, write to silver table.",
#     )

#     t4 = PythonOperator(
#         task_id="gold_aggregate",
#         python_callable=gold_aggregate,
#         doc_md="Aggregate silver by country/year, write to gold summary table.",
#     )

#     t5 = PythonOperator(
#         task_id="notify_kafka",
#         python_callable=notify_kafka,
#         doc_md="Publish a pipeline-complete event to the `pipeline-events` Kafka topic.",
#     )

#     # Linear dependency chain
#     t1 >> t2 >> t3 >> t4 >> t5
