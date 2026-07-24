"""
medallion_pipeline.py
─────────────────────────────────────────────────────────────────────────────
Medallion (Bronze → Silver → Gold) data pipeline DAG.

Uses PyIceberg + PyArrow + pandas instead of PySpark — no JVM required.

Stack:
  - PyIceberg  : reads/writes Iceberg tables via the REST catalog
  - PyArrow    : columnar in-memory format and Parquet I/O
  - pandas     : transformation logic (familiar, lightweight)
  - boto3      : uploads the seed CSV to MinIO
  - kafka-python: publishes the completion event

Flow:
  1. upload_sample_csv  – seed a sample CSV into MinIO (raw-data bucket)
  2. bronze_ingest      – read CSV from S3, write as-is to Iceberg bronze table
  3. silver_clean       – clean/normalise bronze, write to silver table
  4. gold_aggregate     – aggregate silver, write to gold table
  5. notify_kafka       – publish PIPELINE_COMPLETE event

Environment variables (set in docker-compose.yml):
  MINIO_ENDPOINT         http://minio:9000
  ICEBERG_REST_URI       http://iceberg-rest:8181
  KAFKA_BOOTSTRAP        kafka:9092
  AWS_ACCESS_KEY_ID      minioadmin
  AWS_SECRET_ACCESS_KEY  minioadmin
"""

from __future__ import annotations

import io
import json
import os
import textwrap
from datetime import datetime, timedelta
from typing import Any

from airflow.executors.base_executor import PARALLELISM
from airflow.models import DagRun

# Airflow 3.x: import dag and task from the stable `airflow.sdk` interface.
from airflow.sdk import Param, dag, task
from attr import s
from helpers.iceberg import IcebergCatalog
from numpy import dtype
from requests.utils import DEFAULT_ACCEPT_ENCODING
from shared_tasks.csv_importer import import_csv_from_s3

# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
ICEBERG_REST_URI = os.getenv("ICEBERG_REST_URI", "http://iceberg-rest:8181")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

RAW_BUCKET = "raw-data"
RAW_KEY = "customers/customers.csv"

ICEBERG_NS = "mch"

# ──────────────────────────────────────────────────────────────────────────────
# Sample data
# ──────────────────────────────────────────────────────────────────────────────
""" Sample data is uploaded to minio bucket outside of this DAG """


def _catalog():
    """
    Return a PyIceberg REST catalog connected to the local Iceberg REST server.

    PyIceberg catalog properties mirror the REST catalog spec:
      uri       – REST catalog endpoint
      s3.*      – S3FileIO properties for reading/writing the actual data files
    """
    from pyiceberg.catalog.rest import RestCatalog

    return RestCatalog(
        name="local",
        **{
            "uri": ICEBERG_REST_URI,
            "s3.endpoint": MINIO_ENDPOINT,
            "s3.access-key-id": AWS_KEY,
            "s3.secret-access-key": AWS_SECRET,
            "s3.path-style-access": "true",
            # Tell PyArrow's S3FileSystem to use the MinIO endpoint too
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        },
    )


@dag(
    dag_id="medallion_pipeline_mch_point",
    description="CSV → Bronze → Silver → Gold (Iceberg) + Kafka notification",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["medallion", "iceberg", "spark", "kafka"],
    params={
        # the csv separator that should be used when reading the input CSV file. Must be a single character string.
        "csv_sep": Param(default=",", type="string", minLength=1, maxLength=1),
        # an enum param, must be one of three values
    },
)
def sample_data_pipeline(**_):

    # ──────────────────────────────────────────────────────────────────────────────
    # Task 1 – Upload sample CSV to MinIO
    # ──────────────────────────────────────────────────────────────────────────────
    # @task()
    # def upload_sample_csv(**_):
    #     s3 = _s3_client()

    #     existing_buckets = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    #     if RAW_BUCKET not in existing_buckets:
    #         s3.create_bucket(Bucket=RAW_BUCKET)

    #     s3.put_object(Bucket=RAW_BUCKET, Key=RAW_KEY, Body=SAMPLE_CSV.encode())
    #     print(f"Uploaded sample CSV → s3://{RAW_BUCKET}/{RAW_KEY}")

    # ──────────────────────────────────────────────────────────────────────────────
    # Task 2 – Bronze: read raw CSV from S3, write to Iceberg with no changes
    # ──────────────────────────────────────────────────────────────────────────────
    @task()
    def bronze_ingest(params: dict[str, Any], **_):

        ORG = "mch"
        DATASET = "point-forecast"
        DATE = "20260506"
        PARAM = "jp2000d0"
        FILE = "vnut12.lssw.202605061000.jp2000d0.csv"
        RAW_KEY = f"{ORG}/{DATASET}/{DATE}/{FILE}"
        TABLE = f"{DATASET}_{PARAM}"

        return import_csv_from_s3(
            key=RAW_KEY,
            bucket=RAW_BUCKET,
            iceberg_namespace=ICEBERG_NS,
            iceberg_table_name=TABLE,
            separator=";",
            encoding="latin-1",
            dtype={"Date": dtype(str)},
            drop_if_exists=True,  # overwrite for idempotency (safe to re-run, allows for schema evolution if the CSV changes over time
        )

    # ──────────────────────────────────────────────────────────────────────────────
    # Task 3 – Silver: clean and normalise bronze data
    # ──────────────────────────────────────────────────────────────────────────────
    @task()
    def silver_clean(**_):
        import pandas as pd
        import pyarrow as pa
        from pyiceberg.exceptions import NoSuchTableError

        DATASET = "point-forecast"
        PARAM = "jp2000d0"
        BRONZE_TABLE = f"{ICEBERG_NS}.{DATASET}_{PARAM}"
        SILVER_TABLE = f"{ICEBERG_NS}.{DATASET}_{PARAM}_cleaned"

        cat = IcebergCatalog()
        bronze = cat.load_table(BRONZE_TABLE)

        # PyIceberg → PyArrow → pandas
        df = bronze.scan().to_pandas()

        # silver = (
        #     df
        #     # Drop rows missing critical business keys
        #     .dropna(subset=["id", "name", "email"])
        #     # Normalise email to lowercase
        #     .assign(email=lambda x: x["email"].str.lower().str.strip())
        #     # Coerce types
        #     .assign(
        #         age=pd.to_numeric(df["age"], errors="coerce").astype("Int64"),
        #         revenue=pd.to_numeric(df["revenue"], errors="coerce").fillna(0.0),
        #         # Note that we need to convert to [us] precision, iceberg doesn't support
        #         # [ns] and pandas defaults to [ns]
        #         signup_date=pd.to_datetime(
        #             df["signup_date"].astype("datetime64[us]"),
        #             unit="ms",
        #             errors="coerce",
        #         ),
        #     )
        #     # Derive partition helper columns
        #     .assign(
        #         signup_year=lambda x: x["signup_date"].dt.year.astype("Int64"),
        #         signup_month=lambda x: x["signup_date"].dt.month.astype("Int64"),
        #     )
        #     # Drop bronze metadata columns
        #     .drop(columns=["_ingested_at", "_source_file"])
        #     .assign(_cleaned_at=pd.Timestamp.utcnow())
        #     .reset_index(drop=True)
        # )
        silver = (
            df
            # Coerce date column to datetime (example of handling a different date format in silver)
            .assign(
                date=pd.to_datetime(
                    df["Date"].astype("datetime64[us]"),
                    unit="ms",
                    format="%Y%m%d%H%M",
                    errors="coerce",
                )
            )
            # Drop bronze metadata columns
            .drop(columns=["_ingested_at", "_source_file", "Date"])
        )

        print(f"Silver schema:\n{silver.dtypes}\nRow count: {len(silver)}")

        arrow_table = pa.Table.from_pandas(silver, preserve_index=False)

        try:
            tbl = cat.load_table(SILVER_TABLE)
            tbl.overwrite(arrow_table)
        except NoSuchTableError:
            cat.create_table(SILVER_TABLE, schema=arrow_table.schema)
            cat.load_table(SILVER_TABLE).append(arrow_table)

        print("Silver table written ✓")

    # ──────────────────────────────────────────────────────────────────────────────
    # Task 4 – Gold: aggregate silver into a business-ready summary
    # ──────────────────────────────────────────────────────────────────────────────
    @task()
    def gold_aggregate(**_):
        import pandas as pd
        import pyarrow as pa
        from pyiceberg.exceptions import NoSuchTableError

        DATASET = "point-forecast"
        PARAM = "jp2000d0"
        SILVER_TABLE_POINT = f"{ICEBERG_NS}.{DATASET}_{PARAM}_cleaned"
        SILVER_TABLE_POINT_META = f"{ICEBERG_NS}.{DATASET}_meta_point_cleaned"
        GOLD_TABLE = f"{ICEBERG_NS}.{DATASET}_point_denormalised"

        cat = IcebergCatalog()
        silver_point = cat.load_table(SILVER_TABLE_POINT)
        silver_point_meta = cat.load_table(SILVER_TABLE_POINT_META)

        df = silver_point.scan().to_pandas()
        df_meta = silver_point_meta.scan().to_pandas()

        merged = df.merge(
            df_meta,
            left_on=["point_id", "point_type_id"],
            right_on=["point_id", "point_type_id"],
            how="left",
        )

        gold = (
            # df.groupby(["country", "signup_year"], dropna=False)
            # .agg(
            #     customer_count=("id", "count"),
            #     total_revenue=("revenue", "sum"),
            #     avg_revenue=("revenue", "mean"),
            #     avg_age=("age", "mean"),
            #     max_revenue=("revenue", "max"),
            #     min_revenue=("revenue", "min"),
            # )
            # .reset_index()
            # .assign(_aggregated_at=pd.Timestamp.utcnow())
            # .sort_values(["country", "signup_year"])
            merged.reset_index(drop=True)
        )

        print(gold.to_string())
        print(f"Gold row count: {len(gold)}")

        arrow_table = pa.Table.from_pandas(gold, preserve_index=False)

        try:
            tbl = cat.load_table(GOLD_TABLE)
            tbl.overwrite(arrow_table)
        except NoSuchTableError:
            cat.create_table(GOLD_TABLE, schema=arrow_table.schema)
            cat.load_table(GOLD_TABLE).append(arrow_table)

        print("Gold table written ✓")

    # ──────────────────────────────────────────────────────────────────────────────
    # Task 5 – Notify via Kafka
    # ──────────────────────────────────────────────────────────────────────────────
    @task
    def notify_kafka(**context):
        from kafka import KafkaProducer

        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
            retries=5,
            request_timeout_ms=30_000,
        )

        logical_date = context.get("logical_date") or context.get("data_interval_start")

        payload = {
            "event": "PIPELINE_COMPLETE",
            "dag_id": context["dag"].dag_id,
            "run_id": context["run_id"],
            "logical_date": logical_date.isoformat() if logical_date else None,
            "tables": {
                "bronze": BRONZE_TABLE,
                "silver": SILVER_TABLE,
                "gold": GOLD_TABLE,
            },
            "status": "success",
        }

        record = producer.send(
            topic="pipeline-events",
            key=context["dag"].dag_id,
            value=payload,
        ).get(timeout=15)
        producer.flush()
        producer.close()

        print(
            f"Kafka event published → topic={record.topic} "
            f"partition={record.partition} offset={record.offset}"
        )
        print(f"Payload: {json.dumps(payload, indent=2)}")

    # t1 = upload_sample_csv()
    t2 = bronze_ingest()
    t3 = silver_clean()
    t4 = gold_aggregate()
    # t5 = notify_kafka()

    # Linear dependency chain
    t2 >> t3 >> t4  # >> t5


sample_data_pipeline_dag = sample_data_pipeline()

# ──────────────────────────────────────────────────────────────────────────────
# DAG definition
# ──────────────────────────────────────────────────────────────────────────────
# default_args = {
#     "owner": "data-engineering",
#     "retries": 1,
#     "retry_delay": timedelta(minutes=2),
#     "email_on_failure": False,
# }

# with DAG(
#     dag_id="medallion_pipeline",
#     description="CSV → Bronze → Silver → Gold (Iceberg via PyIceberg) + Kafka notification",
#     start_date=datetime(2024, 1, 1),
#     schedule="@daily",
#     catchup=False,
#     default_args=default_args,
#     tags=["medallion", "iceberg", "pyiceberg", "kafka"],
# ) as dag:

#     t1 = PythonOperator(
#         task_id="upload_sample_csv",
#         python_callable=upload_sample_csv,
#         doc_md="Upload sample customer CSV to MinIO (raw-data bucket).",
#     )

#     t2 = PythonOperator(
#         task_id="bronze_ingest",
#         python_callable=bronze_ingest,
#         doc_md="Read CSV from S3 via boto3, add metadata columns, write to Iceberg bronze table.",
#     )

#     t3 = PythonOperator(
#         task_id="silver_clean",
#         python_callable=silver_clean,
#         doc_md="Read bronze via PyIceberg, clean/normalise, write to silver table.",
#     )

#     t4 = PythonOperator(
#         task_id="gold_aggregate",
#         python_callable=gold_aggregate,
#         doc_md="Aggregate silver by country/year via pandas, write to gold table.",
#     )

#     t5 = PythonOperator(
#         task_id="notify_kafka",
#         python_callable=notify_kafka,
#         doc_md="Publish PIPELINE_COMPLETE event to the `pipeline-events` Kafka topic.",
#     )

# t1 >> t2 >> t3 >> t4 >> t5
