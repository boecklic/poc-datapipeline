# Local Data Lakehouse Stack (Airflow 3.2)

A fully local replica of an AWS-style medallion data lakehouse, using:

| Component | Local Service | AWS Equivalent |
|---|---|---|
| Object storage | **MinIO** | S3 |
| Table format / catalog | **Apache Iceberg REST** | AWS Glue + S3 |
| Streaming | **Apache Kafka** (Confluent) | MSK / Kinesis |
| Orchestration | **Apache Airflow 3.2** | MWAA |
| Compute | **Apache Spark 3.5** (embedded via PySpark) | EMR / Glue Jobs |

---

## Airflow 3.x Architecture (vs 2.x)

Airflow 3 splits the old monolithic `webserver` process into four independent services.

| Airflow 2.x | Airflow 3.x | Notes |
|---|---|---|
| `airflow webserver` | `airflow api-server` | Serves the new React UI + REST API v2 |
| *(built into scheduler)* | `airflow dag-processor` | New standalone service for DAG file parsing |
| `airflow scheduler` | `airflow scheduler` | Unchanged |
| `airflow triggerer` | `airflow triggerer` | Unchanged |

Other notable breaking changes handled in this setup:

- `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` — new required env var pointing workers at the API server.
- `AIRFLOW__API_AUTH__JWT_SECRET` — required for internal service-to-service JWT auth.
- `apache-airflow-providers-fab` must be installed separately (no longer bundled by default).
- DAG imports use `from airflow.sdk import DAG` (new stable public interface).
- `execution_date` context variable removed; use `logical_date` instead.
- REST API is now at `/api/v2/` (v1 removed).

---

## Project Layout

```
.
├── Dockerfile                  # Custom Airflow image with Java + pip deps baked in
├── docker-compose.yml
├── dags/
│   └── medallion_pipeline.py   # Bronze → Silver → Gold DAG
├── logs/                       # Airflow task logs (auto-created)
├── plugins/                    # Airflow plugins (empty)
└── scripts/
    └── seed_and_verify.sh      # Quick health-check script
```

---

## Quick Start

### Prerequisites

- Docker ≥ 24 and Docker Compose v2
- At least **6 GB RAM** allocated to Docker
- Ports **8080, 8082, 8181, 9000, 9001, 9092, 29092** free

### 1 – Build the custom Airflow image

All pip packages (PySpark, Iceberg, Kafka, FAB, etc.) are baked into a single custom image so every Airflow service has identical dependencies. You only need to do this once, or after changing the Dockerfile.

```bash
docker compose build
```

### 2 – Start the stack

```bash
# Set the Airflow UID so volume files are owned by your user (Linux/macOS)
echo "AIRFLOW_UID=$(id -u)" > .env

docker compose up -d
```

Watch `airflow-init` complete (it runs `db migrate` and creates the admin user, then exits):

```bash
docker compose logs -f airflow-init
# Look for: "airflow-init completed successfully"

docker compose logs -f airflow-api-server
# Look for: "Listening at: http://0.0.0.0:8080"
```

### 3 – Verify services

```bash
bash scripts/seed_and_verify.sh
```

### 4 – Trigger the pipeline

Open Airflow at **http://localhost:8080** (admin / admin), find the `medallion_pipeline` DAG and click **Trigger DAG**.

Or via CLI:

```bash
docker exec airflow-scheduler airflow dags trigger medallion_pipeline
```

---

## UI Endpoints

| Service | URL | Credentials |
|---|---|---|
| Airflow (React UI + API) | http://localhost:8080 | admin / admin |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Kafka UI | http://localhost:8082 | – |
| Iceberg REST API | http://localhost:8181/v1/namespaces | – |

---

## Pipeline Architecture

```
MinIO (raw-data/customers.csv)
        │
        ▼ [Task 1] upload_sample_csv
MinIO s3://raw-data/customers/customers.csv
        │
        ▼ [Task 2] bronze_ingest  (Spark)
Iceberg  local.medallion.bronze_customers
  • All raw columns preserved
  • _ingested_at, _source_file metadata added
        │
        ▼ [Task 3] silver_clean  (Spark)
Iceberg  local.medallion.silver_customers
  • Rows with null id/name/email dropped
  • email lowercased & trimmed
  • age / revenue cast to correct types
  • Missing revenue filled with 0.0
  • signup_year / signup_month derived
  • Partitioned by (signup_year, signup_month)
        │
        ▼ [Task 4] gold_aggregate  (Spark)
Iceberg  local.medallion.gold_customer_summary
  • Aggregated by (country, signup_year)
  • customer_count, total_revenue, avg_revenue,
    avg_age, max_revenue, min_revenue
  • Partitioned by country
        │
        ▼ [Task 5] notify_kafka
Kafka topic: pipeline-events
  • JSON event: PIPELINE_COMPLETE
```

---

## Inspecting Iceberg Tables

```bash
docker exec -it airflow-scheduler bash
```

Inside the container:

```python
python3 - <<'EOF'
from pyspark.sql import SparkSession

spark = (SparkSession.builder
  .appName("inspect")
  .config("spark.jars.packages",
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262")
  .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
  .config("spark.sql.catalog.local.type", "rest")
  .config("spark.sql.catalog.local.uri", "http://iceberg-rest:8181")
  .config("spark.sql.catalog.local.warehouse", "s3a://warehouse/")
  .config("spark.sql.catalog.local.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
  .config("spark.sql.catalog.local.s3.endpoint", "http://minio:9000")
  .config("spark.sql.catalog.local.s3.path-style-access", "true")
  .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
  .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
  .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
  .config("spark.hadoop.fs.s3a.path.style.access", "true")
  .config("spark.sql.extensions",
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
  .master("local[*]")
  .getOrCreate())

spark.sql("SHOW NAMESPACES IN local").show()
spark.table("local.medallion.gold_customer_summary").show(truncate=False)
EOF
```

---

## Consuming the Kafka Event

```bash
docker exec -it kafka \
  kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic pipeline-events \
  --from-beginning
```

---

## Using the REST API v2

```bash
# List DAGs
curl -s -u admin:admin http://localhost:8080/api/v2/dags | python3 -m json.tool

# Trigger a DAG run
curl -s -u admin:admin -X POST \
  http://localhost:8080/api/v2/dags/medallion_pipeline/dagRuns \
  -H "Content-Type: application/json" \
  -d '{"dag_run_id": "manual_test_1"}' | python3 -m json.tool
```

---

## Migrating to AWS

| Local | AWS replacement |
|---|---|
| MinIO endpoint | Remove `fs.s3a.endpoint`; use IAM roles instead of static keys |
| Iceberg REST catalog | Replace with AWS Glue catalog (`org.apache.iceberg.aws.glue.GlueCatalog`) |
| Kafka `kafka:9092` | Update `bootstrap_servers` to your MSK endpoint |
| Airflow (Docker) | Deploy to Amazon MWAA; DAG code is unchanged |
| Spark local mode | Submit to EMR / AWS Glue; change `.master("local[*]")` to cluster mode |

---

## Stopping the Stack

```bash
docker compose down          # keep volumes
docker compose down -v       # destroy all data
```
