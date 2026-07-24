#!/usr/bin/env bash
# scripts/seed_and_verify.sh
# Run this from the project root AFTER all services are healthy.
# Usage:  bash scripts/seed_and_verify.sh

set -euo pipefail

echo "──────────────────────────────────────────────"
echo " Medallion Stack – Seed & Verify (Airflow 3.2)"
echo "──────────────────────────────────────────────"

# 1. MinIO bucket list
echo ""
echo "[1] MinIO buckets:"
docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin --quiet 2>/dev/null || true
docker exec minio mc ls local/ 2>/dev/null || echo "  (run after minio-init completes)"

# 2. Iceberg catalog namespaces
echo ""
echo "[2] Iceberg REST catalog namespaces:"
curl -s http://localhost:8181/v1/namespaces | python3 -m json.tool 2>/dev/null || echo "  (catalog not yet ready)"

# 3. Kafka topics
echo ""
echo "[3] Kafka topics:"
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null || echo "  (kafka not yet ready)"

# 4. Airflow API server health  (Airflow 3.x: /api/v2/version replaces /health)
echo ""
echo "[4] Airflow API server version:"
curl -s http://localhost:8080/api/v2/version | python3 -m json.tool 2>/dev/null || echo "  (airflow-api-server not yet ready)"

# 5. List DAGs via REST API v2  (Airflow 3.x uses /api/v2/)
echo ""
echo "[5] DAGs registered (REST API v2):"
curl -s -u admin:admin http://localhost:8080/api/v2/dags | \
  python3 -c "import sys,json; dags=json.load(sys.stdin).get('dags',[]); [print(' -', d['dag_id']) for d in dags]" \
  2>/dev/null || echo "  (not ready yet)"

echo ""
echo "──────────────────────────────────────────────"
echo " All checks done."
echo " UI endpoints:"
echo "   Airflow    →  http://localhost:8080  (admin/admin)"
echo "   MinIO      →  http://localhost:9001  (minioadmin/minioadmin)"
echo "   Kafka UI   →  http://localhost:8082"
echo "   Iceberg    →  http://localhost:8181/v1/namespaces"
echo "   JupyterLab →  http://localhost:8888  (topsecret)"
echo "──────────────────────────────────────────────"
