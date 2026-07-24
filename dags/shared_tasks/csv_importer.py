import io
from typing import Any

import pandas as pd
import pyarrow as pa
from helpers.iceberg import IcebergCatalog
from helpers.s3 import S3Client
from numpy import dtype
from outcome import Value
from pyiceberg.exceptions import NoSuchTableError


def import_csv_from_s3(
    key: str,
    bucket: str,
    iceberg_namespace: str,
    iceberg_table_name: str,
    separator: str = ",",
    encoding: str = "utf-8",
    dtype: dict[Any, dtype] | None = None,
    drop_if_exists: bool = False,
    **kwargs,
) -> None:
    print("Hello world")
    s3 = S3Client()
    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(
        io.BytesIO(obj["Body"].read()),
        sep=separator,
        encoding=encoding,
        dtype=dtype,
        **kwargs,
    )

    # Add metadata columns
    df["_ingested_at"] = pd.Timestamp.utcnow()
    df["_source_file"] = f"s3://{bucket}/{key}"

    print(f"Bronze schema:\n{df.dtypes}\nRow count: {len(df)}")

    cat = IcebergCatalog()
    cat.create_namespace_if_not_exists(iceberg_namespace)

    arrow_table = pa.Table.from_pandas(df, preserve_index=False)

    # Overwrite for idempotency (safe to re-run)
    iceberg_table = f"{iceberg_namespace}.{iceberg_table_name}"

    # Drop existing table if it exists to allow for schema evolution (e.g. new columns in the CSV)
    if drop_if_exists:
        try:
            cat.drop_table(iceberg_table)
            print(f"Dropped existing table {iceberg_table} for schema evolution")
        except NoSuchTableError:
            pass

    try:
        tbl = cat.load_table(iceberg_table)
        tbl.overwrite(arrow_table)
    except NoSuchTableError:
        cat.create_table(iceberg_table, schema=arrow_table.schema)
        cat.load_table(iceberg_table).append(arrow_table)
    except ValueError as e:
        print(f"Error writing to Iceberg table: {e}")
        raise

    print("Bronze table written ✓")
