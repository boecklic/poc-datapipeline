import os

from pyiceberg.catalog.rest import RestCatalog

ICEBERG_REST_URI = os.getenv("ICEBERG_REST_URI", "http://iceberg-rest:8181")
AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")


def IcebergCatalog():
    """
    Return a PyIceberg REST catalog connected to the local Iceberg REST server.

    PyIceberg catalog properties mirror the REST catalog spec:
      uri       – REST catalog endpoint
      s3.*      – S3FileIO properties for reading/writing the actual data files
    """

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
