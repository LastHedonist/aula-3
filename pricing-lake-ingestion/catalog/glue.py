import boto3
from utils.logger import get_logger

log = get_logger(__name__)
_glue = boto3.client("glue")

DATABASE = "pricing_lake"
TABLE = "prices"


def register_partition(bucket: str, date: str, provider: str) -> None:
    """Register or update partition in Glue Data Catalog after Parquet upload."""
    location = f"s3://{bucket}/parquet/{date}/{provider}/"
    try:
        _glue.batch_create_partition(
            DatabaseName=DATABASE,
            TableName=TABLE,
            PartitionInputList=[{
                "Values": [date, provider],
                "StorageDescriptor": {
                    "Location": location,
                    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                    },
                },
            }],
        )
        log.info(f"Registered Glue partition date={date} provider={provider}")
    except _glue.exceptions.AlreadyExistsException:
        # Partition exists — update location (idempotent)
        _glue.update_partition(
            DatabaseName=DATABASE,
            TableName=TABLE,
            PartitionValueList=[date, provider],
            PartitionInput={
                "Values": [date, provider],
                "StorageDescriptor": {"Location": location},
            },
        )
        log.info(f"Updated existing Glue partition date={date} provider={provider}")
