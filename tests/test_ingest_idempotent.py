from moto import mock_aws
import boto3
from src.cli import partitioned_key
from src.dataset import generate
from src.storage_client import StorageClient
from src.config import settings


@mock_aws
def test_ingest_flow_idempotent():
    s3 = boto3.client("s3", region_name=settings.AWS_REGION)
    s3.create_bucket(Bucket=settings.BUCKET_NAME)
    df = generate("rides", 1000, "2025-09-21")
    client = StorageClient()
    key = partitioned_key("curated", "rides", "2025-09-21", "csv", part=0, compress="gzip")
    r1 = client.put_csv(df, key, compress="gzip")
    r2 = client.put_csv(df, key, compress="gzip")
    assert r1 in {"uploaded", "skipped"}
    assert r2 == "skipped"