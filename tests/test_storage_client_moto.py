import pandas as pd
from moto import mock_aws
import boto3
from src.storage_client import StorageClient
from src.config import settings


@mock_aws
def test_atomic_idempotent_csv_upload():
    s3 = boto3.client("s3", region_name=settings.AWS_REGION)
    s3.create_bucket(Bucket=settings.BUCKET_NAME)
    df = pd.DataFrame({"a": [1, 2, 3]})
    client = StorageClient()
    key = "curated/rides/date=2025-09-21/part-00000.csv.gz"
    r1 = client.put_csv(df, key, compress="gzip")
    r2 = client.put_csv(df, key, compress="gzip")
    assert r1 in {"uploaded", "skipped"}
    assert r2 == "skipped"
    obj = s3.get_object(Bucket=settings.BUCKET_NAME, Key=key)
    assert obj["ResponseMetadata"]["HTTPStatusCode"] == 200


@mock_aws
def test_presign_get_put():
    s3 = boto3.client("s3", region_name=settings.AWS_REGION)
    s3.create_bucket(Bucket=settings.BUCKET_NAME)
    client = StorageClient()
    url_get = client.presign_get("some/key.txt", expires=300)
    url_put = client.presign_put("some/key.txt", expires=300)
    assert "X-Amz-Signature" in url_get
    assert "X-Amz-Signature" in url_put