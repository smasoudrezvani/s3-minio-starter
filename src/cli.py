import click
from .config import settings
from .storage_client import StorageClient
from .dataset import generate
from .logging_utils import configure_logger


logger = configure_logger("cli")


def partitioned_key(prefix: str, dataset: str, day: str, ext: str, part: int = 0, compress: str | None = None) -> str:
    """Construct a partitioned object key for a dataset and date."""
    suffix = f".{ext}"
    if compress == "gzip" and ext == "csv":
        suffix = ".csv.gz"
    key = f"{prefix.rstrip('/')}/{dataset}/date={day}/part-{part:05d}{suffix}"
    return key


@click.group()
def cli():
    """Command-line interface for S3/MinIO data operations."""
    pass


@cli.command()
@click.option("--dataset", default=None, help="Dataset name, e.g., rides.")
@click.option("--n", default=100000, type=int, help="Number of rows to generate.")
@click.option("--date", "day", required=True, help="ISO date, e.g., 2025-09-21.")
@click.option("--format", "fmt", type=click.Choice(["csv", "parquet"]), default="csv")
@click.option("--compress", type=click.Choice(["gzip", "none"]), default="gzip")
@click.option("--target-prefix", default=None, help="Target prefix, e.g., curated/.")
@click.option("--env", "env_tag", default="dev", help="Object tag env value.")
def ingest(dataset, n, day, fmt, compress, target_prefix, env_tag):
    """Generate synthetic data and upload to S3/MinIO atomically and idempotently."""
    ds = dataset or settings.DATASET_NAME
    df = generate(ds, n, day)
    client = StorageClient()
    prefix = target_prefix or settings.DEFAULT_PREFIX
    key = partitioned_key(prefix, ds, day, fmt, part=0, compress=compress)
    tags = {"env": env_tag, "dataset": ds}
    meta = {"schema_version": "1"}
    if fmt == "csv":
        comp = None if compress == "none" else "gzip"
        result = client.put_csv(df, key, compress=comp, tags=tags, metadata=meta)
    else:
        result = client.put_parquet(df, key, tags=tags, metadata=meta)
    logger.info(f"Ingest result: {result} | key={key}")


@cli.command()
@click.argument("prefix")
def ls(prefix):
    """List object keys under a given prefix."""
    client = StorageClient()
    keys = client.list(prefix)
    for k in keys:
        click.echo(k)


@cli.command("presign-get")
@click.option("--key", required=True, help="Object key to sign for GET.")
@click.option("--expires", type=int, default=None, help="Seconds until expiration.")
def presign_get_cmd(key, expires):
    """Create a presigned GET URL."""
    client = StorageClient()
    url = client.presign_get(key, expires=expires)
    click.echo(url)


@cli.command("presign-put")
@click.option("--key", required=True, help="Object key to sign for PUT.")
@click.option("--expires", type=int, default=None, help="Seconds until expiration.")
def presign_put_cmd(key, expires):
    """Create a presigned PUT URL."""
    client = StorageClient()
    url = client.presign_put(key, expires=expires)
    click.echo(url)


if __name__ == "__main__":
    cli()