# s3-minio-starter

A lean, AWS-practical S3/MinIO project that demonstrates:
- Environment-driven config for AWS S3 or existing MinIO
- Atomic and idempotent writes (staging → commit, SHA256 metadata)
- Multipart uploads with retries and jitter
- Partitioned keys and basic tags/metadata
- Presigned GET/PUT URLs
- Minimal IAM samples
- Tests with moto

## Quickstart

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
cp .env.example .env
