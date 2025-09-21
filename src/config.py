from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    """Environment-driven configuration for S3/MinIO and pipeline defaults."""

    STORAGE_BACKEND: str = Field(default="s3")
    BUCKET_NAME: str = Field(default="your-bucket")
    AWS_REGION: str = Field(default="eu-west-1")
    AWS_PROFILE: str | None = Field(default=None)
    AWS_ROLE_ARN: str | None = Field(default=None)
    ENDPOINT_URL: str | None = Field(default=None)
    USE_PATH_STYLE: int = Field(default=0)
    PRESIGN_DEFAULT_EXPIRES: int = Field(default=900)
    DATASET_NAME: str = Field(default="rides")
    DEFAULT_PREFIX: str = Field(default="curated/")

    class Config:
        env_file = ".env"


settings = Settings()