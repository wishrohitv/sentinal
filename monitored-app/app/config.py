from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_environment: str = "local"
    app_version: str = "0.1.0"
    monitored_service_id: str = "monitored-api"
    database_url: str = "postgresql+psycopg2://monitored:monitored-local-password@monitored-db:5432/monitored"
    db_pool_size: int = Field(default=2, ge=1, le=32)
    db_max_overflow: int = Field(default=0, ge=0, le=16)
    db_pool_timeout: float = Field(default=2.0, gt=0, le=60)
    max_db_pool_size: int = Field(default=5, ge=1, le=64)
    control_token: str = ""
    test_endpoints_enabled: bool = True
    log_path: str = "/var/log/monitored-app/app.jsonl"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
