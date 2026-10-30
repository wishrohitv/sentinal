from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    database_url: str = "sqlite+aiosqlite:///./sentinel.db"
    nebius_api_key: str = ""
    nebius_base_url: str = "https://api.tokenfactory.nebius.com/v1"
    nvidia_model_name: str = ""
    ai_provider: str = "demo"
    telemetry_provider: Literal["simulator", "prometheus_loki"] = "simulator"
    prometheus_url: str = "http://prometheus:9090"
    loki_url: str = "http://loki:3100"
    monitored_service_id: str = "monitored-api"
    monitored_app_url: str = "http://monitored-app:8000"
    monitored_control_token: str = ""
    monitored_pool_max_size: int = 5
    http_error_rate_threshold: float = 0.20
    db_timeout_rate_threshold: float = 0.10
    sentinel_detection_interval_seconds: int = 5
    verification_window_seconds: int = 30
    verification_poll_interval_seconds: int = 5
    telemetry_request_timeout_seconds: float = 5.0
    max_metric_samples: int = 500
    sentinel_max_tool_calls: int = 8
    sentinel_investigation_timeout: int = 90
    sentinel_5xx_threshold: float = 0.10
    sentinel_db_pool_threshold: float = 0.90
    sentinel_detection_window_minutes: int = 5
    sentinel_verification_timeout: int = 30
    cors_origins: str = "http://localhost:5173"
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    @property
    def nemotron_available(self) -> bool:
        return bool(self.nebius_api_key and self.nvidia_model_name)

    @property
    def effective_provider(self) -> str:
        if self.ai_provider.lower() in {"nemotron", "nebius"} and self.nemotron_available:
            return "nemotron"
        return "demo"


settings = Settings()
