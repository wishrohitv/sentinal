from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    database_url: str = "sqlite+aiosqlite:///./sentinel.db"
    nebius_api_key: str = ""
    nebius_base_url: str = "https://api.tokenfactory.nebius.com/v1"
    nvidia_model_name: str = ""
    ai_provider: str = "demo"
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
        if self.ai_provider.lower() == "nemotron" and self.nemotron_available:
            return "nemotron"
        return "demo"


settings = Settings()
