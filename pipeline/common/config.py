from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    eventhub_namespace: str
    eventhub_topic: str = "transactions-raw"
    eventhub_connection_string: str

    bronze_table: str = "sentinel.bronze_transactions"
    bronze_checkpoint_path: str

    stream_max_retries: int = 5
    stream_backoff_base_seconds: float = 2.0

    @property
    def kafka_bootstrap_servers(self) -> str:
        # event hubs serves the kafka protocol on 9093 only - there is no plaintext 9092 port
        return f"{self.eventhub_namespace}.servicebus.windows.net:9093"


@lru_cache
def get_settings() -> Settings:
    return Settings()
