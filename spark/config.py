import os


def required_env(key: str) -> str:
    value = os.getenv(key)
    if value is None or value.strip() == "":
        raise ValueError(f"환경변수 {key} 가 없습니다. docker-compose env_file 또는 .env를 확인하세요.")
    return value


POSTGRES_CONTAINER_HOST = required_env("POSTGRES_CONTAINER_HOST")
POSTGRES_DB = required_env("POSTGRES_DB")
POSTGRES_USER = required_env("POSTGRES_USER")
POSTGRES_PASSWORD = required_env("POSTGRES_PASSWORD")
POSTGRES_PORT = required_env("POSTGRES_PORT")

POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_CONTAINER_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

KAFKA_BOOTSTRAP_SERVERS = required_env("KAFKA_DOCKER_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = required_env("KAFKA_TOPIC")

RAW_DELIVERY_EVENT_PATH = required_env("RAW_DELIVERY_EVENT_PATH")
PROCESSED_DELIVERY_EVENT_PATH = required_env("PROCESSED_DELIVERY_EVENT_PATH")
RAW_CHECKPOINT_PATH = required_env("RAW_CHECKPOINT_PATH")