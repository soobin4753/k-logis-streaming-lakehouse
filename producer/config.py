import os
from dotenv import load_dotenv

load_dotenv()


def required_env(key: str) -> str:
    value = os.getenv(key)
    if value is None or value.strip() == "":
        raise ValueError(f"환경변수 {key} 가 없습니다. .env 파일을 확인하세요.")
    return value


KAFKA_BOOTSTRAP_SERVERS = required_env("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = required_env("KAFKA_TOPIC")

POSTGRES_HOST = required_env("POSTGRES_HOST")
POSTGRES_DB = required_env("POSTGRES_DB")
POSTGRES_USER = required_env("POSTGRES_USER")
POSTGRES_PASSWORD = required_env("POSTGRES_PASSWORD")
POSTGRES_PORT = required_env("POSTGRES_PORT")

CLEAN_DATASET_PATH = required_env("CLEAN_DATASET_PATH")