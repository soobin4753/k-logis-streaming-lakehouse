import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()


def required_env(key: str) -> str:
    value = os.getenv(key)
    if value is None or value.strip() == "":
        raise ValueError(f"환경변수 {key} 가 없습니다. .env 파일을 확인하세요.")
    return value


def get_postgres_conn():
    return psycopg2.connect(
        host=required_env("POSTGRES_HOST"),
        dbname=required_env("POSTGRES_DB"),
        user=required_env("POSTGRES_USER"),
        password=required_env("POSTGRES_PASSWORD"),
        port=required_env("POSTGRES_PORT"),
    )