import csv
import io
import os
import subprocess
from dotenv import load_dotenv

load_dotenv("/Users/ndev/Desktop/ndev25/.env")

PSQL = "/Applications/Postgres.app/Contents/Versions/17/bin/psql"


def get_conn_string():
    host     = os.getenv("POSTGRES_HOST", "localhost")
    port     = os.getenv("POSTGRES_PORT", "5432")
    db       = os.getenv("POSTGRES_DB")
    user     = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")

    if "neon.tech" in (host or ""):
        return f"postgresql://{user}:{password}@{host}:{port}/{db}?sslmode=require&channel_binding=require"
    else:
        return f"postgresql://{user}:{password}@{host}:{port}/{db}"


def ejecutar_sql(sql):
    result = subprocess.run(
        [PSQL, get_conn_string(), "-c", sql],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        raise Exception(result.stderr)
    return result.stdout


def ejecutar_sql_fetch(sql):
    """Ejecuta un SELECT y retorna lista de dicts (parsea output CSV de psql)."""
    result = subprocess.run(
        [PSQL, get_conn_string(), "--csv", "-c", sql],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        raise Exception(result.stderr)
    reader = csv.DictReader(io.StringIO(result.stdout))
    return list(reader)


def ejecutar_sql_file(path):
    result = subprocess.run(
        [PSQL, get_conn_string(), "-f", path],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        raise Exception(result.stderr)
    return result.stdout
