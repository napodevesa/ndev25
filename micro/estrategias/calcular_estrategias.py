import subprocess
import os
import logging
from datetime import datetime
from dotenv import load_dotenv

load_dotenv("/Users/ndev/Desktop/ndev25/.env")

RUN_ID = datetime.now().strftime("%Y%m%d_%H%M")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


def main():
    host     = os.getenv("POSTGRES_HOST")
    user     = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    dbname   = os.getenv("POSTGRES_DB")
    port     = os.getenv("POSTGRES_PORT", "5432")

    conn_string = (
        f"postgresql://{user}:{password}@{host}:{port}"
        f"/{dbname}?sslmode=require&channel_binding=require"
    )

    sql_path = os.path.join(
        os.path.dirname(__file__),
        "calcular_estrategias.sql"
    )

    psql_path = "/Applications/Postgres.app/Contents/Versions/17/bin/psql"

    logging.info(f"run_id: {RUN_ID}")
    logging.info(f"Corriendo: {sql_path}")

    result = subprocess.run(
        [psql_path, conn_string, "-f", sql_path],
        capture_output=True,
        text=True
    )

    print(result.stdout)

    if result.returncode != 0:
        logging.error(f"FAIL: {result.stderr}")
    else:
        logging.info("OK — todas las estrategias calculadas")
        print("=== RESUMEN ===")
        print("Dividendos, Buy&Hold, Cash Flow, The Wheel, Crecimiento")
        print("Correr: SELECT COUNT(*) FROM estrategias.dividendos;")


if __name__ == "__main__":
    main()
