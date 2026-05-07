import subprocess
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv("/Users/ndev/Desktop/ndev25/.env")

PSQL_PATH = "/Applications/Postgres.app/Contents/Versions/17/bin"
BASE = "/Users/ndev/Desktop/ndev25"

host     = os.getenv("POSTGRES_HOST")
port     = os.getenv("POSTGRES_PORT", "5432")
db       = os.getenv("POSTGRES_DB")
user     = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")

NEON_CONN = f"postgresql://{user}:{password}@{host}:{port}/{db}?sslmode=require&channel_binding=require"

def main():
    fecha = datetime.now().strftime("%Y%m%d")
    dump_path = f"{BASE}/ndev25_backup_{fecha}.dump"

    print(f"\n{'='*60}")
    print(f"  SYNC A NEON — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}")

    # Paso 1 — Dump local
    print("\n📦 Creando dump local...")
    result = subprocess.run([
        f"{PSQL_PATH}/pg_dump",
        "-h", "localhost",
        "-p", "5433",
        "-U", "ndev",
        "-d", "ndev25",
        "--no-owner", "--no-acl",
        "-F", "c",
        "-f", dump_path
    ])

    if result.returncode != 0:
        print("❌ Dump falló")
        return

    size = os.path.getsize(dump_path) / 1024 / 1024
    print(f"✅ Dump creado — {size:.1f} MB")

    # Paso 2 — Restore en Neon
    print("\n☁️  Subiendo a Neon...")
    result = subprocess.run([
        f"{PSQL_PATH}/pg_restore",
        "--no-owner", "--no-acl", "--no-privileges",
        "--clean", "--if-exists",
        "-d", NEON_CONN,
        dump_path
    ])

    print("✅ Sync completo — Neon actualizado")
    print(f"\n{'='*60}\n")

if __name__ == "__main__":
    main()
