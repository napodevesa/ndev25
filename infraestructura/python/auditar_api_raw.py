import psycopg2
from datetime import datetime

# Configuración de conexión
dbname = "ndev25"
user = "ndev"
password = "ndev"
host = "localhost"
port = "5433"

# Conectar a la base de datos
conn = psycopg2.connect(
    dbname=dbname,
    user=user,
    password=password,
    host=host,
    port=port
)
cursor = conn.cursor()

# Listado de tablas a auditar
tablas_api_raw = [
    "income_statement_anual",
    "income_statement_quarter",
    "ratios_historicos_anual",
    "ratios_historicos_quarter"
]

# Función de auditoría
def auditar_tabla(tabla):
    try:
        cursor.execute(f"""
            SELECT MIN(calendarYear), MAX(calendarYear), COUNT(*)
            FROM api_raw.{tabla};
        """)
        min_year, max_year, cantidad = cursor.fetchone()
        
        cursor.execute("""
            INSERT INTO infraestructura.estado_api_raw (tabla, min_year, max_year, cantidad_registros)
            VALUES (%s, %s, %s, %s);
        """, (tabla, min_year, max_year, cantidad))
        
        conn.commit()
        print(f"✅ Auditoría registrada para {tabla}: {min_year} - {max_year} ({cantidad} registros)")
    
    except Exception as e:
        print(f"❌ Error al auditar {tabla}: {e}")
        conn.rollback()

# Ejecutar auditoría para cada tabla
for tabla in tablas_api_raw:
    auditar_tabla(tabla)

# Cerrar conexión
cursor.close()
conn.close()
print("✅ Auditoría de #api_raw completada.")
