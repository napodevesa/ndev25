-- Crea la tabla infraestructura.update_logs
-- Utilizada para registrar logs de ejecución incremental de los scripts de actualización en api_raw
-- Permite auditoría de cargas, errores, y control de ETL


CREATE TABLE infraestructura.update_logs (
    id SERIAL PRIMARY KEY,
    schema_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    ticker TEXT,
    execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status TEXT CHECK (status IN ('success', 'fail')) NOT NULL,
    message TEXT
);
