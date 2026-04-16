CREATE SCHEMA IF NOT EXISTS infraestructura;

CREATE TABLE IF NOT EXISTS infraestructura.update_logs (
    id BIGSERIAL PRIMARY KEY,
    schema_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    ticker TEXT,
    status TEXT NOT NULL,
    message TEXT,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Índices clave para auditoría
CREATE INDEX IF NOT EXISTS idx_update_logs_table
    ON infraestructura.update_logs (schema_name, table_name);

CREATE INDEX IF NOT EXISTS idx_update_logs_status
    ON infraestructura.update_logs (status);

CREATE INDEX IF NOT EXISTS idx_update_logs_ticker
    ON infraestructura.update_logs (ticker);

CREATE INDEX IF NOT EXISTS idx_update_logs_created_at
    ON infraestructura.update_logs (created_at);
