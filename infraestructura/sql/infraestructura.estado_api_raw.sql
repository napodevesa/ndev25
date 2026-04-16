CREATE TABLE infraestructura.estado_api_raw (
    id SERIAL PRIMARY KEY,
    tabla TEXT NOT NULL,                       -- Ej: 'income_statement_anual'
    fecha DATE DEFAULT CURRENT_DATE,           -- Fecha de ejecución
    min_year INTEGER,                          -- Año más antiguo registrado
    max_year INTEGER,                          -- Año más reciente registrado
    cantidad_registros BIGINT,                 -- Cantidad total de registros en la tabla
    creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
