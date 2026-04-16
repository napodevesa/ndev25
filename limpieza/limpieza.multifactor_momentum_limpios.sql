-- ============================================================
-- INSERT: limpieza.multifactor_momentum_limpios
-- Fecha hardcodeada: modificar DATE '2026-03-25' según el mes
-- ============================================================

CREATE TABLE IF NOT EXISTS limpieza.multifactor_momentum_limpios (
    ticker              TEXT             NOT NULL,
    fecha_de_consulta   DATE             NOT NULL,
    price_1m            DOUBLE PRECISION,
    price_12m           DOUBLE PRECISION,
    momentum_12m_1m     DOUBLE PRECISION,
    source              TEXT,
    created_at          TIMESTAMP WITHOUT TIME ZONE,
    updated_at          TIMESTAMP WITHOUT TIME ZONE,
    CONSTRAINT multifactor_momentum_limpios_pk
        PRIMARY KEY (ticker, fecha_de_consulta)
);

INSERT INTO limpieza.multifactor_momentum_limpios (
    ticker,
    fecha_de_consulta,
    price_1m,
    price_12m,
    momentum_12m_1m,
    source,
    created_at,
    updated_at
)
SELECT
    t.ticker,
    t.fecha_de_consulta,
    CASE WHEN t.price_1m  <= 0 THEN NULL ELSE t.price_1m  END,
    CASE WHEN t.price_12m <= 0 THEN NULL ELSE t.price_12m END,
    CASE
        WHEN t.price_1m  <= 0
          OR t.price_12m <= 0
          OR t.momentum_12m_1m::text = 'NaN'
        THEN NULL
        ELSE t.momentum_12m_1m
    END,
    t.source,
    t.created_at,
    t.updated_at
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY ticker
               ORDER BY fecha_de_consulta DESC, updated_at DESC
           ) AS rn
    FROM api_raw.multifactor_momentum
    WHERE fecha_de_consulta = DATE '2026-04-06'   -- << CAMBIAR CADA MES
) t
WHERE t.rn = 1;

-- Verificación
SELECT COUNT(*) AS registros_insertados
FROM limpieza.multifactor_momentum_limpios
WHERE fecha_de_consulta = DATE '2026-04-06';