-- ============================================================
-- INSERT: limpieza.multifactor_keymetrics_ttm_limpios
-- Fecha hardcodeada: modificar DATE '2026-03-25' según el mes
-- ============================================================

INSERT INTO limpieza.multifactor_keymetrics_ttm_limpios (
    ticker,
    fecha_de_consulta,
    freecashflowyieldttm,
    marketcapttm,
    netdebttoebitdattm,
    roicttm,
    created_at,
    updated_at
)
SELECT
    t.ticker,
    t.fecha_de_consulta,
    CASE WHEN t.freecashflowyieldttm = 0 THEN NULL ELSE t.freecashflowyieldttm END,
    CASE WHEN t.marketcapttm         = 0 THEN NULL ELSE t.marketcapttm END,
    CASE WHEN t.netdebttoebitdattm   = 0 THEN NULL ELSE t.netdebttoebitdattm END,
    CASE WHEN t.roicttm              = 0 THEN NULL ELSE t.roicttm END,
    t.created_at,
    t.updated_at
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY ticker
               ORDER BY fecha_de_consulta DESC, updated_at DESC
           ) AS rn
    FROM api_raw.multifactor_keymetrics_ttm
    WHERE fecha_de_consulta = DATE '2026-04-07'   -- << CAMBIAR CADA MES
) t
WHERE t.rn = 1;

-- Verificación
SELECT COUNT(*) AS registros_insertados
FROM limpieza.multifactor_keymetrics_ttm_limpios
WHERE fecha_de_consulta = DATE '2026-04-07';