-- ============================================================
-- TABLA: universos.stock_opciones_2026
-- Universo base del sistema — empresas con market cap >= $150M
-- Excluye micro-caps y nano-caps
-- ============================================================

CREATE TABLE universos.stock_opciones_2026 (
    ticker          TEXT        PRIMARY KEY,
    marketcapttm    NUMERIC
);

-- ============================================================
-- INSERT desde api_raw.multifactor_keymetrics_ttm
-- Fecha de consulta: 2026-02-02
-- Filtro: marketcapttm >= 150.000.000
-- ============================================================

INSERT INTO universos.stock_opciones_2026 (
    ticker,
    marketcapttm
)
SELECT
    ticker,
    marketcapttm
FROM api_raw.multifactor_keymetrics_ttm
WHERE marketcapttm >= 150000000
  AND fecha_de_consulta = '2026-02-02';

-- ============================================================
-- VERIFICACIÓN
-- ============================================================

SELECT COUNT(*) AS total_empresas FROM universos.stock_opciones_2026;