-- ============================================================
-- INSERT: limpieza.multifactor_ratios_ttm_limpios
-- Fecha hardcodeada: modificar DATE '2026-03-25' según el mes
-- ============================================================

INSERT INTO limpieza.multifactor_ratios_ttm_limpios (
    ticker,
    fecha_de_consulta,
    grossprofitmarginttm,
    operatingprofitmarginttm,
    netprofitmarginttm,
    returnonequityttm,
    priceearningsratiottm,
    priceearningstogrowthratiottm,
    pricetosalesratiottm,
    pricetofreecashflowsratiottm,
    currentratiottm,
    quickratiottm,
    cashflowtodebtratiottm,
    debtratiottm,
    debtequityratiottm,
    interestcoveragettm,
    freecashflowpersharettm,
    payoutratiottm,
    created_at,
    updated_at
)
SELECT
    t.ticker,
    t.fecha_de_consulta,
    -- Margins / Quality
    CASE WHEN t.grossprofitmarginttm          = 0 THEN NULL ELSE t.grossprofitmarginttm END,
    CASE WHEN t.operatingprofitmarginttm      = 0 THEN NULL ELSE t.operatingprofitmarginttm END,
    CASE WHEN t.netprofitmarginttm            = 0 THEN NULL ELSE t.netprofitmarginttm END,
    CASE WHEN t.returnonequityttm             = 0 THEN NULL ELSE t.returnonequityttm END,
    -- Value
    CASE WHEN t.priceearningsratiottm         <= 0 THEN NULL ELSE t.priceearningsratiottm END,
    CASE WHEN t.priceearningstogrowthratiottm <= 0 THEN NULL ELSE t.priceearningstogrowthratiottm END,
    CASE WHEN t.pricetosalesratiottm          <= 0 THEN NULL ELSE t.pricetosalesratiottm END,
    CASE WHEN t.pricetofreecashflowsratiottm  <= 0 THEN NULL ELSE t.pricetofreecashflowsratiottm END,
    -- Liquidity
    CASE WHEN t.currentratiottm               = 0 THEN NULL ELSE t.currentratiottm END,
    CASE WHEN t.quickratiottm                 = 0 THEN NULL ELSE t.quickratiottm END,
    -- Solvency / Leverage
    CASE WHEN t.cashflowtodebtratiottm        = 0 THEN NULL ELSE t.cashflowtodebtratiottm END,
    CASE WHEN t.debtratiottm                  = 0 THEN NULL ELSE t.debtratiottm END,
    CASE WHEN t.debtequityratiottm            = 0 THEN NULL ELSE t.debtequityratiottm END,
    -- Coverage / Cashflow
    CASE WHEN t.interestcoveragettm           = 0 THEN NULL ELSE t.interestcoveragettm END,
    CASE WHEN t.freecashflowpersharettm       = 0 THEN NULL ELSE t.freecashflowpersharettm END,
    t.payoutratiottm,
    t.created_at,
    t.updated_at
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY ticker
               ORDER BY fecha_de_consulta DESC, updated_at DESC
           ) AS rn
    FROM api_raw.multifactor_ratios_ttm
    WHERE fecha_de_consulta = DATE '2026-04-07'   -- << CAMBIAR CADA MES
) t
WHERE t.rn = 1;

-- Verificación
SELECT COUNT(*) AS registros_insertados
FROM limpieza.multifactor_ratios_ttm_limpios
WHERE fecha_de_consulta = DATE '2026-04-07';