-- =============================================================================
-- calcular_estrategias.sql
-- Pobla las 5 tablas del schema estrategias desde las tablas fuente.
-- snapshot_date = primer día del mes en curso (dinámico).
-- Uso: psql "postgresql://..." -f calcular_estrategias.sql
-- =============================================================================


-- ── 1. DIVIDENDOS ─────────────────────────────────────────────────────────────
TRUNCATE TABLE estrategias.dividendos;

INSERT INTO estrategias.dividendos
SELECT DISTINCT ON (e.ticker)
    e.ticker,
    DATE_TRUNC('month', CURRENT_DATE)::DATE AS snapshot_date,
    b.companyname,
    e.sector,
    e.market_cap_tier,
    e.quality_score,
    e.value_score,
    e.altman_z_score,
    e.altman_zona,
    e.piotroski_score,
    e.roic_signo,
    e.deuda_signo,
    e.rsi_14_semanal,
    e.precio_vs_ma200,
    e.sector_alineado,
    r.dividend_yield,
    r.debt_to_equity_ratio,
    r.interest_coverage_ratio,
    r.operating_profit_margin,
    k.roic,
    k.net_debt_to_ebitda,
    e.momentum_3m,
    e.momentum_6m,
    ROW_NUMBER() OVER (ORDER BY r.dividend_yield DESC) AS ranking
FROM seleccion.enriquecimiento e
LEFT JOIN universos.all_usa_common_equity_base b ON b.ticker = e.ticker
LEFT JOIN ingest.ratios_ttm r
    ON r.ticker = e.ticker
    AND r.fecha_consulta = (SELECT MAX(fecha_consulta) FROM ingest.ratios_ttm)
LEFT JOIN ingest.keymetrics k
    ON k.ticker = e.ticker
    AND k.fecha_consulta = (SELECT MAX(fecha_consulta) FROM ingest.keymetrics)
WHERE e.snapshot_date = (SELECT MAX(snapshot_date) FROM seleccion.enriquecimiento)
  AND r.dividend_yield > 0.02
  AND e.altman_zona = 'safe'
  AND e.piotroski_score >= 5
  AND r.debt_to_equity_ratio < 1.0
  AND r.interest_coverage_ratio > 3
  AND e.quality_score >= 55
ORDER BY e.ticker, r.dividend_yield DESC
LIMIT 15;


-- ── 2. BUY & HOLD ─────────────────────────────────────────────────────────────
TRUNCATE TABLE estrategias.buy_hold;

INSERT INTO estrategias.buy_hold
SELECT DISTINCT ON (e.ticker)
    e.ticker,
    DATE_TRUNC('month', CURRENT_DATE)::DATE AS snapshot_date,
    b.companyname,
    e.sector,
    e.market_cap_tier,
    e.quality_score,
    e.value_score,
    e.altman_z_score,
    e.altman_zona,
    e.piotroski_score,
    e.roic_signo,
    e.deuda_signo,
    e.rsi_14_semanal,
    e.precio_vs_ma200,
    e.sector_alineado,
    r.dividend_yield,
    r.operating_profit_margin,
    r.interest_coverage_ratio,
    k.roic,
    k.net_debt_to_ebitda,
    ROW_NUMBER() OVER (ORDER BY e.quality_score DESC) AS ranking
FROM seleccion.enriquecimiento e
LEFT JOIN universos.all_usa_common_equity_base b ON b.ticker = e.ticker
LEFT JOIN ingest.ratios_ttm r
    ON r.ticker = e.ticker
    AND r.fecha_consulta = (SELECT MAX(fecha_consulta) FROM ingest.ratios_ttm)
LEFT JOIN ingest.keymetrics k
    ON k.ticker = e.ticker
    AND k.fecha_consulta = (SELECT MAX(fecha_consulta) FROM ingest.keymetrics)
WHERE e.snapshot_date = (SELECT MAX(snapshot_date) FROM seleccion.enriquecimiento)
  AND e.quality_score >= 75
  AND e.altman_zona = 'safe'
  AND e.piotroski_score >= 6
  AND e.roic_signo = 1
  AND e.deuda_signo = -1
  AND r.operating_profit_margin > 0.15
  AND r.interest_coverage_ratio > 5
ORDER BY e.ticker, e.quality_score DESC
LIMIT 15;


-- ── 3. CASH FLOW ──────────────────────────────────────────────────────────────
TRUNCATE TABLE estrategias.cash_flow;

INSERT INTO estrategias.cash_flow
SELECT DISTINCT ON (e.ticker)
    e.ticker,
    DATE_TRUNC('month', CURRENT_DATE)::DATE AS snapshot_date,
    b.companyname,
    e.sector,
    e.market_cap_tier,
    e.quality_score,
    e.value_score,
    e.altman_z_score,
    e.altman_zona,
    e.piotroski_score,
    e.roic_signo,
    e.deuda_signo,
    e.rsi_14_semanal,
    e.sector_alineado,
    r.dividend_yield,
    d.instrumento,
    d.score_conviccion,
    d.flag_timing,
    o.put_strike,
    o.put_delta,
    o.put_theta,
    o.put_iv,
    o.put_dte,
    o.nivel_iv,
    o.sizing,
    ROW_NUMBER() OVER (ORDER BY d.score_conviccion DESC) AS ranking
FROM seleccion.enriquecimiento e
LEFT JOIN universos.all_usa_common_equity_base b ON b.ticker = e.ticker
LEFT JOIN agente.decision d
    ON d.ticker = e.ticker
    AND d.snapshot_date = e.snapshot_date
LEFT JOIN agente_opciones.trade_decision_opciones o
    ON o.ticker = e.ticker
    AND o.snapshot_date = e.snapshot_date
LEFT JOIN ingest.ratios_ttm r
    ON r.ticker = e.ticker
    AND r.fecha_consulta = (SELECT MAX(fecha_consulta) FROM ingest.ratios_ttm)
WHERE e.snapshot_date = (SELECT MAX(snapshot_date) FROM seleccion.enriquecimiento)
  AND d.instrumento IN ('cash_secured_put', 'bull_put_spread')
  AND d.trade_status = 'active'
  AND o.nivel_iv IN ('media', 'alta')
  AND d.score_conviccion >= 70
  AND e.altman_zona IN ('safe', 'grey')
ORDER BY e.ticker, d.score_conviccion DESC
LIMIT 15;


-- ── 4. THE WHEEL ──────────────────────────────────────────────────────────────
TRUNCATE TABLE estrategias.the_wheel;

INSERT INTO estrategias.the_wheel
SELECT DISTINCT ON (e.ticker)
    e.ticker,
    DATE_TRUNC('month', CURRENT_DATE)::DATE AS snapshot_date,
    b.companyname,
    e.sector,
    e.market_cap_tier,
    e.quality_score,
    e.value_score,
    e.altman_z_score,
    e.altman_zona,
    e.piotroski_score,
    e.roic_signo,
    e.deuda_signo,
    e.rsi_14_semanal,
    e.sector_alineado,
    r.dividend_yield,
    r.operating_profit_margin,
    r.current_ratio,
    o.nivel_iv,
    o.put_strike,
    o.put_delta,
    o.put_theta,
    o.put_dte,
    ROW_NUMBER() OVER (ORDER BY e.quality_score DESC, r.dividend_yield DESC NULLS LAST) AS ranking
FROM seleccion.enriquecimiento e
LEFT JOIN universos.all_usa_common_equity_base b ON b.ticker = e.ticker
LEFT JOIN ingest.ratios_ttm r
    ON r.ticker = e.ticker
    AND r.fecha_consulta = (SELECT MAX(fecha_consulta) FROM ingest.ratios_ttm)
LEFT JOIN agente_opciones.trade_decision_opciones o
    ON o.ticker = e.ticker
    AND o.snapshot_date = e.snapshot_date
WHERE e.snapshot_date = (SELECT MAX(snapshot_date) FROM seleccion.enriquecimiento)
  AND e.quality_score >= 65
  AND e.altman_zona = 'safe'
  AND e.piotroski_score >= 6
  AND o.nivel_iv IN ('media', 'alta')
  AND r.operating_profit_margin > 0.10
  AND r.current_ratio > 1.0
ORDER BY e.ticker, e.quality_score DESC
LIMIT 10;


-- ── 5. CRECIMIENTO ────────────────────────────────────────────────────────────
TRUNCATE TABLE estrategias.crecimiento;

INSERT INTO estrategias.crecimiento
SELECT DISTINCT ON (e.ticker)
    e.ticker,
    DATE_TRUNC('month', CURRENT_DATE)::DATE AS snapshot_date,
    b.companyname,
    e.sector,
    e.market_cap_tier,
    e.quality_score,
    e.value_score,
    e.altman_z_score,
    e.altman_zona,
    e.piotroski_score,
    e.roic_signo,
    e.deuda_signo,
    e.rsi_14_semanal,
    e.precio_vs_ma200,
    e.momentum_3m,
    e.momentum_6m,
    e.momentum_12m,
    e.sector_alineado,
    r.dividend_yield,
    r.operating_profit_margin,
    k.roic,
    ROW_NUMBER() OVER (ORDER BY e.momentum_6m DESC, e.quality_score DESC) AS ranking
FROM seleccion.enriquecimiento e
LEFT JOIN universos.all_usa_common_equity_base b ON b.ticker = e.ticker
LEFT JOIN ingest.ratios_ttm r
    ON r.ticker = e.ticker
    AND r.fecha_consulta = (SELECT MAX(fecha_consulta) FROM ingest.ratios_ttm)
LEFT JOIN ingest.keymetrics k
    ON k.ticker = e.ticker
    AND k.fecha_consulta = (SELECT MAX(fecha_consulta) FROM ingest.keymetrics)
WHERE e.snapshot_date = (SELECT MAX(snapshot_date) FROM seleccion.enriquecimiento)
  AND e.momentum_3m > 5
  AND e.momentum_6m > 8
  AND e.quality_score >= 55
  AND e.rsi_14_semanal BETWEEN 45 AND 70
  AND e.sector_alineado = 'ALIGNED'
  AND r.operating_profit_margin > 0.10
ORDER BY e.ticker, e.momentum_6m DESC
LIMIT 15;
