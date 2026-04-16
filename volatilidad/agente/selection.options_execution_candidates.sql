INSERT INTO selection.options_execution_candidates (
    ticker,
    snapshot_date,
    options_snapshot_date,

    opcion,
    contract_type,
    strike,
    vto,
    dte,

    delta,
    gamma,
    theta,
    vega,
    iv,

    open_interest,
    volume,
    vwap,
    close_price,

    option_status,
    rejection_reason,

    agent_version
)

WITH base_trades AS (
    -- 1️⃣ Solo trades válidos definidos por el Agente 1+2
    SELECT
        ticker,
        snapshot_date,
        exposicion_buscada
    FROM selection.trade_decisions_agent
    WHERE instrumento = 'cash_secured_put'
      AND trade_status = 'active'
),

options_raw AS (
    -- 2️⃣ Opciones PUT desde Polygon (ya filtradas por script)
    SELECT
        ticker,
        opcion,
        contract_type,
        strike,
        vto,
        dte,
        delta,
        gamma,
        theta,
        vega,
        iv,
        oi        AS open_interest,
        volume,
        vwap,
        close_price,
        fecha     AS options_snapshot_date
    FROM volatilidad.salud_opciones
    WHERE contract_type = 'put'
),

joined AS (
    -- 3️⃣ Join táctico (SOLO por ticker)
    SELECT
        t.ticker,
        t.snapshot_date,
        o.options_snapshot_date,

        o.opcion,
        o.contract_type,
        o.strike,
        o.vto,
        o.dte,

        o.delta,
        o.gamma,
        o.theta,
        o.vega,
        o.iv,

        o.open_interest,
        o.volume,
        o.vwap,
        o.close_price,

        t.exposicion_buscada
    FROM base_trades t
    JOIN options_raw o
      ON t.ticker = o.ticker
),

evaluated AS (
    -- 4️⃣ Evaluación determinística de reglas
    SELECT *,
        CASE
            WHEN open_interest < 100
                THEN 'rejected'
            WHEN volume < 50
                THEN 'rejected'
            WHEN dte < 25 OR dte > 45
                THEN 'rejected'
            WHEN delta < -0.35 OR delta > -0.25
                THEN 'rejected'
            ELSE 'candidate'
        END AS option_status,

        CASE
            WHEN open_interest < 100
                THEN 'Insufficient open interest'
            WHEN volume < 50
                THEN 'Insufficient volume'
            WHEN dte < 25 OR dte > 45
                THEN 'DTE outside allowed window'
            WHEN delta < -0.35 OR delta > -0.25
                THEN 'Delta outside allowed range'
            ELSE NULL
        END AS rejection_reason
    FROM joined
)

SELECT
    ticker,
    snapshot_date,
    options_snapshot_date,

    opcion,
    contract_type,
    strike,
    vto,
    dte,

    delta,
    gamma,
    theta,
    vega,
    iv,

    open_interest,
    volume,
    vwap,
    close_price,

    option_status,
    rejection_reason,

    'v1.0' AS agent_version
FROM evaluated;
