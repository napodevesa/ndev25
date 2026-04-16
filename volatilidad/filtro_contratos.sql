SELECT
    ticker,
    opcion,
    strike,
    vto,
    dte,

    delta,
    gamma,
    theta,
    vega,

    iv,
    oi,
    volume,
    vwap,
    close_price,

    fecha
FROM volatilidad.salud_opciones
WHERE
    contract_type = 'put'

    -- Volatilidad cara
    AND iv > 0.5

    -- Delta target short put
    AND delta BETWEEN -0.35 AND -0.25

    -- Theta window óptimo
    AND dte BETWEEN 25 AND 45

    -- Liquidez estructural
    AND oi >= 100

    -- Confirmación de actividad
    AND (
        volume >= 5
        OR volume::NUMERIC / NULLIF(oi, 0) >= 0.01
        OR (oi >= 300 AND volume >= 1)
    )

    -- Precio pagado > precio promedio
    --AND close_price > vwap

ORDER BY
    iv DESC,
    oi DESC;
