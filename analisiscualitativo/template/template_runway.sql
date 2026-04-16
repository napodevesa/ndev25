INSERT INTO AnalisisCualitativo.growth_runway (
    ticker,
    fecha_analisis,

    tamano_mercado_score,
    expansion_vector_score,
    competitive_saturation_score,
    regulatory_risk_score,
    execution_complexity_score,

    growth_runway_score_total,
    nota_growth_runway
) VALUES (
    '',
    '2026-01-05',

    -- ¿Qué tan grande es el mercado actual y potencial realista?
    -- 1 = nicho muy chico / 10 = mercado amplio o expandible
    ,

    -- Capacidad de crecer vía nuevas indicaciones, productos, geografías o M&A
    -- 1 = core muy limitado / 10 = múltiples vectores creíbles
    ,

    -- Nivel de saturación competitiva actual y futura
    -- 1 = saturación alta / 10 = espacio aún amplio
    ,

    -- Riesgo regulatorio y dependencia de aprobaciones, reembolsos o cambios normativos
    -- 1 = riesgo alto / 10 = riesgo bajo
    ,

    -- Complejidad de ejecutar el crecimiento (I+D, trials, capex, talento)
    -- 1 = ejecución compleja / 10 = ejecución relativamente simple
    ,

    -- growth_runway_score_total (totalscore / 5)
    
    ,

    -- Nota cualitativa (2–3 líneas)
    ''
);
