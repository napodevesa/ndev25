CREATE TABLE AnalisisCualitativo.growth_runway (
    ticker TEXT NOT NULL,
    fecha_analisis DATE NOT NULL,

    -- ¿Qué tan grande es el mercado actual y potencial realista?
    -- 1 = nicho muy chico / 3 = mercado amplio o expandible
    tamano_mercado_score SMALLINT CHECK (tamano_mercado_score BETWEEN 1 AND 10),

    -- Capacidad de crecer vía nuevas indicaciones, productos, geografías o M&A
    -- 1 = core muy limitado / 3 = múltiples vectores creíbles
    expansion_vector_score SMALLINT CHECK (expansion_vector_score BETWEEN 1 AND 10),

    -- Nivel de saturación competitiva actual y futura
    -- 1 = saturación alta / 3 = espacio aún amplio
    competitive_saturation_score SMALLINT CHECK (competitive_saturation_score BETWEEN 1 AND 10),

    -- Riesgo regulatorio y dependencia de aprobaciones, reembolsos o cambios normativos
    -- 1 = riesgo alto / 3 = riesgo bajo
    regulatory_risk_score SMALLINT CHECK (regulatory_risk_score BETWEEN 1 AND 10),

    -- Complejidad de ejecutar el crecimiento (I+D, trials, capex, talento)
    -- 1 = ejecución compleja / 3 = ejecución relativamente simple
    execution_complexity_score SMALLINT CHECK (execution_complexity_score BETWEEN 1 AND 10),

    -- Score total/5
    growth_runway_score_total SMALLINT CHECK (growth_runway_score_total BETWEEN 0 AND 100),

    -- Nota cualitativa corta (máx 2–3 líneas)
    nota_growth_runway TEXT,

    PRIMARY KEY (ticker, fecha_analisis)
);