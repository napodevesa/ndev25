CREATE TABLE AnalisisCualitativo.moat (
    ticker TEXT NOT NULL,
    fecha_analisis DATE NOT NULL,

    -- ¿Qué tan doloroso es sacar a esta empresa del cliente?
    switching_costs_score SMALLINT CHECK (switching_costs_score BETWEEN 1 AND 10),

    -- ¿Puede cobrar más sin perder clientes?
    pricing_power_score SMALLINT CHECK (pricing_power_score BETWEEN 1 AND 10),

    -- ¿Cuánto tarda otro en copiar esto, aunque tenga capital?
    barriers_score SMALLINT CHECK (barriers_score BETWEEN 1 AND 10),

    -- ¿El capital trabaja mejor acá que en cualquier otro lado?
    roic_durability_score SMALLINT CHECK (roic_durability_score BETWEEN 1 AND 10),

    -- ¿Cada dólar retenido hace más fuerte el foso?
    reinvestment_score SMALLINT CHECK (reinvestment_score BETWEEN 1 AND 10),

    -- ¿Este equipo directivo amplía o erosiona el moat existente?
    management_quality_score SMALLINT CHECK (management_quality_score BETWEEN 1 AND 10),

    -- Score total expresado como porcentaje (0–100)
    moat_score_total NUMERIC(5,2),

    -- Nota cualitativa corta (2–3 líneas máx)
    nota_cualitativa TEXT,

    PRIMARY KEY (ticker, fecha_analisis)
);
