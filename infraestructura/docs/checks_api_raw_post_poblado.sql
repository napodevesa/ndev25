---Verificar rangos de años cargados

SELECT MIN(calendarYear), MAX(calendarYear), COUNT(*) FROM api_raw.income_statement_anual;
SELECT MIN(calendarYear), MAX(calendarYear), COUNT(*) FROM api_raw.income_statement_quarter;
SELECT MIN(calendarYear), MAX(calendarYear), COUNT(*) FROM api_raw.ratios_historicos_anual;
SELECT MIN(calendarYear), MAX(calendarYear), COUNT(*) FROM api_raw.ratios_historicos_quarter;

--Verificar integridad de claves primarias
--Asegúrate de que no existan duplicados de claves primarias que puedan indicar errores en ON CONFLICT.
--La consulta debe devolver cero resultados.

SELECT ticker, date, COUNT(*)
FROM api_raw.income_statement_anual
GROUP BY ticker, date
HAVING COUNT(*) > 1;

--Verificar consistencia de columnas clave
--Estos valores pueden tener algunos NULL por limitaciones de la API, pero no deberían ser la mayoría.

SELECT COUNT(*) FROM api_raw.income_statement_anual WHERE revenue IS NULL;
SELECT COUNT(*) FROM api_raw.ratios_historicos_anual WHERE returnOnEquity IS NULL;
SELECT COUNT(*) FROM api_raw.income_statement_quarter WHERE revenue IS NULL;
SELECT COUNT(*) FROM api_raw.ratios_historicos_quarter WHERE returnOnEquity IS NULL;

--Verificar active_tickers
--Confirma que api_raw.active_tickers contenga todos los tickers que deseas analizar.
SELECT COUNT(*) FROM api_raw.active_tickers;
SELECT * FROM api_raw.active_tickers LIMIT 10;
