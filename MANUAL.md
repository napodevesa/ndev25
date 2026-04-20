# MANUAL DE OPERACIÓN — SISTEMA DE INVERSIÓN CUANTITATIVO

Este documento describe cómo operar, verificar y mantener el sistema capa por capa.
Está escrito para ser leído antes de ejecutar cualquier pipeline.

---

## CAPA MACRO

### 1. Propósito

La capa Macro determina el **estado del ciclo económico** de la economía de Estados Unidos monitoreando 15 indicadores de la Fed (FRED). Su output es un único campo: `estado_macro` con valor `EXPANSION | SLOWDOWN | CONTRACTION | RECOVERY`, más un score de riesgo de 0 a 100.

Este estado actúa como **filtro superior de todo el sistema**: condiciona qué sectores se favorecen (capa 2), qué señales se generan para las empresas (capas 3–4) y qué estrategias de opciones se seleccionan (capa 5). Si el diagnóstico macro es incorrecto, los errores se propagan hacia abajo.

---

### 2. Arquitectura de datos

| Schema | Tabla / Vista | Tipo | Descripción | Filas actuales |
|--------|--------------|------|-------------|---------------|
| `macro` | `macro_series` | Tabla | Catálogo de series FRED: ID, nombre, unidad, frecuencia | 17 |
| `macro` | `macro_raw` | Tabla | Valores históricos descargados de FRED, con semáforo y nota por fila | 19 |
| `macro` | `macro_diagnostico` | Tabla | Snapshot del diagnóstico por cada corrida del pipeline | 1 |
| `macro` | `macro_notas_ai` | Tabla | Nota cualitativa generada por Claude por cada diagnóstico | 1 |
| `macro` | `v_macro_pivot` | Vista | Una fila con el valor más reciente de cada serie (transpuesta) | — |
| `macro` | `v_semaforo` | Vista | Aplica los umbrales CASE WHEN sobre v_macro_pivot → verde/amarillo/rojo por indicador | — |
| `macro` | `v_diagnostico` | Vista | Agrega los 12 semáforos en conteos (n_verdes, n_rojos) y calcula estado_macro, score_riesgo y confianza | — |

**Flujo interno de las vistas:**
```
macro_raw → v_macro_pivot → v_semaforo → v_diagnostico
```
Las tres vistas son en tiempo real: no almacenan datos. La tabla `macro_diagnostico` persiste un snapshot al final de cada corrida.

---

### 3. Indicadores monitoreados

Estado actual al 31/03/2026 (último dato en DB):

| Serie ID | Nombre | Frecuencia | Unidad | Último valor | Semáforo | Nota actual |
|----------|--------|-----------|--------|-------------|----------|-------------|
| `UNRATE` | Tasa de Desempleo | Mensual | % | 4.40 | 🟡 amarillo | Mercado laboral enfriando |
| `DFEDTARU` | Tasa Fed Funds (objetivo sup) | Diaria | % | 3.75 | 🟡 amarillo | Política restrictiva moderada |
| `IPC_ANUAL` | IPC Anual (%) | Mensual | % | 2.66 | 🟡 amarillo | Inflación elevada |
| `CORE_ANUAL` | IPC Subyacente Anual (%) | Mensual | % | 2.73 | — sin nota | Calculada: var. a/a de CPILFESL |
| `T10Y2Y` | Curva Rendimiento 10Y-2Y | Diaria | pp | 0.51 | 🟢 verde | Curva normal |
| `DGS2` | Bono 2Y | Diaria | % | 3.82 | 🟡 amarillo | Tasas cortas elevadas |
| `DGS10` | Bono 10Y | Diaria | % | 4.35 | 🟡 amarillo | Tasas largas presionando |
| `A191RL1Q225SBEA` | PIB Real (var. trimestral) | Trimestral | % | 0.70 | 🟡 amarillo | Crecimiento débil |
| `PAYEMS` | Nóminas no agrícolas (NFP) | Mensual | miles | 158.466 | 🟢 verde | Creación de empleo sana |
| `RSAFS` | Ventas minoristas | Mensual | M USD | 733.537 | 🟢 verde | Consumo sólido |
| `T5YIE` | Expectativas inflación 5Y | Diaria | % | 2.54 | 🟡 amarillo | Expectativas subiendo |
| `NFCI` | Cond. financieras (NFCI) | Semanal | índice | −0.475 | 🟢 verde | Condiciones laxas |
| `VIXCLS` | VIX (rezago 1 día) | Diaria | puntos | 30.61 | 🔴 rojo | Pánico / stress sistémico |
| `PERMIT` | Permisos de construcción | Mensual | miles | 1.386 | 🟡 amarillo | Construcción moderada |
| `TOTALSL` | Crédito al consumo | Mensual | M USD | 5.114.679 | 🟡 amarillo | Crédito elevado, posible estrés |

**Nota:** `CPIAUCSL` y `CPILFESL` se descargan como base para calcular `IPC_ANUAL` y `CORE_ANUAL` (variación a/a), pero no tienen semáforo propio.

**Umbrales de semáforo** (definidos en `vistas_macro.sql → v_semaforo`):

| Indicador | Verde | Amarillo | Rojo |
|-----------|-------|----------|------|
| Desempleo | ≤ 4.0% | ≤ 5.5% | > 5.5% |
| Fed Funds | ≤ 2.5% | ≤ 4.5% | > 4.5% |
| IPC anual | ≤ 2.5% | ≤ 4.0% | > 4.0% |
| Core anual | ≤ 2.5% | ≤ 4.0% | > 4.0% |
| Curva 10Y-2Y | ≥ 0.5 pp | ≥ −0.01 pp | < −0.01 pp |
| PIB trim | > 1.5% | > 0.0% | ≤ 0.0% |
| VIX | ≤ 20 | ≤ 30 | > 30 |
| Expectativas inf | ≤ 2.5% | ≤ 3.0% | > 3.0% |
| NFCI | ≤ 0.0 | ≤ 0.5 | > 0.5 |

---

### 4. Scripts

#### `macro_diagnostico_agente.ipynb` — Celda 0: Ingesta FRED

**Qué hace:** Descarga el valor más reciente de cada serie en `macro_series` vía la API de FRED. Para las series `IPC_ANUAL` y `CORE_ANUAL` calcula la variación interanual a partir de `CPIAUCSL` y `CPILFESL`. Inserta una fila por serie en `macro.macro_raw` con su semáforo y nota. Usa `ON CONFLICT (serie_id, fecha_dato) DO UPDATE` para actualizaciones idempotentes.

**Inputs:**
- Variables de entorno: `FRED_API_KEY`, `POSTGRES_*`
- Tabla fuente: `macro.macro_series` (lee los `serie_id` a descargar)
- API: `https://api.stlouisfed.org/fred/series/observations`

**Output:**
- Filas nuevas o actualizadas en `macro.macro_raw`
- Log en consola: una línea por serie con `[OK]` o `[ERROR]`

**Tiempo estimado:** 30–60 segundos (15 requests HTTP secuenciales, sin rate limit agresivo)

**Comando:**
```bash
cd /Users/ndev/Desktop/ndev25/macro
jupyter nbconvert --to notebook --execute macro_diagnostico_agente.ipynb \
  --output macro_diagnostico_agente_out.ipynb
```
O ejecutar celda a celda desde Jupyter si se necesita diagnóstico interactivo.

---

#### `macro_diagnostico_agente.ipynb` — Celda 1: Diagnóstico + Nota AI

**Qué hace:** Lee el diagnóstico calculado por las vistas SQL (`v_diagnostico`) y verifica si ya existe nota AI para ese diagnóstico en `macro_notas_ai`. Si no existe, construye un prompt con todos los indicadores y el estado calculado, llama a Claude (`claude-sonnet-4-20250514`), parsea el JSON de respuesta y lo inserta en `macro.macro_notas_ai`.

**Inputs:**
- Variables de entorno: `ANTHROPIC_API_KEY`, `POSTGRES_*`
- Tabla fuente: `macro.macro_diagnostico` (último registro sin nota AI)
- Modelo: `claude-sonnet-4-20250514`, `max_tokens` no especificado en código visto (default del SDK)

**Output:**
- Una fila nueva en `macro.macro_notas_ai` con:
  - `resumen`, `riesgos`, `outlook`
  - `score_sentimiento`, `score_recesion`, `score_inflacion` (escala 0–100)
  - `tokens_usados`, `prompt_version`

**Tiempo estimado:** 10–20 segundos (una llamada a Claude API)

**Nota importante:** La celda busca diagnósticos **sin nota** vía LEFT JOIN. Si ya existe nota para el diagnóstico del día, no llama a Claude (es idempotente).

**Comando:** mismo notebook, celda 1 o ejecución completa del notebook.

---

#### `macro_agente.ipynb` — Notebook de exploración

**Qué hace:** Notebook de análisis exploratorio y visualización. Contiene versiones previas del código de ingesta usadas durante el desarrollo. **No forma parte del pipeline de producción.** Útil para debug, visualización de series históricas y pruebas de nuevos indicadores.

**No ejecutar en producción.** Contiene API key hardcodeada (legacy) que fue reemplazada por variable de entorno.

---

#### `vistas_macro.sql`

**Qué hace:** Define las tres vistas (`v_macro_pivot`, `v_semaforo`, `v_diagnostico`) y la estructura de las tablas `macro_diagnostico` y `macro_notas_ai`. Solo se ejecuta una vez al crear el schema o cuando se modifican umbrales.

**Para cambiar un umbral:** editar los CASE WHEN en `v_semaforo` y ejecutar el CREATE OR REPLACE VIEW correspondiente.

---

### 5. Orden de ejecución

```
macro_diagnostico_agente.ipynb (Celda 0)
  → Descarga FRED → macro.macro_raw
      ↓
  Las vistas SQL calculan en tiempo real:
  macro_raw → v_macro_pivot → v_semaforo → v_diagnostico
      ↓
macro_diagnostico_agente.ipynb (Celda 1)
  → Lee v_diagnostico → llama Claude → macro.macro_notas_ai
```

**Importante:** Las vistas calculan automáticamente al ser consultadas. No hay paso de "ejecutar las vistas" — el pipeline Python solo necesita insertar en `macro_raw` y luego leer de `v_diagnostico`.

---

### 6. Queries de verificación

Ejecutar después de cada corrida para confirmar que todo funcionó:

**1. Verificar que macro_raw tiene datos frescos:**
```sql
SELECT serie_id, fecha_dato, semaforo, descargado_en
FROM macro.macro_raw
ORDER BY descargado_en DESC
LIMIT 5;
```
Esperado: `descargado_en` del día de hoy.

**2. Verificar el estado_macro actual:**
```sql
SELECT estado_macro, score_riesgo, confianza,
       n_verdes, n_amarillos, n_rojos,
       regla_disparada, calculado_en
FROM macro.macro_diagnostico
ORDER BY calculado_en DESC
LIMIT 1;
```
Esperado: fila con `calculado_en` del día de hoy.

**3. Verificar semáforos individuales calculados por las vistas:**
```sql
SELECT s_desempleo, s_fed, s_ipc, s_core,
       s_curva, s_pib, s_vix, s_expectativas, s_nfci,
       s_ventas, s_permisos, s_credito,
       n_verdes, n_amarillos, n_rojos
FROM macro.v_diagnostico;
```
Esperado: todos los campos con valor `verde`, `amarillo` o `rojo`. Si alguno es NULL, la serie correspondiente no tiene dato en `macro_raw`.

**4. Verificar que hay nota AI del día:**
```sql
SELECT estado_macro, score_sentimiento, score_recesion,
       score_inflacion, generado_en, tokens_usados
FROM macro.macro_notas_ai
ORDER BY generado_en DESC
LIMIT 1;
```
Esperado: `generado_en` de hoy. Si falta, el diagnóstico no tenía nota o Claude falló.

**5. Verificar cobertura de series (detecta series sin dato reciente):**
```sql
SELECT ms.serie_id, ms.nombre, ms.frecuencia,
       MAX(r.fecha_dato) AS ultimo_dato,
       CURRENT_DATE - MAX(r.fecha_dato) AS dias_de_atraso
FROM macro.macro_series ms
LEFT JOIN macro.macro_raw r ON ms.serie_id = r.serie_id
GROUP BY ms.serie_id, ms.nombre, ms.frecuencia
ORDER BY dias_de_atraso DESC NULLS FIRST;
```
Esperado: series diarias con ≤1 día de atraso, mensuales con ≤35 días, trimestrales con ≤95 días. `A191RL1Q225SBEA` (PIB) siempre tendrá atraso alto — es normal.

---

### 7. Errores comunes y soluciones

| Error | Síntoma | Causa | Solución |
|-------|---------|-------|----------|
| `429 Too Many Requests` | Log muestra error HTTP en alguna serie | Rate limit de FRED API (gratuito: ~120 req/min) | El script es secuencial — normalmente no lo alcanza. Si ocurre, agregar `time.sleep(1)` entre requests y reintentar. |
| `EnvironmentError: Faltan variables de entorno` | El notebook falla en la primera celda | `.env` no cargado o variables faltantes | Verificar que `.env` existe en el directorio de ejecución y contiene `FRED_API_KEY`, `POSTGRES_*`. |
| Serie con valor `'.'` en FRED | `NULL` en `macro_raw.valor` | FRED devuelve `'.'` para datos no disponibles | El código usa `pd.to_numeric(..., errors='coerce')`. La serie quedará con `valor = NULL`. El semáforo de esa serie será `NULL` y el indicador no contará ni como verde ni rojo. Verificar con query 5. |
| `CORE_ANUAL` / `IPC_ANUAL` sin semáforo | El semáforo aparece vacío en la DB | Estas series son calculadas internamente (no descargadas de FRED) y el código de inserción de semáforo puede no cubrirlas | Verificar que el pipeline calcula la variación a/a antes de insertar. Son series derivadas de `CPIAUCSL` y `CPILFESL`. |
| Nota AI no generada | `macro_notas_ai` sin fila del día | Claude API falló o `ANTHROPIC_API_KEY` inválida | Verificar key en `.env`. Verificar que `macro_diagnostico` tiene fila del día sin nota (LEFT JOIN en la query de la celda 1). Reejecutar solo la celda 1. |
| `v_diagnostico` devuelve NULL en estado_macro | `estado_macro` es NULL en la vista | `macro_raw` está vacío o las series clave (PIB, IPC) no tienen datos | Verificar query 5. Si `macro_raw` tiene 0 filas, reejecutar la celda 0 completa. |
| Diagnóstico duplicado | `macro_diagnostico` tiene 2 filas con el mismo `run_id` | El notebook se ejecutó dos veces el mismo día | No es crítico — la app siempre lee `ORDER BY calculado_en DESC LIMIT 1`. Para limpiar: `DELETE FROM macro.macro_diagnostico WHERE id NOT IN (SELECT MAX(id) FROM macro.macro_diagnostico GROUP BY run_id)`. |

---

### 8. Frecuencia de ejecución

| Script | Frecuencia | Momento | Justificación |
|--------|-----------|---------|---------------|
| `macro_diagnostico_agente.ipynb` (Celda 0 — ingesta) | **Semanal** | Lunes antes de abrir mercado (9:00 AM ET) | Los indicadores mensuales y trimestrales no cambian más seguido. Las series diarias (VIX, curva, Fed Funds) sí cambian, pero el diagnóstico macro se recalibra semanalmente. |
| `macro_diagnostico_agente.ipynb` (Celda 1 — nota AI) | **Semanal** | Inmediatamente después de la celda 0 | La nota AI se genera una vez por diagnóstico. Si el diagnóstico no cambió, no genera nota nueva (es idempotente). |
| Verificación manual (queries 1–5) | **Semanal** | Después de cada ejecución | Confirmar cobertura antes de correr el resto del pipeline. |

**No es necesario correr la capa macro más de una vez por semana** salvo que ocurra un evento macroeconómico extraordinario (decisión de emergencia de la Fed, dato de empleo muy disruptivo, etc.).

---

### 9. Output esperado

Cuando el pipeline corre correctamente, el log de la celda 0 muestra una línea por serie:

```
[20260331_1710] Iniciando ingesta FRED — 15 series
[OK] UNRATE          → 4.40%     (2026-02-01) semaforo=amarillo
[OK] DFEDTARU        → 3.75%     (2026-03-31) semaforo=amarillo
[OK] IPC_ANUAL       → 2.66%     (2026-02-01) semaforo=amarillo
[OK] CORE_ANUAL      → 2.73%     (2026-02-01)
[OK] T10Y2Y          → 0.5100pp  (2026-03-31) semaforo=verde
[OK] DGS2            → 3.82%     (2026-03-30) semaforo=amarillo
[OK] DGS10           → 4.35%     (2026-03-30) semaforo=amarillo
[OK] A191RL1Q225SBEA → 0.70%     (2025-10-01) semaforo=amarillo
[OK] PAYEMS          → 158466K   (2026-02-01) semaforo=verde
[OK] RSAFS           → 733537M   (2026-01-01) semaforo=verde
[OK] T5YIE           → 2.54%     (2026-03-31) semaforo=amarillo
[OK] NFCI            → -0.4750   (2026-03-20) semaforo=verde
[OK] VIXCLS          → 30.61     (2026-03-30) semaforo=rojo
[OK] PERMIT          → 1386K     (2026-01-01) semaforo=amarillo
[OK] TOTALSL         → 5114679M  (2026-01-01) semaforo=amarillo
[20260331_1710] Ingesta completa. 15/15 series OK.
```

La celda 1 (nota AI) muestra:

```
[20260331_1810] Diagnóstico pendiente encontrado: id=1, estado=SLOWDOWN
[20260331_1810] Llamando a Claude claude-sonnet-4-20250514...
[20260331_1810] Nota AI generada. Tokens usados: XXXX
[20260331_1810] Insertada en macro.macro_notas_ai id=1
```

**Estado actual de la DB** (último diagnóstico al 31/03/2026):

```
estado_macro : SLOWDOWN
score_riesgo : 42 / 100
confianza    : baja
n_verdes     : 3  (T10Y2Y, PAYEMS, RSAFS)
n_amarillos  : 8  (UNRATE, DFEDTARU, IPC_ANUAL, CORE_ANUAL, DGS2, DGS10, A191RL1Q225SBEA, T5YIE, PERMIT, TOTALSL)
n_rojos      : 1  (VIXCLS — VIX en 30.61)
regla        : "PIB amarillo o múltiples señales de deterioro"

Nota AI (sentimiento 35/100 | recesión 30/100 | inflación 45/100):
"La economía estadounidense muestra señales claras de desaceleración con
un crecimiento del PIB que se ha enfriado al 0.70% trimestral, mientras
el desempleo se mantiene en 4.40%, sugiriendo un mercado laboral que
comienza a aflojarse. El VIX elevado en 30.61 refleja alta volatilidad
e incertidumbre en los mercados, mientras que la inflación persiste
ligeramente por encima del objetivo de la Fed, limitando el margen de
maniobra de política monetaria."
```

---

## CAPA SECTOR

### 1. Propósito

La capa Sector clasifica y rankea **63 ETFs** (11 sectoriales SPDR, 49 de industria, 2 de refugio y 1 benchmark) según su fuerza relativa, momentum y volumen. Su output es un ranking de ETFs con estado (`LEADING_STRONG | LEADING_WEAK | NEUTRAL | LAGGING`) y un flag `alineacion_macro` (`ALIGNED | NEUTRAL`) que indica si el ETF favorece el régimen macro actual.

Esta capa responde a dos preguntas:
- ¿Qué sectores están liderando o rezagando al mercado?
- ¿Cuáles de esos líderes están alineados con el ciclo económico que identificó la capa Macro?

El output alimenta directamente la pantalla de Sectores en `app.py` y el contexto del Asistente.

---

### 2. Arquitectura de datos

| Schema | Tabla / Vista | Tipo | Descripción | Filas actuales |
|--------|--------------|------|-------------|---------------|
| `sector` | `sector_etfs` | Tabla | Catálogo de los 63 ETFs: ticker, nombre, tipo, sector_gics, industria, macro_regime | 63 |
| `sector` | `sector_raw` | Tabla | Precios OHLCV diarios históricos descargados de Polygon | — |
| `sector` | `sector_snapshot` | Tabla | Snapshot de indicadores técnicos calculados por corrida (run_id) | ~63 por run |
| `sector` | `sector_diagnostico_tecnico` | Tabla | Diagnóstico agregado por grupo DEFENSIVOS / CÍCLICOS / MIXTOS | 1 por corrida |
| `sector` | `sector_notas_ai` | Tabla | Nota cualitativa generada por Claude por cada corrida | 1 por corrida |
| `sector` | `v_sector_scores` | Vista | Calcula `score_momentum`, `score_volumen` y `score_total` desde el último snapshot | — |
| `sector` | `v_sector_ranking` | Vista | Agrega el estado determinístico y el ranking (global y dentro del sector) | — |
| `sector` | `v_sector_diagnostico` | Vista | Vista resumen de una sola fila: conteos, top picks, señal de rotación | — |

**Flujo interno:**
```
sector_raw → (cálculo en Python) → sector_snapshot
sector_snapshot → v_sector_scores → v_sector_ranking → v_sector_diagnostico
```

Las vistas son en tiempo real. `sector_snapshot` persiste un registro por ETF por corrida. La tabla `sector_diagnostico_tecnico` y `sector_notas_ai` almacenan el snapshot al final de cada ejecución.

---

### 3. Universo de ETFs

| Tipo | Cantidad | Ejemplos |
|------|----------|---------|
| `sector` | 11 | XLB, XLC, XLE, XLF, XLI, XLK, XLP, XLRE, XLU, XLV, XLY |
| `industria` | 49 | FAN, PBJ, ICLN, SOXX, XOP, IBB, UTG, XRT, IYR, … |
| `refugio` | 2 | GLD, TLT |
| `benchmark` | 1 | SPY |

El catálogo completo está en `sector.sector_etfs`. La columna `tipo` diferencia los grupos; la columna `macro_regime` indica en qué régimen macro se favorece ese ETF (ej.: `CONTRACTION` para GLD/TLT).

Para las vistas de ranking, los rankings globales incluyen todos los tipos. La vista `v_sector_diagnostico` filtra solo `tipo = 'industria'` para los top picks y los conteos de estado.

---

### 4. Indicadores calculados

Todos los indicadores se calculan en Python (script `sector_precios.py`) con datos OHLCV de Polygon y se guardan en `sector.sector_snapshot` una fila por ETF por corrida:

| Columna | Descripción |
|---------|-------------|
| `rs_vs_spy` | Ratio de fuerza relativa: `close_etf / close_spy` |
| `rsi_rs_diario` | RSI(14) calculado sobre la serie `rs_vs_spy` diaria |
| `rsi_rs_semanal` | RSI(14) sobre la serie `rs_vs_spy` semanal |
| `slope_rs` | Pendiente lineal (regresión) de los últimos 60 días de `rs_vs_spy` |
| `rs_percentil` | Percentil actual de `rs_vs_spy` en la ventana de 52 semanas (0–100) |
| `ret_1m` | Retorno en 1 mes (%) |
| `ret_3m` | Retorno en 3 meses (%) |
| `ret_6m` | Retorno en 6 meses (%) |
| `ret_1a` | Retorno en 1 año (%) |
| `dist_max_52w` | Distancia porcentual desde el máximo de 52 semanas (negativo = caída) |
| `vol_ratio` | Ratio de volumen actual vs. media de 20 días |
| `obv_slope` | Pendiente del OBV (On-Balance Volume) — positivo / cero / negativo |
| `rsi_precio` | RSI(14) del precio de cierre del ETF |
| `estado_macro` | Estado macro heredado de `macro.macro_diagnostico` al momento del run |
| `alineacion_macro` | `ALIGNED` si el ETF favorece el régimen actual; `NEUTRAL` en caso contrario |

---

### 5. Motor de scoring

El scoring se calcula en la vista `sector.v_sector_scores` (archivo `sector/vistas_v_sector_scores.sql`). Son tres scores de 0 a 100, todos redondeados a 1 decimal:

**score_momentum** — fuerza relativa y retornos:
```sql
COALESCE(rsi_rs_semanal, 50) * 0.40
+ LEAST(100, GREATEST(0, COALESCE(ret_3m, 0) * 2 + 50)) * 0.35
+ COALESCE(rs_percentil, 50) * 0.25
```

**score_volumen** — confirmación de volumen y dirección OBV:
```sql
LEAST(100, GREATEST(0, (COALESCE(vol_ratio, 1.0) - 0.5) * 100)) * 0.60
+ CASE
    WHEN obv_slope > 0 THEN 75
    WHEN obv_slope = 0 THEN 50
    ELSE 25
  END * 0.40
```

**score_total** — ponderación final:
```sql
score_momentum * 0.65 + score_volumen * 0.35
```

**Clasificación de estado** (vista `v_sector_ranking`):

| Estado | Condición |
|--------|-----------|
| `LEADING_STRONG` | `score_total >= 65` AND `score_volumen >= 60` |
| `LEADING_WEAK` | `score_momentum >= 60` AND `score_volumen < 60` |
| `LAGGING` | `score_total < 35` |
| `NEUTRAL` | el resto |

**Señal de rotación** (vista `v_sector_diagnostico`):

| Señal | Condición |
|-------|-----------|
| `ROTACION_CLARA` | `n_leading_strong >= 5` |
| `ROTACION_MODERADA` | `n_leading_strong >= 3` |
| `MERCADO_DEBIL` | `n_lagging >= 30` |
| `SIN_TENDENCIA` | el resto |

---

### 6. Diagnóstico técnico sectorial

La tabla `sector.sector_diagnostico_tecnico` (generada por `sector_diagnostico_tecnico.py.ipynb`) clasifica los ETFs sectoriales en tres grupos y calcula un score promedio por grupo:

| Grupo | ETFs incluidos |
|-------|---------------|
| `DEFENSIVOS` | XLV, XLP, XLU, GLD, TLT |
| `CICLICOS` | XLK, XLY, XLF, XLI, XLE |
| `MIXTOS` | XLB, XLRE, XLC |

Las columnas son `score_defensivos`, `score_ciclicos`, `score_mixtos` (promedios de `score_total` por grupo), `top_3_lideres`, `top_3_rezagados`, y `diagnostico_sector` con valores:

| diagnostico_sector | Condición |
|-------------------|-----------|
| `CONFIRMA_EXPANSION` | Cíclicos lideran sobre defensivos |
| `CONFIRMA_SLOWDOWN` | Defensivos lideran sobre cíclicos |
| `CONFIRMA_CONTRACTION` | Defensivos muy altos, cíclicos muy bajos |
| `SEÑAL_MIXTA` | Sin diferencia clara entre grupos |

La columna `coherencia` (`ALTA | MEDIA | BAJA`) indica qué tan consistente es la señal con el `estado_macro` de la capa Macro.

---

### 7. Scripts

El pipeline de la capa Sector tiene tres notebooks en `sector/`:

#### `ingesta.ipynb` (sector_precios)
- **Fuente**: Polygon API (endpoint `/v2/aggs/ticker/{ticker}/range/1/day/`)
- **Rate limit**: 5 segundos de espera entre requests (12 calls/min, plan Starter)
- **Ventana histórica**: `DIAS_HIST = 730` días (≈2 años)
- **Proceso**: descarga OHLCV → calcula todos los indicadores de `sector_snapshot` → inserta con `ON CONFLICT (ticker, fecha) DO UPDATE` en `sector_raw` → calcula snapshot → inserta en `sector_snapshot`
- **Loguea en**: `output_ingest/` con `run_id` formato `YYYYMMDD_HHMM`

#### `sector_diagnostico_tecnico.py.ipynb`
- **Sin llamadas a APIs externas** — trabaja solo con la DB
- Lee `sector.sector_snapshot` del último `run_id`
- Clasifica ETFs en DEFENSIVOS / CÍCLICOS / MIXTOS
- Calcula scores por grupo y determina `diagnostico_sector` y `coherencia`
- Inserta en `sector.sector_diagnostico_tecnico`

#### `sector_ai.ipynb`
- **Lee**: `sector.v_sector_diagnostico` (una fila resumen) + top 10 industrias de `sector.v_sector_ranking`
- **Llama a**: Claude API (`claude-sonnet-4-20250514`, max_tokens=1000)
- **Genera**: nota cualitativa sobre rotación sectorial, alineación con macro, riesgos
- **Inserta en**: `sector.sector_notas_ai`

---

### 8. Orden de ejecución

```
1. ingesta.ipynb            → descarga precios + calcula sector_snapshot
2. sector_diagnostico_tecnico.py.ipynb  → diagnóstico por grupos
3. sector_ai.ipynb          → nota cualitativa Claude
```

Debe ejecutarse **después** de que la capa Macro esté actualizada, porque `sector_snapshot` hereda `estado_macro` y `alineacion_macro` de `macro.macro_diagnostico`.

---

### 9. Queries de verificación

```sql
-- 1. Top 10 industrias con scores y estado
SELECT ticker, industria, sector_gics, estado, alineacion_macro,
       score_momentum, score_volumen, score_total, rank_total,
       ret_3m, rsi_rs_semanal
FROM sector.v_sector_ranking
WHERE tipo = 'industria'
ORDER BY rank_total
LIMIT 10;

-- 2. Diagnóstico resumen del universo (una fila)
SELECT * FROM sector.v_sector_diagnostico;

-- 3. Solo LEADING_STRONG alineados con macro
SELECT ticker, industria, score_total, ret_3m, ret_6m, alineacion_macro
FROM sector.v_sector_ranking
WHERE estado = 'LEADING_STRONG'
  AND alineacion_macro = 'ALIGNED'
ORDER BY score_total DESC;

-- 4. Top industria de cada sector padre
SELECT DISTINCT ON (sector_etf)
       sector_etf, ticker, industria, score_total, estado
FROM sector.v_sector_ranking
WHERE tipo = 'industria'
ORDER BY sector_etf, score_total DESC;

-- 5. Diagnóstico técnico por grupos (último)
SELECT fecha, score_defensivos, score_ciclicos, score_mixtos,
       top_3_lideres, top_3_rezagados, diagnostico_sector, coherencia
FROM sector.sector_diagnostico_tecnico
ORDER BY fecha DESC LIMIT 1;

-- 6. Confirmar que el último snapshot tiene filas
SELECT run_id, COUNT(*) AS n_etfs, MAX(calculado_en) AS calculado_en
FROM sector.sector_snapshot
GROUP BY run_id
ORDER BY run_id DESC LIMIT 3;
```

---

### 10. Errores comunes

| Error | Causa probable | Solución |
|-------|---------------|---------|
| `sector_snapshot` vacío | `ingesta.ipynb` no se ejecutó | Ejecutar ingesta primero; verificar query 6 |
| `alineacion_macro = NULL` en snapshot | `macro.macro_diagnostico` sin datos | Ejecutar pipeline Macro completo antes |
| Polygon rate limit error 429 | Muchos requests en poco tiempo | El script tiene `sleep(5)` por ticker; verificar que no se ejecutó dos veces en paralelo |
| `v_sector_diagnostico` sin top picks | No hay ETFs `ALIGNED + LEADING` | Normal si el mercado está débil; revisar `n_aligned` |
| `sector_diagnostico_tecnico` sin filas | Notebook 2 no se ejecutó | Ejecutar `sector_diagnostico_tecnico.py.ipynb` después de la ingesta |
| `sector_notas_ai` sin nota nueva | Notebook 3 no se ejecutó o falló la API | Revisar logs y variable `ANTHROPIC_API_KEY` en `.env` |

---

### 11. Frecuencia y dependencias

| Ejecución | Frecuencia | Dependencia previa |
|-----------|-----------|-------------------|
| `ingesta.ipynb` | Semanal (lunes post-apertura) | `macro.macro_diagnostico` actualizado |
| `sector_diagnostico_tecnico.py.ipynb` | Semanal (inmediatamente después) | `sector.sector_snapshot` del run actual |
| `sector_ai.ipynb` | Semanal (al final) | `sector_diagnostico_tecnico` + `v_sector_diagnostico` |

**No hay dependencia de ninguna capa inferior** (Micro, Agente). La capa Sector puede ejecutarse de forma independiente siempre que Macro esté actualizado.

---

### 12. Output esperado

Estado actual al 05/04/2026 (`run_id = 20260405_2114`):

```
run_id           : 20260405_2114
estado_macro     : SLOWDOWN
señal_rotacion   : SIN_TENDENCIA

Universo (49 industrias):
  n_leading_strong : 2
  n_leading_weak   : 22
  n_neutral        : 13
  n_lagging        : 10
  n_aligned        : 4   (LEADING alineados con SLOWDOWN)
  score_universo   : 48.8 / 100

Top picks alineados con macro : PBJ, UTG, IBB
Top picks globales            : FAN, PBJ, ICLN

Top 5 industrias por score_total:
  1. FAN   — LEADING_WEAK   / NEUTRAL  — score 77.8
  2. PBJ   — LEADING_STRONG / ALIGNED  — score 77.2
  3. ICLN  — LEADING_STRONG / NEUTRAL  — score 73.0
  4. SOXX  — LEADING_WEAK   / NEUTRAL  — score 68.5
  5. XOP   — LEADING_WEAK   / NEUTRAL  — score 67.0

Diagnóstico técnico sectorial (10/04/2026):
  score_defensivos : 60.30
  score_ciclicos   : 50.44
  score_mixtos     : 62.23
  top_3_lideres    : XLE, XLI, XLRE
  top_3_rezagados  : XLK, XLF, XLY
  diagnostico      : SEÑAL_MIXTA
  coherencia       : MEDIA
```

---

## CAPA MICRO

### 1. Propósito

La capa MICRO es el núcleo cuantitativo del sistema. Toma un universo de ~3.027 empresas USA, calcula scores de calidad y valor para cada una, filtra las ~748 con balances sanos, las enriquece con indicadores técnicos y regresiones históricas, y finalmente aplica un motor determinístico que genera señales de inversión concretas (instrumento, sizing, convicción).

**Pregunta que responde:** "¿Cuáles son las mejores empresas dado el contexto macro y sectorial actual?"

**Conexión con capas anteriores:**
- Consume `estado_macro` de MACRO → ajusta el `macro_factor` (0.70–1.10) y define si se prefieren instrumentos de income (SLOWDOWN/CONTRACTION) o equity (EXPANSION/RECOVERY).
- Consume `sector_alineado` de SECTOR → penaliza/bonifica en el score de convicción (+5 puntos si ALIGNED).

---

### 2. Arquitectura de datos — 3 schemas

| Schema | Tabla | Descripción | Filas actuales |
|--------|-------|-------------|---------------|
| `ingest` | `ratios_ttm` | Ratios TTM de FMP (22 métricas por empresa) | 3.027 |
| `ingest` | `keymetrics` | Key metrics TTM de FMP (15 métricas por empresa) | 3.021 |
| `ingest` | `precios` | Precios EOD ajustados por dividendos, 2 años historia | 1.457.911 |
| `ingest` | `scores_salud` | Altman Z-score y Piotroski F-score desde FMP | 744 |
| `ingest` | `keymetrics_hist` | Histórico anual de key metrics (hasta 5 FY por empresa) | 3.718 |
| `seleccion` | `scores` | Scores Quality + Value + Multifactor para todo el universo | 3.021 |
| `seleccion` | `universo` | Empresas que pasan el filtro absoluto de calidad | 748 |
| `seleccion` | `enriquecimiento` | Universo enriquecido: técnicos + salud + regresiones + macro | 748 |
| `agente` | `decision` | Motor determinístico: señales activas + no_trade | 206 activas |
| `agente` | `top` | Top 25 por score_conviccion | 25 |
| `agente` | `notas_ai` | Nota cualitativa generada por Claude | 1 por snapshot |

---

### 3. Universo de empresas

- **Fuente:** `universos.stock_opciones_2026` — 3.027 empresas USA comunes, sin microcaps
- **Criterio de inclusión:** acciones con opciones listadas en mercados USA (NYSE, NASDAQ, AMEX), excluye ETFs, REITs, ADRs y empresas con market cap < ~$300M
- **Ingesta:** todos los scripts de `micro/ingest/` leen de esta tabla para obtener la lista de tickers

---

### 4. Pipeline completo

```
universos.stock_opciones_2026  (3.027 tickers)
         │
         ├── ingest_ratios_ttm.py   ──→  ingest.ratios_ttm    (22 métricas TTM)
         ├── ingest_keymetrics.py   ──→  ingest.keymetrics     (15 métricas TTM)
         └── ingest_precios.py      ──→  ingest.precios        (EOD incremental)
                  │
                  ▼
         calcular_scores.py ─────────→  seleccion.scores       (3.021 empresas)
                  │
                  ▼
         aplicar_filtro.py ──────────→  seleccion.universo     (748 empresas)
                  │
         ┌────────┴────────────────────────────────────────────┐
         │ ingest_scores.py       → ingest.scores_salud         │
         │ ingest_keymetrics_hist.py → ingest.keymetrics_hist   │  (datos extra)
         └────────┬────────────────────────────────────────────┘
                  ▼
         enriquecer.py ─────────────→  seleccion.enriquecimiento (748 empresas)
                  │
                  ▼
         agente_decision.py ─────────→  agente.decision         (206 activas)
                                   └──→  agente.top             (25 top)
                  │
                  ▼
         micro_ai.py ───────────────→  agente.notas_ai          (nota Claude)
```

---

### 5. Factores del multifactor

El score final combina Quality 60% + Value 40%. Todos los factores se calculan como percentiles dentro del universo (0–100), con clip en p1/p99 para outliers.

| Dimensión | Factor | Métrica fuente | Peso | Por qué |
|-----------|--------|---------------|------|---------|
| **QUALITY** | ROIC | `keymetrics.roic` | 25% | Retorno sobre capital — indicador central de ventaja competitiva |
| QUALITY | Margen operativo | `ratios_ttm.operating_profit_margin` | 20% | Eficiencia operativa del negocio |
| QUALITY | Calidad de FCF | `ratios_ttm.free_cash_flow_operating_cash_flow_ratio` | 20% | Conversión real de EBIT en caja; penaliza accruals |
| QUALITY | Cobertura intereses | `ratios_ttm.interest_coverage_ratio` | 20% | Solidez frente a deuda financiera |
| QUALITY | Income quality | `keymetrics.income_quality` | 15% | FCF vs. net income — detecta earnings de baja calidad |
| **VALUE** | P/FCF | `ratios_ttm.price_to_free_cash_flow_ratio` | 35% | Valuación sobre flujo real, no contable |
| VALUE | EV/EBITDA | `keymetrics.ev_to_ebitda` | 30% | Múltiplo más robusto que P/E — neutro a estructura de capital |
| VALUE | P/E | `ratios_ttm.price_to_earnings_ratio` | 20% | Valuación estándar; negativos se neutralizan (→ 0) |
| VALUE | P/B | `ratios_ttm.price_to_book_ratio` | 15% | Complemento de valuación relativa a activos |

Los factores VALUE usan `percentil_menor_mejor()` — ratio más bajo = percentil más alto. P/E y P/FCF negativos se convierten en 0 (una empresa con pérdidas no es "barata").

---

### 6. Filtro absoluto

De 3.021 empresas con scores calculados, pasan al universo de trabajo aquellas que cumplen **los 4 criterios simultáneamente:**

| Criterio | Umbral | Lógica |
|----------|--------|--------|
| ROIC | > 4% | La empresa genera retorno real sobre capital — excluye negocios que destruyen valor |
| Net Debt / EBITDA | < 3.0 | Deuda manejable — puede servir obligaciones sin comprometer operaciones |
| Debt / Equity | < 0.8 | Balance sólido — evita empresas altamente apalancadas |
| FCF por acción | > 0 | Genera caja real — no solo utilidades contables |

**Resultado actual:** 748 empresas pasan el filtro (de 3.021 con datos completos).

Distribución por market cap tier:

| Tier | Empresas |
|------|----------|
| Mid | 258 |
| Large | 248 |
| Small | 191 |
| Micro | 51 |

Top 5 sectores en el universo filtrado:

| Sector | Empresas |
|--------|----------|
| Financial Services | 150 |
| Industrials | 146 |
| Technology | 128 |
| Healthcare | 84 |
| Consumer Cyclical | 76 |

---

### 7. Enriquecimiento

Para cada una de las 748 empresas del universo, `enriquecer.py` calcula y guarda en `seleccion.enriquecimiento`:

**Técnicos** (calculados desde `ingest.precios`, mínimo 210 días de historia):

| Campo | Descripción |
|-------|-------------|
| `rsi_14_semanal` | RSI 14 períodos sobre precios semanales |
| `precio_vs_ma200` | Distancia porcentual al precio de la MA200 |
| `dist_max_52w` | Distancia al máximo de 52 semanas |
| `volatilidad_30d` / `volatilidad_90d` | Desviación estándar de retornos diarios |
| `volume_ratio_20d` | Volumen promedio 20d / volumen promedio 90d |
| `obv_slope` | Pendiente de OBV (On-Balance Volume) — confirma dirección de volumen |
| `momentum_3m` / `momentum_6m` / `momentum_12m` | Retorno total en períodos de 63 / 126 / 252 ruedas |

**Salud financiera** (desde FMP `stable/financial-scores`):

| Campo | Descripción | Clasificación |
|-------|-------------|--------------|
| `altman_z_score` | Score de probabilidad de distress financiero | safe > 2.99, grey 1.81–2.99, distress < 1.81 |
| `altman_zona` | Categoría textual | safe / grey / distress |
| `piotroski_score` | Score 0–9 de solidez fundamental (F-Score) | fuerte ≥ 7, neutral 4–6, debil ≤ 3 |
| `piotroski_categoria` | Categoría textual | fuerte / neutral / debil |

**Regresiones anuales** (desde `ingest.keymetrics_hist` — scipy.stats.linregress):

| Campo | Descripción |
|-------|-------------|
| `roic_tendencia` | Pendiente de la regresión lineal de ROIC en los últimos FYs |
| `roic_signo` | 1 = mejorando, -1 = deteriorando, 0 = plano |
| `roic_r2` | R² de la regresión (confiabilidad estadística) |
| `roic_confiable` | True si n ≥ 3 observaciones y R² ≥ 0.50 |
| `deuda_tendencia` | Pendiente de la regresión de Net Debt/EBITDA |
| `deuda_signo` | -1 = bajando (positivo), 1 = subiendo (negativo) |
| `deuda_r2` / `deuda_confiable` | Igual que ROIC |

**Conexión macro/sector:**

| Campo | Origen |
|-------|--------|
| `estado_macro` | `macro.macro_diagnostico` — estado vigente del ciclo |
| `sector_etf` | Mapeado desde sector GICS (ej: Technology → XLK) |
| `sector_alineado` | ALIGNED si el ETF sectorial está en estado LEADING / RECOVERY_STRONG en `sector.v_sector_ranking` |

---

### 8. Motor determinístico — agente.decision

El motor es un único `INSERT ... WITH ... SELECT` con 8 CTEs encadenados. Sin reglas hardcodeadas en Python — toda la lógica vive en SQL.

**CTE 1 — tendencia:** Clasifica la trayectoria fundamental de la empresa comparando `roic_signo`, `roic_confiable`, `deuda_signo` y `deuda_confiable`:
- `mejora_estructural` → ROIC ↑ confiable + deuda ↓ confiable
- `mejora_parcial` → ROIC ↑ + deuda ↓ (sin confirmar estadísticamente)
- `deterioro` → ROIC ↓ confiable
- `sin_tendencia` → resto

**CTE 2 — contexto:** Clasifica la calidad estructural del negocio:
- `structural_quality` → quality ≥ 70 + value ≥ 40 + Altman ≥ 2.99 + Piotroski ≥ 6 + ROIC ↑
- `solid_but_expensive` → quality ≥ 70 + Altman ≥ 2.99 (pero sin value atractivo)
- `improving` → quality ≥ 50 + ROIC ↑ + deuda ↓
- `structural_risk` → Altman < 1.81 OR Piotroski ≤ 3 OR ROIC ↓ confiable → excluye de señales

**CTE 3 — timing:** Clasifica la entrada técnica:
- `good_entry` → RSI 40–65 + sobre MA200 + volumen normal
- `pullback_in_uptrend` → RSI < 40 + sobre MA200 (caída temporal en tendencia alcista)
- `overbought` → RSI > 75
- `below_ma200` → precio > 5% por debajo de MA200 → excluye de señales
- `neutral` → resto

**CTE 4 — macro_ajuste:** Multiplica el sizing por un factor según estado macro:
- EXPANSION → 1.10 | RECOVERY → 1.05 | SLOWDOWN → 0.85 | CONTRACTION → 0.70
- En SLOWDOWN/CONTRACTION activa `preferir_income = TRUE`

**CTE 5 — instrumentacion:** Decide el instrumento:
- `none` si contexto = `structural_risk` o timing = `below_ma200`
- `cash_secured_put` si `preferir_income` (SLOWDOWN/CONTRACTION)
- `stock` si contexto = `structural_quality` + timing = `good_entry` o `pullback_in_uptrend` + OBV slope > 0
- `cash_secured_put` si contexto `structural_quality` + `overbought`, o contextos `solid_but_expensive` / `improving`

**CTE 6 — sizing:** Position size 0.0–1.0, producto de quality/value score × bonos Piotroski × bono tendencia × bono timing × macro_factor.

**CTE 7 — conviccion:** Score 0–100 combinando quality (35%) + value (15%) + bonus timing (10–30) + bonus Piotroski (5–15) + bonus tendencia (0–10) + bonus sector alineado (+5).

**CTE 8 — final / flag_timing:** Rank global por score_conviccion + clasificación del flag de timing:
- `tecnico_confirmado` → good_entry + OBV > 0
- `pullback_comprable` → pullback_in_uptrend
- `esperar_pullback` → overbought
- `macro_defensivo` → preferir_income activo
- `fundamental_only` → resto

---

### 9. Scripts — orden de ejecución

#### MENSUAL (primer lunes del mes, post earnings)

| Script | Qué hace | Tiempo est. | Comando |
|--------|----------|-------------|---------|
| `ingest_ratios_ttm.py` | Descarga 22 ratios TTM de FMP para 3.027 tickers | ~12 min | `python micro/ingest/ingest_ratios_ttm.py` |
| `ingest_keymetrics.py` | Descarga 15 key metrics TTM de FMP | ~12 min | `python micro/ingest/ingest_keymetrics.py` |
| `ingest_precios.py` | Actualiza EOD incremental (solo días nuevos) | ~2 min | `python micro/ingest/ingest_precios.py` |
| `calcular_scores.py` | Calcula Quality + Value + Multifactor para todo el universo | < 1 min | `python micro/seleccion/calcular_scores.py` |
| `aplicar_filtro.py` | Aplica 4 filtros absolutos → seleccion.universo | < 1 min | `python micro/seleccion/aplicar_filtro.py` |
| `ingest_scores.py` | Descarga Altman Z + Piotroski para las 748 empresas | ~3 min | `python micro/seleccion/ingest_scores.py` |
| `enriquecer.py` | Calcula técnicos + regresiones + conecta macro/sector | ~5 min | `python micro/seleccion/enriquecer.py` |
| `agente_decision.py` | Ejecuta motor determinístico → agente.decision + agente.top | < 1 min | `python micro/agente/agente_decision.py` |
| `micro_ai.py` | Genera nota cualitativa con Claude | < 1 min | `python micro/agente/micro_ai.py` |

**Total estimado:** ~36 minutos. Los dos primeros scripts son los más lentos (API rate limit de FMP con sleep 0.25s entre requests).

#### ANUAL (primer lunes de enero)

| Script | Qué hace | Tiempo est. | Comando |
|--------|----------|-------------|---------|
| `ingest_keymetrics_hist.py` | Descarga histórico anual de ROIC/deuda para empresas del universo que no tienen datos del FY anterior | ~3 min (incremental) | `python micro/seleccion/ingest_keymetrics_hist.py` |

La lógica es incremental: solo procesa tickers sin datos para el año fiscal anterior (LEFT JOIN por año). Si el universo no cambió y ya hay datos del FY previo, el script sale en segundos.

---

### 10. Queries de verificación

```sql
-- 1. ¿Cuántas empresas en ingest.ratios_ttm del último run?
SELECT fecha_consulta, COUNT(*) AS n
FROM ingest.ratios_ttm
GROUP BY fecha_consulta
ORDER BY fecha_consulta DESC
LIMIT 3;
-- Esperado: ~3.027 para la fecha más reciente

-- 2. ¿El score promedio del universo es ~50 (por construcción del percentil)?
SELECT ROUND(AVG(quality_score),1) AS avg_quality,
       ROUND(AVG(value_score),1)   AS avg_value,
       ROUND(AVG(multifactor_score),1) AS avg_multi
FROM seleccion.scores
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM seleccion.scores);
-- Esperado: quality ~50, value ~50, multi ~50

-- 3. ¿Cuántas empresas pasaron el filtro absoluto?
SELECT snapshot_date, COUNT(*) AS n_universo
FROM seleccion.universo
GROUP BY snapshot_date
ORDER BY snapshot_date DESC
LIMIT 1;
-- Actual: 748

-- 4. ¿El enriquecimiento tiene RSI y MA200 calculados?
SELECT COUNT(*) AS total,
       COUNT(rsi_14_semanal) AS con_rsi,
       COUNT(precio_vs_ma200) AS con_ma200,
       COUNT(altman_z_score) AS con_altman
FROM seleccion.enriquecimiento
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM seleccion.enriquecimiento);
-- Esperado: total=748, con valores altos en los tres

-- 5. ¿Cuántas señales activas generó el agente y en qué instrumento?
SELECT instrumento, COUNT(*) AS n
FROM agente.decision
WHERE trade_status = 'active'
  AND snapshot_date = (SELECT MAX(snapshot_date) FROM agente.decision)
GROUP BY instrumento;
-- Actual: cash_secured_put=206, stock=0

-- 6. ¿Distribución por flag_timing?
SELECT flag_timing, COUNT(*) AS n
FROM agente.decision
WHERE trade_status = 'active'
  AND snapshot_date = (SELECT MAX(snapshot_date) FROM agente.decision)
GROUP BY flag_timing ORDER BY n DESC;
-- Actual: macro_defensivo=123, tecnico_confirmado=53, esperar_pullback=25, pullback_comprable=5

-- 7. ¿La nota AI se generó para este snapshot?
SELECT snapshot_date, score_conviction, tokens_usados
FROM agente.notas_ai
ORDER BY generado_en DESC LIMIT 1;
-- Actual: 2026-04-01 | score_conviction=72

-- 8. ¿Cuál es el top 3 actual por score_conviccion?
SELECT rank_conviccion, ticker, score_conviccion, instrumento, flag_timing, sector_alineado
FROM agente.top
ORDER BY rank_conviccion
LIMIT 3;
-- Actual: #1 MO (97.1), #2 HALO (93.6), #3 PTCT (88.7) — todos CSP

-- 9. ¿Hay empresas en distress en el universo?
SELECT altman_zona, COUNT(*) AS n
FROM seleccion.enriquecimiento
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM seleccion.enriquecimiento)
GROUP BY altman_zona;

-- 10. ¿Cuántos tickers del universo tienen regresión ROIC confiable?
SELECT roic_signo, roic_confiable, COUNT(*) AS n
FROM seleccion.enriquecimiento
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM seleccion.enriquecimiento)
GROUP BY roic_signo, roic_confiable
ORDER BY roic_signo, roic_confiable;
```

---

### 11. Errores comunes

| Error | Causa | Solución |
|-------|-------|----------|
| `numeric field overflow` | Columna NUMERIC(10,6) recibe valor > 9999 (ej: P/E extremo) | Usar NUMERIC(16,4) o NUMERIC(20,2) según si es ratio o monto |
| `can't adapt type numpy.bool_` | psycopg2 no acepta tipos numpy nativos | Usar `bool()`, `int()`, `float()` explícitos; o función `safe(val)` |
| `column X of table Y specified more than once` | INSERT con CTEs referencia la misma columna dos veces | Revisar alias en el SELECT final del CTE |
| Fechas distintas entre tablas | JOIN entre ratios_ttm y keymetrics con misma `fecha_consulta` que pueden no coincidir | Cada tabla usa su propia subconsulta `MAX(fecha_consulta)` independiente |
| `column reference X is ambiguous` en CTEs | El nombre de columna existe en la tabla base y en un CTE con ese nombre | Usar alias explícito en el SELECT del CTE base: `m.estado_macro AS estado_macro_actual` |
| `ingest.scores_salud` vacía | Se ejecutó antes que `aplicar_filtro.py` | El orden es obligatorio: scores_salud lee de `seleccion.universo` |
| Señales 0 en agente.decision | `seleccion.enriquecimiento` vacío o sin RSI calculado | Verificar que enriquecer.py completó exitosamente (MIN_DIAS = 210 puntos de precios) |

---

### 12. Frecuencia y dependencias

```
[MACRO]          macro_fred.py → macro_ai.py
    │
    │  (estado_macro → enriquecer.py, agente_decision.py)
    ▼
[SECTOR]         sector_precios.py → sector_ai.py
    │
    │  (sector_alineado → enriquecer.py, agente_decision.py)
    ▼
[MICRO INGEST]   ingest_ratios_ttm.py  (mensual)
                 ingest_keymetrics.py  (mensual)
                 ingest_precios.py     (semanal — incremental)
    │
    ▼
[MICRO SELECCIÓN] calcular_scores.py → aplicar_filtro.py
    │
    ├── ingest_scores.py           (mensual, requiere universo)
    ├── ingest_keymetrics_hist.py  (anual, enero)
    │
    ▼
[MICRO ENRICH]   enriquecer.py
                 (requiere: MACRO + SECTOR + ingest.scores_salud + ingest.keymetrics_hist)
    │
    ▼
[AGENTE]         agente_decision.py → micro_ai.py
```

**Dependencias críticas:**
- MACRO y SECTOR deben correr antes de `enriquecer.py` — si no hay datos macro recientes, el script igual corre pero `estado_macro` queda desactualizado.
- `ingest_scores.py` y `enriquecer.py` requieren que `aplicar_filtro.py` ya haya corrido (leen de `seleccion.universo`).
- `ingest_keymetrics_hist.py` es anual: si ya existen datos del FY anterior para todos los tickers, el script termina inmediatamente sin llamadas a la API.
- `micro_ai.py` requiere que `agente_decision.py` haya corrido (lee `MAX(snapshot_date)` de `agente.decision`). Es idempotente: si ya existe nota para ese snapshot_date, sale sin llamar a Claude.

---

### 13. Output esperado — último run

**Snapshot:** 2026-04-01 | **Estado macro:** SLOWDOWN

**Señales activas:** 206 (todas `cash_secured_put` — SLOWDOWN activa `preferir_income`)

| Instrumento | Contexto | Señales |
|-------------|----------|---------|
| cash_secured_put | improving | 114 |
| cash_secured_put | solid_but_expensive | 76 |
| cash_secured_put | structural_quality | 16 |

| Flag timing | Señales |
|-------------|---------|
| macro_defensivo | 123 |
| tecnico_confirmado | 53 |
| esperar_pullback | 25 |
| pullback_comprable | 5 |

**Top 3 por score_conviccion:**

| Rank | Ticker | Score | Instrumento | Flag | Sector alineado |
|------|--------|-------|-------------|------|----------------|
| #1 | MO | 97.1 | cash_secured_put | macro_defensivo | ALIGNED |
| #2 | HALO | 93.6 | cash_secured_put | tecnico_confirmado | ALIGNED |
| #3 | PTCT | 88.7 | cash_secured_put | tecnico_confirmado | ALIGNED |

**Nota AI (agente.notas_ai):** score_conviction = 72 / 100

```
