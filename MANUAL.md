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
