# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Sistema de inversión cuantitativo multifactor en Python + PostgreSQL. 5 capas encadenadas que van desde análisis macro hasta estrategias de opciones por empresa.

## Database

- **PostgreSQL** en `localhost:5433`, base de datos `ndev25`
- Credenciales y API keys en `.env` (ver `.env.example` como referencia)
- 12 schemas: `macro`, `sector`, `api_raw`, `limpieza`, `procesados`, `modelados`, `multifactor`, `seleccion`, `agente`, `agente_opciones`, `universos`, `infraestructura`

No hay comandos de build/test/lint tradicionales — el sistema se ejecuta notebook a notebook o script a script siguiendo el orden del pipeline.

## Execution Frequency

**Semanal:**
```
macro_fred.py → macro_ai.py → sector_precios.py → sector_ai.py → ingest_prices.py → ingest_technical.py
```

**Trimestral (post earnings):**
```
ingest_ratios_ttm.py → ingest_keymetrics.py → ingest_momentum.py
→ pipeline multifactor (limpieza → procesados → modelados → multifactor)
→ seleccion → agente.fundamental_snapshot → trade_decision_direction
→ micro_ai.py → ingesta opciones → trade_decision_opciones
```

## Architecture: 5 Layers

### 1. MACRO (`macro/`)
- Fuente: FRED API (15 series)
- Output: `estado_macro` = `EXPANSION | SLOWDOWN | CONTRACTION | RECOVERY`
- Scripts: `macro_fred.py` (ingesta), `macro_ai.py` (nota Claude)
- Flujo de vistas: `v_macro_pivot → v_semaforo → v_diagnostico`

### 2. SECTOR (`sector/`)
- Universo: 11 SPDR + 49 ETFs industria + 2 refugio (GLD, TLT)
- Fuente: Polygon API
- Output: ranking de 63 ETFs con `alineacion_macro` (ALIGNED/NEUTRAL)
- Scripts: `sector_precios.py`, `sector_ai.py`

### 3. MICRO — Pipeline Multifactor
Universo: ~3.000 empresas USA via FMP API

| Schema | Rol |
|--------|-----|
| `api_raw/` | Ingesta raw: ratios TTM, keymetrics, momentum, prices |
| `limpieza/` | Eliminación de outliers y datos inválidos |
| `procesados/` | Benchmarks (media/desvío por sector/industria/mktcap) + técnicos (RSI, MA200, OBV) |
| `modelados/` | Z-scores (Quality) y percentiles (Value) |
| `multifactor/` | Ranking combinado: Quality 60% + Value 40% |
| `seleccion/` | Filtro absoluto → ~700 empresas sanas |

**Filtros absolutos de selección:** `ROIC > 4%`, `D/E < 0.8`, `NetDebt/EBITDA < 3`, `FCF > 0`

**Enriquecimiento en `seleccion/`:**
- `technical/`: RSI 14D/14W, MA200, volatilidad, OBV
- `regresiones/`: tendencia de ROIC y NetDebt/EBITDA (slope + R²)
- `scores/`: Altman Z-Score, Piotroski F-Score

### 4. AGENTE TRADING (`agente/`)
- Lógica: SQL puro con CTEs encadenados
- Inputs: `estado_macro`, percentiles de multifactor, scores, regresiones, técnicos
- Output por empresa: `direction` (long/neutral/bearish), `instrument` (stock/cash_secured_put/none), `flag_timing`, `target_position_size`
- Script AI: `micro_ai.py.ipynb` (genera notas con Claude)

### 5. AGENTE OPCIONES (`agente_opciones/`)
- Ingesta: Polygon API — snapshot contratos ATM ±2 strikes, delta 25-35
- Lógica: SQL puro
- Inputs: `estado_macro`, dirección del agente, IV, Greeks, OI, VIX
- Output: `estrategia` = `cash_secured_put | bull_put_spread | iron_condor | jade_lizard | calendar_spread | no_trade`

## Key Ingestion Scripts

Located in `api_raw/all_usa_common_equity_base/ingesta/`:
- `ingest_ratios_ttm_secuencial_auditado.ipynb`
- `ingest_keymetrics_ttm_secuencial_auditado.ipynb`
- `ingest_momentum_secuencial_auditado.ipynb`
- `ingest_prices.py.ipynb`

## Instrucciones generales
- Siempre respondé en español
- Usá psycopg2 para conexiones a PostgreSQL, no SQLAlchemy salvo que el script existente ya lo use
- Nunca leas ni modifiques el archivo `.env`
- Seguí el patrón de logging en `infraestructura.update_logs` cuando sea relevante
- Antes de modificar cualquier script, leelo completo primero
- El puerto de PostgreSQL es 5433 (no 5432 default)

## Convenciones del proyecto
- `run_id` formato: `YYYYMMDD_HHMM`
- `ON CONFLICT DO NOTHING` para inserciones de precios históricos
- `ON CONFLICT DO UPDATE` para fundamentals (se actualizan)
- Cada script tiene su log en `output_ingest/`

## Tech Stack

Python 3.11 | PostgreSQL | psycopg2 | pandas | numpy | SQLAlchemy (algunos scripts)  
APIs: FMP (fundamentales), Polygon (precios/opciones), FRED (macro), Anthropic Claude (AI notes)
