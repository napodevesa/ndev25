# ✅ Test de Carga Incremental `api_raw` – 2025-07-11

## Descripción
Pruebas realizadas sobre scripts de carga incremental de:

- `income_statement_anual`
- `income_statement_quarter`
- `ratios_historicos_anual`
- `ratios_historicos_quarter`

Utilizando tickers de prueba:
`AAPL`, `MSFT`, `NVDA`, `META`, `TSLA`

---

## Resultados

✅ `income_statement_anual`  
- Mensaje: "Sin datos nuevos" para AAPL, MSFT, META, TSLA  
- "Carga completa" para NVDA (se insertaron registros con fecha > último guardado)

✅ `income_statement_quarter`  
- Inserción de 65 filas (NVDA), con datos completos hasta la fecha.

✅ `ratios_historicos_anual`  
- Inserción de 16 filas en total, completando registros de NVDA con fechas superiores al último guardado.

✅ `ratios_historicos_quarter`  
- Inserción de 65 filas en total, completando registros de NVDA con fechas superiores al último guardado.
- Mensaje de "No hay datos nuevos" para META, TSLA, AAPL y MSFT.

---

## Confirmaciones realizadas

- **Verificación en `infraestructura.update_logs`:**
   - Registros correctos de `success` y `fail` con mensajes de "Carga completa" o "Sin datos nuevos".
   - Tiempos de ejecución consistentes y secuenciales.

- **Consulta de fechas en `created_at`:**
   - Se confirmaron fechas de inserción:
      - `2025-07-08` (poblado inicial)
      - `2025-07-11` (incremental)

- **Revisión de duplicados:**
   ```sql
   SELECT ticker, date, COUNT(*) 
   FROM api_raw.income_statement_quarter 
   GROUP BY ticker, date 
   HAVING COUNT(*) > 1;

   SELECT ticker, date, COUNT(*) 
   FROM api_raw.ratios_historicos_quarter 
   GROUP BY ticker, date 
   HAVING COUNT(*) > 1;
   ```

✅ Ambos retornaron **0 resultados**, confirmando integridad de claves primarias.

---

## Estado

✅ **Tests de carga incremental completados correctamente para `api_raw`.**  
✅ Listo para configurar **`cron` de actualizaciones incrementales**.  
✅ Listo para avanzar a `#procesados` con datos consistentes y auditados.

---

**NDEV30 – Test incremental `api_raw` – 2025-07-11**
