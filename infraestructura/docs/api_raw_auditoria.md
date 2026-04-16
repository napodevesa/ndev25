
## 📄 `infraestructura/docs/api_raw_auditoria.md`

````markdown
# Auditoría de #api_raw en NDEV30

## 📌 Propósito

Este módulo permite auditar de forma automática las tablas del esquema `api_raw` tras cada carga o actualización incremental, registrando:

- Rango de años (`MIN(calendarYear)` y `MAX(calendarYear)`) de cada tabla.
- Cantidad total de registros.
- Fecha de ejecución.
- Timestamps de creación para trazabilidad.

---

## ⚙️ Tabla de control

La auditoría se guarda en:

```sql
infraestructura.estado_api_raw
````

con las columnas:

* `id`: identificador incremental.
* `tabla`: nombre de la tabla auditada.
* `fecha`: fecha de la auditoría.
* `min_year`: año más antiguo en la tabla.
* `max_year`: año más reciente en la tabla.
* `cantidad_registros`: cantidad total de registros.
* `creado_en`: timestamp de creación del registro.

---

## 🛠️ Script de auditoría

* Ubicación: `infraestructura/scripts/auditar_api_raw.py`
* Utiliza `psycopg2` y se ejecuta tras cada carga o actualización de `#api_raw`.

**Ejemplo de ejecución:**

```bash
python infraestructura/scripts/auditar_api_raw.py
```

---

## 🗂️ Tablas auditadas

* `api_raw.income_statement_anual`
* `api_raw.income_statement_quarter`
* `api_raw.ratios_historicos_anual`
* `api_raw.ratios_historicos_quarter`

---

## 🗓️ Ejemplo de resultado

Al **2025-07-08**, el estado registrado fue:

| Tabla                       | Min Year | Max Year | Registros |
| --------------------------- | -------- | -------- | --------- |
| income\_statement\_anual    | 2010     | 2025     | 78,345    |
| income\_statement\_quarter  | 2010     | 2026     | 285,314   |
| ratios\_historicos\_anual   | 2010     | 2025     | 78,449    |
| ratios\_historicos\_quarter | 2010     | 2026     | 288,881   |

---

## 🚩 Buenas prácticas

✅ Ejecutar este script tras **cada carga o actualización incremental** en `#api_raw`.
✅ Monitorear periódicamente la tabla `infraestructura.estado_api_raw` para identificar anomalías en rangos o volumen de datos.
✅ Integrar a **cron** junto a las actualizaciones para mantener auditorías automáticas diarias/semanales.
✅ Documentar semanalmente en `infraestructura/docs/auditoria_semanal.md` o en `Notion` el estado de infraestructura para control de evolución.

---

## 🔮 Futuro

✅ Integrar visualización de auditorías en **Metabase/Dash** para monitoreo de infraestructura en tiempo real.
✅ Configurar alertas automáticas por Telegram/Email si se detectan cambios anómalos en rangos de años o cantidad de registros.

---

```

---

## ✅ Qué hacer ahora

✅ Crea en tu repo:
```

infraestructura/docs/api\_raw\_auditoria.md

```
✅ Copia el contenido anterior.  
✅ Añade fecha y firma opcional si deseas.  
✅ Ejecútalo tras cada carga/actualización.

---

Avísame cuando lo tengas listo, y avanzamos con **el siguiente paso: scripts de actualización incremental con `update_logs`, para cerrar #api_raw esta semana de forma limpia y operativa en NDEV30.**
```
