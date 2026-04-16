
## 🗂️ `infraestructura/docs/` – Documentación NDEV25

Este directorio centraliza la **documentación técnica y operativa** de la infraestructura de análisis de empresas de **NDEV25**.

Su objetivo es mantener **trazabilidad, orden y claridad** en:

✅ Auditorías de carga de datos
✅ Registros de backups
✅ Checklists de control de calidad
✅ Uso correcto de los scripts de carga y actualización
✅ Logs de procesos automáticos

---

### 📄 Contenido actual

| Archivo                                  | Descripción breve                                                               |
| ---------------------------------------- | ------------------------------------------------------------------------------- |
| `api_raw_auditoria.md`                   | Auditoría de `api_raw`: procesos, tablas y estrategias de control.              |
| `backups_registro.md`                    | Registro de backups realizados con fecha, tamaño y ubicación de almacenamiento. |
| `checks_api_raw_post_poblado.sql`        | Checklist SQL para verificar consistencia tras la carga inicial de `api_raw`.   |
| `test_api_raw_incremental_2025-07-11.md` | Resultados del test de carga incremental de `api_raw` (2025-07-11).             |
| `update_logs.md`                         | Guía y detalles de la tabla `infraestructura.update_logs` para auditoría.       |
| `uso_scripts_api_raw.md`                 | Instrucciones de uso de scripts de carga incremental y completa para `api_raw`. |

---

### 🚀 Cómo utilizar esta carpeta

✅ **Antes de correr scripts de actualización:**

* Revisa `uso_scripts_api_raw.md` para saber qué script utilizar y cómo.
* Verifica el checklist `checks_api_raw_post_poblado.sql` si haces cambios masivos.

✅ **Después de ejecutar scripts:**

* Consulta `update_logs.md` para entender cómo auditar tus procesos.
* Anota resultados relevantes en `test_api_raw_incremental_2025-07-11.md` o en nuevos `.md`.

✅ **Backups:**

* Registra cada backup en `backups_registro.md` con fecha y ubicación.

✅ **Auditorías:**

* Usa `api_raw_auditoria.md` como referencia de estructura y mantenimiento de `api_raw`.

---

### 🛠️ Buenas prácticas

✅ Mantener todos los `.md` con **fechas en nombres si son reportes de ejecuciones específicas** (ej: `test_api_raw_incremental_YYYY-MM-DD.md`).
✅ Usar lenguaje claro, con bullets o tablas para que sea fácil de escanear.
✅ Cada vez que completes una tarea importante (test, backup, auditoría), registrar en esta carpeta.

---

### 🚩 Próximos pasos sugeridos

* Crear nuevos `.md` para auditorías de `procesados`, `modelos` y `backtesting` cuando avances de fase.
* Mantener `docs/` como **única carpeta de documentación centralizada**, sin subdividir innecesariamente.

---

**Proyecto NDEV25 – Infraestructura – 2025**
**Autor: Napo DEVESA**

---

