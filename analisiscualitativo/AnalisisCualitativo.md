# AnalisisCualitativo.moat — README

## Objetivo

La tabla `AnalisisCualitativo.moat` documenta **evaluaciones cualitativas manuales** sobre la **durabilidad del moat** de empresas previamente seleccionadas mediante filtros cuantitativos (multifactores, Z-Scores, salud financiera y visualizaciones).

Este esquema **no genera señales de inversión**, no alimenta modelos automáticos y no debe utilizarse para ranking mecánico. Su función es **ordenar el criterio humano**, dejar trazabilidad y reducir sesgos.

---

## Principios de diseño

* 🧠 **Evaluación humana explícita**: el moat no se automatiza.
* 🧱 **Separación de capas**: cualitativo ≠ cuantitativo.
* 📅 **Contexto temporal**: el moat puede cambiar con el tiempo.
* 📉 **Anti–falsa precisión**: escalas cortas (1–3).
* 🧾 **Auditable**: cada decisión queda documentada.

---

## Estructura de la tabla

Cada fila representa una *foto mental* del moat de una empresa en una fecha determinada.

**Clave primaria**

* `(ticker, fecha_analisis)`

**Métricas cualitativas (1–3)**

1. **switching_costs_score**
   *¿Qué tan doloroso es sacar a esta empresa del cliente?*

2. **pricing_power_score**
   *¿Puede cobrar más sin perder clientes?*

3. **barriers_score**
   *¿Cuánto tarda otro en copiar esto, aunque tenga capital?*

4. **roic_durability_score**
   *¿El capital trabaja mejor acá que en cualquier otro lado, de forma sostenida?*

5. **reinvestment_score**
   *¿Cada dólar retenido hace más fuerte el foso?*

6. **management_quality_score**
   *¿Este equipo directivo amplía o erosiona el moat existente?*

**Score agregado**

* **moat_score_total**: porcentaje (0–100), calculado como:

```
(score_obtenido / score_maximo) * 100
```

**Nota cualitativa**

* **nota_cualitativa**: máximo 2–3 líneas, sin storytelling.

---

## Escala de puntuación (1–3)

* **1** → Débil / frágil / circunstancial
* **2** → Defendible / razonable
* **3** → Fuerte / estructural

No se permiten decimales.

---

## Interpretación del % de moat

El porcentaje **no es una señal mecánica**, sino una guía semántica.

| Moat %      | Etiqueta                    | Interpretación                              |
| ----------- | --------------------------- | ------------------------------------------- |
| **≥ 80**    | Moat estructural fuerte     | Foso claro, duradero y difícil de erosionar |
| **70 – 79** | Moat defendible             | Ventaja real con puntos de vigilancia       |
| **60 – 69** | Moat limitado / concentrado | Dependiente de pocos factores               |
| **< 60**    | Moat débil / transitorio    | Ventaja frágil o circunstancial             |

### Regla clave

> El rango **condiciona la conversación**, no la decisión.

Ejemplos:

* Moat alto → aceptar múltiplos mayores, foco en durabilidad.
* Moat medio → exigir valuación y margen de seguridad.
* Moat bajo → seguimiento o casos tácticos.

---

## Reglas de uso (muy importantes)

* ❌ El moat **no mejora** porque el precio cae.
* ❌ El moat **no empeora** porque el precio sube.
* ❌ No recalcular scores históricos si cambia el framework.
* ✅ Actualizar solo ante cambios estructurales:

  * disrupción competitiva
  * cambios regulatorios
  * cambio de management
  * deterioro estructural del ROIC

Frecuencia recomendada: **máximo una vez por año por empresa**.

---

## Relación con el sistema de inversión

Pipeline conceptual:

1. Filtros cuantitativos (multifactores, salud)
2. Visualizaciones y análisis exploratorio
3. Selección de empresas
4. **Análisis cualitativo de moat (este schema)**
5. Decisión humana final

Este esquema **complementa** al modelo cuantitativo; no compite con él.

---

## Nota final

> *El número ordena la mente. La decisión sigue siendo humana.*

Este README define el marco conceptual para asegurar consistencia, disciplina y trazabilidad en el análisis cualitativo del moat.
