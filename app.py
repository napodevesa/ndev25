import os
import psycopg2
import psycopg2.extras
import anthropic
import pandas as pd
import plotly.express as px
import streamlit as st
from datetime import date
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="Sistema de Inversión",
    page_icon="📊",
    layout="wide",
)

# ── Conexión ────────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5433)),
        dbname=os.getenv("POSTGRES_DB", "ndev25"),
        user=os.getenv("POSTGRES_USER", "ndev"),
        password=os.getenv("POSTGRES_PASSWORD", "ndev"),
    )

@st.cache_data(ttl=300)
def query(sql: str) -> pd.DataFrame:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    return pd.DataFrame(rows)


# ── Queries ─────────────────────────────────────────────────────────────────

SQL_MACRO = """
SELECT estado_macro, score_riesgo, confianza,
       n_verdes, n_amarillos, n_rojos,
       regla_disparada, ultima_actualizacion,
       desempleo, fed_funds, ipc_anual, core_anual,
       curva_10y2y, pib_trim, vix, expectativas_inf, nfci,
       s_desempleo, s_fed, s_ipc, s_core, s_curva, s_pib,
       s_vix, s_expectativas, s_nfci, s_ventas, s_permisos, s_credito
FROM macro.v_diagnostico
LIMIT 1
"""

SQL_MACRO_RAW = """
SELECT
    r.serie_id,
    ms.nombre,
    ms.unidad,
    r.valor,
    r.semaforo,
    r.nota_semaforo,
    r.fecha_dato
FROM macro.macro_raw r
JOIN macro.macro_series ms ON r.serie_id = ms.serie_id
WHERE (r.serie_id, r.fecha_dato) IN (
    SELECT serie_id, MAX(fecha_dato)
    FROM macro.macro_raw
    GROUP BY serie_id
)
ORDER BY r.serie_id
"""

SQL_MACRO_NOTA = """
SELECT resumen, riesgos, outlook,
       score_sentimiento, score_recesion, score_inflacion,
       generado_en
FROM macro.macro_notas_ai
ORDER BY generado_en DESC
LIMIT 1
"""

SQL_SECTOR = """
SELECT estado_macro, top_tickers_aligned, top_tickers_global,
       señal_rotacion, n_aligned, n_leading_strong,
       n_leading_weak, n_neutral, n_lagging, score_universo
FROM sector.v_sector_diagnostico
LIMIT 1
"""

SQL_SECTOR_RANKING = """
SELECT ticker, industria, sector_gics, estado,
       alineacion_macro, score_total, ret_3m, rsi_rs_semanal,
       score_momentum, score_volumen
FROM sector.v_sector_ranking
WHERE tipo = 'industria'
ORDER BY rank_total
"""

SQL_SECTOR_DIAG_TEC = """
SELECT score_defensivos, score_ciclicos, score_mixtos,
       top_3_lideres, top_3_rezagados,
       diagnostico_sector, estado_macro, coherencia, nota
FROM sector.sector_diagnostico_tecnico
ORDER BY fecha DESC
LIMIT 1
"""

SQL_SECTOR_NOTA = """
SELECT resumen, oportunidades, riesgos
FROM sector.sector_notas_ai
ORDER BY generado_en DESC
LIMIT 1
"""

SQL_MICRO = """
SELECT ticker, sector, industry, market_cap_tier,
       quality_score, value_score, multifactor_score,
       multifactor_rank, multifactor_percentile,
       rsi_14_semanal, precio_vs_ma200,
       volume_ratio_20d, obv_slope,
       momentum_3m, momentum_6m, momentum_12m,
       altman_z_score, altman_zona,
       piotroski_score, piotroski_categoria,
       roic_tendencia, roic_signo, roic_r2, roic_confiable,
       deuda_tendencia, deuda_signo, deuda_r2, deuda_confiable,
       estado_macro, sector_etf, sector_alineado
FROM seleccion.enriquecimiento
WHERE snapshot_date = (SELECT MAX(snapshot_date)
                       FROM seleccion.enriquecimiento)
ORDER BY multifactor_rank
"""

SQL_DIRECCION = """
SELECT ticker, contexto, instrumento, direccion,
       flag_timing, target_position_size, notas_pre_trade,
       snapshot_date
FROM agente.trade_decision_direction
WHERE trade_status = 'active'
ORDER BY target_position_size DESC, ticker
"""

SQL_METRICAS_SISTEMA = """
SELECT
    (SELECT COUNT(*) FROM universos.stock_opciones_2026)                                    AS universo_inicial,
    (SELECT COUNT(*) FROM seleccion.universo
     WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM seleccion.universo))             AS pasan_calidad,
    (SELECT COUNT(*) FROM agente.decision
     WHERE trade_status = 'active'
       AND snapshot_date = (SELECT MAX(snapshot_date) FROM agente.decision))                AS señales_activas,
    (SELECT COUNT(*) FROM agente.top
     WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM agente.top))                     AS top_seleccion
"""

SQL_TOP_SELECCION = """
SELECT d.ticker, d.snapshot_date,
       d.sector, d.industry, d.market_cap_tier,
       d.contexto, d.instrumento, d.flag_timing,
       d.score_conviccion, d.rank_conviccion,
       d.target_position_size, d.trade_status,
       d.sector_alineado, d.estado_macro,
       e.quality_score, e.value_score,
       e.altman_z_score, e.piotroski_score,
       e.rsi_14_semanal, e.precio_vs_ma200,
       e.volume_ratio_20d
FROM agente.decision d
JOIN seleccion.enriquecimiento e
    ON  e.ticker = d.ticker
    AND e.snapshot_date = d.snapshot_date
WHERE d.snapshot_date = (SELECT MAX(snapshot_date) FROM agente.decision)
  AND d.trade_status = 'active'
ORDER BY d.rank_conviccion
"""

SQL_OPCIONES = """
SELECT o.ticker, o.snapshot_date,
       o.direccion, o.contexto, o.tendencia_fundamental,
       o.estado_macro, o.regimen_vix, o.vix,
       o.nivel_iv, o.iv_promedio, o.term_structure,
       o.liquidez, o.estrategia, o.delta_objetivo,
       o.put_strike, o.put_delta, o.put_theta,
       o.put_iv, o.put_dte,
       o.call_strike, o.call_delta, o.call_theta,
       o.sizing, o.trade_status, o.notas,
       e.quality_score, e.value_score,
       e.altman_z_score, e.piotroski_score,
       e.rsi_14_semanal, e.sector, e.industry,
       e.roic_signo, e.deuda_signo
FROM agente_opciones.trade_decision_opciones o
JOIN seleccion.enriquecimiento e
    ON  e.ticker = o.ticker
    AND e.snapshot_date = o.snapshot_date
WHERE o.snapshot_date = (SELECT MAX(snapshot_date)
                         FROM agente_opciones.trade_decision_opciones)
  AND o.trade_status = 'active'
ORDER BY o.sizing DESC
"""


# ── Helpers ─────────────────────────────────────────────────────────────────

ESTADO_COLOR = {
    "EXPANSION":   ("🟢", "#28a745"),
    "RECOVERY":    ("🟡", "#ffc107"),
    "SLOWDOWN":    ("🟡", "#ffc107"),
    "CONTRACTION": ("🔴", "#dc3545"),
}

def semaforo(estado: str) -> tuple[str, str]:
    return ESTADO_COLOR.get(estado.upper(), ("⚪", "#6c757d"))

SEMAFORO_BG = {
    "verde":    ("#057a55", "#f3faf7"),
    "amarillo": ("#c27803", "#fdf6b2"),
    "rojo":     ("#e02424", "#fdf2f2"),
}

def tarjeta_indicador(label: str, valor: str, estado: str,
                      nota: str = "", fecha: str = "") -> str:
    """Devuelve HTML de una tarjeta con color según estado del semáforo."""
    key = estado.lower() if estado else ""
    border, bg = SEMAFORO_BG.get(key, ("#6b7280", "#f9fafb"))
    nota_html  = f'<div style="font-size:.75rem;color:#6b7280;margin-top:5px;">{nota}</div>' if nota else ""
    fecha_html = f'<div style="font-size:.7rem;color:#9ca3af;margin-top:3px;">{fecha}</div>' if fecha else ""
    return (
        f'<div style="background:{bg};border-left:5px solid {border};'
        f'padding:12px 14px;border-radius:6px;height:100%;">'
        f'<div style="font-size:.78rem;color:#6b7280;margin-bottom:4px;">{label}</div>'
        f'<div style="font-size:1.3rem;font-weight:700;color:{border};">{valor}</div>'
        f'{nota_html}{fecha_html}'
        f'</div>'
    )


# ── Página 1: MACRO ──────────────────────────────────────────────────────────

def _fmt(val, decimales=2, sufijo="") -> str:
    """Formatea un valor numérico o devuelve '—' si es nulo."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "—"
    return f"{float(val):.{decimales}f}{sufijo}"

def pagina_macro():
    st.title("Macro")

    macro    = query(SQL_MACRO)
    raw      = query(SQL_MACRO_RAW)
    nota     = query(SQL_MACRO_NOTA)

    if macro.empty:
        st.error("Sin datos en macro.v_diagnostico.")
        return

    row = macro.iloc[0]
    estado = str(row["estado_macro"])
    emoji, color = semaforo(estado)

    # ── Encabezado de estado ────────────────────────────────────────────────
    col_estado, col_riesgo, col_confianza, col_verdes, col_amarillos, col_rojos = st.columns(6)
    with col_estado:
        st.markdown(
            f'<div style="background:{color}22;border-left:6px solid {color};'
            f'padding:16px;border-radius:6px;">'
            f'<div style="font-size:1.8rem;font-weight:700;color:{color};">{emoji} {estado}</div>'
            f'<div style="color:#888;font-size:.8rem;">Estado macro</div></div>',
            unsafe_allow_html=True,
        )
    with col_riesgo:
        st.metric("Score de riesgo", f"{int(row['score_riesgo'])}/100")
    with col_confianza:
        confianza = row.get("confianza") or "—"
        st.metric("Confianza", confianza.capitalize())
    with col_verdes:
        st.metric("🟢 Verdes",    int(row["n_verdes"]    or 0))
    with col_amarillos:
        st.metric("🟡 Amarillos", int(row["n_amarillos"] or 0))
    with col_rojos:
        st.metric("🔴 Rojos",     int(row["n_rojos"]     or 0))

    fecha = pd.to_datetime(row["ultima_actualizacion"]).strftime("%d/%m/%Y %H:%M") if row.get("ultima_actualizacion") else "—"
    st.caption(f"Última actualización: {fecha}")
    if row.get("regla_disparada"):
        st.caption(f"Regla disparada: *{row['regla_disparada']}*")

    st.divider()

    # ── Semáforos desde macro_raw en grilla 4 columnas ──────────────────────
    st.subheader("Indicadores")

    if not raw.empty:
        # Formateo del valor según unidad
        def fmt_valor(val, unidad: str) -> str:
            if val is None or (isinstance(val, float) and pd.isna(val)):
                return "—"
            v = float(val)
            u = (unidad or "").strip().lower()
            if u == "%":
                return f"{v:.2f}%"
            if u == "pp":
                return f"{v:.3f} pp"
            if u == "índice":
                return f"{v:.4f}"
            if u == "miles":
                return f"{v:,.0f} K"
            if u == "m usd":
                return f"${v/1_000_000:.2f}T"
            if u == "puntos":
                return f"{v:.2f}"
            return f"{v:.4f}"

        series = raw.to_dict("records")
        for i in range(0, len(series), 4):
            fila = series[i:i+4]
            cols = st.columns(4)
            for col, s in zip(cols, fila):
                valor_fmt = fmt_valor(s["valor"], s["unidad"])
                fecha_fmt = pd.to_datetime(s["fecha_dato"]).strftime("%d/%m/%Y") if s.get("fecha_dato") else ""
                html = tarjeta_indicador(
                    label  = s["nombre"],
                    valor  = valor_fmt,
                    estado = s.get("semaforo") or "",
                    nota   = s.get("nota_semaforo") or "",
                    fecha  = fecha_fmt,
                )
                col.markdown(html, unsafe_allow_html=True)
            st.markdown("<div style='margin-bottom:10px'></div>", unsafe_allow_html=True)
    else:
        st.info("Sin datos en macro.macro_raw.")

    st.divider()

    # ── Nota AI ─────────────────────────────────────────────────────────────
    st.subheader("Nota AI")
    if not nota.empty:
        n = nota.iloc[0]
        fecha_nota = pd.to_datetime(n["generado_en"]).strftime("%d/%m/%Y %H:%M") if n.get("generado_en") else "—"

        c1, c2, c3, c4 = st.columns(4)
        c1.caption(f"Generado: {fecha_nota}")
        if n.get("score_sentimiento") is not None:
            c2.metric("Sentimiento",  f"{int(n['score_sentimiento'])}/10")
        if n.get("score_recesion") is not None:
            c3.metric("Riesgo recesión", f"{int(n['score_recesion'])}/10")
        if n.get("score_inflacion") is not None:
            c4.metric("Riesgo inflación", f"{int(n['score_inflacion'])}/10")

        if n.get("resumen"):
            st.markdown(f"**Resumen**\n\n{n['resumen']}")

        tab1, tab2, tab3 = st.tabs(["Outlook", "Riesgos", "Nota completa"])
        with tab1:
            st.write(n.get("outlook") or "—")
        with tab2:
            st.write(n.get("riesgos") or "—")
        with tab3:
            nota_df = query("SELECT nota_completa FROM macro.macro_notas_ai ORDER BY generado_en DESC LIMIT 1")
            if not nota_df.empty:
                st.write(nota_df.iloc[0]["nota_completa"] or "—")
    else:
        st.info("Sin notas AI disponibles.")


# ── Página 2: SECTORES ───────────────────────────────────────────────────────

def pagina_sectores():
    st.title("Sectores")

    sector   = query(SQL_SECTOR)
    diag_tec = query(SQL_SECTOR_DIAG_TEC)
    ranking  = query(SQL_SECTOR_RANKING)
    nota     = query(SQL_SECTOR_NOTA)

    # ── Sección 1: Header con contexto global ───────────────────────────────
    ROTACION_BADGE = {
        "ROTACION_CLARA":    ("#057a55", "#f3faf7", "Rotación clara"),
        "ROTACION_MODERADA": ("#c27803", "#fdf6b2", "Rotación moderada"),
        "SIN_TENDENCIA":     ("#6b7280", "#f9fafb", "Sin tendencia"),
        "MERCADO_DEBIL":     ("#e02424", "#fdf2f2", "Mercado débil"),
    }

    if not sector.empty:
        s = sector.iloc[0]
        estado_macro  = str(s.get("estado_macro") or "—")
        señal         = str(s.get("señal_rotacion") or "SIN_TENDENCIA").upper()
        score_univ    = s.get("score_universo")

        col_macro, col_badge, col_score = st.columns([2, 3, 1])

        with col_macro:
            emoji, color = semaforo(estado_macro)
            st.markdown(
                f'<div style="padding:10px 0;">'
                f'<span style="font-size:.85rem;color:#6b7280;">Estado macro</span><br>'
                f'<span style="font-size:1.4rem;font-weight:700;color:{color};">'
                f'{emoji} {estado_macro}</span></div>',
                unsafe_allow_html=True,
            )

        with col_badge:
            border, bg, label = ROTACION_BADGE.get(señal, ("#6b7280", "#f9fafb", señal))
            st.markdown(
                f'<div style="padding:10px 0;">'
                f'<span style="font-size:.85rem;color:#6b7280;">Señal de rotación</span><br>'
                f'<span style="background:{bg};color:{border};border:1.5px solid {border};'
                f'padding:4px 14px;border-radius:20px;font-weight:700;font-size:1rem;">'
                f'{label}</span></div>',
                unsafe_allow_html=True,
            )

        with col_score:
            if score_univ is not None:
                st.metric("Score universo", f"{float(score_univ):.1f}")

    st.divider()

    # ── Diagnóstico Técnico Sectorial ────────────────────────────────────────
    st.subheader("Diagnóstico Técnico Sectorial")

    if not diag_tec.empty:
        dt = diag_tec.iloc[0]

        # Fila 1 — RSI por grupo
        def _color_rsi(v):
            if v is None or pd.isna(v):
                return "#6b7280"
            return "#057a55" if float(v) > 65 else ("#c27803" if float(v) >= 50 else "#e02424")

        c1, c2, c3 = st.columns(3)
        for col, label, campo in [
            (c1, "RSI Defensivos", "score_defensivos"),
            (c2, "RSI Cíclicos",   "score_ciclicos"),
            (c3, "RSI Mixtos",     "score_mixtos"),
        ]:
            val = dt.get(campo)
            val_f = f"{float(val):.1f}" if val is not None and not pd.isna(val) else "—"
            color = _color_rsi(val)
            col.markdown(
                f'<div style="border-left:5px solid {color};background:{color}11;'
                f'padding:12px 16px;border-radius:6px;">'
                f'<div style="font-size:.8rem;color:#6b7280;">{label}</div>'
                f'<div style="font-size:2rem;font-weight:700;color:{color};">{val_f}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )

        st.markdown("<div style='margin-top:14px'></div>", unsafe_allow_html=True)

        # Fila 2 — Diagnóstico y coherencia
        DIAG_BADGE = {
            "CONFIRMA_SLOWDOWN":    ("#1d4ed8", "#eff6ff", "Confirma Slowdown"),
            "CONFIRMA_EXPANSION":   ("#057a55", "#f3faf7", "Confirma Expansión"),
            "CONFIRMA_CONTRACTION": ("#e02424", "#fdf2f2", "Confirma Contracción"),
            "SEÑAL_MIXTA":          ("#6b7280", "#f9fafb", "Señal mixta"),
        }
        COHERENCIA_BADGE = {
            "ALTA":       ("#057a55", "#f3faf7", "Alta"),
            "MEDIA":      ("#c27803", "#fdf6b2", "Media"),
            "CONTRADICE": ("#e02424", "#fdf2f2", "Contradice"),
        }

        col_diag, col_coh = st.columns(2)
        with col_diag:
            diag_key = str(dt.get("diagnostico_sector") or "").upper()
            bd, bg, bl = DIAG_BADGE.get(diag_key, ("#6b7280", "#f9fafb", diag_key or "—"))
            st.markdown(
                f'<div style="font-size:.8rem;color:#6b7280;margin-bottom:6px;">Diagnóstico sectorial</div>'
                f'<span style="background:{bg};color:{bd};border:1.5px solid {bd};'
                f'padding:6px 18px;border-radius:20px;font-weight:700;font-size:1.05rem;">'
                f'{bl}</span>',
                unsafe_allow_html=True,
            )
        with col_coh:
            coh_key = str(dt.get("coherencia") or "").upper()
            cd, cg, cl = COHERENCIA_BADGE.get(coh_key, ("#6b7280", "#f9fafb", coh_key or "—"))
            st.markdown(
                f'<div style="font-size:.8rem;color:#6b7280;margin-bottom:6px;">Coherencia</div>'
                f'<span style="background:{cg};color:{cd};border:1.5px solid {cd};'
                f'padding:6px 18px;border-radius:20px;font-weight:700;font-size:1.05rem;">'
                f'{cl}</span>',
                unsafe_allow_html=True,
            )

        st.markdown("<div style='margin-top:14px'></div>", unsafe_allow_html=True)

        # Fila 3 — Líderes y rezagados
        col_lid, col_rez = st.columns(2)
        with col_lid:
            st.markdown("**Top líderes sectoriales**")
            st.write(dt.get("top_3_lideres") or "—")
        with col_rez:
            st.markdown("**Top rezagados**")
            st.write(dt.get("top_3_rezagados") or "—")

        st.markdown("<div style='margin-top:10px'></div>", unsafe_allow_html=True)

        # Fila 4 — Nota explicativa
        if dt.get("nota"):
            st.info(dt["nota"])

        # Fila 5 — Coherencia con macro
        macro_dice  = str(dt.get("estado_macro") or "—")
        sector_dice = str(dt.get("diagnostico_sector") or "—")
        coh_key2    = str(dt.get("coherencia") or "").upper()
        COHERENCIA_ICONO = {
            "ALTA":       ("✅", "Alineados"),
            "MEDIA":      ("⚠️", "Parcialmente alineados"),
            "CONTRADICE": ("❌", "Contradicción — revisar señales"),
        }
        icono, coh_txt = COHERENCIA_ICONO.get(coh_key2, ("—", coh_key2))
        st.markdown(
            f'<div style="background:#f9fafb;border:1px solid #e5e7eb;border-radius:8px;'
            f'padding:14px 18px;margin-top:4px;display:flex;align-items:center;gap:0;">'
            f'<span style="color:#6b7280;font-size:.9rem;">Macro dice: </span>'
            f'<b style="margin:0 6px;">{macro_dice}</b>'
            f'<span style="color:#6b7280;font-size:1.4rem;margin:0 10px;">→</span>'
            f'<span style="color:#6b7280;font-size:.9rem;">Sectores: </span>'
            f'<b style="margin:0 6px;">{sector_dice}</b>'
            f'<span style="margin-left:16px;font-size:.95rem;">{icono} {coh_txt}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )
    else:
        st.info("Sin datos en sector.sector_diagnostico_tecnico.")

    st.divider()

    # ── Sección 2: Tarjetas agrupadas por estado ────────────────────────────
    st.subheader("Industrias por estado")

    ORDEN_ESTADOS = ["LEADING_STRONG", "LEADING_WEAK", "NEUTRAL", "LAGGING"]
    LABEL_ESTADOS = {
        "LEADING_STRONG": "Leading Strong",
        "LEADING_WEAK":   "Leading Weak",
        "NEUTRAL":        "Neutral",
        "LAGGING":        "Lagging",
    }
    COLOR_ESTADOS = {
        "LEADING_STRONG": "#057a55",
        "LEADING_WEAK":   "#c27803",
        "NEUTRAL":        "#6b7280",
        "LAGGING":        "#e02424",
    }

    if not ranking.empty:
        grupos = {e: ranking[ranking["estado"] == e] for e in ORDEN_ESTADOS}
        cols_estado = st.columns(4)

        for col, estado in zip(cols_estado, ORDEN_ESTADOS):
            df_grupo = grupos[estado]
            color_e  = COLOR_ESTADOS[estado]
            with col:
                st.markdown(
                    f'<div style="font-weight:700;color:{color_e};'
                    f'font-size:1rem;margin-bottom:8px;border-bottom:2px solid {color_e};'
                    f'padding-bottom:4px;">{LABEL_ESTADOS[estado]} ({len(df_grupo)})</div>',
                    unsafe_allow_html=True,
                )
                for _, row in df_grupo.iterrows():
                    # Badge alineacion_macro
                    alin = str(row.get("alineacion_macro") or "")
                    if alin == "ALIGNED":
                        badge_color, badge_bg, badge_txt = "#057a55", "#f3faf7", "ALIGNED"
                    else:
                        badge_color, badge_bg, badge_txt = "#6b7280", "#f9fafb", "NEUTRAL"

                    # ret_3m con color
                    ret = row.get("ret_3m")
                    if ret is not None and not pd.isna(ret):
                        ret_v    = float(ret)
                        ret_str  = f"{ret_v:+.2f}%"
                        ret_col  = "#057a55" if ret_v >= 0 else "#e02424"
                    else:
                        ret_str, ret_col = "—", "#6b7280"

                    rsi = row.get("rsi_rs_semanal")
                    rsi_str = f"{float(rsi):.1f}" if rsi is not None and not pd.isna(rsi) else "—"

                    score = row.get("score_total")
                    score_v = float(score) / 100 if score is not None and not pd.isna(score) else 0.0

                    industria = str(row.get("industria") or row.get("ticker") or "")
                    ticker    = str(row.get("ticker") or "")

                    st.markdown(
                        f'<div style="background:#f9fafb;border:1px solid #e5e7eb;'
                        f'border-radius:6px;padding:10px 12px;margin-bottom:8px;">'
                        f'<div style="font-size:1.1rem;font-weight:700;color:#111827;">{ticker}</div>'
                        f'<div style="font-size:.75rem;color:#6b7280;margin-bottom:6px;'
                        f'white-space:nowrap;overflow:hidden;text-overflow:ellipsis;" '
                        f'title="{industria}">{industria}</div>'
                        f'<span style="background:{badge_bg};color:{badge_color};'
                        f'border:1px solid {badge_color};padding:1px 8px;border-radius:12px;'
                        f'font-size:.7rem;font-weight:600;">{badge_txt}</span>'
                        f'<div style="margin-top:6px;display:flex;gap:12px;">'
                        f'<span style="font-size:.82rem;">Ret 3m: <b style="color:{ret_col};">{ret_str}</b></span>'
                        f'<span style="font-size:.82rem;">RSI: <b>{rsi_str}</b></span>'
                        f'</div></div>',
                        unsafe_allow_html=True,
                    )
                    st.progress(min(max(score_v, 0.0), 1.0),
                                text=f"Score {float(score):.1f}" if score is not None and not pd.isna(score) else "Score —")
    else:
        st.info("Sin datos en sector.v_sector_ranking.")

    st.divider()

    # ── Sección 3: Nota AI ──────────────────────────────────────────────────
    st.subheader("Nota sectorial AI")
    if not nota.empty:
        n = nota.iloc[0]
        if n.get("resumen"):
            st.markdown(f"**Resumen**\n\n{n['resumen']}")
        tab1, tab2 = st.tabs(["Oportunidades", "Riesgos"])
        with tab1:
            st.write(n.get("oportunidades") or "—")
        with tab2:
            st.write(n.get("riesgos") or "—")
    else:
        st.info("Sin notas sectoriales disponibles.")

    st.divider()

    # ── Sección 4: Gráfico de burbujas ──────────────────────────────────────
    st.subheader("Momentum relativo — Ret 3m vs RSI semanal")
    if not ranking.empty:
        df_plot = ranking.dropna(subset=["ret_3m", "rsi_rs_semanal"]).copy()
        df_plot["ret_3m"]        = df_plot["ret_3m"].astype(float)
        df_plot["rsi_rs_semanal"] = df_plot["rsi_rs_semanal"].astype(float)
        df_plot["score_total"]   = df_plot["score_total"].fillna(50).astype(float)
        df_plot["color_label"]   = df_plot["alineacion_macro"].apply(
            lambda x: "Aligned" if str(x) == "ALIGNED" else "Neutral"
        )

        fig = px.scatter(
            df_plot,
            x="ret_3m",
            y="rsi_rs_semanal",
            size="score_total",
            color="color_label",
            color_discrete_map={"Aligned": "#057a55", "Neutral": "#9ca3af"},
            hover_name="ticker",
            hover_data={"industria": True, "ret_3m": ":.2f",
                        "rsi_rs_semanal": ":.1f", "color_label": False,
                        "score_total": ":.1f"},
            labels={
                "ret_3m":         "Retorno 3m (%)",
                "rsi_rs_semanal": "RSI fuerza relativa semanal",
                "color_label":    "Alineación macro",
            },
            size_max=30,
        )
        fig.update_layout(
            plot_bgcolor="#ffffff",
            paper_bgcolor="#ffffff",
            xaxis=dict(zeroline=True, zerolinecolor="#e5e7eb", gridcolor="#f3f4f6"),
            yaxis=dict(zeroline=True, zerolinecolor="#e5e7eb", gridcolor="#f3f4f6"),
            margin=dict(l=20, r=20, t=20, b=20),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Sin datos para el gráfico.")


# ── Página 3: MICRO ──────────────────────────────────────────────────────────

ALTMAN_ZONA_COLOR = {
    "safe":     ("#057a55", "#f3faf7"),
    "grey":     ("#c27803", "#fefce8"),
    "distress": ("#e02424", "#fef2f2"),
}

SIGNO_LABEL = {1: "↑", 0: "→", -1: "↓"}


def pagina_micro():
    st.title("Micro — Universo de trabajo")

    df = query(SQL_MICRO)

    if df.empty:
        st.info("Sin datos en seleccion.enriquecimiento.")
        return

    # Conversiones numéricas
    for col in ["quality_score", "value_score", "multifactor_score", "multifactor_rank",
                "multifactor_percentile", "rsi_14_semanal", "precio_vs_ma200",
                "volume_ratio_20d", "momentum_3m", "momentum_6m", "momentum_12m",
                "altman_z_score", "piotroski_score", "roic_signo", "deuda_signo"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # ── Métricas resumen ─────────────────────────────────────────────────────
    n_aligned       = int((df["sector_alineado"] == "ALIGNED").sum())
    n_roic_mejora   = int((df["roic_signo"] == 1).sum())
    n_deuda_baja    = int((df["deuda_signo"] == -1).sum())
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Empresas en universo", len(df))
    c2.metric("Sector ALIGNED",       n_aligned)
    c3.metric("ROIC mejorando",        n_roic_mejora)
    c4.metric("Deuda bajando",         n_deuda_baja)

    st.divider()

    # ── Filtros ──────────────────────────────────────────────────────────────
    ff1, ff2, ff3, ff4, ff5 = st.columns(5)
    with ff1:
        opts_sec = ["Todos"] + sorted(df["sector"].dropna().unique().tolist())
        f_sec = st.selectbox("Sector", opts_sec)
    with ff2:
        opts_cap = ["Todos"] + sorted(df["market_cap_tier"].dropna().unique().tolist())
        f_cap = st.selectbox("Market cap", opts_cap)
    with ff3:
        opts_az = ["Todos"] + sorted(df["altman_zona"].dropna().unique().tolist())
        f_az = st.selectbox("Altman zona", opts_az)
    with ff4:
        opts_pio = ["Todos"] + sorted(df["piotroski_categoria"].dropna().unique().tolist())
        f_pio = st.selectbox("Piotroski cat.", opts_pio)
    with ff5:
        opts_alin = ["Todos"] + sorted(df["sector_alineado"].dropna().unique().tolist())
        f_alin = st.selectbox("Alineado", opts_alin)

    dff = df.copy()
    if f_sec  != "Todos": dff = dff[dff["sector"]            == f_sec]
    if f_cap  != "Todos": dff = dff[dff["market_cap_tier"]   == f_cap]
    if f_az   != "Todos": dff = dff[dff["altman_zona"]       == f_az]
    if f_pio  != "Todos": dff = dff[dff["piotroski_categoria"] == f_pio]
    if f_alin != "Todos": dff = dff[dff["sector_alineado"]   == f_alin]

    st.caption(f"{len(dff)} empresa(s) mostrada(s)")

    # ── Tabla principal ──────────────────────────────────────────────────────
    def zona_badge(zona: str | None) -> str:
        if not zona:
            return "—"
        color, bg = ALTMAN_ZONA_COLOR.get(zona, ("#6b7280", "#f9fafb"))
        return f'<span style="background:{bg};color:{color};border:1px solid {color};padding:1px 8px;border-radius:10px;font-size:.72rem;font-weight:600;">{zona}</span>'

    for _, row in dff.iterrows():
        ticker   = str(row.get("ticker") or "")
        sector_s = str(row.get("sector") or "—")[:24]
        cap      = str(row.get("market_cap_tier") or "—")
        az_zona  = str(row.get("altman_zona") or "")
        alin     = str(row.get("sector_alineado") or "")
        rank_v   = row.get("multifactor_rank")
        multi_v  = row.get("multifactor_score")
        rank_f   = f"#{int(rank_v)}" if pd.notna(rank_v) else "—"
        multi_f  = f"{float(multi_v):.1f}" if pd.notna(multi_v) else "—"
        alin_ico = "✅" if alin == "ALIGNED" else "⬜"

        titulo = (
            f"{rank_f}  {ticker}  ·  {sector_s}  ·  {cap}  ·  "
            f"{alin_ico}  ·  Score {multi_f}"
        )

        with st.expander(titulo):
            az_v = row.get("altman_z_score")
            if pd.notna(az_v):
                az_color, az_bg = ALTMAN_ZONA_COLOR.get(az_zona, ("#6b7280", "#f9fafb"))
                st.markdown(
                    f'<div style="background:{az_bg};border-left:4px solid {az_color};'
                    f'padding:4px 10px;border-radius:4px;margin-bottom:8px;font-size:.8rem;">'
                    f'Altman Z: <strong>{float(az_v):.2f}</strong> — {zona_badge(az_zona)}</div>',
                    unsafe_allow_html=True,
                )

            col_a, col_b, col_c, col_d = st.columns(4)

            # Col A — Scores
            with col_a:
                st.markdown("**Scores**")
                qs = row.get("quality_score")
                vs = row.get("value_score")
                ms = row.get("multifactor_score")
                st.metric("Quality",     f"{float(qs):.1f}"  if pd.notna(qs) else "—")
                st.metric("Value",       f"{float(vs):.1f}"  if pd.notna(vs) else "—")
                st.metric("Multifactor", f"{float(ms):.1f}"  if pd.notna(ms) else "—")

            # Col B — Técnico
            with col_b:
                st.markdown("**Técnico**")
                rsi = row.get("rsi_14_semanal")
                ma  = row.get("precio_vs_ma200")
                m3  = row.get("momentum_3m")
                m6  = row.get("momentum_6m")
                if pd.notna(rsi):
                    rv = float(rsi)
                    rc = "#057a55" if 40 <= rv <= 65 else ("#e02424" if rv < 40 else "#c27803")
                    st.markdown(f'RSI 14s: <span style="color:{rc};font-weight:700;">{rv:.1f}</span>', unsafe_allow_html=True)
                else:
                    st.markdown("RSI 14s: —")
                if pd.notna(ma):
                    mv = float(ma); mc = "#057a55" if mv >= 0 else "#e02424"
                    st.markdown(f'vs MA200: <span style="color:{mc};font-weight:700;">{mv:+.2f}%</span>', unsafe_allow_html=True)
                else:
                    st.markdown("vs MA200: —")
                st.markdown(f"Mom 3m: **{f'{float(m3):.1f}%' if pd.notna(m3) else '—'}**")
                st.markdown(f"Mom 6m: **{f'{float(m6):.1f}%' if pd.notna(m6) else '—'}**")

            # Col C — Salud / signos
            with col_c:
                st.markdown("**Salud**")
                ps = row.get("piotroski_score")
                rs = row.get("roic_signo")
                rc_conf = row.get("roic_confiable")
                ds = row.get("deuda_signo")
                dc_conf = row.get("deuda_confiable")
                if pd.notna(ps):
                    pv = int(ps)
                    pc = "#057a55" if pv >= 7 else ("#c27803" if pv >= 4 else "#e02424")
                    st.markdown(f'Piotroski: <span style="color:{pc};font-weight:700;">{pv}</span>', unsafe_allow_html=True)
                else:
                    st.markdown("Piotroski: —")
                roic_lbl = SIGNO_LABEL.get(int(rs) if pd.notna(rs) else None, "—")
                deuda_lbl = SIGNO_LABEL.get(int(ds) if pd.notna(ds) else None, "—")
                conf_r = "✓" if rc_conf else "—"
                conf_d = "✓" if dc_conf else "—"
                st.markdown(f"ROIC: **{roic_lbl}** {conf_r}")
                st.markdown(f"Deuda: **{deuda_lbl}** {conf_d}")

            # Col D — Regresiones
            with col_d:
                st.markdown("**Regresiones**")
                for lbl, trend_col, r2_col, conf_col in [
                    ("ROIC",  "roic_tendencia",  "roic_r2",  "roic_confiable"),
                    ("Deuda", "deuda_tendencia", "deuda_r2", "deuda_confiable"),
                ]:
                    tv = row.get(trend_col)
                    r2 = row.get(r2_col)
                    cv = row.get(conf_col)
                    tv_f = f"{float(tv):+.4f}" if pd.notna(tv) else "—"
                    r2_f = f"R²={float(r2):.2f}" if pd.notna(r2) else ""
                    cv_f = " ✓" if cv else ""
                    st.markdown(f"{lbl}: **{tv_f}** {r2_f}{cv_f}")

            ind = str(row.get("industry") or "—")
            st.caption(f"{sector_s} · {ind} · {cap}")


# ── Página 4: ESTRATEGIA ─────────────────────────────────────────────────────

# Helpers de badges para esta página
def _badge(texto: str, color: str, bg: str) -> str:
    return (
        f'<span style="background:{bg};color:{color};border:1px solid {color};'
        f'padding:2px 10px;border-radius:12px;font-size:.75rem;font-weight:600;">'
        f'{texto}</span>'
    )

FLAG_TIMING_ICONO = {
    "tecnico_confirmado": "🟢",
    "pullback_comprable": "🟡",
    "macro_defensivo":    "🔵",
    "fundamental_only":   "⚪",
}

INSTRUMENTO_STYLE = {
    "stock":            ("#057a55", "#f3faf7", "Stock"),
    "cash_secured_put": ("#1d4ed8", "#eff6ff", "CSP"),
}

def pagina_estrategia():
    st.title("Estrategia")

    df = query(SQL_TOP_SELECCION)

    if df.empty:
        st.info("Sin datos en agente.decision.")
        return

    # Conversiones de tipo para comparaciones seguras
    for col_num in ["quality_score", "value_score", "score_conviccion",
                    "altman_z_score", "rsi_14_semanal", "precio_vs_ma200", "volume_ratio_20d"]:
        df[col_num] = pd.to_numeric(df[col_num], errors="coerce")
    for col_int in ["piotroski_score", "rank_conviccion"]:
        df[col_int] = pd.to_numeric(df[col_int], errors="coerce")

    # ── Sección 1: Métricas resumen ──────────────────────────────────────────
    n_stock   = int((df["instrumento"] == "stock").sum())
    n_csp     = int((df["instrumento"] == "cash_secured_put").sum())
    n_aligned = int((df["sector_alineado"] == "ALIGNED").sum())
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total seleccionadas",  len(df))
    c2.metric("Stock",                n_stock)
    c3.metric("Cash-secured put",     n_csp)
    c4.metric("Sector alineado",      n_aligned)

    st.divider()

    # ── Sección 2: Filtros ───────────────────────────────────────────────────
    fc1, fc2, fc3 = st.columns(3)
    with fc1:
        opts_inst = ["Todos"] + sorted(df["instrumento"].dropna().unique().tolist())
        f_inst = st.selectbox("Instrumento", opts_inst)
    with fc2:
        opts_alin = ["Todos"] + sorted(df["sector_alineado"].dropna().unique().tolist())
        f_alin = st.selectbox("Sector alineado", opts_alin)
    with fc3:
        opts_cap = ["Todos"] + sorted(df["market_cap_tier"].dropna().unique().tolist())
        f_cap = st.selectbox("Market cap", opts_cap)

    dff = df.copy()
    if f_inst != "Todos":
        dff = dff[dff["instrumento"] == f_inst]
    if f_alin != "Todos":
        dff = dff[dff["sector_alineado"] == f_alin]
    if f_cap != "Todos":
        dff = dff[dff["market_cap_tier"] == f_cap]

    st.caption(f"{len(dff)} empresa(s) mostrada(s)")

    # ── Sección 3 + 4: Tabla con expanders ──────────────────────────────────
    for _, row in dff.iterrows():
        inst      = str(row.get("instrumento") or "")
        alin      = str(row.get("sector_alineado") or "")
        timing    = str(row.get("flag_timing") or "")
        ticker    = str(row.get("ticker") or "")
        rank      = row.get("rank_conviccion")
        score     = row.get("score_conviccion")
        pos_size  = row.get("target_position_size")
        sector_s  = str(row.get("sector") or "—")

        # Colores de instrumento
        inst_color, inst_bg, inst_lbl = INSTRUMENTO_STYLE.get(inst, ("#6b7280", "#f9fafb", inst))
        # Badge alineación
        alin_txt = "✅ ALIGNED" if alin == "ALIGNED" else "⬜ NEUTRAL"
        # Ícono timing
        timing_icono = FLAG_TIMING_ICONO.get(timing, "⚪")
        # Score formateado
        score_f   = f"{float(score):.0f}" if score is not None and not pd.isna(score) else "—"
        pos_f     = f"{float(pos_size):.2f}" if pos_size is not None and not pd.isna(pos_size) else "—"
        rank_f    = f"#{int(rank)}" if rank is not None and not pd.isna(rank) else "—"

        # Título del expander: resumen de una línea
        titulo = (
            f"{rank_f}  {ticker}  ·  {sector_s[:28]}  ·  "
            f"{inst_lbl}  ·  {timing_icono} {timing.replace('_',' ')}  ·  "
            f"Score {score_f}  ·  Size {pos_f}"
        )

        with st.expander(titulo):
            # Barra de score al tope del expander
            score_norm = min(max(float(score) / 100 if score is not None and not pd.isna(score) else 0, 0), 1)
            st.progress(score_norm, text=f"Score de convicción: {score_f}/100")

            # Badges de instrumento y alineación
            st.markdown(
                _badge(inst_lbl, inst_color, inst_bg) + "&nbsp;&nbsp;" +
                _badge(alin_txt, "#057a55" if alin == "ALIGNED" else "#6b7280",
                       "#f3faf7" if alin == "ALIGNED" else "#f9fafb"),
                unsafe_allow_html=True,
            )
            st.markdown("<div style='margin-bottom:10px'></div>", unsafe_allow_html=True)

            col_a, col_b, col_c = st.columns(3)

            # ── Columna A: FUNDAMENTAL ───────────────────────────────────────
            with col_a:
                st.markdown("**FUNDAMENTAL**")
                qs = row.get("quality_score")
                vs = row.get("value_score")
                st.metric("Quality score", f"{float(qs):.1f}" if qs is not None and not pd.isna(qs) else "—")
                st.metric("Value score",   f"{float(vs):.1f}" if vs is not None and not pd.isna(vs) else "—")
                ctx = str(row.get("contexto") or "")
                if ctx:
                    st.markdown(
                        _badge(ctx.replace("_", " ").title(), "#4b5563", "#f3f4f6"),
                        unsafe_allow_html=True,
                    )

            # ── Columna B: TÉCNICO ───────────────────────────────────────────
            with col_b:
                st.markdown("**TÉCNICO**")
                rsi = row.get("rsi_14_semanal")
                if rsi is not None and not pd.isna(rsi):
                    rsi_v = float(rsi)
                    rsi_color = "#057a55" if 40 <= rsi_v <= 65 else ("#e02424" if rsi_v < 40 else "#c27803")
                    st.markdown(
                        f'RSI semanal: <span style="color:{rsi_color};font-weight:700;">{rsi_v:.1f}</span>',
                        unsafe_allow_html=True,
                    )
                else:
                    st.markdown("RSI semanal: —")

                ma = row.get("precio_vs_ma200")
                if ma is not None and not pd.isna(ma):
                    ma_v     = float(ma)
                    ma_color = "#057a55" if ma_v >= 0 else "#e02424"
                    st.markdown(
                        f'vs MA200: <span style="color:{ma_color};font-weight:700;">{ma_v:+.2f}%</span>',
                        unsafe_allow_html=True,
                    )
                else:
                    st.markdown("vs MA200: —")

                vol = row.get("volume_ratio_20d")
                vol_f = f"{float(vol):.2f}x" if vol is not None and not pd.isna(vol) else "—"
                st.markdown(f"Vol ratio 20d: **{vol_f}**")

            # ── Columna C: SALUD ─────────────────────────────────────────────
            with col_c:
                st.markdown("**SALUD**")

                az = row.get("altman_z_score")
                if az is not None and not pd.isna(az):
                    az_v     = float(az)
                    az_color = "#057a55" if az_v > 2.99 else ("#c27803" if az_v >= 1.81 else "#e02424")
                    st.markdown(
                        f'Altman Z: <span style="color:{az_color};font-weight:700;">{az_v:.2f}</span>',
                        unsafe_allow_html=True,
                    )
                else:
                    st.markdown("Altman Z: —")

                ps = row.get("piotroski_score")
                if ps is not None and not pd.isna(ps):
                    ps_v     = int(ps)
                    ps_color = "#057a55" if ps_v >= 7 else ("#c27803" if ps_v >= 5 else "#e02424")
                    st.markdown(
                        f'Piotroski F: <span style="color:{ps_color};font-weight:700;">{ps_v}</span>',
                        unsafe_allow_html=True,
                    )
                else:
                    st.markdown("Piotroski F: —")

            # Info de sector/industria/cap
            ind   = str(row.get("industry") or "—")
            cap   = str(row.get("market_cap_tier") or "—")
            st.caption(f"{sector_s} · {ind} · {cap}")


# ── Página 5: ESTRATEGIA OPCIONES ────────────────────────────────────────────

ESTRATEGIA_STYLE = {
    "cash_secured_put": ("#057a55", "#f3faf7", "CSP"),
    "bull_put_spread":  ("#1d4ed8", "#eff6ff", "Bull Put"),
    "iron_condor":      ("#c27803", "#fefce8", "Iron Condor"),
    "jade_lizard":      ("#c2410c", "#fff7ed", "Jade Lizard"),
    "calendar_spread":  ("#7c3aed", "#f5f3ff", "Calendar"),
}
REGIMEN_VIX_COLOR = {
    "panico":       "#e02424",
    "elevado":      "#c27803",
    "normal":       "#057a55",
    "complacencia": "#6b7280",
}
TERM_ICONO = {
    "backwardation": "📉 Backwardation",
    "contango":      "📈 Contango",
    "flat":          "➡️ Flat",
}
LIQUIDEZ_ICONO = {
    "liquido":      "💧 Líquido",
    "semi_liquido": "⚠️ Semi-líquido",
}
TENDENCIA_COLOR = {
    "mejora_estructural": "#057a55",
    "mejora_parcial":     "#c27803",
    "deterioro":          "#e02424",
}

IV_COLOR = {
    "baja":  "#6b7280",
    "media": "#c27803",
    "alta":  "#057a55",
}


def pagina_opciones():
    st.title("Estrategia Opciones")

    df = query(SQL_OPCIONES)

    if df.empty:
        st.info("No hay estrategias activas.")
        return

    for col_num in ["iv_promedio", "put_strike", "put_delta", "put_theta",
                    "put_iv", "put_dte", "call_strike", "call_delta", "call_theta",
                    "sizing", "vix", "delta_objetivo",
                    "quality_score", "value_score", "altman_z_score", "piotroski_score",
                    "rsi_14_semanal"]:
        if col_num in df.columns:
            df[col_num] = pd.to_numeric(df[col_num], errors="coerce")

    # ── Sección 1: Métricas resumen (4 columnas) ─────────────────────────────
    n_total  = len(df)
    n_bps    = int((df["estrategia"] == "bull_put_spread").sum())
    n_csp    = int((df["estrategia"] == "cash_secured_put").sum())
    iv_mean  = df["iv_promedio"].mean()
    vix_row  = df.dropna(subset=["vix"]).iloc[0] if not df.dropna(subset=["vix"]).empty else None

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total estrategias activas", n_total)
    c2.metric("Bull Put Spread", n_bps, delta=f"CSP: {n_csp}", delta_color="off")

    if vix_row is not None:
        vix_v   = float(vix_row["vix"])
        regimen = str(vix_row.get("regimen_vix") or "").lower()
        reg_col = REGIMEN_VIX_COLOR.get(regimen, "#6b7280")
        c3.metric("VIX", f"{vix_v:.2f}")
        c3.markdown(
            f'<span style="background:{reg_col}22;color:{reg_col};'
            f'border:1px solid {reg_col};padding:2px 10px;border-radius:12px;'
            f'font-weight:700;font-size:.8rem;">{regimen.upper()}</span>',
            unsafe_allow_html=True,
        )
    else:
        c3.metric("VIX", "—")

    if not pd.isna(iv_mean):
        c4.metric("IV promedio universo", f"{iv_mean:.1%}")
    else:
        c4.metric("IV promedio universo", "—")

    st.divider()

    # ── Sección 2: Filtros ───────────────────────────────────────────────────
    fc1, fc2, fc3 = st.columns(3)
    with fc1:
        opts_est = ["Todas"] + sorted(df["estrategia"].dropna().unique().tolist())
        f_est    = st.selectbox("Estrategia", opts_est)
    with fc2:
        opts_iv  = ["Todas"] + sorted(df["nivel_iv"].dropna().unique().tolist())
        f_iv     = st.selectbox("Nivel IV", opts_iv)
    with fc3:
        opts_liq = ["Todas"] + sorted(df["liquidez"].dropna().unique().tolist())
        f_liq    = st.selectbox("Liquidez", opts_liq)

    dff = df.copy()
    if f_est != "Todas": dff = dff[dff["estrategia"] == f_est]
    if f_iv  != "Todas": dff = dff[dff["nivel_iv"]   == f_iv]
    if f_liq != "Todas": dff = dff[dff["liquidez"]   == f_liq]

    st.caption(f"{len(dff)} estrategia(s) mostrada(s)")

    # ── Sección 3: Tabla principal ────────────────────────────────────────────
    tabla_cols = ["ticker", "estrategia", "nivel_iv",
                  "put_strike", "put_dte", "put_delta", "put_theta", "sizing"]
    tabla = dff[tabla_cols].copy()
    tabla.columns = ["Ticker", "Estrategia", "Nivel IV",
                     "Strike", "DTE", "Delta", "Theta", "Sizing"]

    def color_estrategia(val):
        colores = {
            "cash_secured_put": "background-color: #d4edda; color: #155724",
            "bull_put_spread":  "background-color: #cce5ff; color: #004085",
            "iron_condor":      "background-color: #fff3cd; color: #856404",
            "jade_lizard":      "background-color: #ffe5d0; color: #7c3200",
            "calendar_spread":  "background-color: #e2d9f3; color: #432874",
        }
        return colores.get(val, "")

    def fmt_sizing_bar(val):
        if pd.isna(val):
            return ""
        pct = min(max(float(val), 0), 1)
        return f"{pct:.0%}"

    tabla_display = tabla.style.map(color_estrategia, subset=["Estrategia"]).format({
        "Strike": lambda v: f"${v:.2f}"  if pd.notna(v) else "—",
        "DTE":    lambda v: f"{int(v)}d" if pd.notna(v) else "—",
        "Delta":  lambda v: f"{v:.3f}"   if pd.notna(v) else "—",
        "Theta":  lambda v: f"{v:.4f}"   if pd.notna(v) else "—",
        "Sizing": fmt_sizing_bar,
    })
    st.dataframe(tabla_display, use_container_width=True, hide_index=True)

    st.divider()

    # ── Sección 4: Expanders por ticker ──────────────────────────────────────
    for _, row in dff.iterrows():
        est      = str(row.get("estrategia") or "")
        ticker   = str(row.get("ticker") or "")
        nivel_iv = str(row.get("nivel_iv") or "")
        strike   = row.get("put_strike")
        dte      = row.get("put_dte")
        delta_ob = row.get("delta_objetivo")
        sizing   = row.get("sizing")

        est_color, est_bg, est_lbl = ESTRATEGIA_STYLE.get(est, ("#6b7280", "#f9fafb", est))
        strike_s = f"${float(strike):.2f}" if pd.notna(strike) else "—"
        dte_s    = f"{int(dte)}d"          if pd.notna(dte)    else "—"
        delta_s  = f"{float(delta_ob):.2f}" if pd.notna(delta_ob) else "—"
        sizing_s = f"{float(sizing):.0%}"  if pd.notna(sizing) else "—"
        iv_col   = IV_COLOR.get(nivel_iv.lower(), "#6b7280")

        titulo = (
            f"{ticker}  ·  {est_lbl}  ·  "
            f"Strike {strike_s}  ·  DTE {dte_s}  ·  "
            f"Δ {delta_s}  ·  IV {nivel_iv}  ·  Size {sizing_s}"
        )

        with st.expander(titulo):

            # Badge estrategia
            st.markdown(
                f'<span style="background:{est_bg};color:{est_color};'
                f'border:1.5px solid {est_color};padding:4px 14px;border-radius:20px;'
                f'font-weight:700;font-size:.95rem;">{est_lbl}</span>',
                unsafe_allow_html=True,
            )
            st.markdown("<div style='margin-bottom:10px'></div>", unsafe_allow_html=True)

            col_a, col_b, col_c = st.columns(3)

            # ── Columna A: CONTRATO ──────────────────────────────────────────
            with col_a:
                st.markdown("**CONTRATO**")
                if pd.notna(strike):
                    st.metric("Put Strike", strike_s)
                st.markdown(f"Vencimiento en **{dte_s}**")
                st.markdown(f"Delta objetivo: **{delta_s}**")
                call_s = row.get("call_strike")
                if pd.notna(call_s):
                    st.markdown(f"Call Strike: **${float(call_s):.2f}**")

            # ── Columna B: VOLATILIDAD ───────────────────────────────────────
            with col_b:
                st.markdown("**VOLATILIDAD**")
                st.markdown(
                    f'Nivel IV: <span style="color:{iv_col};font-weight:700;">'
                    f'{nivel_iv.upper() if nivel_iv else "—"}</span>',
                    unsafe_allow_html=True,
                )
                iv_prom = row.get("iv_promedio")
                if pd.notna(iv_prom):
                    st.markdown(f"IV promedio: **{float(iv_prom):.1%}**")
                put_iv = row.get("put_iv")
                if pd.notna(put_iv):
                    st.markdown(f"IV contrato: **{float(put_iv):.1%}**")
                term = str(row.get("term_structure") or "").lower()
                if term:
                    st.markdown(TERM_ICONO.get(term, f"➡️ {term}"))
                regimen = str(row.get("regimen_vix") or "").lower()
                if regimen:
                    reg_col = REGIMEN_VIX_COLOR.get(regimen, "#6b7280")
                    st.markdown(
                        f'Régimen VIX: <span style="color:{reg_col};font-weight:700;">'
                        f'{regimen.upper()}</span>',
                        unsafe_allow_html=True,
                    )

            # ── Columna C: FUNDAMENTAL ───────────────────────────────────────
            with col_c:
                st.markdown("**FUNDAMENTAL**")

                qs = row.get("quality_score")
                vs = row.get("value_score")
                if pd.notna(qs): st.markdown(f"Quality score: **{float(qs):.1f}**")
                if pd.notna(vs): st.markdown(f"Value score: **{float(vs):.1f}**")

                az = row.get("altman_z_score")
                if pd.notna(az):
                    az_zona = "safe" if float(az) >= 2.99 else ("grey" if float(az) >= 1.81 else "distress")
                    az_color, az_bg = ALTMAN_ZONA_COLOR.get(az_zona, ("#6b7280", "#f9fafb"))
                    st.markdown(
                        f'Altman Z: <span style="background:{az_bg};color:{az_color};'
                        f'padding:1px 8px;border-radius:10px;font-weight:600;">'
                        f'{float(az):.2f} ({az_zona})</span>',
                        unsafe_allow_html=True,
                    )

                pio = row.get("piotroski_score")
                if pd.notna(pio):
                    pio_v   = int(pio)
                    pio_col = "#057a55" if pio_v >= 7 else ("#c27803" if pio_v >= 5 else "#e02424")
                    st.markdown(
                        f'Piotroski: <span style="color:{pio_col};font-weight:700;">'
                        f'{pio_v}/9</span>',
                        unsafe_allow_html=True,
                    )

                roic_s = row.get("roic_signo")
                if roic_s is not None and not pd.isna(roic_s):
                    roic_txt = "↑ ROIC mejora" if int(roic_s) == 1 else "↓ ROIC cae"
                    roic_col = "#057a55" if int(roic_s) == 1 else "#e02424"
                    st.markdown(
                        f'<span style="color:{roic_col};">{roic_txt}</span>',
                        unsafe_allow_html=True,
                    )

                tend = str(row.get("tendencia_fundamental") or "")
                if tend:
                    tend_col = TENDENCIA_COLOR.get(tend, "#6b7280")
                    tend_lbl = tend.replace("_", " ").title()
                    st.markdown(
                        f'Tendencia: <span style="color:{tend_col};font-weight:700;">'
                        f'{tend_lbl}</span>',
                        unsafe_allow_html=True,
                    )

            # ── Pie del expander ─────────────────────────────────────────────
            st.markdown("<div style='margin-top:8px'></div>", unsafe_allow_html=True)
            theta = row.get("put_theta")
            if pd.notna(theta):
                st.markdown(f"Prima diaria estimada: **${float(theta):.4f}**")

            sizing_norm = min(max(float(sizing) if pd.notna(sizing) else 0, 0), 1)
            st.progress(sizing_norm, text=f"Sizing: {sizing_s}")

            notas = row.get("notas")
            if notas:
                st.caption(f"📝 {notas}")


# ── Página 0: CÓMO FUNCIONA ──────────────────────────────────────────────────

def pagina_como_funciona():

    # ── Sección 1: Hero ──────────────────────────────────────────────────────
    st.markdown(
        '<h1 style="font-size:2.4rem;font-weight:800;margin-bottom:4px;">'
        'Sistema de Inversión Cuantitativo</h1>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<p style="font-size:1.15rem;color:#6b7280;margin-top:0;">Análisis sistemático del mercado en 5 capas</p>',
        unsafe_allow_html=True,
    )
    st.markdown(
        "El mercado genera señales contradictorias a diario. Este sistema elimina el ruido "
        "aplicando un proceso jerárquico y reproducible: primero entiende el ciclo económico, "
        "luego identifica los sectores favorecidos, después selecciona las empresas de mayor "
        "calidad dentro de esos sectores, y finalmente construye señales accionables con el "
        "instrumento adecuado para cada momento — acciones directas u opciones con prima positiva."
    )

    st.divider()

    # ── Sección 2: Pipeline visual ───────────────────────────────────────────
    CAPAS = [
        ("📊", "MACRO",      "Ciclo económico",       "FRED API · 15 indicadores · estado EXPANSION / SLOWDOWN / CONTRACTION / RECOVERY"),
        ("🔄", "SECTORES",   "Rotación sectorial",    "63 ETFs rankeados · alineación con el ciclo macro · señal de rotación"),
        ("🏢", "EMPRESAS",   "Calidad + Valor",       "~700 empresas filtradas · z-score Quality · percentil Value · Altman Z · Piotroski F"),
        ("🎯", "ESTRATEGIA", "Señales accionables",   "Dirección por ticker · instrumento óptimo · timing técnico · score de convicción"),
        ("📈", "OPCIONES",   "Premium y cobertura",   "Strikes por Greeks · IV relativa · estructura de term · estrategia por régimen"),
    ]

    cols = st.columns(11)   # 5 tarjetas + 4 flechas + 2 laterales de relleno
    col_idx = [1, 3, 5, 7, 9]
    arrow_idx = [2, 4, 6, 8]

    for ci, (icono, nombre, sub, desc) in zip(col_idx, CAPAS):
        with cols[ci]:
            st.markdown(
                f'<div style="border:1px solid #e5e7eb;border-radius:10px;padding:16px 12px;'
                f'text-align:center;height:100%;">'
                f'<div style="font-size:1.8rem;">{icono}</div>'
                f'<div style="font-weight:700;font-size:.95rem;margin:6px 0 2px;">{nombre}</div>'
                f'<div style="color:#374151;font-size:.82rem;font-weight:600;margin-bottom:6px;">{sub}</div>'
                f'<div style="color:#9ca3af;font-size:.72rem;line-height:1.4;">{desc}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )
    for ai in arrow_idx:
        with cols[ai]:
            st.markdown(
                '<div style="text-align:center;padding-top:28px;font-size:1.4rem;color:#d1d5db;">→</div>',
                unsafe_allow_html=True,
            )

    st.divider()

    # ── Sección 3: Cómo leer cada página ────────────────────────────────────
    st.subheader("Cómo leer cada página")
    tabla_guia = pd.DataFrame([
        {
            "Página":          "Macro",
            "Qué muestra":     "El estado del ciclo económico basado en 15 indicadores de la Fed (FRED)",
            "Cómo interpretarlo": "Verde = expansión, condiciones favorables para activos de riesgo. Rojo = contracción, reducir exposición.",
        },
        {
            "Página":          "Sectores",
            "Qué muestra":     "Ranking de 63 ETFs sectoriales e industriales con su momentum relativo al S&P 500",
            "Cómo interpretarlo": "LEADING_STRONG = sectores con más fuerza relativa. ALIGNED = coinciden con lo que el ciclo macro favorece.",
        },
        {
            "Página":          "Micro",
            "Qué muestra":     "Las ~700 empresas que pasaron el filtro de salud financiera, con sus scores de calidad y valor",
            "Cómo interpretarlo": "Quality %ile alto = empresa de alta rentabilidad vs. sus pares. Piotroski ≥7 = sólida en los 9 criterios contables.",
        },
        {
            "Página":          "Estrategia",
            "Qué muestra":     "Las empresas con señal activa: instrumento recomendado, timing técnico y score de convicción",
            "Cómo interpretarlo": "Score alto + ALIGNED + tecnico_confirmado = mayor convicción. CSP = venta de put cubierta con efectivo.",
        },
        {
            "Página":          "Estrategia Opciones",
            "Qué muestra":     "La estrategia de opciones específica para cada ticker: strikes, delta, theta, DTE",
            "Cómo interpretarlo": "Estrategia elegida según IV, régimen de VIX y dirección. Mayor sizing = mayor convicción del sistema.",
        },
    ])
    st.table(tabla_guia)

    st.divider()

    # ── Sección 4: Glosario ──────────────────────────────────────────────────
    with st.expander("Ver glosario de términos"):
        GLOSARIO = [
            ("RSI (Relative Strength Index)",
             "Oscilador de momentum de 0 a 100. Por encima de 65 indica sobrecompra; por debajo de 40, sobreventa. "
             "Aquí se usa en versión semanal para filtrar ruido de corto plazo."),
            ("Altman Z-Score",
             "Modelo de predicción de quiebra basado en 5 ratios financieros. Z > 2.99 = zona segura (verde). "
             "1.81–2.99 = zona gris (amarillo). < 1.81 = zona de distress (rojo)."),
            ("Piotroski F-Score",
             "Puntaje de 0 a 9 que evalúa rentabilidad, apalancamiento y eficiencia operativa. "
             "≥7 = empresa financieramente sólida. <5 = señales de deterioro."),
            ("Cash Secured Put (CSP)",
             "Estrategia de opciones que consiste en vender una opción put respaldada por el efectivo necesario para comprar "
             "las acciones si se ejerce. Genera prima inmediata; ideal cuando se quiere entrar a un precio menor al actual."),
            ("ALIGNED",
             "Indica que el sector del ticker coincide con los sectores favorecidos por el estado macro actual. "
             "Un ticker ALIGNED tiene viento a favor del ciclo económico."),
            ("Score de convicción",
             "Puntuación de 0 a 100 que combina calidad fundamental, alineación sectorial, timing técnico y salud financiera. "
             "Determina el ranking y el tamaño de posición sugerido."),
            ("flag_timing",
             "Clasifica el momento técnico de entrada: tecnico_confirmado = precio sobre MA200 + RSI favorable; "
             "pullback_comprable = caída dentro de tendencia alcista; macro_defensivo = solo por fundamentos en entorno difícil."),
            ("MA200 (Media móvil de 200 ruedas)",
             "Indicador de tendencia de largo plazo. Precio por encima = tendencia alcista estructural. "
             "precio_vs_ma200 muestra el porcentaje de distancia respecto a esa media."),
        ]
        for termino, definicion in GLOSARIO:
            st.markdown(f"**{termino}**")
            st.markdown(f"{definicion}")
            st.markdown("---")

    st.divider()

    # ── Sección 5: Embudo de selección ──────────────────────────────────────
    st.subheader("Estado actual del sistema")
    metricas = query(SQL_METRICAS_SISTEMA)
    if not metricas.empty:
        m = metricas.iloc[0]
        n1 = int(m["universo_inicial"] or 0)
        n2 = int(m["pasan_calidad"]    or 0)
        n3 = int(m["señales_activas"]  or 0)
        n4 = int(m["top_seleccion"]    or 0)

        PASOS = [
            (n1, "Universo USA",             "universos.stock_opciones_2026"),
            (n2, "Filtro calidad de balance", "ROIC · D/E · NetDebt/EBITDA · FCF"),
            (n3, "Señal activa",             "Dirección + timing técnico"),
            (n4, "Mayor convicción",         "agente.top · score ≥ umbral"),
        ]

        # Fila de métricas
        cols_m = st.columns(4)
        for col, (n, label, detalle) in zip(cols_m, PASOS):
            col.metric(label, f"{n:,}".replace(",", "."))
            col.caption(detalle)

        # Embudo visual con caracteres unicode
        # Calcula anchos proporcionales (mínimo 4 chars, máximo 32)
        max_n = max(n1, 1)
        def barra(n: int, ancho_max: int = 32, min_ancho: int = 4) -> str:
            ancho = max(round(n / max_n * ancho_max), min_ancho)
            return "█" * ancho

        st.markdown("<div style='margin-top:18px'></div>", unsafe_allow_html=True)

        pasos_html = ""
        for i, (n, label, _) in enumerate(PASOS):
            barra_str = barra(n)
            pct = f"({n/n1:.0%})" if i > 0 else ""
            flecha = '<span style="color:#9ca3af;font-size:1.1rem;"> ▼ </span>' if i < len(PASOS) - 1 else ""
            pasos_html += (
                f'<div style="margin:2px 0;">'
                f'<span style="font-family:monospace;color:#1d4ed8;font-size:.95rem;">{barra_str}</span>'
                f'  <span style="font-size:.9rem;font-weight:600;">{n:,} empresas</span>'
                f'  <span style="color:#9ca3af;font-size:.85rem;">{label} {pct}</span>'
                f'</div>{flecha}'
            )

        st.markdown(pasos_html, unsafe_allow_html=True)

    st.divider()

    # ── Sección 6: Disclaimer ────────────────────────────────────────────────
    st.info(
        "**Aviso legal** — Este sistema es una herramienta de análisis cuantitativo para uso "
        "personal e informativo. No constituye asesoramiento financiero, recomendación de inversión "
        "ni oferta de compra o venta de valores. Toda decisión de inversión es responsabilidad "
        "exclusiva del usuario. Los resultados pasados no garantizan rendimientos futuros. "
        "Las opciones son instrumentos complejos que pueden implicar pérdida total del capital invertido."
    )


# ── Página 6: ASISTENTE ──────────────────────────────────────────────────────

SYSTEM_PROMPT = """\
Sos un asistente especializado en análisis cuantitativo de inversiones. \
Trabajás con un sistema multifactor que analiza más de 3.000 empresas USA en 5 capas: \
MACRO → SECTORES → EMPRESAS → ESTRATEGIA → OPCIONES.

En cada consulta recibís el estado actual del sistema. \
Usá esos datos para responder con precisión.

Reglas estrictas:
- Respondé SOLO sobre inversiones y el sistema
- Siempre aclará que no es asesoramiento financiero
- Sé conciso — máximo 3 párrafos
- Fundamentá con los datos reales del sistema
- Si no tenés el dato en el contexto, decilo claramente
- No inventes datos ni señales que no estén en el contexto\
"""


@st.cache_data(ttl=300)
def leer_contexto_sistema() -> str:
    """Lee el estado actual de la DB y lo devuelve como string formateado."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

            cur.execute("""
                SELECT estado_macro, score_riesgo, confianza
                FROM macro.macro_diagnostico
                ORDER BY calculado_en DESC LIMIT 1
            """)
            macro = cur.fetchone() or {}

            cur.execute("""
                SELECT señal_rotacion, top_tickers_aligned,
                       top_tickers_global, score_universo, n_leading_strong
                FROM sector.v_sector_diagnostico
                LIMIT 1
            """)
            sector = cur.fetchone() or {}

            cur.execute("""
                SELECT COUNT(*) AS total,
                       COUNT(*) FILTER (WHERE instrumento = 'stock') AS n_stock,
                       COUNT(*) FILTER (WHERE instrumento = 'cash_secured_put') AS n_csp,
                       MODE() WITHIN GROUP (ORDER BY flag_timing) AS timing_dominante
                FROM agente.decision
                WHERE trade_status = 'active'
                  AND snapshot_date = (SELECT MAX(snapshot_date)
                                       FROM agente.decision)
            """)
            señales = cur.fetchone() or {}

            cur.execute("""
                SELECT ticker, sector, instrumento,
                       score_conviccion, flag_timing,
                       sector_alineado
                FROM agente.top
                WHERE snapshot_date = (SELECT MAX(snapshot_date)
                                       FROM agente.top)
                ORDER BY rank_conviccion
                LIMIT 5
            """)
            top5 = cur.fetchall() or []

            cur.execute("""
                SELECT diagnostico_sector, coherencia, nota,
                       score_defensivos, score_ciclicos
                FROM sector.sector_diagnostico_tecnico
                ORDER BY fecha DESC LIMIT 1
            """)
            diag_tec = cur.fetchone() or {}

    # Armar tabla top 5
    lineas_top5 = []
    for r in top5:
        score = f"{float(r['score_conviccion']):.1f}" if r.get("score_conviccion") else "—"
        lineas_top5.append(
            f"  {r.get('ticker','?'):<6} | {str(r.get('sector',''))[:22]:<22} | "
            f"{str(r.get('instrumento','')):<18} | score {score} | "
            f"timing: {r.get('flag_timing','—')} | "
            f"alineado: {r.get('sector_alineado','—')}"
        )
    tabla_top5 = "\n".join(lineas_top5) if lineas_top5 else "  Sin datos"

    return f"""\
ESTADO DEL SISTEMA — {date.today().strftime('%d/%m/%Y')}

MACRO:
- Estado: {macro.get('estado_macro', '—')} | Score riesgo: {macro.get('score_riesgo', '—')}
- Confianza: {macro.get('confianza', '—')}

SECTORES:
- Señal rotación: {sector.get('señal_rotacion', '—')}
- Top alineados con macro: {sector.get('top_tickers_aligned', '—')}
- Score universo: {sector.get('score_universo', '—')}
- Leading strong: {sector.get('n_leading_strong', '—')}
- Diagnóstico técnico: {diag_tec.get('diagnostico_sector', '—')}
- Coherencia macro/sector: {diag_tec.get('coherencia', '—')}
- RSI Defensivos: {diag_tec.get('score_defensivos', '—')} | RSI Cíclicos: {diag_tec.get('score_ciclicos', '—')}
- Nota técnica: {diag_tec.get('nota', '—')}

SEÑALES ACTIVAS:
- Total: {señales.get('total', 0)} | Stock: {señales.get('n_stock', 0)} | CSP: {señales.get('n_csp', 0)}
- Timing dominante: {señales.get('timing_dominante', '—')}

TOP 5 EMPRESAS POR CONVICCIÓN:
{tabla_top5}
"""


def llamar_claude_chat(
    system: str,
    contexto: str,
    pregunta: str,
    historial: list[dict],
) -> str:
    """Llama a Claude con historial completo y contexto del sistema."""
    cliente = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

    # Construir mensajes: historial previo + pregunta actual con contexto
    messages = []
    for msg in historial:
        messages.append({"role": msg["role"], "content": msg["content"]})

    messages.append({
        "role": "user",
        "content": f"Contexto actual del sistema:\n{contexto}\n\nPregunta: {pregunta}",
    })

    respuesta = cliente.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1000,
        system=system,
        messages=messages,
    )
    return respuesta.content[0].text


def pagina_asistente():
    st.title("🤖 Asistente del Sistema")
    st.caption(
        "Preguntame sobre el estado del mercado, las señales activas "
        "o cómo interpretar los indicadores."
    )

    if "ANTHROPIC_API_KEY" not in os.environ or not os.environ["ANTHROPIC_API_KEY"]:
        st.error("Falta la variable ANTHROPIC_API_KEY en el archivo .env.")
        return

    # Inicializar historial
    if "chat_historial" not in st.session_state:
        st.session_state.chat_historial = []

    # Mostrar historial previo
    for msg in st.session_state.chat_historial:
        with st.chat_message(msg["role"]):
            st.write(msg["content"])

    # Input del usuario
    if pregunta := st.chat_input("Ej: ¿Cuáles son las mejores empresas esta semana?"):

        with st.chat_message("user"):
            st.write(pregunta)

        contexto = leer_contexto_sistema()

        with st.chat_message("assistant"):
            with st.spinner("Analizando el sistema..."):
                try:
                    respuesta = llamar_claude_chat(
                        system=SYSTEM_PROMPT,
                        contexto=contexto,
                        pregunta=pregunta,
                        historial=st.session_state.chat_historial,
                    )
                except anthropic.APIError as e:
                    respuesta = f"Error al contactar la API de Claude: {e}"

            st.write(respuesta)

        st.session_state.chat_historial.append({"role": "user",      "content": pregunta})
        st.session_state.chat_historial.append({"role": "assistant", "content": respuesta})

    # Botón limpiar historial
    if st.session_state.chat_historial:
        if st.button("🗑️ Limpiar conversación"):
            st.session_state.chat_historial = []
            st.rerun()


# ── Página 7: TRACK RECORD ───────────────────────────────────────────────────

SQL_TR_METRICAS = """
SELECT pnl_total_usd, win_rate, sharpe, max_drawdown_pct,
       n_trades, n_wins, n_losses, n_open,
       pnl_promedio_usd, pnl_mejor_trade, pnl_peor_trade,
       pnl_stock, pnl_opciones, win_rate_stock, win_rate_opciones,
       fecha_calculo
FROM cartera.metricas_resumen
WHERE periodo = 'total'
ORDER BY fecha_calculo DESC LIMIT 1
"""

SQL_TR_TRADES = """
SELECT ticker, instrumento, estrategia,
       fecha_entrada, precio_entrada,
       fecha_salida, precio_salida,
       pnl_usd, pnl_pct, resultado,
       estado_macro, flag_timing, score_conviccion
FROM cartera.trade_results
ORDER BY fecha_entrada DESC
LIMIT 20
"""

SQL_TR_EQUITY = """
SELECT fecha, retorno_acum_pct, spy_retorno_acum, alpha_acumulado
FROM cartera.vs_benchmark
ORDER BY fecha ASC
"""

SQL_TR_POR_INSTRUMENTO = """
SELECT instrumento,
       COUNT(*) AS n_trades,
       SUM(pnl_usd) AS pnl_total,
       ROUND(AVG(CASE WHEN resultado = 'win' THEN 1.0 ELSE 0.0 END) * 100, 1) AS win_rate
FROM cartera.trade_results
WHERE resultado != 'open'
GROUP BY instrumento
ORDER BY pnl_total DESC
"""


def pagina_track_record():
    st.title("📊 Track Record")
    st.caption("Performance del sistema — IBKR Paper Trading · Actualización semanal")

    metricas = query(SQL_TR_METRICAS)
    trades   = query(SQL_TR_TRADES)
    equity   = query(SQL_TR_EQUITY)
    por_inst = query(SQL_TR_POR_INSTRUMENTO)

    hay_datos = not metricas.empty or not trades.empty

    # ── Sección 1: Header / estado ───────────────────────────────────────────
    if not hay_datos:
        st.info(
            "📊 **Track record en construcción.** "
            "El sistema comenzó a registrar operaciones recientemente. "
            "Los resultados se actualizan semanalmente desde IBKR Paper Trading. "
            "Esta página mostrará la equity curve, win rate, Sharpe y detalle de trades "
            "una vez que haya posiciones cerradas."
        )
        st.divider()
        st.caption(
            "Track record generado desde IBKR Paper Trading. "
            "Actualización semanal via Flex Query. "
            "No constituye asesoramiento financiero."
        )
        return

    # ── Sección 2: Métricas globales ─────────────────────────────────────────
    m = metricas.iloc[0]

    def _fv(campo, decimales=2):
        v = m.get(campo)
        return float(v) if v is not None and not pd.isna(v) else None

    pnl     = _fv("pnl_total_usd")
    wr      = _fv("win_rate")
    sharpe  = _fv("sharpe")
    mdd     = _fv("max_drawdown_pct")

    c1, c2, c3, c4 = st.columns(4)

    with c1:
        pnl_str = f"${pnl:+,.2f}" if pnl is not None else "—"
        st.metric("P&L Total", pnl_str)
        if pnl is not None:
            color = "#057a55" if pnl >= 0 else "#e02424"
            st.markdown(f'<span style="color:{color};font-weight:600;">'
                        f'{"▲" if pnl >= 0 else "▼"} {"Positivo" if pnl >= 0 else "Negativo"}'
                        f'</span>', unsafe_allow_html=True)

    with c2:
        wr_str = f"{wr * 100:.1f}%" if wr is not None else "—"
        st.metric("Win Rate", wr_str)
        if wr is not None:
            color = "#057a55" if wr >= 0.60 else "#c27803"
            st.markdown(f'<span style="color:{color};font-size:.82rem;">'
                        f'{"≥ 60% ✓" if wr >= 0.60 else "< 60%"}</span>',
                        unsafe_allow_html=True)

    with c3:
        sharpe_str = f"{sharpe:.2f}" if sharpe is not None else "—"
        st.metric("Sharpe Ratio", sharpe_str)
        if sharpe is not None:
            color = "#057a55" if sharpe >= 1 else "#c27803"
            st.markdown(f'<span style="color:{color};font-size:.82rem;">'
                        f'{"≥ 1.0 ✓" if sharpe >= 1 else "< 1.0"}</span>',
                        unsafe_allow_html=True)

    with c4:
        mdd_str = f"{mdd:.2f}%" if mdd is not None else "—"
        st.metric("Max Drawdown", mdd_str)

    # Submétrica de trades
    n_t = int(m.get("n_trades") or 0)
    n_w = int(m.get("n_wins")   or 0)
    n_l = int(m.get("n_losses") or 0)
    n_o = int(m.get("n_open")   or 0)
    st.caption(f"Trades totales: {n_t}  ·  Wins: {n_w}  ·  Losses: {n_l}  ·  Open: {n_o}")

    st.divider()

    # ── Sección 3: Equity curve ──────────────────────────────────────────────
    st.subheader("Retorno acumulado vs SPY")
    if equity.empty:
        st.warning(
            "El gráfico de performance se generará cuando haya "
            "al menos 2 semanas de trades."
        )
    else:
        for col_num in ["retorno_acum_pct", "spy_retorno_acum", "alpha_acumulado"]:
            equity[col_num] = pd.to_numeric(equity[col_num], errors="coerce")
        equity["fecha"] = pd.to_datetime(equity["fecha"])

        fig = px.line(
            equity,
            x="fecha",
            y=["retorno_acum_pct", "spy_retorno_acum"],
            labels={
                "value": "Retorno acumulado (%)",
                "fecha": "Fecha",
                "variable": "Serie",
            },
            color_discrete_map={
                "retorno_acum_pct":  "#057a55",
                "spy_retorno_acum":  "#9ca3af",
            },
        )
        fig.update_traces(selector={"name": "retorno_acum_pct"}, name="Sistema")
        fig.update_traces(selector={"name": "spy_retorno_acum"}, name="SPY")

        # Área sombreada del alpha
        alpha_pos = equity[equity["alpha_acumulado"] >= 0]
        alpha_neg = equity[equity["alpha_acumulado"] < 0]
        if not alpha_pos.empty:
            fig.add_traces(px.area(
                alpha_pos, x="fecha", y="alpha_acumulado",
                color_discrete_sequence=["#057a5540"],
            ).data)
        if not alpha_neg.empty:
            fig.add_traces(px.area(
                alpha_neg, x="fecha", y="alpha_acumulado",
                color_discrete_sequence=["#e0242440"],
            ).data)

        fig.update_layout(
            plot_bgcolor="#ffffff",
            paper_bgcolor="#ffffff",
            xaxis=dict(gridcolor="#f3f4f6"),
            yaxis=dict(gridcolor="#f3f4f6", ticksuffix="%"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            margin=dict(l=20, r=20, t=20, b=20),
        )
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ── Sección 4: Performance por instrumento ───────────────────────────────
    st.subheader("Performance por instrumento")
    if por_inst.empty:
        st.info("Sin trades cerrados todavía.")
    else:
        # Agrupar opciones multi-pata bajo una categoría
        def _grupo(inst: str) -> str:
            inst = (inst or "").lower()
            if inst == "stock":
                return "stock"
            if inst == "cash_secured_put":
                return "cash_secured_put"
            return "opciones"

        por_inst["grupo"] = por_inst["instrumento"].apply(_grupo)
        agrupado = (
            por_inst.groupby("grupo")
            .agg(n_trades=("n_trades", "sum"),
                 pnl_total=("pnl_total", "sum"),
                 win_rate=("win_rate", "mean"))
            .reset_index()
        )

        LABELS = {
            "stock":            ("Acciones",         "#057a55", "#f3faf7"),
            "cash_secured_put": ("Cash-Secured Put",  "#1d4ed8", "#eff6ff"),
            "opciones":         ("Opciones multi-pata","#7c3aed", "#f5f3ff"),
        }
        ORDEN = ["stock", "cash_secured_put", "opciones"]
        cols_inst = st.columns(3)

        for col, grupo in zip(cols_inst, ORDEN):
            fila = agrupado[agrupado["grupo"] == grupo]
            lbl, brd, bg = LABELS[grupo]
            with col:
                if fila.empty:
                    st.markdown(
                        f'<div style="border:1px solid #e5e7eb;border-radius:8px;'
                        f'padding:16px;text-align:center;color:#9ca3af;">'
                        f'<b>{lbl}</b><br>Sin trades</div>',
                        unsafe_allow_html=True,
                    )
                else:
                    r        = fila.iloc[0]
                    pnl_v    = float(r["pnl_total"] or 0)
                    wr_v     = float(r["win_rate"]  or 0)
                    pnl_color = "#057a55" if pnl_v >= 0 else "#e02424"
                    st.markdown(
                        f'<div style="background:{bg};border:1px solid {brd};'
                        f'border-radius:8px;padding:16px;">'
                        f'<div style="font-weight:700;color:{brd};margin-bottom:8px;">{lbl}</div>'
                        f'<div style="font-size:.85rem;">Trades: <b>{int(r["n_trades"])}</b></div>'
                        f'<div style="font-size:.85rem;">P&L: '
                        f'<b style="color:{pnl_color};">${pnl_v:+,.2f}</b></div>'
                        f'<div style="font-size:.85rem;">Win rate: <b>{wr_v:.1f}%</b></div>'
                        f'</div>',
                        unsafe_allow_html=True,
                    )

    st.divider()

    # ── Sección 5: Trades recientes ──────────────────────────────────────────
    st.subheader("Trades recientes")
    if trades.empty:
        st.info("Sin trades registrados todavía.")
    else:
        for col_num in ["pnl_usd", "pnl_pct", "score_conviccion", "precio_entrada", "precio_salida"]:
            trades[col_num] = pd.to_numeric(trades[col_num], errors="coerce")

        RESULTADO_BG = {"win": "#f3faf7", "loss": "#fdf2f2", "open": "#f9fafb"}
        RESULTADO_COLOR = {"win": "#057a55", "loss": "#e02424", "open": "#6b7280"}

        for _, row in trades.iterrows():
            res       = str(row.get("resultado") or "open")
            bg        = RESULTADO_BG.get(res, "#f9fafb")
            res_color = RESULTADO_COLOR.get(res, "#6b7280")

            pnl_v   = row.get("pnl_usd")
            pnl_pct = row.get("pnl_pct")
            pnl_str = f"${float(pnl_v):+,.2f}" if pnl_v is not None and not pd.isna(pnl_v) else "—"
            pct_str = f"({float(pnl_pct):+.1f}%)" if pnl_pct is not None and not pd.isna(pnl_pct) else ""
            pnl_color = "#057a55" if (pnl_v is not None and not pd.isna(pnl_v) and float(pnl_v) >= 0) else "#e02424"

            score   = row.get("score_conviccion")
            score_s = f"{float(score):.0f}" if score is not None and not pd.isna(score) else "—"
            macro   = str(row.get("estado_macro") or "—")
            timing  = str(row.get("flag_timing")  or "—").replace("_", " ")
            inst    = str(row.get("instrumento")  or "—")
            ticker  = str(row.get("ticker")       or "—")
            f_ent   = str(row.get("fecha_entrada") or "—")
            f_sal   = str(row.get("fecha_salida")  or "abierto")

            st.markdown(
                f'<div style="background:{bg};border:1px solid #e5e7eb;border-radius:6px;'
                f'padding:10px 14px;margin-bottom:6px;display:flex;gap:16px;align-items:center;">'
                f'<span style="font-weight:700;font-size:1rem;min-width:52px;">{ticker}</span>'
                f'<span style="color:#6b7280;font-size:.82rem;min-width:60px;">{f_ent}</span>'
                f'<span style="font-size:.82rem;min-width:110px;">{inst}</span>'
                f'<span style="font-weight:700;color:{pnl_color};min-width:90px;">{pnl_str} {pct_str}</span>'
                f'<span style="background:{bg};color:{res_color};border:1px solid {res_color};'
                f'padding:1px 8px;border-radius:10px;font-size:.72rem;font-weight:700;">{res.upper()}</span>'
                f'<span style="color:#9ca3af;font-size:.78rem;">{macro} · {timing} · score {score_s}</span>'
                f'<span style="color:#9ca3af;font-size:.75rem;margin-left:auto;">salida: {f_sal}</span>'
                f'</div>',
                unsafe_allow_html=True,
            )

    st.divider()

    # ── Sección 6: Nota al pie ───────────────────────────────────────────────
    st.caption(
        "Track record generado desde IBKR Paper Trading. "
        "Actualización semanal via Flex Query. "
        "No constituye asesoramiento financiero."
    )


# ── Navegación ───────────────────────────────────────────────────────────────

PAGINAS = {
    "Cómo funciona":        pagina_como_funciona,
    "Macro":                pagina_macro,
    "Sectores":             pagina_sectores,
    "Micro":                pagina_micro,
    "Estrategia":           pagina_estrategia,
    "Estrategia Opciones":  pagina_opciones,
    "📊 Track Record":      pagina_track_record,
    "🤖 Asistente":         pagina_asistente,
}

with st.sidebar:
    st.title("Sistema de Inversión")
    st.divider()
    seleccion = st.radio("Navegación", list(PAGINAS.keys()), label_visibility="collapsed")

PAGINAS[seleccion]()
